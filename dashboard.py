import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns

# --- הגדרות עיצוב ---
st.set_page_config(page_title="London Energy Dashboard", layout="wide")


# פונקציה להתחברות ל-SQLite (ה-GOLD שלנו)
def get_connection():
    return sqlite3.connect("dashboard.db")


# --- פונקציות טעינת נתונים ---
def load_daily_profile():
    con = get_connection()
    df = pd.read_sql("SELECT * FROM daily_profile", con)
    con.close()
    return df


def load_weather_corr():
    con = get_connection()
    df = pd.read_sql("SELECT * FROM weather_correlation", con)
    con.close()
    return df


def load_demographics():
    con = get_connection()
    df = pd.read_sql("SELECT * FROM demographics", con)
    con.close()
    return df


def load_top_consumers():
    con = get_connection()
    df = pd.read_sql("SELECT * FROM top_consumers", con)
    con.close()
    return df

def load_weekly_patterns():
    con = get_connection()
    df = pd.read_sql("SELECT * FROM weekly_patterns", con)
    con.close()
    return df


# --- תפריט צד (Sidebar) ---
st.sidebar.title("ניווט")
page = st.sidebar.radio("בחר עמוד:", ["הסיפור והשאלות", "ניתוח ויזואלי (גרפים)", "נתונים גולמיים", "פידבק משתמש"])

# --- עמוד 1: הסיפור והשאלות ---
if page == "הסיפור והשאלות":
    st.title("⚡ דפוסי צריכת חשמל בלונדון")
    st.markdown("""
    ### הסיפור שלנו
    בפרויקט זה חקרנו נתוני Big Data של מונים חכמים בלונדון כדי להבין: **מה משפיע על צריכת החשמל?**
    האם זה מזג האוויר? המעמד הסוציו-אקונומי? או השעה ביום?

    ### שאלות המחקר (Business Questions):
    1. **פרופיל יומי:** מתי צורכים הכי הרבה חשמל במהלך היום?
    2. **מזג אוויר:** האם יש קשר ישיר בין קור (טמפרטורה) לצריכה?
    3. **דמוגרפיה:** האם שכונות עשירות צורכות יותר משכונות קשות יום?
    4. **חריגים:** כיצד מתפלגת הצריכה בין הצרכנים "הכבדים"?
    """)

    st.info("הנתונים עובדו מתוך מאגר של כ-167 מיליון רשומות באמצעות DuckDB, וזוקקו ל-SQLite לצורך תצוגה מהירה.")

# --- עמוד 2: ניתוח ויזואלי ---
elif page == "ניתוח ויזואלי (גרפים)":
    st.title("📊 ניתוח ויזואלי מעמיק")

    # 1. גרף יומי (Matplotlib)
    st.subheader("1. מתי במהלך היום צורכים הכי הרבה חשמל בבתי האב בלונדון")
    df_daily = load_daily_profile()

    fig1, ax1 = plt.subplots(figsize=(10, 4))
    # יצירת ציר זמן רציף לצורך הגרף
    df_daily['time_float'] = df_daily['hour_of_day'] + df_daily['minute_of_hour'] / 60

    ax1.plot(df_daily['time_float'], df_daily['avg_consumption'], color='orange', linewidth=2)
    ax1.set_title("Average Energy Consumption by Hour of Day")
    ax1.set_xlabel("Hour (0-24)")
    ax1.set_ylabel("Avg kWh")
    ax1.grid(True, alpha=0.3)
    st.pyplot(fig1)
    st.markdown("**תובנה:** ניתן לראות שהצריכה הכי גובהה היא בשעות הערב וזה הגיוני כי כולם בבית אחרי העבודות באיזור 19-20 "
                "בנוסף אנחנו רואים שהצריכה הכי נמוכה של היום היא בשעות הבוקר במוקדמות באיזור 4-5")

    st.divider()

    # 2. גרף מזג אוויר (Seaborn)
    st.subheader("2. האם בלונדון צורכים יותר חשמל כשקר")
    df_weather = load_weather_corr()

    fig2, ax2 = plt.subplots(figsize=(10, 5))
    sns.scatterplot(data=df_weather, x='max_temp', y='total_daily_energy', hue='precip_type', ax=ax2,
                    palette='coolwarm')
    ax2.set_title("Daily Energy vs. Max Temperature")
    st.pyplot(fig2)
    st.markdown("**תובנה:** ככל שהטמפרטורה יותר נמוכה כך צריכת החשמל בבתים עולה."
                "מה שהגיוני כי כשקר מפעילים יותר חימום")


    st.divider()

    # 3. גרף אינטראקטיבי 1 - דמוגרפיה
    st.subheader("3. מי צורך יותר? (השוואה דמוגרפית - אינטראקטיבי)")
    df_demo = load_demographics()

    # בחירת קבוצות להשוואה
    all_groups = df_demo['demographic_group'].unique().tolist()
    selected_groups = st.multiselect("בחר קבוצות אוכלוסייה להשוואה:", all_groups, default=all_groups[:5])

    if selected_groups:
        filtered_df = df_demo[df_demo['demographic_group'].isin(selected_groups)]
        st.bar_chart(filtered_df.set_index('demographic_group')['avg_daily_kwh'])
    else:
        st.warning("נא לבחור לפחות קבוצה אחת.")

    st.divider()

    # 4. גרף אינטראקטיבי 2 - התפלגות צרכנים
    st.subheader("4. התפלגות הצרכנים הכבדים (Rank - אינטראקטיבי)")
    df_top = load_top_consumers()

    percentile_filter = st.slider("סנן לפי אחוזון עליון (Percentile):", 1, 100, 100)
    filtered_top = df_top[df_top['percentile'] <= percentile_filter]

    fig4, ax4 = plt.subplots()
    ax4.hist(filtered_top['total_kwh'], bins=20, color='purple', alpha=0.7)
    ax4.set_title(f"Distribution of Top {percentile_filter}% Consumers")
    ax4.set_xlabel("Total kWh")
    st.pyplot(fig4)

    st.divider()

    # 5. גרף בונוס - ימי השבוע
    st.subheader("5. האם צורכים יותר חשמל בסופש אל מול אמצע שבוע")
    df_week = load_weekly_patterns()

    # סידור הימים לפי סדר הגיוני ולא אלפביתי
    days_order = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    df_week['day_name'] = pd.Categorical(df_week['day_name'], categories=days_order, ordered=True)
    df_week = df_week.sort_values('day_name')

    fig5, ax5 = plt.subplots(figsize=(10, 5))
    # צבעים שונים לסופ"ש ויום חול
    colors = ['red' if x == 'Weekend' else 'skyblue' for x in df_week['day_type']]

    ax5.bar(df_week['day_name'], df_week['avg_daily_consumption'], color=colors)
    ax5.set_title("Average Daily Consumption by Day of Week")
    ax5.set_ylabel("Avg kWh per Day")
    plt.xticks(rotation=45)
    st.pyplot(fig5)
    st.markdown("**תובנה:** כפי שניתן לראות בגרף למעלה אין באמת הבדל משמעותי בין צריכת חשמל בימי אמצע השבוע מול הסופש וזה הגיוני כי מדובר בבתי אב ולא במפעלים שלא פועלים בסופש.")

# --- עמוד 3: נתונים גולמיים ---
elif page == "נתונים גולמיים":
    st.title("📋 הצצה לנתונים (Raw Data Samples)")


    con = get_connection()

    st.subheader("טבלת צריכת חשמל (Energy)")
    df_energy = pd.read_sql("SELECT * FROM sample_energy", con)
    # צביעה מותנית (Highlight) לערכים גבוהים
    st.dataframe(df_energy.style.highlight_max(axis=0, color='lightgreen'))

    st.subheader("טבלת מזג אוויר (Weather)")
    df_weather_sample = pd.read_sql("SELECT * FROM sample_weather", con)
    st.dataframe(df_weather_sample)

    st.subheader("טבלת נתונים דמוגרפיים (Households)")
    # קריאת דוגמית נתוני הבתים
    df_households = pd.read_sql("SELECT * FROM sample_households", con)
    st.dataframe(df_households)

    con.close()

# --- עמוד 4: פידבק משתמש ---
elif page == "פידבק משתמש":
    st.title("📝 ספר לנו מה דעתך")

    with st.form("feedback_form"):
        name = st.text_input("שם:")
        rating = st.slider("דרג את הדאשבורד (1-5):", 1, 5, 5)
        comments = st.text_area("הערות נוספות:")
        submitted = st.form_submit_button("שלח פידבק")

        if submitted:
            con = get_connection()
            # יצירת טבלה אם לא קיימת
            con.execute("CREATE TABLE IF NOT EXISTS user_feedback (name TEXT, rating INTEGER, comments TEXT)")
            con.execute("INSERT INTO user_feedback VALUES (?, ?, ?)", (name, rating, comments))
            con.commit()
            con.close()
            st.success("תודה! הפידבק נשמר בהצלחה בדאטה בייס.")

    # הצגת פידבקים קודמים
    st.divider()
    st.subheader("פידבקים אחרונים:")
    con = get_connection()
    try:
        feedbacks = pd.read_sql("SELECT * FROM user_feedback", con)
        st.dataframe(feedbacks)
    except:
        st.info("עדיין אין פידבקים.")
    con.close()