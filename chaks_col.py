import duckdb
import glob
import os

# נחפש את הקובץ הראשון בתיקייה כדי לבדוק אותו
files = glob.glob("raw_data/*.csv")

if not files:
    print("❌ שגיאה: התיקייה raw_data ריקה! ודא שהעתקת לשם את קבצי ה-CSV.")
else:
    first_file = files[0]
    print(f"🔍 בודק את הקובץ: {first_file}")

    con = duckdb.connect()
    try:
        # נדפיס את רשימת העמודות שהמערכת מזהה
        columns = con.execute(f"DESCRIBE SELECT * FROM read_csv('{first_file}', AUTO_DETECT=True)").fetchall()

        print("\n--- שמות העמודות בקובץ שלך ---")
        for col in columns:
            print(f"שם עמודה: {col[0]} | סוג: {col[1]}")

    except Exception as e:
        print(f"שגיאה בקריאה: {e}")