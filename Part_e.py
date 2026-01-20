import duckdb
import sqlite3
import pandas as pd

# חיבורים למאגרים
duck_db_path = "london_smart_meters_full.ducklake/year=*/**/*.parquet" # עובדים על המלא
# duck_db_path = "london_smart_meters_sample.ducklake/year=*/**/*.parquet"
weather_path = "london_smart_meters_full.ducklake/weather.parquet"
households_csv = "informations_households.csv"
sqlite_db_path = "dashboard.db"

con = duckdb.connect()

print("--- מתחיל בתהליך ETL (DuckDB -> SQLite) ---")

# טעינת טבלאות
print("1. טוען נתוני בתים ומזג אוויר...")
con.execute(f"CREATE OR REPLACE VIEW weather AS SELECT * FROM '{weather_path}'")
con.execute(f"CREATE OR REPLACE VIEW households AS SELECT * FROM read_csv('{households_csv}', AUTO_DETECT=True)")
con.execute(f"CREATE OR REPLACE VIEW energy AS SELECT * FROM '{duck_db_path}'")

# פתיחת חיבור
sqlite_con = sqlite3.connect(sqlite_db_path)

# שאלה ראשונה על פרופיל צריכה יומי
print("2. מעבד שאילתה 1: פרופיל יומי...")
q1 = """
SELECT 
    CAST(HOUR(timestamp) AS INTEGER) as hour_of_day,
    CAST(MINUTE(timestamp) AS INTEGER) as minute_of_hour,
    AVG(energy) as avg_consumption
FROM energy
GROUP BY 1, 2
ORDER BY 1, 2
"""
df1 = con.execute(q1).df()
df1.to_sql("daily_profile", sqlite_con, if_exists="replace", index=False)


# פרופיל צריכה יומי מול המזג אוויר
print("3. מעבד שאילתה 2: השפעת טמפרטורה...")
q2 = """
SELECT 
    CAST(e.timestamp AS DATE) as date,
    SUM(e.energy) as total_daily_energy,
    AVG(w.temperatureMax) as max_temp,
    AVG(w.temperatureMin) as min_temp,
    MAX(w.precipType) as precip_type
FROM energy e
JOIN weather w ON CAST(e.timestamp AS DATE) = CAST(w.time AS DATE)
GROUP BY 1
ORDER BY 1
"""
df2 = con.execute(q2).df()
df2.to_sql("weather_correlation", sqlite_con, if_exists="replace", index=False)


# צריכה לפי דמוגרפיה
print("4. מעבד שאילתה 3: קבוצות דמוגרפיות...")
q3 = """
SELECT 
    h.Acorn_grouped as demographic_group,
    AVG(e.energy) * 48 as avg_daily_kwh -- המרה ליומי
FROM energy e
JOIN households h ON e.LCLid = h.LCLid
WHERE h.Acorn_grouped IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
"""
df3 = con.execute(q3).df()
df3.to_sql("demographics", sqlite_con, if_exists="replace", index=False)


# דירוך של צרכנים חריגים
print("5. מעבד שאילתה 4: צרכנים חריגים (Window Functions)...")
q4 = """
WITH user_totals AS (
    SELECT 
        LCLid, 
        SUM(energy) as total_kwh
    FROM energy
    GROUP BY 1
    HAVING SUM(energy) > 100 -- סינון רעשים
)
SELECT 
    LCLid,
    total_kwh,
    RANK() OVER (ORDER BY total_kwh DESC) as rank_place,
    NTILE(100) OVER (ORDER BY total_kwh DESC) as percentile
FROM user_totals
ORDER BY rank_place
LIMIT 10000
"""
df4 = con.execute(q4).df()
df4.to_sql("top_consumers", sqlite_con, if_exists="replace", index=False)

#צריכה של אמצע שבוע אל מול סופש
print("6. מעבד שאילתה 5 (בונוס): ימי השבוע...")
q5 = """
SELECT 
    DAYNAME(timestamp) as day_name,
    CASE 
        WHEN DAYOFWEEK(timestamp) IN (0, 6) THEN 'Weekend' 
        ELSE 'Weekday' 
    END as day_type,
    AVG(energy) * 48 as avg_daily_consumption
FROM energy
GROUP BY 1, 2
ORDER BY 3 DESC
"""
df5 = con.execute(q5).df()
df5.to_sql("weekly_patterns", sqlite_con, if_exists="replace", index=False)

# שמירה של חלק מהשורות בטבלאות כדי שנוכל להציג אותם בדאבורד
print("6. שומר דוגמיות נתונים גולמיים (Raw Samples)...")

# דוגמית אנרגיה
con.execute("SELECT * FROM energy LIMIT 200").df().to_sql("sample_energy", sqlite_con, if_exists="replace", index=False)
# דוגמית מזג אוויר
con.execute("SELECT * FROM weather LIMIT 200").df().to_sql("sample_weather", sqlite_con, if_exists="replace", index=False)
# דוגמית בתים
con.execute("SELECT * FROM households LIMIT 200").df().to_sql("sample_households", sqlite_con, if_exists="replace", index=False)


sqlite_con.close()
con.close()
print("\n--- סיימנו! הקובץ dashboard.db מוכן. ---")