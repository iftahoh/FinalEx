import duckdb
import os

# --- הגדרות נתיבים ---
# כל הנתונים של החשמל
source_csv_pattern = "raw_data/*.csv"
# נתוני מזג האוויר
weather_csv_path = "weather_daily_darksky.csv"

# התקיות של המאגר גם המלא וגם החלקי
full_lake_path = "london_smart_meters_full.ducklake"
sample_lake_path = "london_smart_meters_sample.ducklake"

# יצירת התקיות עצמם - תיקן את הבאג של הנתיב
if not os.path.exists(full_lake_path):
    os.makedirs(full_lake_path)
if not os.path.exists(sample_lake_path):
    os.makedirs(sample_lake_path)
# יצירת חיבור בין המאגר אליו
con = duckdb.connect()

print("---Start Making Data Lake---")

# --- Weather Data Build ---
print("\n# Process The Weather Daily Data")
try:
    # הכנסת כל הנתונים למאגר
    con.execute(f"""
        COPY (SELECT * FROM read_csv('{weather_csv_path}', AUTO_DETECT=True)) 
        TO '{full_lake_path}/weather.parquet' (FORMAT PARQUET);
    """)
    # הוספת מדגם כדי שנוכל להגיש
    con.execute(f"""
        COPY (SELECT * FROM read_csv('{weather_csv_path}', AUTO_DETECT=True)) 
        TO '{sample_lake_path}/weather.parquet' (FORMAT PARQUET);
    """)
    print("--- Process Complete ---")
except Exception as e:
    print(f"--- Error In Weather Data {e}")


# --- Energy Data Build ---
# UNPIVOT - הפיכות עמודות לשורות
print("\n --- Create The Full Lake Data ---")

query_full = f"""
COPY (
    WITH raw_data AS (
        SELECT * FROM read_csv('{source_csv_pattern}', HEADER=True, AUTO_DETECT=True)
    ),
    unpivoted_data AS (
        UNPIVOT raw_data
        ON COLUMNS('hh_.*')
        INTO NAME half_hour_code VALUE energy
    )
    SELECT 
        LCLid,
        (day::TIMESTAMP + (CAST(regexp_extract(half_hour_code, '[0-9]+') AS INTEGER) * INTERVAL 30 MINUTE)) as timestamp,
        energy,
        YEAR(timestamp) as year,
        MONTH(timestamp) as month
    FROM unpivoted_data
    WHERE energy IS NOT NULL 
) TO '{full_lake_path}' (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE);
"""

con.execute(query_full)
print(f"Complete To Build Full Data Lake {full_lake_path}")


print("\n---Create The Part Data Lake---")
# כאן אנחנו דוגמים 1% מהימים ואז עושים להם UNPIVOT
query_sample = f"""
COPY (
    WITH raw_data AS (
        SELECT * FROM read_csv('{source_csv_pattern}', HEADER=True, AUTO_DETECT=True)
        USING SAMPLE 1% (bernoulli) 
    ),
    unpivoted_data AS (
        UNPIVOT raw_data
        ON COLUMNS('hh_.*')
        INTO NAME half_hour_code VALUE energy
    )
    SELECT 
        LCLid,
        (day::TIMESTAMP + (CAST(regexp_extract(half_hour_code, '[0-9]+') AS INTEGER) * INTERVAL 30 MINUTE)) as timestamp,
        energy,
        YEAR(timestamp) as year,
        MONTH(timestamp) as month
    FROM unpivoted_data
    WHERE energy IS NOT NULL
) TO '{sample_lake_path}' (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE);
"""

con.execute(query_sample)
print(f"Complete Part Data Lake {sample_lake_path}")

con.close()
print("\n---Done---")