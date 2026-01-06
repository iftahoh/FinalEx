import duckdb


# db_path = "london_smart_meters_sample.ducklake/year=*/**/*.parquet"  # לבדיקה על הקטן
db_path = "london_smart_meters_full.ducklake/year=*/**/*.parquet"      # להרצת אמת על הגדול

con = duckdb.connect()

print(f"--- בודק את המאגר (נתוני חשמל בלבד): {db_path} ---\n")


try:
    # מכיוון שאנחנו מסננים רק את תיקיות השנים, זה יתן לנו את המספר האמיתי של קריאות החשמל
    total_rows = con.execute(f"SELECT COUNT(*) FROM '{db_path}'").fetchone()[0]
    print(f"סה\"כ שורות במאגר האנרגיה: {total_rows:,}")
except Exception as e:
    print(f"שגיאה בקריאת המאגר: {e}")

# 2. בדיקת ערכים חסרים (NULLs)
print("\n--- בדיקת ערכים חסרים (NULLs) ---")
# אלו העמודות שקיימות רק בנתוני החשמל
columns = ['LCLid', 'timestamp', 'energy', 'year', 'month']

for col in columns:
    try:
        null_count = con.execute(f"""
            SELECT COUNT(*) 
            FROM '{db_path}' 
            WHERE "{col}" IS NULL
        """).fetchone()[0]
        print(f"עמודה {col}: {null_count} ערכי NULL")
    except Exception as e:
        print(f"שגיאה בעמודה {col}: {e}")


file_count = con.execute(f"SELECT COUNT(*) FROM glob('{db_path}')").fetchone()[0]
print(f"\nמספר קבצי Parquet של נתוני חשמל: {file_count}")

# 4. הצגת דוגמית
print("\n--- 5 שורות ראשונות לדוגמה ---")
print(con.execute(f"SELECT * FROM '{db_path}' LIMIT 5").df())

con.close()