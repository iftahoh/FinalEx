import duckdb


db_path = "london_smart_meters_sample.ducklake/year=*/**/*.parquet"  # לבדיקה על הקטן
#db_path = "london_smart_meters_full.ducklake/year=*/**/*.parquet"      # להרצת אמת על הגדול

con = duckdb.connect()

print(f"Checking The DataBase: {db_path} ---\n")


try:
    # מכיוון שאנחנו מסננים רק את תיקיות השנים, זה יתן לנו את המספר האמיתי של קריאות החשמל
    total_rows = con.execute(f"SELECT COUNT(*) FROM '{db_path}'").fetchone()[0]
    print(f"The Amount Of Line In The DataBase {total_rows:,}")
except Exception as e:
    print(f"Error In Loading The DataBase {e}")

# 2. בדיקת ערכים חסרים (NULLs)
print("\n--- Checking The Amount Of NULLs In Each Colum (NULLs) ---")
# אלו העמודות שקיימות רק בנתוני החשמל
columns = ['LCLid', 'timestamp', 'energy', 'year', 'month']

for col in columns:
    try:
        null_count = con.execute(f"""
            SELECT COUNT(*) 
            FROM '{db_path}' 
            WHERE "{col}" IS NULL
        """).fetchone()[0]
        print(f"Colum {col}: {null_count}  NULL Value")
    except Exception as e:
        print(f"Error In Colum: {col}: {e}")


file_count = con.execute(f"SELECT COUNT(*) FROM glob('{db_path}')").fetchone()[0]
print(f"\nThe Amount Of Parquet Files Of Energy Data: {file_count}")

# 4. הצגת דוגמית
print("\n--- The Five First Rows ---")
print(con.execute(f"SELECT * FROM '{db_path}' LIMIT 5").df())

con.close()