import pandas as pd
import pyodbc
from datetime import date

# === Step 1: Read CSV ===
df = pd.read_csv("hospitalisation_details_cleaned.csv")

# === Step 2: Clean column names ===
df.columns = [col.strip().lower().replace("?", "").replace(" ", "_") for col in df.columns]

# === Step 3: Clean data ===
df['customer_id'] = df['customer_id'].astype(str).str.extract(r'(\d+)')
df['year'] = pd.to_numeric(df['year'], errors='coerce')
df['date'] = pd.to_numeric(df['date'], errors='coerce')

month_map = {'jan':1, 'feb':2, 'mar':3, 'apr':4, 'may':5, 'jun':6,
             'jul':7, 'aug':8, 'sep':9, 'oct':10, 'nov':11, 'dec':12}
df['month'] = df['month'].astype(str).str[:3].str.lower().map(month_map)

df['children'] = df['children'].astype(str).str.extract(r'(\d+)')
df['children'] = pd.to_numeric(df['children'], errors='coerce').fillna(0)

df['charges'] = df['charges'].astype(str).str.replace(r'[^\d.]', '', regex=True)
df['charges'] = pd.to_numeric(df['charges'], errors='coerce')

df['hospital_tier'] = df['hospital_tier'].astype(str).str.extract(r'(\d+)')
df['city_tier'] = df['city_tier'].astype(str).str.extract(r'(\d+)')

df['state_id'] = pd.to_numeric(df['state_id'].astype(str).str.extract(r'(\d+)')[0], errors='coerce')

# === Step 4: Drop rows with missing critical values ===
df.dropna(subset=[
    'customer_id', 'year', 'month', 'date',
    'charges', 'hospital_tier', 'city_tier', 'state_id'
], inplace=True)

# === Step 5: Rename and cast types ===
df = df.rename(columns={'date': 'day'})
df = df.astype({
    "customer_id": "int",
    "year": "int",
    "month": "int",
    "day": "int",
    "children": "int",
    "charges": "float",
    "hospital_tier": "int",
    "city_tier": "int",
    "state_id": "int"
})

# === Step 6: Connect to SQL Server ===
conn = pyodbc.connect(
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=INSHA\SQLEXPRESS;'
    r'DATABASE=MedicalDB;'
    r'Trusted_Connection=yes;'
)
cursor = conn.cursor()

# === Step 7: Create required tables ===

cursor.execute("""
IF OBJECT_ID('hospitalisation_details_cleaned', 'U') IS NOT NULL DROP TABLE hospitalisation_details_cleaned;
CREATE TABLE hospitalisation_details_cleaned (
    customer_id INT,
    admission_date DATE,
    children INT,
    charges FLOAT,
    hospital_tier INT,
    city_tier INT,
    state_id INT
);
""")

cursor.execute("""
IF OBJECT_ID('names', 'U') IS NOT NULL DROP TABLE names;
CREATE TABLE names (
    customer_id INT PRIMARY KEY,
    name NVARCHAR(100)
);
""")

cursor.execute("""
IF OBJECT_ID('medical_examinations', 'U') IS NOT NULL DROP TABLE medical_examinations;
CREATE TABLE medical_examinations (
    customer_id INT PRIMARY KEY,
    BMI FLOAT,
    smoker NVARCHAR(10),
    any_transplant NVARCHAR(10),
    cancer_history NVARCHAR(10),
    numberofmajorsurgeries INT,
    health_issues NVARCHAR(10)
);
""")

# === Step 8: Insert data into hospitalisation_details_cleaned ===
for _, row in df.iterrows():
    try:
        admission_date = date(int(row['year']), int(row['month']), int(row['day']))
        cursor.execute("""
            INSERT INTO hospitalisation_details_cleaned (
                customer_id, admission_date, children,
                charges, hospital_tier, city_tier, state_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, row['customer_id'], admission_date, row['children'], row['charges'],
             row['hospital_tier'], row['city_tier'], row['state_id'])
    except Exception as e:
        print(f"Error inserting row {row['customer_id']}: {e}")

# === Step 9: Insert sample data into names and medical_examinations ===
names_data = [
    (2323, 'Amit Kumar'),
    (2322, 'Neha Sharma'),
    (2321, 'Ravi Yadav'),
    (2320, 'Rina Gupta'),
    (2319, 'Saurabh Singh')
]
cursor.executemany("INSERT INTO names (customer_id, name) VALUES (?, ?)", names_data)

med_data = [
    (2323, 36.5, 'yes', 'no', 'no', 1, 'yes'),
    (2322, 29.2, 'no', 'no', 'yes', 0, 'no'),
    (2321, 41.0, 'yes', 'yes', 'no', 2, 'yes'),
    (2320, 38.4, 'no', 'no', 'yes', 1, 'yes'),
    (2319, 27.3, 'yes', 'no', 'no', 0, 'no')
]
cursor.executemany("""
    INSERT INTO medical_examinations (
        customer_id, BMI, smoker, any_transplant,
        cancer_history, numberofmajorsurgeries, health_issues
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
""", med_data)

conn.commit()

# === Step 10: Define function to run queries ===
def run_query(title, query):
    print(f"\n==== {title} ====")
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    df_result = pd.DataFrame.from_records(rows, columns=columns)
    print(df_result)

# === Step 11: Run all 16 queries ===

run_query("1. First 5 Rows", "SELECT TOP 5 * FROM hospitalisation_details_cleaned")

run_query("2. Average Charges", "SELECT AVG(charges) as avg_charges FROM hospitalisation_details_cleaned")

run_query("3. Charges > 700", """
SELECT TOP 10 customer_id, YEAR(admission_date) AS year, charges  
FROM hospitalisation_details_cleaned 
WHERE charges > 700
""")

run_query("4. Names with BMI > 35", """
SELECT TOP 10 n.name, YEAR(h.admission_date) AS year, h.charges
FROM hospitalisation_details_cleaned AS h
JOIN medical_examinations AS m ON h.customer_id = m.customer_id
JOIN names AS n ON n.customer_id = m.customer_id
WHERE m.BMI > 35
""")

run_query("5. Major Surgeries ≥ 1", """
SELECT TOP 10 n.customer_id, n.name
FROM names AS n
JOIN medical_examinations AS m ON n.customer_id = m.customer_id
WHERE m.numberofmajorsurgeries >= 1
""")

run_query("6. Avg Charges by Tier (2000)", """
SELECT hospital_tier, AVG(charges) as avg_charges 
FROM hospitalisation_details_cleaned 
WHERE YEAR(admission_date) = 2000 
GROUP BY hospital_tier
""")

run_query("7. Smokers with Transplant", """
SELECT m.customer_id, m.BMI, h.charges 
FROM medical_examinations AS m
JOIN hospitalisation_details_cleaned AS h ON h.customer_id = m.customer_id
WHERE m.smoker = 'yes' AND m.any_transplant = 'yes'
""")

run_query("8. Cancer History or 2+ Surgeries", """
SELECT TOP 10 n.name 
FROM names AS n
JOIN medical_examinations AS m ON m.customer_id = n.customer_id
WHERE m.cancer_history = 'Yes' OR m.numberofmajorsurgeries >= 2
""")

run_query("9. Max Surgeries (Top 1)", """
SELECT TOP 1 n.customer_id, n.name 
FROM names AS n
JOIN medical_examinations AS m ON m.customer_id = n.customer_id
ORDER BY m.numberofmajorsurgeries DESC
""")

run_query("10. City Tier of Surgical Patients", """
SELECT TOP 10 n.customer_id, n.name, h.city_tier
FROM hospitalisation_details_cleaned AS h
JOIN medical_examinations AS m ON h.customer_id = m.customer_id
JOIN names AS n ON n.customer_id = m.customer_id
WHERE m.numberofmajorsurgeries > 0
""")

run_query("11. Avg BMI by City Tier (1995)", """
SELECT h.city_tier, AVG(m.BMI) AS avg_bmi 
FROM hospitalisation_details_cleaned AS h
JOIN medical_examinations AS m ON h.customer_id = m.customer_id
WHERE YEAR(h.admission_date) = 1995
GROUP BY h.city_tier
""")

run_query("12. Health Issues & BMI > 30", """
SELECT TOP 10 n.customer_id, n.name, h.charges
FROM hospitalisation_details_cleaned AS h
JOIN medical_examinations AS m ON h.customer_id = m.customer_id
JOIN names AS n ON n.customer_id = m.customer_id
WHERE m.health_issues = 'yes' AND m.BMI > 30
""")

run_query("13. Max Charges per Year", """
SELECT TOP 10 YEAR(h.admission_date) AS year, n.name, h.city_tier, MAX(h.charges) AS max_charges
FROM hospitalisation_details_cleaned AS h
JOIN names AS n ON n.customer_id = h.customer_id
GROUP BY YEAR(h.admission_date), n.name, h.city_tier
HAVING MAX(h.charges) = (
    SELECT MAX(charges) FROM hospitalisation_details_cleaned WHERE YEAR(admission_date) = YEAR(h.admission_date)
)
""")

run_query("14. Top 3 by Avg Yearly Charges", """
WITH YearlyCharges AS ( 
    SELECT customer_id, YEAR(admission_date) AS year, AVG(charges) AS avg_yearly_charges 
    FROM hospitalisation_details_cleaned 
    GROUP BY customer_id, YEAR(admission_date)
)
SELECT TOP 3 n.name, y.avg_yearly_charges 
FROM names AS n
JOIN YearlyCharges AS y ON y.customer_id = n.customer_id
ORDER BY y.avg_yearly_charges DESC
""")

run_query("15. Top 10 by Total Charges with RANK", """
SELECT TOP 10 n.name, SUM(h.charges) AS total_charges, 
       RANK() OVER (ORDER BY SUM(h.charges) DESC) AS charges_rank
FROM hospitalisation_details_cleaned AS h
JOIN names AS n ON n.customer_id = h.customer_id
GROUP BY n.name
ORDER BY charges_rank ASC
""")

run_query("16. Year with Most Hospitalizations", """
WITH YearlyHospitalizations AS (
    SELECT YEAR(admission_date) AS year, COUNT(*) AS num_hospitalizations
    FROM hospitalisation_details_cleaned
    GROUP BY YEAR(admission_date)
)
SELECT year, num_hospitalizations
FROM YearlyHospitalizations
WHERE num_hospitalizations = (
    SELECT MAX(num_hospitalizations) FROM YearlyHospitalizations
)
""")

# === Step 12: Close connection ===
cursor.close()
conn.close()
print(f"\n✅ Done! Inserted: {len(df)} rows and ran 16 SQL queries.")
