import os
import pandas as pd
from sqlalchemy import create_engine, text
import urllib

# -----------------------------
# ✅ EDIT IF NEEDED
# -----------------------------
SERVER = r"localhost\SQLEXPRESS"          # same as SSMS server
DB = "ecom_analytics"                      # your database name
DRIVER = "ODBC Driver 18 for SQL Server"   # you have this installed ✅

# Your CSV folder (based on your screenshot/project path)
RAW_DIR = r"C:\Users\varun\Desktop\ecom-analytics\data_raw"
# -----------------------------

files_to_tables = {
    "olist_orders_dataset.csv": "stg_orders",
    "olist_order_items_dataset.csv": "stg_order_items",
    "olist_order_payments_dataset.csv": "stg_order_payments",
    "olist_customers_dataset.csv": "stg_customers",
    "olist_products_dataset.csv": "stg_products",
    "olist_order_reviews_dataset.csv": "stg_order_reviews",
    "olist_sellers_dataset.csv": "stg_sellers",
    "olist_geolocation_dataset.csv": "stg_geolocation",
    "product_category_name_translation.csv": "stg_category_translation",
}

# Build Windows Auth connection string
params = urllib.parse.quote_plus(
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DB};"
    f"Trusted_Connection=yes;"
    f"TrustServerCertificate=yes;"
)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True
)

# Create schema if not exists
with engine.begin() as conn:
    conn.execute(text("""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'staging')
            EXEC('CREATE SCHEMA staging');
    """))

# Load each CSV into staging.<table>
for filename, table in files_to_tables.items():
    path = os.path.join(RAW_DIR, filename)

    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ File not found: {path}")

    print(f"\nLoading {filename} -> staging.{table}")

    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]

    df.to_sql(
        table,
        engine,
        schema="staging",
        if_exists="replace",
        index=False,
        chunksize=5000
    )

    print(f"✅ Loaded {len(df):,} rows into staging.{table}")

print("\n🎉 Done: All CSVs loaded into SQL Server staging schema.")