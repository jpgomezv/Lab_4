import sqlite3
import pandas as pd
import os
from utils import setup_logger

logger = setup_logger('load_dw')

def create_tables(cursor):
    """Creates the star schema tables with explicit foreign keys to enforce integrity."""
    logger.info("Executing DDL to create dimensional and fact tables...")

    # dim_date
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id INTEGER PRIMARY KEY,
        full_date TEXT,
        year INTEGER,
        month INTEGER,
        month_name TEXT,
        day_of_week TEXT
    )''')

    # dim_product
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dim_product (
        product_id INTEGER PRIMARY KEY,
        product_name TEXT
    )''')

    # dim_customer
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_id INTEGER PRIMARY KEY,
        customer_key INTEGER
    )''')

    # dim_location
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dim_location (
        location_id INTEGER PRIMARY KEY,
        country TEXT
    )''')

    # fact_sales
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fact_sales (
        sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
        invoice_id INTEGER,
        product_id INTEGER,
        customer_id INTEGER,
        location_id INTEGER,
        date_id INTEGER,
        quantity INTEGER,
        price REAL,
        total_revenue REAL,
        FOREIGN KEY (product_id) REFERENCES dim_product (product_id),
        FOREIGN KEY (customer_id) REFERENCES dim_customer (customer_id),
        FOREIGN KEY (location_id) REFERENCES dim_location (location_id),
        FOREIGN KEY (date_id) REFERENCES dim_date (date_id)
    )''')
    
def load_data(db_path: str, data_dir: str):
    logger.info(f"Connecting to SQLite database at {db_path}...")
    conn = sqlite3.connect(db_path)
    
    # Enforce foreign keys manually since python sqlite3 doesn't by default always
    cursor = conn.cursor()
    cursor.execute('PRAGMA foreign_keys = ON;')
    
    # Drop tables if exist logic
    cursor.execute("DROP TABLE IF EXISTS fact_sales")
    cursor.execute("DROP TABLE IF EXISTS dim_date")
    cursor.execute("DROP TABLE IF EXISTS dim_product")
    cursor.execute("DROP TABLE IF EXISTS dim_customer")
    cursor.execute("DROP TABLE IF EXISTS dim_location")
    
    create_tables(cursor)
    
    # 1. Load Dimensions (No FKs)
    logger.info("Loading dim_date...")
    dim_date = pd.read_csv(os.path.join(data_dir, 'dim_date.csv'))
    dim_date.to_sql('dim_date', conn, if_exists='append', index=False)
    
    logger.info("Loading dim_product, dim_customer, dim_location...")
    dim_product = pd.read_csv(os.path.join(data_dir, 'dim_product.csv'))
    dim_product.to_sql('dim_product', conn, if_exists='append', index=False)
    
    dim_customer = pd.read_csv(os.path.join(data_dir, 'dim_customer.csv'))
    dim_customer.to_sql('dim_customer', conn, if_exists='append', index=False)
    
    dim_location = pd.read_csv(os.path.join(data_dir, 'dim_location.csv'))
    dim_location.to_sql('dim_location', conn, if_exists='append', index=False)
    
    # 2. Load Fact Table (Has FKs)
    logger.info("Loading fact_sales...")
    fact_sales = pd.read_csv(os.path.join(data_dir, 'fact_sales.csv'))
    fact_sales.to_sql('fact_sales', conn, if_exists='append', index=False)
    
    # Check counts
    cursor.execute("SELECT COUNT(*) FROM fact_sales")
    count = cursor.fetchone()[0]
    logger.info(f"Total rows loaded into fact_sales: {count}")
    
    conn.commit()
    conn.close()
    logger.info("Data loaded successfully.")

if __name__ == "__main__":
    os.makedirs('data/processed', exist_ok=True)
    load_data('data/processed/data_warehouse.db', 'data/processed')
