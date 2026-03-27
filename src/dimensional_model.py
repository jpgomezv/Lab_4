import pandas as pd
import os
from utils import setup_logger

logger = setup_logger('dimensional_model')

def create_dimensional_model(input_path: str, output_dir: str):
    logger.info(f"Loading transformed data from {input_path}")
    df = pd.read_csv(input_path)
    
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. dim_product
    logger.info("Building dim_product...")
    unique_products = df['product'].dropna().unique()
    dim_product = pd.DataFrame({'product_name': unique_products})
    dim_product['product_id'] = range(1, len(dim_product) + 1)
    # Reorder columns
    dim_product = dim_product[['product_id', 'product_name']]
    dim_product.to_csv(os.path.join(output_dir, 'dim_product.csv'), index=False)
    
    # 2. dim_customer
    logger.info("Building dim_customer...")
    unique_customers = df['customer_id'].dropna().unique()
    dim_customer = pd.DataFrame({'customer_key': unique_customers})
    dim_customer['customer_id'] = range(1, len(dim_customer) + 1)
    dim_customer = dim_customer[['customer_id', 'customer_key']]
    dim_customer.to_csv(os.path.join(output_dir, 'dim_customer.csv'), index=False)
    
    # 3. dim_location
    logger.info("Building dim_location...")
    unique_locations = df['country'].dropna().unique()
    dim_location = pd.DataFrame({'country': unique_locations})
    dim_location['location_id'] = range(1, len(dim_location) + 1)
    dim_location = dim_location[['location_id', 'country']]
    dim_location.to_csv(os.path.join(output_dir, 'dim_location.csv'), index=False)
    
    # 4. dim_date
    logger.info("Building dim_date (all dates in 2023)...")
    date_range = pd.date_range(start='2023-01-01', end='2023-12-31')
    dim_date = pd.DataFrame({'full_date': date_range})
    dim_date['date_id'] = dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
    dim_date['year'] = dim_date['full_date'].dt.year
    dim_date['month'] = dim_date['full_date'].dt.month
    dim_date['month_name'] = dim_date['full_date'].dt.strftime('%B')
    dim_date['day_of_week'] = dim_date['full_date'].dt.day_name()
    dim_date['full_date'] = dim_date['full_date'].dt.strftime('%Y-%m-%d')
    dim_date = dim_date[['date_id', 'full_date', 'year', 'month', 'month_name', 'day_of_week']]
    dim_date.to_csv(os.path.join(output_dir, 'dim_date.csv'), index=False)
    
    # 5. fact_sales
    logger.info("Building fact_sales...")
    fact_sales = df.copy()
    
    # Merge to get surrogate keys
    # Product
    fact_sales = fact_sales.merge(dim_product, left_on='product', right_on='product_name', how='left')
    # Customer
    fact_sales = fact_sales.merge(dim_customer, left_on='customer_id', right_on='customer_key', suffixes=('', '_surrogate'), how='left')
    # Location
    fact_sales = fact_sales.merge(dim_location, on='country', how='left')
    # Date (format original invoice_date to join on date_id)
    fact_sales['date_id'] = pd.to_datetime(fact_sales['invoice_date']).dt.strftime('%Y%m%d').astype(int)
    
    # Rename columns and select what's needed
    # Wait, the customer_id in fact_sales needs to be the surrogate
    fact_sales.rename(columns={'customer_id_surrogate': 'customer_id_fk'}, inplace=True)
    
    # Select final columns
    fact_sales['sale_id'] = range(1, len(fact_sales) + 1)
    
    final_cols = [
        'sale_id', 'invoice_id', 'product_id', 
        'customer_id_fk', 'location_id', 'date_id', 
        'quantity', 'price', 'total_revenue'
    ]
    
    fact_sales = fact_sales[final_cols]
    fact_sales.rename(columns={'customer_id_fk': 'customer_id'}, inplace=True)
    
    fact_sales.to_csv(os.path.join(output_dir, 'fact_sales.csv'), index=False)
    logger.info(f"Dimensional model creation complete. Fact sales shape: {fact_sales.shape}")
    
    # Let's write the model_description as well
    create_model_description()

def create_model_description():
    report_md = """# Star Schema Data Model

## ERD Diagram
```mermaid
erDiagram
    fact_sales {
        int sale_id PK
        int invoice_id
        int product_id FK
        int customer_id FK
        int location_id FK
        int date_id FK
        int quantity
        float price
        float total_revenue
    }
    dim_product {
        int product_id PK
        string product_name
    }
    dim_customer {
        int customer_id PK
        string customer_key
    }
    dim_location {
        int location_id PK
        string country
    }
    dim_date {
        int date_id PK
        date full_date
        int year
        int month
        string month_name
        string day_of_week
    }
    fact_sales }O--|| dim_product : references
    fact_sales }O--|| dim_customer : references
    fact_sales }O--|| dim_location : references
    fact_sales }O--|| dim_date : references
```

## Table Descriptions
1. **dim_product**: Contains unique products parsed from raw data.
2. **dim_customer**: Links surrogate customer_id to raw source customer_key.
3. **dim_location**: Maps country strings to surrogate location_id.
4. **dim_date**: A complete date dimension for the year 2023 with rich date attributes.
5. **fact_sales**: Transactional data storing quantities and revenues matched to dimension keys.
"""
    with open('reports/model_description.md', 'w') as f:
        f.write(report_md)
    logger.info("Saved reports/model_description.md")

if __name__ == "__main__":
    create_dimensional_model('data/processed/retail_transformed.csv', 'data/processed')
