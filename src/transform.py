import pandas as pd
import os
from utils import setup_logger

logger = setup_logger('transform')

def transform_data(input_path: str, output_path: str):
    logger.info(f"Loading cleaned data for transformation from {input_path}")
    df = pd.read_csv(input_path)
    
    # 1. Standardize country
    logger.info("Standardizing 'country' column...")
    country_map = {
        'CO': 'Colombia',
        'colombia': 'Colombia'
    }
    # First map exact replacements
    df['country'] = df['country'].replace(country_map)
    # Then title case to catch 'ecuador' -> 'Ecuador'
    df['country'] = df['country'].str.strip().str.title()
    
    # 2. Parse invoice_date
    logger.info("Parsing 'invoice_date' to datetime...")
    # Using dayfirst=False as default but relying on pandas robust inference
    # Note: pandas to_datetime handles mixed format nicely if we let it infer.
    # We will coerce errors to NaT and drop them if any unparseable strings exist (e.g. 'unknown').
    df['invoice_date'] = pd.to_datetime(df['invoice_date'], format='mixed', errors='coerce', dayfirst=False)
    
    # Drop any rows where invoice_date could not be parsed
    df = df.dropna(subset=['invoice_date'])
    
    # Extract temporal features
    logger.info("Extracting year, month, day_of_week...")
    df['year'] = df['invoice_date'].dt.year
    df['month'] = df['invoice_date'].dt.month
    df['day_of_week'] = df['invoice_date'].dt.day_name()
    
    # 3. Cast customer_id
    logger.info("Casting 'customer_id' to Int64...")
    df['customer_id'] = df['customer_id'].astype('Int64')
    
    # 4. Normalize product
    logger.info("Normalizing 'product' casing...")
    df['product'] = df['product'].str.strip().str.title()
    
    # 5. Feature engineering: revenue_bin
    logger.info("Creating 'revenue_bin' (Low/Medium/High) using qcut...")
    # Handling duplicates or narrow bins with duplicates='drop'
    df['revenue_bin'] = pd.qcut(df['total_revenue'], q=3, labels=['Low', 'Medium', 'High'])
    
    # Validate final counts
    logger.info(f"Transformed dataset shape: {df.shape}")
    logger.info("Transformation complete. Sample:\n" + str(df.head()))
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"Transformed dataset saved to {output_path}")
    return df

if __name__ == "__main__":
    transform_data('data/processed/retail_clean.csv', 'data/processed/retail_transformed.csv')
