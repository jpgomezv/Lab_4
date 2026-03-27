import pandas as pd
import numpy as np
from utils import setup_logger

logger = setup_logger('extract_profile')

def extract_data(filepath: str) -> pd.DataFrame:
    """Loads the dataset from the given filepath."""
    logger.info(f"Loading data from {filepath}")
    try:
        df = pd.read_csv(filepath)
        logger.info(f"Successfully loaded {len(df)} rows.")
        return df
    except Exception as e:
        logger.error(f"Error loading {filepath}: {e}")
        raise

def profile_data(df: pd.DataFrame):
    """Profiles the DataFrame and prints the required metrics."""
    logger.info("Profiling dataset...")
    
    # Shape, dtypes, memory
    logger.info(f"Shape: {df.shape[0]} rows, {df.shape[1]} columns")
    logger.info("Data types:")
    logger.info(f"\n{df.dtypes}")
    logger.info(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Missing values
    missing = df.isnull().sum()
    missing_pct = (missing / len(df)) * 100
    
    # Include null-like strings in invoice_date for completeness analysis
    date_null_like = df['invoice_date'].isin(['N/A', 'NULL', '', 'NaN']).sum()
    
    logger.info("\nMissing values (Count | Percentage):")
    for col in df.columns:
        logger.info(f"  {col}: {missing[col]} ({missing_pct[col]:.2f}%)")
    logger.info(f"  - Hidden missing in invoice_date (e.g. 'NULL'): {date_null_like}")

    # Cardinality
    categorical_cols = ['product', 'country', 'invoice_date']
    logger.info("\nCardinality of categorical columns:")
    for col in categorical_cols:
        logger.info(f"  {col}: {df[col].nunique()} unique values")

    # Descriptive statistics
    numeric_cols = ['quantity', 'price', 'total_revenue']
    logger.info("\nDescriptive statistics:")
    logger.info(f"\n{df[numeric_cols].describe()}")
    
    # Duplicate invoice_id values
    dup_invoices = df.duplicated(subset=['invoice_id'], keep=False).sum()
    logger.info(f"\nDuplicate invoice_id rows: {dup_invoices}")
    
    # Inaccurate total_revenue
    # Safe check: if price or quantity is NaN, we skip the comparison or fill with 0
    calculated_revenue = df['quantity'] * df['price']
    
    # total_revenue ≠ quantity × price (within tolerance ±0.01)
    tolerance = 0.01
    inaccurate_totals = (~np.isclose(df['total_revenue'], calculated_revenue, atol=tolerance, equal_nan=True)).sum()
    # Or more strictly where not null and difference > 0.01
    valid_mask = df['total_revenue'].notnull() & calculated_revenue.notnull()
    inaccurate_totals = (abs(df.loc[valid_mask, 'total_revenue'] - calculated_revenue[valid_mask]) > tolerance).sum()
    logger.info(f"Rows where total_revenue != quantity * price: {inaccurate_totals}")

    # Date formats distribution
    # We will regex match formats
    # YYYY-MM-DD
    fmt1 = df['invoice_date'].str.match(r'^\d{4}-\d{2}-\d{2}$', na=False).sum()
    # YYYY/MM/DD
    fmt2 = df['invoice_date'].str.match(r'^\d{4}/\d{2}/\d{2}$', na=False).sum()
    # DD-MM-YYYY
    fmt3 = df['invoice_date'].str.match(r'^\d{2}-\d{2}-\d{4}$', na=False).sum()
    other_fmt = len(df) - fmt1 - fmt2 - fmt3 - df['invoice_date'].isnull().sum() - date_null_like
    
    logger.info("\nDistribution of invoice_date formats:")
    logger.info(f"  YYYY-MM-DD: {fmt1}")
    logger.info(f"  YYYY/MM/DD: {fmt2}")
    logger.info(f"  DD-MM-YYYY: {fmt3}")
    logger.info(f"  Other/Invalid: {other_fmt + date_null_like}")
    
    # Future dates (> 2023-12-31)
    # Convert carefully with coerce
    parsed_dates = pd.to_datetime(df['invoice_date'], errors='coerce', dayfirst=False)
    # Wait, some are dayfirst. A simple string match or trying to parse multiple ways.
    # To just count > 2023 easily: find years. 
    # extract years:
    years = df['invoice_date'].str.extract(r'(20\d{2})')[0].astype(float)
    future_dates = (years > 2023).sum()
    logger.info(f"Count of future dates (> 2023): {future_dates}")

    logger.info("\nProfiling Complete.")
    
def main():
    df = extract_data('data/raw/retail_etl_dataset.csv')
    profile_data(df)

if __name__ == "__main__":
    main()
