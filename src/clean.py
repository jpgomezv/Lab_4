import pandas as pd
import numpy as np
import os
from utils import setup_logger

logger = setup_logger('clean')

def clean_data(input_path: str, output_path: str):
    logger.info(f"Loading raw data for cleaning from {input_path}")
    df = pd.read_csv(input_path)
    
    initial_count = len(df)
    logger.info(f"Initial row count: {initial_count}")
    logger.info(f"Initial nulls per column:\n{df.isnull().sum()}")
    
    # 1. Duplicate invoice_id
    dups_mask = df.duplicated(subset=['invoice_id'], keep='first')
    dups_count = dups_mask.sum()
    df = df[~dups_mask]
    logger.info(f"Dropped {dups_count} duplicate invoice_id rows.")

    # 2. Negative quantity
    neg_qty_mask = df['quantity'] < 1
    neg_qty_count = neg_qty_mask.sum()
    df = df[~neg_qty_mask]
    logger.info(f"Dropped {neg_qty_count} rows with quantity < 1.")

    # 3. Negative price
    neg_price_mask = df['price'] < 0.01
    neg_price_count = neg_price_mask.sum()
    df = df[~neg_price_mask]
    logger.info(f"Dropped {neg_price_count} rows with price < 0.01.")

    # 4. NULL customer_id
    null_cust_mask = df['customer_id'].isnull()
    null_cust_count = null_cust_mask.sum()
    df = df[~null_cust_mask]
    logger.info(f"Dropped {null_cust_count} rows with NULL customer_id.")

    # 5. Null-like invoice_date
    null_date_vals = ['N/A', 'NULL', '', 'NaN']
    null_like_date_mask = df['invoice_date'].isnull() | df['invoice_date'].isin(null_date_vals)
    null_like_date_count = null_like_date_mask.sum()
    df = df[~null_like_date_mask]
    logger.info(f"Dropped {null_like_date_count} rows with null-like invoice_date.")

    # 6. Future invoice_date (> 2023)
    # Extract year cleanly
    years = df['invoice_date'].str.extract(r'(20\d{2})')[0].astype(float)
    future_date_mask = years > 2023
    future_date_count = future_date_mask.sum()
    df = df[~future_date_mask]
    logger.info(f"Dropped {future_date_count} rows with future invoice_date.")

    # 7. Inaccurate total_revenue
    calc_rev = df['quantity'] * df['price']
    inaccurate_rev_mask = abs(df['total_revenue'] - calc_rev) > 0.01
    # also handle nan in total_revenue if it exists
    inaccurate_rev_mask = inaccurate_rev_mask | df['total_revenue'].isnull()
    inaccurate_rev_count = inaccurate_rev_mask.sum()
    df = df[~inaccurate_rev_mask]
    logger.info(f"Dropped {inaccurate_rev_count} rows with inaccurate total_revenue.")

    final_count = len(df)
    logger.info(f"Final row count: {final_count} (Dropped {initial_count - final_count} total rows)")
    logger.info(f"Final nulls per column:\n{df.isnull().sum()}")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"Cleaned dataset saved to {output_path}")
    return df

if __name__ == "__main__":
    clean_data('data/raw/retail_etl_dataset.csv', 'data/processed/retail_clean.csv')
