import os
from utils import setup_logger
from extract import extract_data, profile_data
from validate_input import validate_raw_data
from quality_analysis import generate_quality_report
from clean import clean_data
from transform import transform_data
from validate_output import validate_clean_data
from dimensional_model import create_dimensional_model
from load_dw import load_data
from analysis import run_analysis

def main():
    logger = setup_logger('main_pipeline')
    logger.info("=== Starting Retail ETL Pipeline ===")
    
    # Define paths
    raw_data_path = 'data/raw/retail_etl_dataset.csv'
    clean_data_path = 'data/processed/retail_clean.csv'
    transformed_data_path = 'data/processed/retail_transformed.csv'
    db_path = 'data/processed/data_warehouse.db'
    reports_dir = 'reports'
    
    os.makedirs('data/processed', exist_ok=True)
    os.makedirs(reports_dir, exist_ok=True)

    # Task a: Extract & Profile
    logger.info("--- Task a: Extract & Profile ---")
    df_raw = extract_data(raw_data_path)
    profile_data(df_raw)

    # Task b: Input Validation
    logger.info("--- Task b: Input Validation (Great Expectations) ---")
    validate_raw_data(raw_data_path)

    # Task c: Quality Analysis
    logger.info("--- Task c: Data Quality Analysis ---")
    generate_quality_report(os.path.join(reports_dir, 'quality_report.md'))

    # Task d: Cleaning
    logger.info("--- Task d: Cleaning ---")
    clean_data(raw_data_path, clean_data_path)

    # Task e: Transformation
    logger.info("--- Task e: Transformation ---")
    transform_data(clean_data_path, transformed_data_path)

    # Task f: Post-Transformation Validation
    logger.info("--- Task f: Post-Transformation Validation ---")
    validate_clean_data(transformed_data_path)

    # Task g: Dimensional Modeling
    logger.info("--- Task g: Dimensional Modeling ---")
    create_dimensional_model(transformed_data_path, 'data/processed')

    # Task h: Load
    logger.info("--- Task h: Load to SQLite DW ---")
    load_data(db_path, 'data/processed')

    # Task i: Business Analysis
    logger.info("--- Task i: Business Analysis (KPIs & Charts) ---")
    run_analysis(db_path, reports_dir)

    logger.info("=== ETL Pipeline Complete ===")

if __name__ == "__main__":
    main()