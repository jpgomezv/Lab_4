import great_expectations as gx
import pandas as pd
from utils import setup_logger

logger = setup_logger('validate_output')

def validate_clean_data(filepath: str):
    logger.info("Initializing Great Expectations context...")
    context = gx.get_context(mode="file")
    
    logger.info(f"Loading transformed data from {filepath}")
    df = pd.read_csv(filepath)
    # Ensure invoice_date is datetime for proper type checking if supported, or we rely on the dataset
    
    data_source_name = "retail_clean_source"
    try:
        datasource = context.data_sources.get(data_source_name)
    except Exception:
        datasource = context.data_sources.add_pandas(name=data_source_name)

    data_asset_name = "retail_clean_asset"
    try:
        data_asset = datasource.get_asset(data_asset_name)
    except Exception:
        data_asset = datasource.add_dataframe_asset(name=data_asset_name)

    try:
        batch_definition = data_asset.add_batch_definition_whole_dataframe("whole_dataframe")
    except ValueError:
        batch_definition = data_asset.get_batch_definition("whole_dataframe")
    batch_parameters = {"dataframe": df}

    suite_name = "clean_data_suite"
    try:
        suite = context.suites.get(name=suite_name)
    except Exception:
        suite = gx.ExpectationSuite(name=suite_name)
        context.suites.add(suite)

    logger.info("Building clean data expectations...")
    
    # Same as raw
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="invoice_date"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="invoice_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="quantity", min_value=1))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=0.01))
    
    products = ['Mouse', 'Printer', 'Monitor', 'Laptop', 'Phone', 'Headphones', 'Tablet', 'Keyboard']
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="product", value_set=products))

    # Transformed columns
    countries = ['Colombia', 'Ecuador', 'Peru', 'Chile']
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="country", value_set=countries))
    
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="month", min_value=1, max_value=12))
    
    bins = ['Low', 'Medium', 'High']
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="revenue_bin", value_set=bins))
    
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="total_revenue", min_value=0.01))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="invoice_id"))
    
    # Save validation
    validation_definition_name = "clean_data_validation"
    try:
        context.validation_definitions.delete(validation_definition_name)
    except Exception:
        pass
        
    validation_definition = gx.ValidationDefinition(
        name=validation_definition_name,
        data=batch_definition,
        suite=suite,
    )
    context.validation_definitions.add(validation_definition)
    
    logger.info("Running validation on transformed data...")
    validation_results = validation_definition.run(batch_parameters=batch_parameters)
    
    logger.info("Validation complete. Generating Data Docs...")
    context.build_data_docs()
    
    success = validation_results.success
    results = validation_results.results
    
    logger.info(f"Clean Suite success: {success}")
    for res in results:
        exp_type = res.expectation_config.type
        kwargs = res.expectation_config.kwargs
        col = kwargs.get('column')
        metric = res.result
        success_pct = 100
        if 'unexpected_percent' in metric:
            success_pct = 100 - metric['unexpected_percent']
        logger.info(f"Expectation {exp_type} on {col}: Pass {success_pct:.2f}%")
        if success_pct < 100:
            logger.warning(f"Failed {exp_type} on {col}: {metric.get('unexpected_count')} unexpected items.")

if __name__ == "__main__":
    validate_clean_data('data/processed/retail_transformed.csv')
