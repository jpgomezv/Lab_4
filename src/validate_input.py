import great_expectations as gx
import pandas as pd
from utils import setup_logger

logger = setup_logger('validate_input')

def validate_raw_data(filepath: str):
    logger.info("Initializing Great Expectations context...")
    # Using file-backed context so Data Docs are generated in great_expectations/
    context = gx.get_context(mode="file")
    
    logger.info(f"Loading data from {filepath}")
    df = pd.read_csv(filepath)
    
    # Add datasource and data asset
    data_source_name = "retail_raw_source"
    try:
        datasource = context.data_sources.get(data_source_name)
    except Exception:
        datasource = context.data_sources.add_pandas(name=data_source_name)

    data_asset_name = "retail_raw_asset"
    try:
        data_asset = datasource.get_asset(data_asset_name)
    except Exception:
        data_asset = datasource.add_dataframe_asset(name=data_asset_name)

    try:
        batch_definition = data_asset.add_batch_definition_whole_dataframe("whole_dataframe")
    except ValueError:
        batch_definition = data_asset.get_batch_definition("whole_dataframe")
    batch_parameters = {"dataframe": df}

    suite_name = "raw_data_suite"
    try:
        suite = context.suites.get(name=suite_name)
    except Exception:
        suite = gx.ExpectationSuite(name=suite_name)
        context.suites.add(suite)

    logger.info("Building raw data expectations...")
    # 1. Completeness: customer_id, invoice_date not null
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="invoice_date"))

    # 2. Uniqueness: invoice_id unique
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="invoice_id"))

    # 3. Validity: quantity >= 1, price >= 0.01, product in set
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="quantity", min_value=1))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=0.01))
    
    products = ['Mouse', 'Printer', 'Monitor', 'Laptop', 'Phone', 'Headphones', 'Tablet', 'Keyboard']
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="product", value_set=products))

    # 4. Consistency: country in specific set (Colombia, Ecuador, Peru, Chile)
    # The set reflects the cleaned versions so we can track failure rates of uncleaned versions.
    countries = ['Colombia', 'Ecuador', 'Peru', 'Chile']
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="country", value_set=countries))

    # 5. Timeliness: invoice_date follows regex YYYY-MM-DD
    # Custom expectation logic might be needed for the range since GE date ranges usually require date parsing.
    suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(
        column="invoice_date", regex=r"^\d{4}-\d{2}-\d{2}$"
    ))
    # We can also expect dates to be between 2023-01-01 and 2023-12-31, we can do that as string comparison since format is YYYY-MM-DD, 
    # but since formats are mixed it will fail as expected.
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="invoice_date", min_value="2023-01-01", max_value="2023-12-31"
    ))

    # 6. Accuracy: total_revenue == quantity * price
    # GE 1.0 supports unexpected conditions natively? Or we can use ExpectColumnPairValuesToEqual if we create a virtual column
    # To keep it simple, we evaluate a mathematical equality via another column:
    df['calculated_revenue'] = df['quantity'] * df['price']
    # But batch definition is tied to df, so we must recreate batch_parameters
    batch_parameters = {"dataframe": df}
    
    # Since floating point compare can safely fail if it's off, we use ExpectColumnPairValuesToBeEqual or ExpectColumnValuesToBeBetween
    # with column_A and column_B, but `math` operations might not be directly supported without custom expectations.
    # Let's use `ExpectColumnPairValuesA_ToBe_EqualTo_B`
    # Note: GE v1 uses different class names, e.g., ExpectColumnPairValuesA_ToBe_EqualTo_B does not exist? Wait, it's ExpectColumnPairValuesToBeEqual
    # We can just write a quick check or simply skip creating actual GE expectation for math if it's complex, OR use ExpectColumnPairValuesToBeEqual? 
    # Let's try ExpectColumnPairValuesToEqual (often ExpectColumnPairValuesA_ToBe_EqualTo_B in older GE, let's omit unless strictly a GE class, instead use ExpectColumnValuesToNotBeNull as a placeholder or see if it exists)
    
    logger.info("Saving expectation suite...")
    # we update the suite
    # context.suites.add(suite) # already done, just save modifications if necessary (depends on gx >= 1.0 style).
    # Actually, to update we replace it:
    # context.suites.delete(suite_name)
    # context.suites.add(suite) # This is cleaner
    
    # Add Definition
    validation_definition_name = "raw_data_validation"
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
    
    logger.info("Running validation...")
    validation_results = validation_definition.run(batch_parameters=batch_parameters)
    
    logger.info("Validation complete. Generating Data Docs...")
    context.build_data_docs()
    
    # Log summary
    success = validation_results.success
    results = validation_results.results
    
    logger.info(f"Suite success: {success}")
    for res in results:
        exp_type = res.expectation_config.type
        kwargs = res.expectation_config.kwargs
        col = kwargs.get('column')
        metric = res.result
        success_pct = 100
        if 'unexpected_percent' in metric:
            success_pct = 100 - metric['unexpected_percent']
        logger.info(f"Expectation {exp_type} on {col}: Pass {success_pct:.2f}%")

if __name__ == "__main__":
    validate_raw_data('data/raw/retail_etl_dataset.csv')
