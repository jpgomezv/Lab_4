import os
from utils import setup_logger

logger = setup_logger('quality_analysis')

def generate_quality_report(output_path: str):
    """Generates the Data Quality Issues and Policies report."""
    logger.info(f"Generating Data Quality Report to {output_path}")

    report_md = """# Data Quality Report

## 1. Data Quality Issues Table
| Column | Issue | Example | Dimension | Business Impact |
| :--- | :--- | :--- | :--- | :--- |
| `invoice_id` | Duplicate IDs | 22951 appears 3x | Uniqueness | Revenue double/triple-counted in BO-1 KPIs |
| `customer_id` | NULL values | NaN in 202 rows | Completeness | Cannot link sales to customers for BO-3 |
| `quantity` | Negative values | -3 in row 5421 | Validity | Negative units corrupt total_revenue (BO-1) |
| `price` | Negative values | -83.02 in row x | Validity | Negative prices distort revenue and ATV (BO-1) |
| `total_revenue` | Calculation mismatch | Rev is 4.00 but qty\*price=4.50 | Accuracy | Over/under reporting total sales figures (BO-1) |
| `country` | Inconsistent casing/codes | 'colombia', 'CO' instead of 'Colombia' | Consistency | Fragmentation of geographical sales analysis (BO-3) |
| `invoice_date` | Mixed date formats | 'YYYY/MM/DD', 'DD-MM-YYYY' | Timeliness | Aggregations by month/time will fail or group incorrectly (BO-2) |
| `invoice_date` | Null-like strings & Future | 'NULL', 'N/A', '2027' | Timeliness | Missing dates or future dates pollute timeline trends (BO-2) |

## 2. Data Quality Policy Proposal
| # | Policy Statement | GE Expectation | Severity | Addresses (BO) |
| :--- | :--- | :--- | :--- | :--- |
| P-01 | invoice_id must be unique across the entire dataset. | `expect_column_values_to_be_unique('invoice_id')` | **Critical** | BO-1, BO-4 |
| P-02 | quantity must be a positive integer (≥ 1). | `expect_column_values_to_be_between('quantity', min_value=1)` | **Critical** | BO-1 |
| P-03 | price must be greater than zero (≥ 0.01). | `expect_column_values_to_be_between('price', min_value=0.01)` | **Critical** | BO-1 |
| P-04 | total_revenue must equal quantity × price (±0.01 tolerance). | Custom / Eq Check | **High** | BO-1 |
| P-05 | country must be one of: {Colombia, Ecuador, Peru, Chile} (title case). | `expect_column_values_to_be_in_set('country', ...)` | **Moderate** | BO-3 |
| P-06 | invoice_date must follow YYYY-MM-DD and fall within 2023. | `expect_column_values_to_match_regex(...)` | **High** | BO-2 |
| P-07 | customer_id must not be null. | `expect_column_values_to_not_be_null('customer_id')` | **Moderate** | BO-3 |
| P-08 | product must belong to the standard catalog of items. | `expect_column_values_to_be_in_set('product', ...)` | **Low** | BO-3 |
"""

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report_md)
    logger.info("Report generated successfully.")

if __name__ == "__main__":
    generate_quality_report('reports/quality_report.md')
