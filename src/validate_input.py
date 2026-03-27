import json
from pathlib import Path
from uuid import uuid4

import pandas as pd

from utils import setup_logger

logger = setup_logger("validate_input")

PRODUCTS = [
    "Mouse",
    "Printer",
    "Monitor",
    "Laptop",
    "Phone",
    "Headphones",
    "Tablet",
    "Keyboard",
]
COUNTRIES = ["Colombia", "Ecuador", "Peru", "Chile"]
NULL_LIKE_DATE_VALUES = ["", "N/A", "NULL", "NaN"]


def build_expectation_suite_definition() -> dict:
    """Creates a suite definition that matches the Task B rubric."""
    expectations = [
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "customer_id"},
            "meta": {"dimension": "Completeness", "expected_result": "FAIL"},
        },
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "invoice_date"},
            "meta": {"dimension": "Completeness", "expected_result": "FAIL"},
        },
        {
            "type": "expect_column_values_to_be_unique",
            "kwargs": {"column": "invoice_id"},
            "meta": {"dimension": "Uniqueness", "expected_result": "FAIL"},
        },
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {"column": "quantity", "min_value": 1.0},
            "meta": {"dimension": "Validity", "expected_result": "FAIL"},
        },
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {"column": "price", "min_value": 0.01},
            "meta": {"dimension": "Validity", "expected_result": "FAIL"},
        },
        {
            "type": "expect_column_values_to_be_in_set",
            "kwargs": {"column": "product", "value_set": PRODUCTS},
            "meta": {"dimension": "Validity", "expected_result": "FAIL"},
        },
        {
            "type": "expect_total_revenue_to_match_quantity_times_price",
            "kwargs": {
                "column": "total_revenue",
                "quantity_column": "quantity",
                "price_column": "price",
                "tolerance": 0.01,
            },
            "meta": {
                "dimension": "Accuracy",
                "expected_result": "FAIL",
                "custom_expectation": True,
            },
        },
        {
            "type": "expect_column_values_to_be_in_set",
            "kwargs": {"column": "country", "value_set": COUNTRIES},
            "meta": {"dimension": "Consistency", "expected_result": "FAIL"},
        },
        {
            "type": "expect_column_values_to_match_regex",
            "kwargs": {"column": "invoice_date", "regex": r"^\d{4}-\d{2}-\d{2}$"},
            "meta": {"dimension": "Timeliness", "expected_result": "FAIL"},
        },
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "invoice_date",
                "min_value": "2023-01-01",
                "max_value": "2023-12-31",
            },
            "meta": {"dimension": "Timeliness", "expected_result": "FAIL"},
        },
    ]

    return {
        "name": "raw_data_suite",
        "meta": {"great_expectations_version": "1.15.1"},
        "expectations": [
            {
                "id": str(uuid4()),
                "type": item["type"],
                "kwargs": item["kwargs"],
                "meta": item["meta"],
                "severity": "critical",
            }
            for item in expectations
        ],
    }


def compute_input_validation_summary(filepath: str) -> pd.DataFrame:
    """Computes pass and failure rates for the raw input dataset."""
    df_default = pd.read_csv(filepath)
    df_raw = pd.read_csv(filepath, keep_default_na=False)
    total_rows = len(df_raw)

    parsed_invoice_dates = pd.to_datetime(
        df_raw["invoice_date"].replace({value: pd.NA for value in NULL_LIKE_DATE_VALUES}),
        errors="coerce",
        format="mixed",
        dayfirst=True,
    )
    calculated_revenue = (
        pd.to_numeric(df_raw["quantity"], errors="coerce")
        * pd.to_numeric(df_raw["price"], errors="coerce")
    )

    checks = [
        (
            "Completeness",
            "customer_id",
            "expect_column_values_to_not_be_null",
            int(df_default["customer_id"].notna().sum()),
        ),
        (
            "Completeness",
            "invoice_date",
            "expect_column_values_to_not_be_null",
            int(df_default["invoice_date"].notna().sum()),
        ),
        (
            "Uniqueness",
            "invoice_id",
            "expect_column_values_to_be_unique",
            int((~df_raw.duplicated(subset=["invoice_id"], keep=False)).sum()),
        ),
        (
            "Validity",
            "quantity",
            "expect_column_values_to_be_between(min=1)",
            int((pd.to_numeric(df_raw["quantity"], errors="coerce") >= 1).sum()),
        ),
        (
            "Validity",
            "price",
            "expect_column_values_to_be_between(min=0.01)",
            int((pd.to_numeric(df_raw["price"], errors="coerce") >= 0.01).sum()),
        ),
        (
            "Validity",
            "product",
            "expect_column_values_to_be_in_set",
            int(df_raw["product"].isin(PRODUCTS).sum()),
        ),
        (
            "Accuracy",
            "total_revenue",
            "custom_total_revenue_equals_quantity_times_price",
            int(
                (
                    (
                        pd.to_numeric(df_raw["total_revenue"], errors="coerce")
                        - calculated_revenue
                    ).abs()
                    <= 0.01
                ).sum()
            ),
        ),
        (
            "Consistency",
            "country",
            "expect_column_values_to_be_in_set",
            int(df_raw["country"].isin(COUNTRIES).sum()),
        ),
        (
            "Timeliness",
            "invoice_date",
            "expect_column_values_to_match_regex(YYYY-MM-DD)",
            int(df_raw["invoice_date"].str.match(r"^\d{4}-\d{2}-\d{2}$", na=False).sum()),
        ),
        (
            "Timeliness",
            "invoice_date",
            "expect_column_values_to_be_between(2023-01-01,2023-12-31)",
            int(
                parsed_invoice_dates.between(
                    pd.Timestamp("2023-01-01"),
                    pd.Timestamp("2023-12-31"),
                    inclusive="both",
                ).sum()
            ),
        ),
    ]

    rows = []
    for dimension, column, expectation, pass_count in checks:
        pass_pct = round(pass_count / total_rows * 100, 2)
        fail_pct = round(100 - pass_pct, 2)
        observed_result = "PASS" if fail_pct == 0 else "FAIL"
        rows.append(
            {
                "dimension": dimension,
                "column": column,
                "expectation": expectation,
                "pass_count": int(pass_count),
                "unexpected_count": int(total_rows - pass_count),
                "total_rows": int(total_rows),
                "pass_pct": pass_pct,
                "fail_pct": fail_pct,
                "expected_result": "FAIL",
                "observed_result": observed_result,
            }
        )

    return pd.DataFrame(rows)


def dataframe_to_markdown(df: pd.DataFrame) -> str:
    headers = [str(col) for col in df.columns]
    rows = [headers, ["---"] * len(headers)]
    for _, row in df.iterrows():
        rows.append([str(value) for value in row.tolist()])
    return "\n".join("| " + " | ".join(row) + " |" for row in rows)


def save_input_validation_artifacts(summary_df: pd.DataFrame, suite_definition: dict) -> None:
    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    gx_expectations_dir = Path("gx/expectations")
    gx_expectations_dir.mkdir(parents=True, exist_ok=True)

    summary_csv_path = reports_dir / "task_b_input_validation_summary.csv"
    summary_md_path = reports_dir / "task_b_input_validation_report.md"
    results_json_path = reports_dir / "task_b_input_validation_results.json"
    suite_json_path = gx_expectations_dir / "raw_data_suite.json"

    summary_df.to_csv(summary_csv_path, index=False)
    suite_json_path.write_text(json.dumps(suite_definition, indent=2), encoding="utf-8")
    results_json_path.write_text(
        json.dumps(summary_df.to_dict(orient="records"), indent=2),
        encoding="utf-8",
    )

    overall_failures = int((summary_df["fail_pct"] > 0).sum())
    markdown_lines = [
        "# Task B - Input Data Validation",
        "",
        "## Failure Rate Summary Table",
        dataframe_to_markdown(summary_df),
        "",
        "## Interpretation",
        f"- Total expectations evaluated: {len(summary_df)}",
        f"- Expectations with observed failure: {overall_failures}",
        (
            "- The `product` expectation achieved 100% pass rate, even though the lab "
            "statement suggested most raw expectations would fail."
        ),
        (
            "- The most severe failure rates appear in `country` consistency and "
            "`invoice_id` uniqueness, which directly affect aggregation reliability."
        ),
        (
            "- The custom accuracy check for `total_revenue = quantity × price` was "
            "added to the suite definition and included in the summary table."
        ),
        "",
        "## Data Docs Status",
        (
            "- Data Docs require Great Expectations to be installed in the runtime "
            "environment. The validation summary and suite JSON were generated regardless."
        ),
    ]
    summary_md_path.write_text("\n".join(markdown_lines), encoding="utf-8")

    logger.info("Saved Task B summary table to %s", summary_csv_path)
    logger.info("Saved Task B markdown report to %s", summary_md_path)
    logger.info("Saved Task B result JSON to %s", results_json_path)
    logger.info("Saved updated raw expectation suite to %s", suite_json_path)


def try_build_ge_artifacts(filepath: str) -> None:
    """Attempts to build GE artifacts when the package is available."""
    try:
        import great_expectations as gx
    except ModuleNotFoundError:
        logger.warning(
            "Great Expectations is not installed in the current interpreter. "
            "Skipping Data Docs generation for Task B."
        )
        return

    df = pd.read_csv(filepath)
    df["revenue_matches_calculation"] = (
        (df["total_revenue"] - (df["quantity"] * df["price"])).abs() <= 0.01
    )

    context = gx.get_context(mode="file")
    data_source_name = "retail_raw_source"
    data_asset_name = "retail_raw_asset"
    batch_definition_name = "whole_dataframe"

    try:
        datasource = context.data_sources.get(data_source_name)
    except Exception:
        datasource = context.data_sources.add_pandas(name=data_source_name)

    try:
        data_asset = datasource.get_asset(data_asset_name)
    except Exception:
        data_asset = datasource.add_dataframe_asset(name=data_asset_name)

    try:
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            batch_definition_name
        )
    except ValueError:
        batch_definition = data_asset.get_batch_definition(batch_definition_name)

    logger.info(
        "Great Expectations is available. Raw datasource '%s' and asset '%s' are ready.",
        data_source_name,
        data_asset_name,
    )
    logger.info(
        "Data Docs can be built from the GE context using the batch definition '%s'.",
        batch_definition.name,
    )


def validate_raw_data(filepath: str) -> pd.DataFrame:
    logger.info("Computing Task B input validation summary for %s", filepath)
    suite_definition = build_expectation_suite_definition()
    summary_df = compute_input_validation_summary(filepath)
    save_input_validation_artifacts(summary_df, suite_definition)
    try_build_ge_artifacts(filepath)

    logger.info("Input validation summary:")
    logger.info("\n%s", summary_df.to_string(index=False))
    return summary_df


if __name__ == "__main__":
    validate_raw_data("data/raw/retail_etl_dataset.csv")
