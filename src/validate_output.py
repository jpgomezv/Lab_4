import json
from pathlib import Path
from uuid import uuid4

import pandas as pd

from utils import setup_logger
from validate_input import (
    compute_input_validation_summary,
    COUNTRIES,
    PRODUCTS,
)

logger = setup_logger("validate_output")

REVENUE_BIN_LABELS = ["Low", "Medium", "High"]


def build_output_suite_definition() -> dict:
    expectations = [
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "customer_id"},
            "meta": {"dimension": "Completeness", "expected_result": "PASS"},
        },
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "invoice_date"},
            "meta": {"dimension": "Completeness", "expected_result": "PASS"},
        },
        {
            "type": "expect_column_values_to_be_unique",
            "kwargs": {"column": "invoice_id"},
            "meta": {"dimension": "Uniqueness", "expected_result": "PASS"},
        },
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {"column": "quantity", "min_value": 1.0},
            "meta": {"dimension": "Validity", "expected_result": "PASS"},
        },
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {"column": "price", "min_value": 0.01},
            "meta": {"dimension": "Validity", "expected_result": "PASS"},
        },
        {
            "type": "expect_column_values_to_be_in_set",
            "kwargs": {"column": "product", "value_set": PRODUCTS},
            "meta": {"dimension": "Validity", "expected_result": "PASS"},
        },
        {
            "type": "expect_total_revenue_to_match_quantity_times_price",
            "kwargs": {
                "column": "total_revenue",
                "quantity_column": "quantity",
                "price_column": "price",
                "tolerance": 0.01,
            },
            "meta": {"dimension": "Accuracy", "expected_result": "PASS"},
        },
        {
            "type": "expect_column_values_to_be_in_set",
            "kwargs": {"column": "country", "value_set": COUNTRIES},
            "meta": {"dimension": "Consistency", "expected_result": "PASS"},
        },
        {
            "type": "expect_column_values_to_match_regex",
            "kwargs": {"column": "invoice_date", "regex": r"^\d{4}-\d{2}-\d{2}$"},
            "meta": {"dimension": "Timeliness", "expected_result": "PASS"},
        },
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "invoice_date",
                "min_value": "2023-01-01",
                "max_value": "2023-12-31",
            },
            "meta": {"dimension": "Timeliness", "expected_result": "PASS"},
        },
        {
            "type": "expect_invoice_date_to_be_parseable_datetime_no_nulls",
            "kwargs": {"column": "invoice_date"},
            "meta": {"dimension": "Timeliness", "expected_result": "PASS"},
        },
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {"column": "month", "min_value": 1.0, "max_value": 12.0},
            "meta": {"dimension": "Timeliness", "expected_result": "PASS"},
        },
        {
            "type": "expect_column_values_to_be_in_set",
            "kwargs": {"column": "revenue_bin", "value_set": REVENUE_BIN_LABELS},
            "meta": {"dimension": "Validity", "expected_result": "PASS"},
        },
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {"column": "total_revenue", "min_value": 0.01},
            "meta": {"dimension": "Validity", "expected_result": "PASS"},
        },
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "invoice_id"},
            "meta": {"dimension": "Uniqueness", "expected_result": "PASS"},
        },
    ]

    return {
        "name": "clean_data_suite",
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


def evaluate_output_expectations(df: pd.DataFrame) -> pd.DataFrame:
    parsed_invoice_dates = pd.to_datetime(
        df["invoice_date"],
        errors="coerce",
        format="mixed",
        dayfirst=True,
    )
    calculated_revenue = df["quantity"] * df["price"]
    total_rows = len(df)

    checks = [
        (
            "Completeness",
            "customer_id",
            "expect_column_values_to_not_be_null",
            int(df["customer_id"].notna().sum()),
            True,
        ),
        (
            "Completeness",
            "invoice_date",
            "expect_column_values_to_not_be_null",
            int(df["invoice_date"].notna().sum()),
            True,
        ),
        (
            "Uniqueness",
            "invoice_id",
            "expect_column_values_to_be_unique",
            int((~df.duplicated(subset=["invoice_id"], keep=False)).sum()),
            True,
        ),
        (
            "Validity",
            "quantity",
            "expect_column_values_to_be_between(min=1)",
            int((df["quantity"] >= 1).sum()),
            True,
        ),
        (
            "Validity",
            "price",
            "expect_column_values_to_be_between(min=0.01)",
            int((df["price"] >= 0.01).sum()),
            True,
        ),
        (
            "Validity",
            "product",
            "expect_column_values_to_be_in_set",
            int(df["product"].isin(PRODUCTS).sum()),
            True,
        ),
        (
            "Accuracy",
            "total_revenue",
            "custom_total_revenue_equals_quantity_times_price",
            int(((df["total_revenue"] - calculated_revenue).abs() <= 0.01).sum()),
            True,
        ),
        (
            "Consistency",
            "country",
            "expect_column_values_to_be_in_set",
            int(df["country"].isin(COUNTRIES).sum()),
            True,
        ),
        (
            "Timeliness",
            "invoice_date",
            "expect_column_values_to_match_regex(YYYY-MM-DD)",
            int(df["invoice_date"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$", na=False).sum()),
            True,
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
            True,
        ),
        (
            "Timeliness",
            "invoice_date",
            "expect_invoice_date_to_be_parseable_datetime_no_nulls",
            int(parsed_invoice_dates.notna().sum()),
            False,
        ),
        (
            "Timeliness",
            "month",
            "expect_column_values_to_be_between(month,1,12)",
            int(df["month"].between(1, 12).sum()),
            False,
        ),
        (
            "Validity",
            "revenue_bin",
            "expect_column_values_to_be_in_set(revenue_bin)",
            int(df["revenue_bin"].isin(REVENUE_BIN_LABELS).sum()),
            False,
        ),
        (
            "Validity",
            "total_revenue",
            "expect_column_values_to_be_between(total_revenue,min=0.01)",
            int((df["total_revenue"] > 0).sum()),
            False,
        ),
        (
            "Uniqueness",
            "invoice_id",
            "expect_column_values_to_not_be_null(invoice_id)",
            int(df["invoice_id"].notna().sum()),
            False,
        ),
    ]

    rows = []
    for dimension, column, expectation, pass_count, in_raw_suite in checks:
        pass_pct = round(pass_count / total_rows * 100, 2)
        rows.append(
            {
                "dimension": dimension,
                "column": column,
                "expectation": expectation,
                "pass_count": int(pass_count),
                "unexpected_count": int(total_rows - pass_count),
                "total_rows": int(total_rows),
                "pass_pct": pass_pct,
                "fail_pct": round(100 - pass_pct, 2),
                "in_raw_suite": in_raw_suite,
                "observed_result": "PASS" if pass_pct == 100 else "FAIL",
            }
        )
    return pd.DataFrame(rows)


def build_comparison_table(
    raw_summary: pd.DataFrame,
    output_summary: pd.DataFrame,
) -> pd.DataFrame:
    raw_lookup = {
        (row["expectation"], row["column"]): row["pass_pct"]
        for _, row in raw_summary.iterrows()
    }

    rows = []
    for _, row in output_summary.iterrows():
        raw_pass_pct = raw_lookup.get((row["expectation"], row["column"]), "N/A")
        clean_pass_pct = row["pass_pct"]

        if raw_pass_pct == "N/A":
            status = "NEW_OK" if clean_pass_pct == 100 else "NEW_FAIL"
        elif raw_pass_pct < 100 and clean_pass_pct == 100:
            status = "RESOLVED"
        elif raw_pass_pct == 100 and clean_pass_pct == 100:
            status = "MAINTAINED"
        elif clean_pass_pct > raw_pass_pct:
            status = "IMPROVED"
        else:
            status = "FAILING"

        rows.append(
            {
                "dimension": row["dimension"],
                "column": row["column"],
                "expectation": row["expectation"],
                "raw_pass_pct": raw_pass_pct,
                "clean_pass_pct": clean_pass_pct,
                "status": status,
            }
        )

    return pd.DataFrame(rows)


def dataframe_to_markdown(df: pd.DataFrame) -> str:
    headers = [str(col) for col in df.columns]
    rows = [headers, ["---"] * len(headers)]
    for _, row in df.iterrows():
        rows.append([str(value) for value in row.tolist()])
    return "\n".join("| " + " | ".join(row) + " |" for row in rows)


def save_output_validation_artifacts(
    comparison_df: pd.DataFrame,
    output_summary: pd.DataFrame,
    suite_definition: dict,
    input_dq_score: float,
    output_dq_score: float,
) -> None:
    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    gx_expectations_dir = Path("gx/expectations")
    gx_expectations_dir.mkdir(parents=True, exist_ok=True)

    comparison_csv = reports_dir / "task_f_output_validation_comparison.csv"
    comparison_md = reports_dir / "task_f_output_validation_report.md"
    comparison_json = reports_dir / "task_f_output_validation_comparison.json"
    dq_json = reports_dir / "task_f_dq_scores.json"
    suite_json = gx_expectations_dir / "clean_data_suite.json"

    comparison_df.to_csv(comparison_csv, index=False)
    comparison_json.write_text(
        json.dumps(comparison_df.to_dict(orient="records"), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    dq_json.write_text(
        json.dumps(
            {
                "input_dq_score": input_dq_score,
                "output_dq_score": output_dq_score,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    suite_json.write_text(json.dumps(suite_definition, indent=2), encoding="utf-8")

    comparison_md.write_text(
        "\n".join(
            [
                "# Task F - Output Validation",
                "",
                "## Comparison Table",
                dataframe_to_markdown(comparison_df),
                "",
                "## DQ Scores",
                f"- Data Quality Score (input): {input_dq_score:.2f}%",
                f"- Data Quality Score (output): {output_dq_score:.2f}%",
            ]
        ),
        encoding="utf-8",
    )

    logger.info("Saved Task F comparison table to %s", comparison_csv)
    logger.info("Saved Task F report to %s", comparison_md)
    logger.info("Saved Task F DQ scores to %s", dq_json)
    logger.info("Saved updated clean expectation suite to %s", suite_json)


def try_build_ge_artifacts(filepath: str) -> None:
    try:
        import great_expectations as gx
    except ModuleNotFoundError:
        logger.warning(
            "Great Expectations is not installed in the current interpreter. "
            "Skipping Data Docs generation for Task F."
        )
        return

    df = pd.read_csv(filepath)
    context = gx.get_context(mode="file")
    data_source_name = "retail_clean_source"
    data_asset_name = "retail_clean_asset"
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
        "Great Expectations is available. Output datasource '%s' and asset '%s' are ready.",
        data_source_name,
        data_asset_name,
    )
    logger.info(
        "Data Docs can be built from the GE context using the batch definition '%s'.",
        batch_definition.name,
    )


def validate_clean_data(
    filepath: str,
    raw_input_path: str = "data/raw/retail_etl_dataset.csv",
) -> pd.DataFrame:
    logger.info("Computing Task F output validation summary for %s", filepath)

    raw_summary_path = Path("reports/task_b_input_validation_summary.csv")
    if raw_summary_path.exists():
        raw_summary = pd.read_csv(raw_summary_path)
    else:
        raw_summary = compute_input_validation_summary(raw_input_path)

    transformed_df = pd.read_csv(filepath)
    output_summary = evaluate_output_expectations(transformed_df)
    comparison_df = build_comparison_table(raw_summary, output_summary)

    input_dq_score = round((raw_summary["observed_result"] == "PASS").mean() * 100, 2)
    output_dq_score = round((output_summary["observed_result"] == "PASS").mean() * 100, 2)

    suite_definition = build_output_suite_definition()
    save_output_validation_artifacts(
        comparison_df=comparison_df,
        output_summary=output_summary,
        suite_definition=suite_definition,
        input_dq_score=input_dq_score,
        output_dq_score=output_dq_score,
    )
    try_build_ge_artifacts(filepath)

    failing_output = output_summary.loc[output_summary["observed_result"] != "PASS", "expectation"]
    if not failing_output.empty:
        raise ValueError(
            "Task F failed because these output expectations did not pass: "
            + ", ".join(failing_output.tolist())
        )

    logger.info("Task F comparison summary:\n%s", comparison_df.to_string(index=False))
    logger.info("Data Quality Score (input): %.2f%%", input_dq_score)
    logger.info("Data Quality Score (output): %.2f%%", output_dq_score)
    return comparison_df


if __name__ == "__main__":
    validate_clean_data("data/processed/retail_transformed.csv")
