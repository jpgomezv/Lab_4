import json
from pathlib import Path

import numpy as np
import pandas as pd

from utils import setup_logger

logger = setup_logger('extract_profile')

NULL_LIKE_DATE_VALUES = ["", "N/A", "NULL", "NaN"]
DATE_FORMAT_PATTERNS = {
    "YYYY-MM-DD": r"^\d{4}-\d{2}-\d{2}$",
    "YYYY/MM/DD": r"^\d{4}/\d{2}/\d{2}$",
    "DD-MM-YYYY": r"^\d{2}-\d{2}-\d{4}$",
}

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

def register_in_memory_datasource(df: pd.DataFrame) -> dict[str, str]:
    """Registers the raw dataframe as an in-memory Pandas datasource in Great Expectations."""
    try:
        import great_expectations as gx
    except ModuleNotFoundError:
        logger.warning(
            "Great Expectations is not installed in the current interpreter. "
            "Skipping datasource registration for this run."
        )
        return {
            "datasource": "not_registered",
            "asset": "not_registered",
            "batch_definition": "not_registered",
            "status": "great_expectations not installed in current interpreter",
        }

    context = gx.get_context(mode="file")
    data_source_name = "retail_profile_source"
    data_asset_name = "retail_profile_asset"
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
        data_asset.add_batch_definition_whole_dataframe(batch_definition_name)
    except ValueError:
        pass

    logger.info(
        "Registered raw dataframe as Great Expectations datasource '%s' / asset '%s'.",
        data_source_name,
        data_asset_name,
    )
    return {
        "datasource": data_source_name,
        "asset": data_asset_name,
        "batch_definition": batch_definition_name,
        "status": "registered",
    }

def dataframe_to_markdown(df: pd.DataFrame) -> str:
    """Converts a dataframe to a simple Markdown table without external dependencies."""
    headers = [str(col) for col in df.columns]
    rows = [headers, ["---"] * len(headers)]
    for _, row in df.iterrows():
        rows.append([str(value) for value in row.tolist()])
    return "\n".join("| " + " | ".join(row) + " |" for row in rows)

def save_profile_artifacts(
    output_dir: str,
    column_summary: pd.DataFrame,
    numeric_stats: pd.DataFrame,
    metrics: dict,
) -> None:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    summary_csv_path = output_path / "task_a_profiling_summary.csv"
    report_md_path = output_path / "task_a_profiling_report.md"
    metrics_json_path = output_path / "task_a_profiling_metrics.json"

    column_summary.to_csv(summary_csv_path, index=False)

    markdown_sections = [
        "# Task A - Extract & Profiling",
        "",
        "## Profiling Summary Table",
        dataframe_to_markdown(column_summary),
        "",
        "## Numeric Statistics",
        dataframe_to_markdown(
            numeric_stats.reset_index().rename(columns={"index": "metric"})
        ),
        "",
        "## Global Profiling Metrics",
        f"- Shape: {metrics['shape'][0]} rows x {metrics['shape'][1]} columns",
        f"- Memory usage (MB): {metrics['memory_usage_mb']:.4f}",
        f"- Duplicate invoice rows (rows in duplicate groups): {metrics['duplicate_invoice_rows']}",
        f"- Extra duplicate rows (removable duplicates): {metrics['extra_duplicate_rows']}",
        f"- Unique invoice_ids with duplicates: {metrics['duplicate_invoice_ids']}",
        f"- Revenue mismatches (|total_revenue - quantity*price| > 0.01): {metrics['revenue_mismatch_rows']}",
        "",
        "## invoice_date Quality Detail",
        f"- YYYY-MM-DD: {metrics['date_format_counts']['YYYY-MM-DD']}",
        f"- YYYY/MM/DD: {metrics['date_format_counts']['YYYY/MM/DD']}",
        f"- DD-MM-YYYY: {metrics['date_format_counts']['DD-MM-YYYY']}",
        f"- Null-like date strings: {metrics['null_like_date_count']}",
        f"- Other invalid date strings: {metrics['other_invalid_date_count']}",
        f"- Future dates (> 2023-12-31): {metrics['future_date_count']}",
        "",
        "## Great Expectations Registration",
        f"- Status: {metrics['ge_registration']['status']}",
        f"- Datasource: {metrics['ge_registration']['datasource']}",
        f"- Asset: {metrics['ge_registration']['asset']}",
        f"- Batch definition: {metrics['ge_registration']['batch_definition']}",
    ]

    report_md_path.write_text("\n".join(markdown_sections), encoding="utf-8")
    metrics_json_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    logger.info("Saved profiling summary table to %s", summary_csv_path)
    logger.info("Saved profiling markdown report to %s", report_md_path)
    logger.info("Saved profiling metrics to %s", metrics_json_path)

def profile_data(
    df: pd.DataFrame,
    source_path: str,
    output_dir: str = "reports",
) -> dict:
    """Profiles the raw dataset, registers a GE datasource, and saves report-ready artifacts."""
    logger.info("Profiling dataset...")
    raw_df = pd.read_csv(source_path, keep_default_na=False)
    ge_registration = register_in_memory_datasource(df)

    memory_usage_mb = df.memory_usage(deep=True).sum() / 1024**2
    missing = df.isnull().sum()
    missing_pct = (missing / len(df)) * 100

    column_summary = pd.DataFrame(
        {
            "column": df.columns,
            "dtype": [str(df[col].dtype) for col in df.columns],
            "missing_count": [int(missing[col]) for col in df.columns],
            "missing_pct": [round(float(missing_pct[col]), 2) for col in df.columns],
            "unique_values": [int(df[col].nunique(dropna=True)) for col in df.columns],
        }
    )

    logger.info("Shape: %s rows, %s columns", df.shape[0], df.shape[1])
    logger.info("Data types:\n%s", df.dtypes)
    logger.info("Memory usage: %.2f MB", memory_usage_mb)
    logger.info("\nProfiling summary table:\n%s", column_summary.to_string(index=False))

    numeric_cols = ["quantity", "price", "total_revenue"]
    numeric_stats = (
        df[numeric_cols]
        .agg(["min", "max", "mean", "median", "std"])
        .round(4)
    )
    logger.info("\nDescriptive statistics:\n%s", numeric_stats)

    duplicate_invoice_rows = int(df.duplicated(subset=["invoice_id"], keep=False).sum())
    extra_duplicate_rows = int(df.duplicated(subset=["invoice_id"], keep="first").sum())
    duplicate_invoice_ids = int(
        df.loc[df.duplicated(subset=["invoice_id"], keep=False), "invoice_id"].nunique()
    )
    logger.info("Duplicate invoice rows (rows in duplicate groups): %s", duplicate_invoice_rows)
    logger.info("Extra duplicate rows (removable duplicates): %s", extra_duplicate_rows)
    logger.info("Unique invoice_ids with duplicates: %s", duplicate_invoice_ids)

    calculated_revenue = df["quantity"] * df["price"]
    revenue_mismatch_rows = int(
        ((df["total_revenue"] - calculated_revenue).abs() > 0.01).fillna(False).sum()
    )
    logger.info("Rows where total_revenue != quantity * price: %s", revenue_mismatch_rows)

    raw_invoice_dates = raw_df["invoice_date"].astype("string")
    date_format_counts = {
        name: int(raw_invoice_dates.str.match(pattern, na=False).sum())
        for name, pattern in DATE_FORMAT_PATTERNS.items()
    }
    null_like_date_count = int(raw_invoice_dates.isin(NULL_LIKE_DATE_VALUES).sum())
    matches_known_format = pd.Series(False, index=raw_invoice_dates.index)
    for pattern in DATE_FORMAT_PATTERNS.values():
        matches_known_format = matches_known_format | raw_invoice_dates.str.match(
            pattern,
            na=False,
        )
    other_invalid_date_count = int(
        (~matches_known_format & ~raw_invoice_dates.isin(NULL_LIKE_DATE_VALUES)).sum()
    )
    parsed_dates = pd.to_datetime(
        raw_invoice_dates.replace({value: pd.NA for value in NULL_LIKE_DATE_VALUES}),
        errors="coerce",
        format="mixed",
        dayfirst=True,
    )
    future_date_count = int((parsed_dates > pd.Timestamp("2023-12-31")).sum())

    logger.info("\nDistribution of invoice_date formats:")
    for name, count in date_format_counts.items():
        logger.info("  %s: %s", name, count)
    logger.info("  Null-like strings: %s", null_like_date_count)
    logger.info("  Other invalid strings: %s", other_invalid_date_count)
    logger.info("  Future dates (> 2023-12-31): %s", future_date_count)

    logger.info("\nCardinality of categorical columns:")
    logger.info("  product: %s unique values", df["product"].nunique(dropna=True))
    logger.info("  country: %s unique values", df["country"].nunique(dropna=True))
    logger.info(
        "  invoice_date raw labels: %s unique values",
        raw_invoice_dates.nunique(dropna=True),
    )

    metrics = {
        "shape": [int(df.shape[0]), int(df.shape[1])],
        "memory_usage_mb": round(float(memory_usage_mb), 4),
        "duplicate_invoice_rows": duplicate_invoice_rows,
        "extra_duplicate_rows": extra_duplicate_rows,
        "duplicate_invoice_ids": duplicate_invoice_ids,
        "revenue_mismatch_rows": revenue_mismatch_rows,
        "date_format_counts": date_format_counts,
        "null_like_date_count": null_like_date_count,
        "other_invalid_date_count": other_invalid_date_count,
        "future_date_count": future_date_count,
        "ge_registration": ge_registration,
    }
    save_profile_artifacts(output_dir, column_summary, numeric_stats, metrics)

    logger.info("\nProfiling Complete.")
    return {
        "column_summary": column_summary,
        "numeric_stats": numeric_stats,
        "metrics": metrics,
    }

def main():
    source_path = "data/raw/retail_etl_dataset.csv"
    df = extract_data(source_path)
    profile_data(df, source_path)

if __name__ == "__main__":
    main()
