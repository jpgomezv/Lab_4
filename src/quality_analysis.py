import json
from pathlib import Path

import pandas as pd

from utils import setup_logger

logger = setup_logger("quality_analysis")

NULL_LIKE_DATE_VALUES = ["", "N/A", "NULL", "NaN"]
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


def dataframe_to_markdown(df: pd.DataFrame) -> str:
    headers = [str(col) for col in df.columns]
    rows = [headers, ["---"] * len(headers)]
    for _, row in df.iterrows():
        rows.append([str(value) for value in row.tolist()])
    return "\n".join("| " + " | ".join(row) + " |" for row in rows)


def build_c1_issues_table(raw_data_path: str) -> pd.DataFrame:
    df_raw = pd.read_csv(raw_data_path, keep_default_na=False)
    df_default = pd.read_csv(raw_data_path)

    invoice_counts = df_raw["invoice_id"].value_counts()
    duplicate_invoice_id = int(invoice_counts[invoice_counts > 1].index[0])
    duplicate_invoice_count = int(invoice_counts.loc[duplicate_invoice_id])

    null_customer_count = int(df_default["customer_id"].isna().sum())

    negative_quantity = df_raw[pd.to_numeric(df_raw["quantity"], errors="coerce") < 1]
    negative_quantity_row = int(negative_quantity.index[0] + 2)
    negative_quantity_value = int(negative_quantity.iloc[0]["quantity"])

    negative_price = df_raw[pd.to_numeric(df_raw["price"], errors="coerce") < 0.01]
    negative_price_row = int(negative_price.index[0] + 2)
    negative_price_value = float(negative_price.iloc[0]["price"])

    calculated_revenue = (
        pd.to_numeric(df_raw["quantity"], errors="coerce")
        * pd.to_numeric(df_raw["price"], errors="coerce")
    )
    revenue_mismatch = df_raw[
        (
            pd.to_numeric(df_raw["total_revenue"], errors="coerce")
            - calculated_revenue
        ).abs()
        > 0.01
    ]
    mismatch_row = int(revenue_mismatch.index[0] + 2)
    mismatch_total = float(revenue_mismatch.iloc[0]["total_revenue"])
    mismatch_expected = float(calculated_revenue.loc[revenue_mismatch.index[0]])

    slash_format_dates = sorted(
        df_raw.loc[
            df_raw["invoice_date"].astype(str).str.match(r"^\d{4}/\d{2}/\d{2}$", na=False),
            "invoice_date",
        ]
        .astype(str)
        .unique()
        .tolist()
    )
    dash_format_dates = sorted(
        df_raw.loc[
            df_raw["invoice_date"].astype(str).str.match(r"^\d{2}-\d{2}-\d{4}$", na=False)
            & df_raw["invoice_date"].astype(str).str.endswith("2023"),
            "invoice_date",
        ]
        .astype(str)
        .unique()
        .tolist()
    )

    parsed_dates = pd.to_datetime(
        df_raw["invoice_date"].replace({value: pd.NA for value in NULL_LIKE_DATE_VALUES}),
        errors="coerce",
        format="mixed",
        dayfirst=True,
    )
    future_dates = df_raw.loc[parsed_dates > pd.Timestamp("2023-12-31"), "invoice_date"]
    future_date_example = str(future_dates.iloc[0])

    country_examples = "'Colombia', 'colombia', 'CO'; 'Ecuador', 'ecuador'"
    mixed_date_example_text = ", ".join(
        f"'{value}'"
        for value in [slash_format_dates[0], dash_format_dates[0]]
    )

    rows = [
        {
            "Column": "invoice_id",
            "Issue": "Duplicate IDs",
            "Example": f"{duplicate_invoice_id} aparece {duplicate_invoice_count} veces",
            "Dimension": "Uniqueness",
            "Business Impact": "Los ingresos pueden contarse dos o más veces en los KPIs de BO-1.",
        },
        {
            "Column": "customer_id",
            "Issue": "NULL values",
            "Example": f"NaN en {null_customer_count} filas",
            "Dimension": "Completeness",
            "Business Impact": "Impide vincular ventas con clientes y afecta los análisis de BO-3.",
        },
        {
            "Column": "quantity",
            "Issue": "Negative values",
            "Example": f"{negative_quantity_value} en la fila {negative_quantity_row}",
            "Dimension": "Validity",
            "Business Impact": "Cantidades negativas distorsionan el cálculo de ingresos y los indicadores de BO-1.",
        },
        {
            "Column": "price",
            "Issue": "Negative values",
            "Example": f"{negative_price_value:.2f} en la fila {negative_price_row}",
            "Dimension": "Validity",
            "Business Impact": "Precios negativos alteran ingresos totales y ticket promedio en BO-1.",
        },
        {
            "Column": "total_revenue",
            "Issue": "Calculation mismatch",
            "Example": (
                f"fila {mismatch_row}: total={mismatch_total:.2f} "
                f"vs quantity×price={mismatch_expected:.2f}"
            ),
            "Dimension": "Accuracy",
            "Business Impact": "Los reportes financieros pueden sobreestimar o subestimar ventas en BO-1.",
        },
        {
            "Column": "country",
            "Issue": "Inconsistent formats",
            "Example": country_examples,
            "Dimension": "Consistency",
            "Business Impact": "Fragmenta el análisis regional y afecta la comparación por país en BO-3.",
        },
        {
            "Column": "invoice_date",
            "Issue": "Mixed date formats",
            "Example": mixed_date_example_text,
            "Dimension": "Timeliness",
            "Business Impact": "Dificulta agregaciones temporales correctas para tendencias mensuales y BO-2.",
        },
        {
            "Column": "invoice_date",
            "Issue": "Null-like and future values",
            "Example": f"'NULL', '' y '{future_date_example}'",
            "Dimension": "Timeliness",
            "Business Impact": "Introduce vacíos y fechas inválidas que contaminan análisis de series de tiempo en BO-2.",
        },
    ]

    return pd.DataFrame(rows)


def build_c2_policy_table() -> pd.DataFrame:
    rows = [
        {
            "Policy ID": "P-01",
            "Policy Statement": "invoice_id debe ser único en todo el dataset.",
            "GE Expectation": "expect_column_values_to_be_unique('invoice_id')",
            "Severity": "Critical",
            "Addresses (BO)": "BO-1, BO-4",
        },
        {
            "Policy ID": "P-02",
            "Policy Statement": "quantity debe ser un entero positivo mayor o igual a 1.",
            "GE Expectation": "expect_column_values_to_be_between('quantity', min_value=1)",
            "Severity": "Critical",
            "Addresses (BO)": "BO-1",
        },
        {
            "Policy ID": "P-03",
            "Policy Statement": "price debe ser mayor que cero.",
            "GE Expectation": "expect_column_values_to_be_between('price', min_value=0.01)",
            "Severity": "Critical",
            "Addresses (BO)": "BO-1",
        },
        {
            "Policy ID": "P-04",
            "Policy Statement": "total_revenue debe ser igual a quantity × price con tolerancia de ±0.01.",
            "GE Expectation": (
                "custom_total_revenue_equals_quantity_times_price("
                "quantity_column='quantity', price_column='price', tolerance=0.01)"
            ),
            "Severity": "High",
            "Addresses (BO)": "BO-1, BO-4",
        },
        {
            "Policy ID": "P-05",
            "Policy Statement": (
                "country debe pertenecer al conjunto {Colombia, Ecuador, Peru, Chile} "
                "en formato estandarizado."
            ),
            "GE Expectation": (
                "expect_column_values_to_be_in_set('country', "
                f"{COUNTRIES})"
            ),
            "Severity": "Moderate",
            "Addresses (BO)": "BO-3",
        },
        {
            "Policy ID": "P-06",
            "Policy Statement": (
                "invoice_date debe seguir el formato YYYY-MM-DD y estar dentro del rango "
                "2023-01-01 a 2023-12-31."
            ),
            "GE Expectation": (
                "expect_column_values_to_match_regex('invoice_date', '^\\\\d{4}-\\\\d{2}-\\\\d{2}$') "
                "+ expect_column_values_to_be_between('invoice_date', "
                "'2023-01-01', '2023-12-31')"
            ),
            "Severity": "High",
            "Addresses (BO)": "BO-2, BO-4",
        },
        {
            "Policy ID": "P-07",
            "Policy Statement": "customer_id no debe contener valores nulos.",
            "GE Expectation": "expect_column_values_to_not_be_null('customer_id')",
            "Severity": "Moderate",
            "Addresses (BO)": "BO-3",
        },
        {
            "Policy ID": "P-08",
            "Policy Statement": "product debe pertenecer al catálogo oficial de productos permitido.",
            "GE Expectation": (
                "expect_column_values_to_be_in_set('product', "
                f"{PRODUCTS})"
            ),
            "Severity": "Low",
            "Addresses (BO)": "BO-3",
        },
    ]
    return pd.DataFrame(rows)


def save_quality_artifacts(
    issue_table: pd.DataFrame,
    policy_table: pd.DataFrame,
    output_path: str,
) -> None:
    reports_dir = Path(output_path).parent
    reports_dir.mkdir(parents=True, exist_ok=True)

    c1_csv_path = reports_dir / "task_c1_data_quality_issues.csv"
    c1_md_path = reports_dir / "task_c1_data_quality_issues.md"
    c1_json_path = reports_dir / "task_c1_data_quality_issues.json"
    c2_csv_path = reports_dir / "task_c2_policy_proposal.csv"
    c2_md_path = reports_dir / "task_c2_policy_proposal.md"
    c2_json_path = reports_dir / "task_c2_policy_proposal.json"

    issue_table.to_csv(c1_csv_path, index=False)
    policy_table.to_csv(c2_csv_path, index=False)
    c1_json_path.write_text(
        json.dumps(issue_table.to_dict(orient="records"), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    c2_json_path.write_text(
        json.dumps(policy_table.to_dict(orient="records"), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    c1_md_lines = [
        "# Task C.1 - Data Quality Issues Table",
        "",
        dataframe_to_markdown(issue_table),
    ]
    c1_md_path.write_text("\n".join(c1_md_lines), encoding="utf-8")

    c2_md_lines = [
        "# Task C.2 - Data Quality Policy Proposal",
        "",
        dataframe_to_markdown(policy_table),
    ]
    c2_md_path.write_text("\n".join(c2_md_lines), encoding="utf-8")

    quality_report_lines = [
        "# Data Quality Report",
        "",
        "## 1. Data Quality Issues Table",
        dataframe_to_markdown(issue_table),
        "",
        "## 2. Data Quality Policy Proposal",
        dataframe_to_markdown(policy_table),
    ]
    Path(output_path).write_text("\n".join(quality_report_lines), encoding="utf-8")

    logger.info("Saved Task C.1 CSV table to %s", c1_csv_path)
    logger.info("Saved Task C.1 markdown table to %s", c1_md_path)
    logger.info("Saved Task C.1 JSON table to %s", c1_json_path)
    logger.info("Saved Task C.2 CSV table to %s", c2_csv_path)
    logger.info("Saved Task C.2 markdown table to %s", c2_md_path)
    logger.info("Saved Task C.2 JSON table to %s", c2_json_path)
    logger.info("Updated consolidated quality report at %s", output_path)


def generate_quality_report(
    output_path: str,
    raw_data_path: str = "data/raw/retail_etl_dataset.csv",
) -> dict[str, pd.DataFrame]:
    """Generates the Task C.1 and C.2 deliverables."""
    logger.info("Generating C.1 data quality issues table from %s", raw_data_path)
    issue_table = build_c1_issues_table(raw_data_path)
    policy_table = build_c2_policy_table()
    save_quality_artifacts(issue_table, policy_table, output_path)
    logger.info("Generated C.1 data quality issues table:\n%s", issue_table.to_string(index=False))
    logger.info("Generated C.2 policy proposal table:\n%s", policy_table.to_string(index=False))
    return {"issues_table": issue_table, "policy_table": policy_table}


if __name__ == "__main__":
    generate_quality_report("reports/quality_report.md")
