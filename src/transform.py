import json
from pathlib import Path

import pandas as pd

from utils import setup_logger

logger = setup_logger("transform")

COUNTRY_LOOKUP = {
    "co": "Colombia",
    "colombia": "Colombia",
    "ecuador": "Ecuador",
    "peru": "Peru",
    "chile": "Chile",
}
REVENUE_BIN_LABELS = ["Low", "Medium", "High"]


def dataframe_to_markdown(df: pd.DataFrame) -> str:
    headers = [str(col) for col in df.columns]
    rows = [headers, ["---"] * len(headers)]
    for _, row in df.iterrows():
        rows.append([str(value) for value in row.tolist()])
    return "\n".join("| " + " | ".join(row) + " |" for row in rows)


def save_transformation_artifacts(
    steps_df: pd.DataFrame,
    summary_payload: dict,
    reports_dir: str,
) -> None:
    reports_path = Path(reports_dir)
    reports_path.mkdir(parents=True, exist_ok=True)

    steps_csv = reports_path / "task_e_transformation_steps.csv"
    steps_md = reports_path / "task_e_transformation_steps.md"
    steps_json = reports_path / "task_e_transformation_steps.json"
    summary_json = reports_path / "task_e_transformation_summary.json"
    summary_md = reports_path / "task_e_transformation_summary.md"

    steps_df.to_csv(steps_csv, index=False)
    steps_json.write_text(
        json.dumps(steps_df.to_dict(orient="records"), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    summary_json.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

    steps_md.write_text(
        "\n".join(
            [
                "# Task E - Transformation Steps",
                "",
                dataframe_to_markdown(steps_df),
            ]
        ),
        encoding="utf-8",
    )
    summary_md.write_text(
        "\n".join(
            [
                "# Task E - Transformation Summary",
                "",
                f"- Filas antes de transformación: {summary_payload['rows_before']}",
                f"- Filas después de transformación: {summary_payload['rows_after']}",
                f"- Diferencia de filas: {summary_payload['row_delta']}",
                f"- Valores finales de `country`: {', '.join(summary_payload['country_values'])}",
                f"- Valores finales de `revenue_bin`: {', '.join(summary_payload['revenue_bin_values'])}",
            ]
        ),
        encoding="utf-8",
    )

    logger.info("Saved Task E transformation steps to %s", steps_csv)
    logger.info("Saved Task E transformation summary to %s", summary_json)


def transform_data(
    input_path: str,
    output_path: str,
    reports_dir: str = "reports",
) -> pd.DataFrame:
    logger.info("Loading cleaned data for transformation from %s", input_path)
    df = pd.read_csv(input_path)
    rows_before = len(df)

    # 1. Standardize country values to the canonical set requested by the lab.
    logger.info("Standardizing 'country' column...")
    raw_country_values = sorted(df["country"].dropna().astype(str).unique().tolist())
    df["country"] = (
        df["country"]
        .astype(str)
        .str.strip()
        .str.casefold()
        .map(COUNTRY_LOOKUP)
    )
    if df["country"].isna().any():
        raise ValueError("Transformation found unmapped country values after standardization.")

    # 2. Parse invoice_date without dropping rows. Invalid strings should have been removed in cleaning.
    logger.info("Parsing 'invoice_date' to datetime...")
    parsed_dates = pd.to_datetime(
        df["invoice_date"],
        format="mixed",
        errors="coerce",
        dayfirst=True,
    )
    invalid_dates = int(parsed_dates.isna().sum())
    if invalid_dates > 0:
        raise ValueError(
            f"Transformation cannot continue because {invalid_dates} invoice_date values are invalid after cleaning."
        )
    df["invoice_date"] = parsed_dates

    logger.info("Extracting year, month, day_of_week...")
    df["year"] = df["invoice_date"].dt.year
    df["month"] = df["invoice_date"].dt.month
    df["day_of_week"] = df["invoice_date"].dt.day_name()

    # 3. Cast customer_id to nullable integer. Sentinel fill is available if nulls reappear in future runs.
    logger.info("Casting 'customer_id' to Int64...")
    customer_nulls_before_cast = int(df["customer_id"].isna().sum())
    if customer_nulls_before_cast > 0:
        df["customer_id"] = df["customer_id"].fillna(-1)
    df["customer_id"] = df["customer_id"].astype("Int64")

    # 4. Normalize product casing and whitespace.
    logger.info("Normalizing 'product' column...")
    raw_product_values = sorted(df["product"].dropna().astype(str).unique().tolist())
    df["product"] = df["product"].astype(str).str.strip().str.title()

    # 5. Feature engineering: quantile-based revenue bins.
    logger.info("Creating 'revenue_bin' with quantile-based bins...")
    df["revenue_bin"] = pd.qcut(
        df["total_revenue"].rank(method="first"),
        q=3,
        labels=REVENUE_BIN_LABELS,
    )

    rows_after = len(df)
    if rows_after != rows_before:
        raise ValueError(
            f"Transformation changed row count from {rows_before} to {rows_after}; this violates Task E."
        )

    # Save as string representation for downstream CSV consumption.
    df["invoice_date"] = df["invoice_date"].dt.strftime("%Y-%m-%d")

    transformed_country_values = sorted(df["country"].dropna().unique().tolist())
    transformed_product_values = sorted(df["product"].dropna().unique().tolist())
    revenue_bin_values = sorted(df["revenue_bin"].astype(str).dropna().unique().tolist())

    steps_df = pd.DataFrame(
        [
            {
                "Transformation Step": "Standardize country",
                "Applied Change": "Mapear variantes y códigos a {Colombia, Ecuador, Peru, Chile}.",
                "Rationale": "Evita fragmentación del análisis regional y soporta BO-3.",
            },
            {
                "Transformation Step": "Parse invoice_date",
                "Applied Change": "Convertir fechas mixtas a datetime estándar y luego guardar en formato YYYY-MM-DD.",
                "Rationale": "Permite análisis temporales confiables y soporta BO-2.",
            },
            {
                "Transformation Step": "Extract temporal attributes",
                "Applied Change": "Crear columnas year, month y day_of_week.",
                "Rationale": "Facilita tendencias mensuales y análisis por día de la semana.",
            },
            {
                "Transformation Step": "Cast customer_id",
                "Applied Change": "Convertir customer_id de float64 a Int64.",
                "Rationale": "Alinea el tipo de dato con el modelado dimensional.",
            },
            {
                "Transformation Step": "Normalize product",
                "Applied Change": "Aplicar strip y title case a los nombres de producto.",
                "Rationale": "Garantiza uniformidad semántica para BO-3.",
            },
            {
                "Transformation Step": "Create revenue_bin",
                "Applied Change": "Crear la variable categórica Low / Medium / High con cuantiles.",
                "Rationale": "Agrega una característica analítica útil para segmentación de ingresos.",
            },
        ]
    )

    summary_payload = {
        "rows_before": int(rows_before),
        "rows_after": int(rows_after),
        "row_delta": int(rows_after - rows_before),
        "raw_country_values": raw_country_values,
        "transformed_country_values": transformed_country_values,
        "raw_product_values": raw_product_values,
        "transformed_product_values": transformed_product_values,
        "country_values": transformed_country_values,
        "revenue_bin_values": revenue_bin_values,
        "customer_nulls_before_cast": customer_nulls_before_cast,
    }
    save_transformation_artifacts(steps_df, summary_payload, reports_dir)

    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info("Transformed dataset shape: %s", df.shape)
    logger.info("Transformed dataset saved to %s", output_path)
    return df


if __name__ == "__main__":
    transform_data("data/processed/retail_clean.csv", "data/processed/retail_transformed.csv")
