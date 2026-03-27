import json
from pathlib import Path

import pandas as pd

from utils import setup_logger

logger = setup_logger('clean')

NULL_LIKE_DATE_VALUES = ['N/A', 'NULL', '', 'NaN']


def dataframe_to_markdown(df: pd.DataFrame) -> str:
    headers = [str(col) for col in df.columns]
    rows = [headers, ['---'] * len(headers)]
    for _, row in df.iterrows():
        rows.append([str(value) for value in row.tolist()])
    return "\n".join("| " + " | ".join(row) + " |" for row in rows)


def save_cleaning_artifacts(
    actions_df: pd.DataFrame,
    before_nulls: pd.Series,
    after_nulls: pd.Series,
    initial_count: int,
    final_count: int,
    reports_dir: str,
) -> None:
    reports_path = Path(reports_dir)
    reports_path.mkdir(parents=True, exist_ok=True)

    before_after_df = pd.DataFrame(
        {
            "column": before_nulls.index,
            "nulls_before": before_nulls.values,
            "nulls_after": after_nulls.reindex(before_nulls.index).values,
        }
    )
    summary_payload = {
        "rows_before": int(initial_count),
        "rows_after": int(final_count),
        "rows_dropped_total": int(initial_count - final_count),
        "rows_removed_by_reason": {
            row["Issue"]: int(row["Rows Removed"]) for _, row in actions_df.iterrows()
        },
    }

    actions_csv = reports_path / "task_d_cleaning_actions.csv"
    actions_md = reports_path / "task_d_cleaning_actions.md"
    actions_json = reports_path / "task_d_cleaning_actions.json"
    before_after_csv = reports_path / "task_d_before_after_summary.csv"
    before_after_md = reports_path / "task_d_before_after_summary.md"
    summary_json = reports_path / "task_d_cleaning_summary.json"

    actions_df.to_csv(actions_csv, index=False)
    before_after_df.to_csv(before_after_csv, index=False)
    actions_json.write_text(
        json.dumps(actions_df.to_dict(orient="records"), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    summary_json.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

    actions_md.write_text(
        "\n".join(
            [
                "# Task D - Cleaning Actions",
                "",
                dataframe_to_markdown(actions_df),
            ]
        ),
        encoding="utf-8",
    )
    before_after_md.write_text(
        "\n".join(
            [
                "# Task D - Before and After Summary",
                "",
                f"- Filas antes de limpieza: {initial_count}",
                f"- Filas después de limpieza: {final_count}",
                f"- Total de filas eliminadas: {initial_count - final_count}",
                "",
                dataframe_to_markdown(before_after_df),
            ]
        ),
        encoding="utf-8",
    )

    logger.info("Saved Task D cleaning actions to %s", actions_csv)
    logger.info("Saved Task D before/after summary to %s", before_after_csv)
    logger.info("Saved Task D cleaning summary to %s", summary_json)


def clean_data(input_path: str, output_path: str, reports_dir: str = 'reports'):
    logger.info(f"Loading raw data for cleaning from {input_path}")
    df = pd.read_csv(input_path)
    
    initial_count = len(df)
    before_nulls = df.isnull().sum()
    logger.info(f"Initial row count: {initial_count}")
    logger.info(f"Initial nulls per column:\n{before_nulls}")
    
    # 1. Duplicate invoice_id
    dups_mask = df.duplicated(subset=['invoice_id'], keep='first')
    dups_count = int(dups_mask.sum())
    df = df[~dups_mask]
    logger.info(f"Dropped {dups_count} duplicate invoice_id rows.")

    # 2. Negative quantity
    neg_qty_mask = df['quantity'] < 1
    neg_qty_count = int(neg_qty_mask.sum())
    df = df[~neg_qty_mask]
    logger.info(f"Dropped {neg_qty_count} rows with quantity < 1.")

    # 3. Negative price
    neg_price_mask = df['price'] < 0.01
    neg_price_count = int(neg_price_mask.sum())
    df = df[~neg_price_mask]
    logger.info(f"Dropped {neg_price_count} rows with price < 0.01.")

    # 4. NULL customer_id
    null_cust_mask = df['customer_id'].isnull()
    null_cust_count = int(null_cust_mask.sum())
    df = df[~null_cust_mask]
    logger.info(f"Dropped {null_cust_count} rows with NULL customer_id.")

    # 5. Null-like invoice_date
    null_like_date_mask = df['invoice_date'].isnull() | df['invoice_date'].isin(NULL_LIKE_DATE_VALUES)
    null_like_date_count = int(null_like_date_mask.sum())
    df = df[~null_like_date_mask]
    logger.info(f"Dropped {null_like_date_count} rows with null-like invoice_date.")

    # 6. Invalid invoice_date strings that cannot be parsed into a real date
    parsed_invoice_dates = pd.to_datetime(
        df['invoice_date'],
        errors='coerce',
        format='mixed',
        dayfirst=True,
    )
    invalid_date_mask = parsed_invoice_dates.isna()
    invalid_date_count = int(invalid_date_mask.sum())
    df = df[~invalid_date_mask]
    logger.info(f"Dropped {invalid_date_count} rows with invalid invoice_date format.")

    # 7. Future invoice_date (> 2023)
    # Extract year cleanly
    years = df['invoice_date'].str.extract(r'(20\d{2})')[0].astype(float)
    future_date_mask = years > 2023
    future_date_count = int(future_date_mask.sum())
    df = df[~future_date_mask]
    logger.info(f"Dropped {future_date_count} rows with future invoice_date.")

    # 8. Inaccurate total_revenue
    calc_rev = df['quantity'] * df['price']
    inaccurate_rev_mask = abs(df['total_revenue'] - calc_rev) > 0.01
    # also handle nan in total_revenue if it exists
    inaccurate_rev_mask = inaccurate_rev_mask | df['total_revenue'].isnull()
    inaccurate_rev_count = int(inaccurate_rev_mask.sum())
    df = df[~inaccurate_rev_mask]
    logger.info(f"Dropped {inaccurate_rev_count} rows with inaccurate total_revenue.")

    final_count = len(df)
    after_nulls = df.isnull().sum()
    logger.info(f"Final row count: {final_count} (Dropped {initial_count - final_count} total rows)")
    logger.info(f"Final nulls per column:\n{after_nulls}")

    actions_df = pd.DataFrame(
        [
            {
                "Issue": "Duplicate invoice_id",
                "Cleaning Strategy": "Eliminar duplicados y conservar la primera ocurrencia",
                "Justification": "Los duplicados inflan artificialmente los ingresos y afectan BO-1.",
                "Rows Removed": dups_count,
            },
            {
                "Issue": "Negative quantity",
                "Cleaning Strategy": "Eliminar filas con quantity < 1",
                "Justification": "No es posible interpretar unidades vendidas negativas en un escenario retail.",
                "Rows Removed": neg_qty_count,
            },
            {
                "Issue": "Negative price",
                "Cleaning Strategy": "Eliminar filas con price < 0.01",
                "Justification": "Los precios negativos invalidan los cálculos financieros de BO-1.",
                "Rows Removed": neg_price_count,
            },
            {
                "Issue": "NULL customer_id",
                "Cleaning Strategy": "Eliminar filas con customer_id nulo",
                "Justification": "Sin identificador de cliente no es posible vincular la venta con la dimensión customer para BO-3.",
                "Rows Removed": null_cust_count,
            },
            {
                "Issue": "Null-like invoice_date",
                "Cleaning Strategy": "Eliminar filas con invoice_date nulo o equivalente a nulo",
                "Justification": "Sin fecha válida no es posible ubicar la transacción en análisis temporales de BO-2.",
                "Rows Removed": null_like_date_count,
            },
            {
                "Issue": "Invalid invoice_date format",
                "Cleaning Strategy": "Eliminar filas con fechas no parseables",
                "Justification": "Las fechas inválidas no pueden transformarse ni integrarse correctamente en la dimensión de tiempo.",
                "Rows Removed": invalid_date_count,
            },
            {
                "Issue": "Future invoice_date",
                "Cleaning Strategy": "Eliminar filas con fechas posteriores a 2023-12-31",
                "Justification": "El laboratorio exige transacciones dentro del año 2023; fechas futuras distorsionan tendencias.",
                "Rows Removed": future_date_count,
            },
            {
                "Issue": "Inaccurate total_revenue",
                "Cleaning Strategy": "Eliminar filas donde total_revenue no coincide con quantity × price",
                "Justification": "Las diferencias de cálculo comprometen la integridad financiera de BO-1.",
                "Rows Removed": inaccurate_rev_count,
            },
        ]
    )

    save_cleaning_artifacts(
        actions_df=actions_df,
        before_nulls=before_nulls,
        after_nulls=after_nulls,
        initial_count=initial_count,
        final_count=final_count,
        reports_dir=reports_dir,
    )
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"Cleaned dataset saved to {output_path}")
    return df

if __name__ == "__main__":
    clean_data('data/raw/retail_etl_dataset.csv', 'data/processed/retail_clean.csv')
