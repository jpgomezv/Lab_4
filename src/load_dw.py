import json
import os
import sqlite3
from pathlib import Path

import pandas as pd

from utils import setup_logger

logger = setup_logger("load_dw")


def create_tables(cursor):
    """Creates the star schema tables with explicit foreign keys to enforce integrity."""
    logger.info("Executing DDL to create dimensional and fact tables...")

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id INTEGER PRIMARY KEY,
        full_date TEXT,
        year INTEGER,
        month INTEGER,
        month_name TEXT,
        day_of_week TEXT
    )"""
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_product (
        product_id INTEGER PRIMARY KEY,
        product_name TEXT
    )"""
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_id INTEGER PRIMARY KEY,
        customer_key INTEGER
    )"""
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_location (
        location_id INTEGER PRIMARY KEY,
        country TEXT
    )"""
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS fact_sales (
        sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
        invoice_id INTEGER,
        product_id INTEGER,
        customer_id INTEGER,
        location_id INTEGER,
        date_id INTEGER,
        quantity INTEGER,
        price REAL,
        total_revenue REAL,
        FOREIGN KEY (product_id) REFERENCES dim_product (product_id),
        FOREIGN KEY (customer_id) REFERENCES dim_customer (customer_id),
        FOREIGN KEY (location_id) REFERENCES dim_location (location_id),
        FOREIGN KEY (date_id) REFERENCES dim_date (date_id)
    )"""
    )


def _query_scalar(cursor, sql: str) -> int:
    cursor.execute(sql)
    return int(cursor.fetchone()[0])


def _build_referential_integrity_artifacts(conn, reports_dir: str):
    Path(reports_dir).mkdir(parents=True, exist_ok=True)
    cursor = conn.cursor()

    table_counts = [
        {
            "Table": "dim_product",
            "Rows Loaded": _query_scalar(cursor, "SELECT COUNT(*) FROM dim_product"),
        },
        {
            "Table": "dim_customer",
            "Rows Loaded": _query_scalar(cursor, "SELECT COUNT(*) FROM dim_customer"),
        },
        {
            "Table": "dim_location",
            "Rows Loaded": _query_scalar(cursor, "SELECT COUNT(*) FROM dim_location"),
        },
        {
            "Table": "dim_date",
            "Rows Loaded": _query_scalar(cursor, "SELECT COUNT(*) FROM dim_date"),
        },
        {
            "Table": "fact_sales",
            "Rows Loaded": _query_scalar(cursor, "SELECT COUNT(*) FROM fact_sales"),
        },
    ]

    integrity_checks = [
        {
            "FK Column": "product_id",
            "Source Table": "fact_sales",
            "Referenced Table": "dim_product",
            "Orphan Rows": _query_scalar(
                cursor,
                """
                SELECT COUNT(*)
                FROM fact_sales f
                LEFT JOIN dim_product d ON f.product_id = d.product_id
                WHERE d.product_id IS NULL
                """,
            ),
        },
        {
            "FK Column": "customer_id",
            "Source Table": "fact_sales",
            "Referenced Table": "dim_customer",
            "Orphan Rows": _query_scalar(
                cursor,
                """
                SELECT COUNT(*)
                FROM fact_sales f
                LEFT JOIN dim_customer d ON f.customer_id = d.customer_id
                WHERE d.customer_id IS NULL
                """,
            ),
        },
        {
            "FK Column": "location_id",
            "Source Table": "fact_sales",
            "Referenced Table": "dim_location",
            "Orphan Rows": _query_scalar(
                cursor,
                """
                SELECT COUNT(*)
                FROM fact_sales f
                LEFT JOIN dim_location d ON f.location_id = d.location_id
                WHERE d.location_id IS NULL
                """,
            ),
        },
        {
            "FK Column": "date_id",
            "Source Table": "fact_sales",
            "Referenced Table": "dim_date",
            "Orphan Rows": _query_scalar(
                cursor,
                """
                SELECT COUNT(*)
                FROM fact_sales f
                LEFT JOIN dim_date d ON f.date_id = d.date_id
                WHERE d.date_id IS NULL
                """,
            ),
        },
    ]

    for row in integrity_checks:
        row["Status"] = "PASS" if row["Orphan Rows"] == 0 else "FAIL"

    cursor.execute("PRAGMA foreign_key_check;")
    fk_check_rows = cursor.fetchall()
    pragma_status = "PASS" if not fk_check_rows else "FAIL"

    counts_df = pd.DataFrame(table_counts)
    integrity_df = pd.DataFrame(integrity_checks)

    counts_csv = Path(reports_dir) / "task_h_load_summary.csv"
    integrity_csv = Path(reports_dir) / "task_h_referential_integrity_checks.csv"
    counts_json = Path(reports_dir) / "task_h_load_summary.json"
    integrity_json = Path(reports_dir) / "task_h_referential_integrity_checks.json"
    report_md = Path(reports_dir) / "task_h_referential_integrity_report.md"

    counts_df.to_csv(counts_csv, index=False)
    integrity_df.to_csv(integrity_csv, index=False)
    counts_json.write_text(json.dumps(table_counts, indent=2), encoding="utf-8")
    integrity_json.write_text(
        json.dumps(
            {
                "pragma_foreign_key_check_status": pragma_status,
                "pragma_foreign_key_check_rows": fk_check_rows,
                "checks": integrity_checks,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    lines = [
        "# Task H - Referential Integrity Check Results",
        "",
        "## Load Summary",
        "| Table | Rows Loaded |",
        "| --- | ---: |",
    ]
    for row in table_counts:
        lines.append(f"| `{row['Table']}` | {row['Rows Loaded']} |")

    lines.extend(
        [
            "",
            "## Referential Integrity Checks",
            "| FK Column | Source Table | Referenced Table | Orphan Rows | Status |",
            "| --- | --- | --- | ---: | --- |",
        ]
    )
    for row in integrity_checks:
        lines.append(
            f"| `{row['FK Column']}` | `{row['Source Table']}` | `{row['Referenced Table']}` | "
            f"{row['Orphan Rows']} | {row['Status']} |"
        )

    lines.extend(
        [
            "",
            f"- `PRAGMA foreign_key_check`: {pragma_status}",
            f"- Total rows in `fact_sales`: {_query_scalar(cursor, 'SELECT COUNT(*) FROM fact_sales')}",
            "- Resultado: no se encontraron registros huérfanos en las claves foráneas del esquema estrella."
            if pragma_status == "PASS"
            else "- Resultado: se detectaron violaciones de integridad referencial y deben corregirse antes del análisis.",
        ]
    )
    report_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    logger.info(f"Saved {counts_csv}")
    logger.info(f"Saved {integrity_csv}")
    logger.info(f"Saved {report_md}")


def load_data(db_path: str, data_dir: str, reports_dir: str = "reports"):
    logger.info(f"Connecting to SQLite database at {db_path}...")
    conn = sqlite3.connect(db_path)

    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")

    cursor.execute("DROP TABLE IF EXISTS fact_sales")
    cursor.execute("DROP TABLE IF EXISTS dim_date")
    cursor.execute("DROP TABLE IF EXISTS dim_product")
    cursor.execute("DROP TABLE IF EXISTS dim_customer")
    cursor.execute("DROP TABLE IF EXISTS dim_location")

    create_tables(cursor)

    logger.info("Loading dim_date...")
    dim_date = pd.read_csv(os.path.join(data_dir, "dim_date.csv"))
    dim_date.to_sql("dim_date", conn, if_exists="append", index=False)

    logger.info("Loading dim_product, dim_customer, dim_location...")
    dim_product = pd.read_csv(os.path.join(data_dir, "dim_product.csv"))
    dim_product.to_sql("dim_product", conn, if_exists="append", index=False)

    dim_customer = pd.read_csv(os.path.join(data_dir, "dim_customer.csv"))
    dim_customer.to_sql("dim_customer", conn, if_exists="append", index=False)

    dim_location = pd.read_csv(os.path.join(data_dir, "dim_location.csv"))
    dim_location.to_sql("dim_location", conn, if_exists="append", index=False)

    logger.info("Loading fact_sales...")
    fact_sales = pd.read_csv(os.path.join(data_dir, "fact_sales.csv"))
    fact_sales.to_sql("fact_sales", conn, if_exists="append", index=False)

    cursor.execute("SELECT COUNT(*) FROM fact_sales")
    count = cursor.fetchone()[0]
    logger.info(f"Total rows loaded into fact_sales: {count}")

    conn.commit()
    _build_referential_integrity_artifacts(conn, reports_dir)
    conn.close()
    logger.info("Data loaded successfully.")


if __name__ == "__main__":
    os.makedirs("data/processed", exist_ok=True)
    load_data("data/processed/data_warehouse.db", "data/processed")
