import json
import os

import pandas as pd

from utils import setup_logger

logger = setup_logger("dimensional_model")


SCHEMA_METADATA = {
    "dim_product": {
        "description": "Dimension de productos con una fila por producto estandarizado.",
        "grain": "Un registro por producto.",
        "columns": [
            ("product_id", "INTEGER", "PK", "Clave sustituta de la dimensión producto."),
            ("product_name", "TEXT", "Attribute", "Nombre del producto estandarizado."),
        ],
    },
    "dim_customer": {
        "description": "Dimension de clientes con clave sustituta para enlazar ventas.",
        "grain": "Un registro por customer_id del origen.",
        "columns": [
            ("customer_id", "INTEGER", "PK", "Clave sustituta de la dimensión cliente."),
            (
                "customer_key",
                "INTEGER",
                "Natural Key",
                "Identificador original del cliente en los datos fuente.",
            ),
        ],
    },
    "dim_location": {
        "description": "Dimension geográfica simplificada a nivel país.",
        "grain": "Un registro por país estandarizado.",
        "columns": [
            ("location_id", "INTEGER", "PK", "Clave sustituta de la dimensión ubicación."),
            ("country", "TEXT", "Attribute", "Nombre del país estandarizado."),
        ],
    },
    "dim_date": {
        "description": "Dimension calendario completa para el año 2023.",
        "grain": "Un registro por día calendario.",
        "columns": [
            ("date_id", "INTEGER", "PK", "Clave de fecha en formato YYYYMMDD."),
            ("full_date", "TEXT", "Attribute", "Fecha completa en formato YYYY-MM-DD."),
            ("year", "INTEGER", "Attribute", "Año de la fecha."),
            ("month", "INTEGER", "Attribute", "Número de mes de la fecha."),
            ("month_name", "TEXT", "Attribute", "Nombre del mes."),
            ("day_of_week", "TEXT", "Attribute", "Nombre del día de la semana."),
        ],
    },
    "fact_sales": {
        "description": "Tabla de hechos con una fila por transacción de venta validada.",
        "grain": "Un registro por transacción limpia y transformada.",
        "columns": [
            ("sale_id", "INTEGER", "PK", "Identificador secuencial de la venta en la tabla de hechos."),
            ("invoice_id", "INTEGER", "Degenerate", "Identificador de factura proveniente del sistema origen."),
            ("product_id", "INTEGER", "FK", "Clave foránea hacia dim_product."),
            ("customer_id", "INTEGER", "FK", "Clave foránea hacia dim_customer."),
            ("location_id", "INTEGER", "FK", "Clave foránea hacia dim_location."),
            ("date_id", "INTEGER", "FK", "Clave foránea hacia dim_date."),
            ("quantity", "INTEGER", "Measure", "Cantidad vendida en la transacción."),
            ("price", "REAL", "Measure", "Precio unitario de la transacción."),
            ("total_revenue", "REAL", "Measure", "Ingreso total calculado para la transacción."),
        ],
    },
}


def create_dimensional_model(input_path: str, output_dir: str, report_dir: str = "reports"):
    logger.info(f"Loading transformed data from {input_path}")
    df = pd.read_csv(input_path)

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(report_dir, exist_ok=True)

    logger.info("Building dim_product...")
    unique_products = sorted(df["product"].dropna().unique())
    dim_product = pd.DataFrame({"product_name": unique_products})
    dim_product["product_id"] = range(1, len(dim_product) + 1)
    dim_product = dim_product[["product_id", "product_name"]]
    dim_product.to_csv(os.path.join(output_dir, "dim_product.csv"), index=False)

    logger.info("Building dim_customer...")
    unique_customers = sorted(df["customer_id"].dropna().astype(int).unique())
    dim_customer = pd.DataFrame({"customer_key": unique_customers})
    dim_customer["customer_id"] = range(1, len(dim_customer) + 1)
    dim_customer = dim_customer[["customer_id", "customer_key"]]
    dim_customer.to_csv(os.path.join(output_dir, "dim_customer.csv"), index=False)

    logger.info("Building dim_location...")
    unique_locations = sorted(df["country"].dropna().unique())
    dim_location = pd.DataFrame({"country": unique_locations})
    dim_location["location_id"] = range(1, len(dim_location) + 1)
    dim_location = dim_location[["location_id", "country"]]
    dim_location.to_csv(os.path.join(output_dir, "dim_location.csv"), index=False)

    logger.info("Building dim_date (all dates in 2023)...")
    date_range = pd.date_range(start="2023-01-01", end="2023-12-31")
    dim_date = pd.DataFrame({"full_date": date_range})
    dim_date["date_id"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["year"] = dim_date["full_date"].dt.year
    dim_date["month"] = dim_date["full_date"].dt.month
    dim_date["month_name"] = dim_date["full_date"].dt.strftime("%B")
    dim_date["day_of_week"] = dim_date["full_date"].dt.day_name()
    dim_date["full_date"] = dim_date["full_date"].dt.strftime("%Y-%m-%d")
    dim_date = dim_date[["date_id", "full_date", "year", "month", "month_name", "day_of_week"]]
    dim_date.to_csv(os.path.join(output_dir, "dim_date.csv"), index=False)

    logger.info("Building fact_sales...")
    fact_sales = df.copy()
    fact_sales = fact_sales.merge(dim_product, left_on="product", right_on="product_name", how="left")
    fact_sales = fact_sales.merge(
        dim_customer,
        left_on="customer_id",
        right_on="customer_key",
        suffixes=("", "_surrogate"),
        how="left",
    )
    fact_sales = fact_sales.merge(dim_location, on="country", how="left")
    fact_sales["date_id"] = pd.to_datetime(fact_sales["invoice_date"]).dt.strftime("%Y%m%d").astype(int)
    fact_sales.rename(columns={"customer_id_surrogate": "customer_id_fk"}, inplace=True)
    fact_sales["sale_id"] = range(1, len(fact_sales) + 1)

    final_cols = [
        "sale_id",
        "invoice_id",
        "product_id",
        "customer_id_fk",
        "location_id",
        "date_id",
        "quantity",
        "price",
        "total_revenue",
    ]

    fact_sales = fact_sales[final_cols]
    fact_sales.rename(columns={"customer_id_fk": "customer_id"}, inplace=True)
    fact_sales.to_csv(os.path.join(output_dir, "fact_sales.csv"), index=False)
    logger.info(f"Dimensional model creation complete. Fact sales shape: {fact_sales.shape}")

    create_model_description(output_dir=output_dir, report_dir=report_dir)


def create_model_description(output_dir: str, report_dir: str = "reports"):
    row_counts = {}
    for table_name in SCHEMA_METADATA:
        csv_path = os.path.join(output_dir, f"{table_name}.csv")
        row_counts[table_name] = int(pd.read_csv(csv_path).shape[0])

    layout_rows = []
    summary_rows = []
    for table_name, table_meta in SCHEMA_METADATA.items():
        summary_rows.append(
            {
                "Table": table_name,
                "Rows": row_counts[table_name],
                "Grain": table_meta["grain"],
                "Description": table_meta["description"],
            }
        )
        for column_name, data_type, key_type, description in table_meta["columns"]:
            layout_rows.append(
                {
                    "Table": table_name,
                    "Column": column_name,
                    "Data Type": data_type,
                    "Key Type": key_type,
                    "Description": description,
                }
            )

    layout_df = pd.DataFrame(layout_rows)
    summary_df = pd.DataFrame(summary_rows)

    layout_csv = os.path.join(report_dir, "task_g_star_schema_layout.csv")
    summary_csv = os.path.join(report_dir, "task_g_star_schema_summary.csv")
    layout_json = os.path.join(report_dir, "task_g_star_schema_layout.json")
    summary_json = os.path.join(report_dir, "task_g_star_schema_summary.json")
    report_md = os.path.join(report_dir, "task_g_star_schema_report.md")
    legacy_md = os.path.join(report_dir, "model_description.md")

    layout_df.to_csv(layout_csv, index=False)
    summary_df.to_csv(summary_csv, index=False)
    with open(layout_json, "w", encoding="utf-8") as file:
        json.dump(layout_rows, file, indent=2, ensure_ascii=False)
    with open(summary_json, "w", encoding="utf-8") as file:
        json.dump(summary_rows, file, indent=2, ensure_ascii=False)

    erd = """```mermaid
erDiagram
    fact_sales {
        int sale_id PK
        int invoice_id
        int product_id FK
        int customer_id FK
        int location_id FK
        int date_id FK
        int quantity
        float price
        float total_revenue
    }
    dim_product {
        int product_id PK
        string product_name
    }
    dim_customer {
        int customer_id PK
        int customer_key
    }
    dim_location {
        int location_id PK
        string country
    }
    dim_date {
        int date_id PK
        date full_date
        int year
        int month
        string month_name
        string day_of_week
    }
    fact_sales }o--|| dim_product : references
    fact_sales }o--|| dim_customer : references
    fact_sales }o--|| dim_location : references
    fact_sales }o--|| dim_date : references
```"""

    lines = [
        "# Star Schema Data Model",
        "",
        "## ERD Diagram",
        erd,
        "",
        "## Table Summary",
        "| Table | Rows | Grain | Description |",
        "| --- | ---: | --- | --- |",
    ]
    for row in summary_rows:
        lines.append(
            f"| `{row['Table']}` | {row['Rows']} | {row['Grain']} | {row['Description']} |"
        )

    lines.extend(
        [
            "",
            "## Column Layout",
            "| Table | Column | Data Type | Key Type | Description |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for row in layout_rows:
        lines.append(
            f"| `{row['Table']}` | `{row['Column']}` | `{row['Data Type']}` | "
            f"{row['Key Type']} | {row['Description']} |"
        )

    lines.extend(
        [
            "",
            "## Relationship Notes",
            "- `fact_sales.product_id` referencia `dim_product.product_id`.",
            "- `fact_sales.customer_id` referencia `dim_customer.customer_id`.",
            "- `fact_sales.location_id` referencia `dim_location.location_id`.",
            "- `fact_sales.date_id` referencia `dim_date.date_id`.",
            "- La granularidad de `fact_sales` es una transacción por fila.",
            "- `invoice_id` se conserva como dimensión degenerada para trazabilidad con el origen.",
        ]
    )

    markdown = "\n".join(lines) + "\n"
    with open(report_md, "w", encoding="utf-8") as file:
        file.write(markdown)
    with open(legacy_md, "w", encoding="utf-8") as file:
        file.write(markdown)

    logger.info(f"Saved {report_md}")
    logger.info(f"Saved {layout_csv}")
    logger.info(f"Saved {summary_csv}")


if __name__ == "__main__":
    create_dimensional_model("data/processed/retail_transformed.csv", "data/processed")
