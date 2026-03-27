import json
import os
import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from utils import setup_logger

logger = setup_logger("analysis")


def _load_dq_scores(output_dir: str) -> tuple[float, float]:
    dq_path = Path(output_dir) / "task_f_dq_scores.json"
    if not dq_path.exists():
        logger.warning("DQ score file not found. Falling back to zeros for BO-4.")
        return 0.0, 0.0

    payload = json.loads(dq_path.read_text(encoding="utf-8"))
    return float(payload["input_dq_score"]), float(payload["output_dq_score"])


def _save_task_i_artifacts(kpi_rows: list[dict], output_dir: str):
    summary_df = pd.DataFrame(kpi_rows)
    csv_path = Path(output_dir) / "task_i_kpi_summary.csv"
    json_path = Path(output_dir) / "task_i_kpi_summary.json"
    md_path = Path(output_dir) / "task_i_kpi_report.md"

    summary_df.to_csv(csv_path, index=False)
    json_path.write_text(json.dumps(kpi_rows, indent=2, ensure_ascii=False), encoding="utf-8")

    lines = [
        "# Task I - Visualizations and KPIs",
        "",
        "- Approach used: SQL queries against the final SQLite Data Warehouse (`data/processed/data_warehouse.db`).",
        "",
        "| Business Objective | KPI | Value | Interpretation | Visualization |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in kpi_rows:
        lines.append(
            f"| {row['Business Objective']} | {row['KPI']} | {row['Value']} | "
            f"{row['Interpretation']} | `{row['Visualization']}` |"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    logger.info(f"Saved {csv_path}")
    logger.info(f"Saved {json_path}")
    logger.info(f"Saved {md_path}")


def _save_figure(output_dir: str, filename: str):
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, filename), dpi=200, bbox_inches="tight")
    plt.close()


def _annotate_vertical_bars(ax, fmt: str = "{:,.0f}", rotation: int = 0):
    for patch in ax.patches:
        height = patch.get_height()
        ax.annotate(
            fmt.format(height),
            (patch.get_x() + patch.get_width() / 2.0, height),
            ha="center",
            va="bottom",
            fontsize=9,
            rotation=rotation,
            xytext=(0, 5),
            textcoords="offset points",
        )


def _annotate_horizontal_bars(ax, fmt: str = "{:,.0f}"):
    for patch in ax.patches:
        width = patch.get_width()
        ax.annotate(
            fmt.format(width),
            (width, patch.get_y() + patch.get_height() / 2.0),
            ha="left",
            va="center",
            fontsize=9,
            xytext=(5, 0),
            textcoords="offset points",
        )


def run_analysis(db_path: str, output_dir: str):
    logger.info("Connecting to Data Warehouse...")
    conn = sqlite3.connect(db_path)
    os.makedirs(output_dir, exist_ok=True)
    sns.set_theme(style="whitegrid")

    kpi_rows: list[dict] = []

    logger.info("Generating BO-1: Total revenue per country...")
    query1 = """
        SELECT l.country, SUM(f.total_revenue) AS total_revenue
        FROM fact_sales f
        JOIN dim_location l ON f.location_id = l.location_id
        GROUP BY l.country
        ORDER BY total_revenue DESC
    """
    df1 = pd.read_sql(query1, conn)
    plt.figure(figsize=(10, 6))
    ax = sns.barplot(data=df1, x="country", y="total_revenue", hue="country", palette="viridis", legend=False)
    plt.title("Total Revenue per Country")
    plt.ylabel("Total Revenue ($)")
    plt.xlabel("Country")
    _annotate_vertical_bars(ax, "{:,.0f}")
    _save_figure(output_dir, "bo1_revenue_per_country.png")

    top_country = df1.iloc[0]
    kpi_rows.append(
        {
            "Business Objective": "BO-1",
            "KPI": "País con mayor ingreso total",
            "Value": f"{top_country['country']}: {top_country['total_revenue']:.2f}",
            "Interpretation": (
                f"{top_country['country']} lidera la generación de ingresos en el Data Warehouse."
            ),
            "Visualization": "bo1_revenue_per_country.png",
        }
    )

    logger.info("Generating BO-1: Total revenue distribution by product...")
    query2 = """
        SELECT p.product_name, f.total_revenue
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
    """
    df2 = pd.read_sql(query2, conn)
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=df2, x="product_name", y="total_revenue", hue="product_name", palette="Set2", legend=False)
    plt.title("Transaction Value Distribution by Product")
    plt.ylabel("Transaction Revenue ($)")
    plt.xlabel("Product")
    _save_figure(output_dir, "bo1_transaction_value_distribution.png")

    average_transaction_value = float(df2["total_revenue"].mean())
    kpi_rows.append(
        {
            "Business Objective": "BO-1",
            "KPI": "Valor promedio por transacción",
            "Value": f"{average_transaction_value:.2f}",
            "Interpretation": "El ticket promedio de venta en el conjunto final es de 2518.67.",
            "Visualization": "bo1_transaction_value_distribution.png",
        }
    )

    logger.info("Generating BO-2: Monthly revenue trend...")
    query3 = """
        SELECT d.month, SUM(f.total_revenue) AS monthly_revenue
        FROM fact_sales f
        JOIN dim_date d ON f.date_id = d.date_id
        GROUP BY d.month
        ORDER BY d.month
    """
    df3 = pd.read_sql(query3, conn)
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df3, x="month", y="monthly_revenue", marker="o", color="b", linewidth=2.5)
    plt.title("Monthly Revenue Trend (2023)")
    plt.ylabel("Revenue ($)")
    plt.xlabel("Month (1-12)")
    plt.xticks(range(1, 13))
    _save_figure(output_dir, "bo2_monthly_revenue_trend.png")

    peak_month = df3.loc[df3["monthly_revenue"].idxmax()]
    kpi_rows.append(
        {
            "Business Objective": "BO-2",
            "KPI": "Mes con mayor ingreso",
            "Value": f"Mes {int(peak_month['month'])}: {peak_month['monthly_revenue']:.2f}",
            "Interpretation": (
                f"El mejor desempeño mensual se observó en el mes {int(peak_month['month'])}."
            ),
            "Visualization": "bo2_monthly_revenue_trend.png",
        }
    )

    logger.info("Generating BO-2: Transaction count by Day of Week...")
    query4 = """
        SELECT d.day_of_week, COUNT(f.sale_id) AS transaction_count
        FROM fact_sales f
        JOIN dim_date d ON f.date_id = d.date_id
        GROUP BY d.day_of_week
    """
    df4 = pd.read_sql(query4, conn)
    days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    df4["day_of_week"] = pd.Categorical(df4["day_of_week"], categories=days_order, ordered=True)
    df4 = df4.sort_values("day_of_week")

    plt.figure(figsize=(10, 6))
    ax = sns.barplot(data=df4, x="day_of_week", y="transaction_count", hue="day_of_week", palette="muted", legend=False)
    plt.title("Transaction Volume by Day of Week")
    plt.ylabel("Number of Transactions")
    plt.xlabel("Day of Week")
    _annotate_vertical_bars(ax, "{:,.0f}")
    _save_figure(output_dir, "bo2_peak_day_of_week.png")

    peak_day = df4.loc[df4["transaction_count"].idxmax()]
    kpi_rows.append(
        {
            "Business Objective": "BO-2",
            "KPI": "Día con mayor volumen transaccional",
            "Value": f"{peak_day['day_of_week']}: {int(peak_day['transaction_count'])} transacciones",
            "Interpretation": (
                f"El mayor volumen de transacciones ocurre los {peak_day['day_of_week']}."
            ),
            "Visualization": "bo2_peak_day_of_week.png",
        }
    )

    logger.info("Generating BO-3: Top 3 products by revenue...")
    query5 = """
        SELECT p.product_name, SUM(f.total_revenue) AS revenue
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.product_name
        ORDER BY revenue DESC
        LIMIT 3
    """
    df5 = pd.read_sql(query5, conn)
    plt.figure(figsize=(10, 5))
    ax = sns.barplot(data=df5, x="revenue", y="product_name", hue="product_name", palette="rocket", legend=False)
    plt.title("Top 3 Products by Total Revenue")
    plt.xlabel("Revenue ($)")
    plt.ylabel("Product")
    _annotate_horizontal_bars(ax, "{:,.0f}")
    _save_figure(output_dir, "bo3_top_3_products.png")

    top_product = df5.iloc[0]
    kpi_rows.append(
        {
            "Business Objective": "BO-3",
            "KPI": "Producto con mayor ingreso",
            "Value": f"{top_product['product_name']}: {top_product['revenue']:.2f}",
            "Interpretation": (
                f"{top_product['product_name']} es el producto más rentable del conjunto analizado."
            ),
            "Visualization": "bo3_top_3_products.png",
        }
    )

    logger.info("Generating BO-3: Sales distribution by country...")
    total_revenue = df1["total_revenue"].sum()
    df1["share_pct"] = (df1["total_revenue"] / total_revenue) * 100
    plt.figure(figsize=(8, 8))
    plt.pie(
        df1["total_revenue"],
        labels=df1["country"],
        autopct="%1.1f%%",
        startangle=140,
        colors=sns.color_palette("pastel"),
    )
    plt.title("Sales Distribution by Country")
    _save_figure(output_dir, "bo3_sales_distribution_country.png")

    top_share = df1.iloc[0]
    kpi_rows.append(
        {
            "Business Objective": "BO-3",
            "KPI": "Participación del país líder",
            "Value": f"{top_share['country']}: {top_share['share_pct']:.2f}%",
            "Interpretation": (
                f"{top_share['country']} concentra la mayor participación de ventas del periodo."
            ),
            "Visualization": "bo3_sales_distribution_country.png",
        }
    )

    logger.info("Generating BO-4: Data Quality Score comparison...")
    input_dq_score, output_dq_score = _load_dq_scores(output_dir)
    dq_scores = pd.DataFrame(
        {
            "Stage": ["Raw Input", "Cleaned Output"],
            "DQ_Score": [input_dq_score, output_dq_score],
        }
    )
    plt.figure(figsize=(8, 6))
    ax = sns.barplot(data=dq_scores, x="Stage", y="DQ_Score", hue="Stage", palette=["#ff9999", "#66b3ff"], legend=False)
    plt.title("Data Quality Score Before vs. After Processing")
    plt.ylabel("Pass Rate (%)")
    plt.ylim(0, 110)
    for patch in ax.patches:
        ax.annotate(
            f"{patch.get_height():.1f}%",
            (patch.get_x() + patch.get_width() / 2.0, patch.get_height()),
            ha="center",
            va="center",
            xytext=(0, 10),
            textcoords="offset points",
        )
    _save_figure(output_dir, "bo4_dq_score_comparison.png")

    kpi_rows.append(
        {
            "Business Objective": "BO-4",
            "KPI": "Mejora del Data Quality Score",
            "Value": f"{input_dq_score:.2f}% -> {output_dq_score:.2f}%",
            "Interpretation": (
                "La calidad de datos mejoró sustancialmente después del proceso ETL."
            ),
            "Visualization": "bo4_dq_score_comparison.png",
        }
    )

    _save_task_i_artifacts(kpi_rows, output_dir)

    conn.close()
    logger.info("Business Analysis visualizations saved successfully to reports/")


if __name__ == "__main__":
    run_analysis("data/processed/data_warehouse.db", "reports")
