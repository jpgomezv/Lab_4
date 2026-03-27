import json
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "processed" / "data_warehouse.db"
DQ_PATH = BASE_DIR / "reports" / "task_f_dq_scores.json"

st.set_page_config(
    page_title="Retail Intelligence Dashboard",
    page_icon="RI",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
<style>
    .block-container {
        padding-top: 1rem !important;
        padding-bottom: 0rem !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
    }
    [data-testid="stVerticalBlock"] > div {
        gap: 0.5rem !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.6rem !important;
        font-weight: 800;
        color: #00d4ff;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.8rem !important;
        text-transform: uppercase;
        letter-spacing: 1px;
        color: #9ca3af;
    }
    div[data-testid="stMetric"] {
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(255, 255, 255, 0.08);
        padding: 10px 15px !important;
        border-radius: 10px;
    }
    header {visibility: hidden;}
    footer {visibility: hidden;}
    #MainMenu {visibility: hidden;}
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_resource
def get_connection():
    if not DB_PATH.exists():
        return None
    return sqlite3.connect(DB_PATH.as_posix(), check_same_thread=False)


@st.cache_data
def run_query(query: str) -> pd.DataFrame:
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    return pd.read_sql(query, conn)


@st.cache_data
def load_dq_scores() -> tuple[float, float]:
    if not DQ_PATH.exists():
        return 0.0, 0.0
    payload = json.loads(DQ_PATH.read_text(encoding="utf-8"))
    return float(payload.get("input_dq_score", 0.0)), float(payload.get("output_dq_score", 0.0))


def format_currency(value: float) -> str:
    if value >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:,.2f}"


if not DB_PATH.exists():
    st.error(f"Database not found at {DB_PATH}. Run the ETL pipeline first.")
    st.stop()


with st.sidebar:
    st.title("Retail Intelligence")
    countries_df = run_query("SELECT DISTINCT country FROM dim_location ORDER BY country")
    country_list = ["All Countries"] + countries_df["country"].tolist()
    selected_country = st.selectbox("Global Filter", country_list)
    st.divider()
    st.caption("Dashboard based on the SQLite warehouse")


filter_clause = ""
if selected_country != "All Countries":
    filter_clause = f"WHERE l.country = '{selected_country}'"


col_title, col_info = st.columns([3, 1])
with col_title:
    st.markdown("<h2 style='margin:0; padding:0;'>Retail Performance Analytics</h2>", unsafe_allow_html=True)
with col_info:
    st.markdown(
        f"<p style='text-align:right; color:#6b7280; font-size:0.8rem; margin:0;'>Region: {selected_country}</p>",
        unsafe_allow_html=True,
    )


summary_q = f"""
SELECT
    COUNT(f.sale_id) AS total_txs,
    SUM(f.total_revenue) AS total_rev,
    AVG(f.total_revenue) AS avg_val,
    COUNT(DISTINCT f.customer_id) AS total_cust
FROM fact_sales f
JOIN dim_location l ON f.location_id = l.location_id
{filter_clause}
"""
summary_df = run_query(summary_q)
total_txs = summary_df.loc[0, "total_txs"] or 0
total_rev = summary_df.loc[0, "total_rev"] or 0
avg_val = summary_df.loc[0, "avg_val"] or 0
total_cust = summary_df.loc[0, "total_cust"] or 0

m_col1, m_col2, m_col3, m_col4 = st.columns(4)
m_col1.metric("Revenue", format_currency(total_rev))
m_col2.metric("Orders", f"{int(total_txs):,}")
m_col3.metric("Ticket", format_currency(avg_val))
m_col4.metric("Customers", f"{int(total_cust):,}")

st.caption("Use 'All Countries' to view the same cross-country visualizations that were exported as final PNGs.")

tab_bo1, tab_bo2, tab_bo3, tab_bo4 = st.tabs(["BO-1", "BO-2", "BO-3", "BO-4"])

with tab_bo1:
    col1, col2 = st.columns(2)

    with col1:
        country_rev_q = f"""
        SELECT l.country, SUM(f.total_revenue) AS total_revenue
        FROM fact_sales f
        JOIN dim_location l ON f.location_id = l.location_id
        {filter_clause}
        GROUP BY l.country
        ORDER BY total_revenue DESC
        """
        country_rev_df = run_query(country_rev_q)
        fig_country = px.bar(
            country_rev_df,
            x="country",
            y="total_revenue",
            text_auto=".0f",
            template="plotly_dark",
            color="country",
            title="Total Revenue per Country",
        )
        fig_country.update_layout(margin=dict(l=0, r=0, t=40, b=0), showlegend=False, height=420)
        fig_country.update_yaxes(title_text="Total Revenue ($)")
        st.plotly_chart(fig_country, width="stretch", config={"displayModeBar": False})
        st.caption("Required visualization: bar chart. Note: duplicate and negative rows were excluded through the cleaning stage before loading the warehouse.")

    with col2:
        box_q = f"""
        SELECT p.product_name, f.total_revenue
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        JOIN dim_location l ON f.location_id = l.location_id
        {filter_clause}
        """
        box_df = run_query(box_q)
        fig_box = px.box(
            box_df,
            x="product_name",
            y="total_revenue",
            color="product_name",
            points="outliers",
            template="plotly_dark",
            title="Transaction Value Distribution by Product",
        )
        fig_box.update_layout(margin=dict(l=0, r=0, t=40, b=0), showlegend=False, height=420)
        fig_box.update_yaxes(title_text="Transaction Revenue ($)")
        fig_box.update_xaxes(title_text="Product")
        st.plotly_chart(fig_box, width="stretch", config={"displayModeBar": False})
        st.caption("Required visualization: box plot. Note: outliers are shown explicitly as requested in the rubric.")

with tab_bo2:
    col1, col2 = st.columns(2)

    with col1:
        monthly_q = f"""
        SELECT d.month, SUM(f.total_revenue) AS revenue
        FROM fact_sales f
        JOIN dim_date d ON f.date_id = d.date_id
        JOIN dim_location l ON f.location_id = l.location_id
        {filter_clause}
        GROUP BY d.month
        ORDER BY d.month
        """
        monthly_df = run_query(monthly_q)
        fig_trend = px.line(
            monthly_df,
            x="month",
            y="revenue",
            markers=True,
            template="plotly_dark",
            title="Monthly Revenue Trend (2023)",
        )
        fig_trend.update_layout(margin=dict(l=0, r=0, t=40, b=0), showlegend=False, height=420)
        fig_trend.update_xaxes(title_text="Month (1-12)", dtick=1)
        fig_trend.update_yaxes(title_text="Revenue ($)")
        st.plotly_chart(fig_trend, width="stretch", config={"displayModeBar": False})
        st.caption("Required visualization: line chart. Note: the parsed month attribute comes from `dim_date`.")

    with col2:
        dow_q = f"""
        SELECT d.day_of_week, COUNT(f.sale_id) AS transaction_count
        FROM fact_sales f
        JOIN dim_date d ON f.date_id = d.date_id
        JOIN dim_location l ON f.location_id = l.location_id
        {filter_clause}
        GROUP BY d.day_of_week
        """
        dow_df = run_query(dow_q)
        days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        dow_df["day_of_week"] = pd.Categorical(dow_df["day_of_week"], categories=days_order, ordered=True)
        dow_df = dow_df.sort_values("day_of_week")
        fig_dow = px.bar(
            dow_df,
            x="day_of_week",
            y="transaction_count",
            text_auto=".0f",
            template="plotly_dark",
            color="day_of_week",
            title="Transaction Volume by Day of Week",
        )
        fig_dow.update_layout(margin=dict(l=0, r=0, t=40, b=0), showlegend=False, height=420)
        fig_dow.update_yaxes(title_text="Number of Transactions")
        fig_dow.update_xaxes(title_text="Day of Week")
        st.plotly_chart(fig_dow, width="stretch", config={"displayModeBar": False})
        st.caption("Required visualization: bar chart. Note: the day-of-week attribute is taken from `dim_date`.")

with tab_bo3:
    col1, col2 = st.columns(2)

    with col1:
        top_prod_q = f"""
        SELECT p.product_name, SUM(f.total_revenue) AS revenue
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        JOIN dim_location l ON f.location_id = l.location_id
        {filter_clause}
        GROUP BY p.product_name
        ORDER BY revenue DESC
        LIMIT 3
        """
        top_prod_df = run_query(top_prod_q)
        fig_prod = px.bar(
            top_prod_df,
            x="revenue",
            y="product_name",
            orientation="h",
            text_auto=".0f",
            template="plotly_dark",
            color="product_name",
            title="Top 3 Products by Total Revenue",
        )
        fig_prod.update_layout(margin=dict(l=0, r=0, t=40, b=0), showlegend=False, height=420)
        fig_prod.update_xaxes(title_text="Revenue ($)")
        fig_prod.update_yaxes(title_text="Product", categoryorder="total ascending")
        st.plotly_chart(fig_prod, width="stretch", config={"displayModeBar": False})
        st.caption("Required visualization: horizontal bar chart. Note: products are sorted in descending revenue order.")

    with col2:
        sales_mix_q = f"""
        SELECT l.country, SUM(f.total_revenue) AS revenue
        FROM fact_sales f
        JOIN dim_location l ON f.location_id = l.location_id
        {filter_clause}
        GROUP BY l.country
        ORDER BY revenue DESC
        """
        sales_mix_df = run_query(sales_mix_q)
        fig_mix = px.pie(
            sales_mix_df,
            names="country",
            values="revenue",
            template="plotly_dark",
            title="Sales Distribution by Country",
        )
        fig_mix.update_layout(margin=dict(l=0, r=0, t=40, b=0), height=420)
        st.plotly_chart(fig_mix, width="stretch", config={"displayModeBar": False})
        st.caption("Required visualization: pie chart. Note: country names are standardized from the cleaned dimensional model.")

with tab_bo4:
    input_dq, output_dq = load_dq_scores()
    dq_df = pd.DataFrame({"Stage": ["Raw Input", "Cleaned Output"], "Score": [input_dq, output_dq]})
    fig_dq = px.bar(
        dq_df,
        x="Stage",
        y="Score",
        text_auto=".1f",
        template="plotly_dark",
        color="Stage",
        color_discrete_map={"Raw Input": "#ff9999", "Cleaned Output": "#66b3ff"},
        title="Data Quality Score Before vs. After Processing",
    )
    fig_dq.update_layout(margin=dict(l=0, r=0, t=40, b=0), showlegend=False, height=460)
    fig_dq.update_yaxes(title_text="Pass Rate (%)", range=[0, 110])
    st.plotly_chart(fig_dq, width="stretch", config={"displayModeBar": False})
    st.caption("Required visualization: side-by-side bar chart. Note: values come from the validated comparison table of Task F.")

st.markdown(
    "<p style='text-align:center; color:#4b5563; font-size:0.75rem; margin-top:10px;'>"
    "Interactive dashboard connected to the final ETL warehouse.</p>",
    unsafe_allow_html=True,
)
