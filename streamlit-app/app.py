import os
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import snowflake.connector
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Load secure credentials strictly from variables
load_dotenv(PROJECT_ROOT / ".env")

st.set_page_config(page_title="Geocart Sales Insights", page_icon="📈", layout="wide")

# --- CSS & THEME FUNCTIONS ---
def theme_tokens(is_dark: bool) -> dict[str, str]:
    if is_dark:
        return {
            "plotly": "plotly_dark",
            "bg": "radial-gradient(circle at top right, rgba(52, 211, 153, 0.16), transparent 28%), linear-gradient(180deg, #0f172a 0%, #111827 100%)",
            "panel": "rgba(15, 23, 42, 0.76)",
            "border": "rgba(148, 163, 184, 0.20)",
            "text": "#e5eef7",
            "muted": "#cbd5e1",
            "accent": "#34d399",
            "accent_alt": "#38bdf8",
            "hero_shadow": "rgba(7, 59, 76, 0.24)",
        }
    return {
        "plotly": "plotly_white",
        "bg": "radial-gradient(circle at top right, rgba(14, 116, 144, 0.14), transparent 28%), linear-gradient(180deg, #f7fbfc 0%, #edf6f7 100%)",
        "panel": "rgba(255, 255, 255, 0.92)",
        "border": "rgba(15, 118, 110, 0.14)",
        "text": "#0f172a",
        "muted": "#475569",
        "accent": "#0f766e",
        "accent_alt": "#0ea5a4",
        "hero_shadow": "rgba(7, 59, 76, 0.16)",
    }

def inject_css(tokens: dict[str, str]) -> None:
    st.markdown(
        f"""
        <style>
        .stApp {{ background: {tokens["bg"]}; }}
        .block-container {{ padding-top: 1.1rem; padding-bottom: 1.4rem; animation: fadeUp 0.45s ease-out; }}
        section[data-testid="stSidebar"] {{ background: {tokens["panel"]}; border-right: 1px solid {tokens["border"]}; }}
        .hero {{ padding: 1.25rem 1.4rem; border-radius: 22px; background: linear-gradient(135deg, #073b4c 0%, #0f766e 52%, #84cc16 100%); color: white; box-shadow: 0 18px 36px {tokens["hero_shadow"]}; margin-bottom: 1rem; animation: fadeUp 0.5s ease-out; }}
        .hero h1 {{ margin: 0; font-size: 2rem; }}
        .hero p {{ margin: 0.45rem 0 0 0; opacity: 0.95; font-size: 1rem; }}
        div[data-testid="stMetric"] {{ background: {tokens["panel"]}; border: 1px solid {tokens["border"]}; padding: 0.85rem 0.95rem; border-radius: 16px; box-shadow: 0 10px 24px rgba(15, 23, 42, 0.05); }}
        .insight-card {{ background: {tokens["panel"]}; border: 1px solid {tokens["border"]}; border-radius: 16px; padding: 0.95rem 1rem; box-shadow: 0 10px 24px rgba(15, 23, 42, 0.05); border-left: 4px solid {tokens["accent"]}; }}
        .alert-card {{ background: rgba(239, 68, 68, 0.1); border: 1px solid rgba(239, 68, 68, 0.3); border-radius: 16px; padding: 0.95rem 1rem; border-left: 4px solid #ef4444; }}
        @keyframes fadeUp {{ from {{ opacity: 0; transform: translateY(10px); }} to {{ opacity: 1; transform: translateY(0); }} }}
        </style>
        """,
        unsafe_allow_html=True,
    )

def compact_number(value: float) -> str:
    if pd.isna(value): return "0"
    abs_value = abs(value)
    if abs_value >= 1_000_000: return f"{value / 1_000_000:.2f}M"
    if abs_value >= 1_000: return f"{value / 1_000:.2f}K"
    return f"{value:,.0f}"

def compact_currency(value: float) -> str: return f"${compact_number(value)}"
def compact_percent(value: float) -> str: return f"{0 if pd.isna(value) else value:.1f}%"

# --- SNOWFLAKE CONNECTION ---
@st.cache_resource(show_spinner="Connecting to Snowflake Data Warehouse...")
def get_snowflake_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )

@st.cache_data(show_spinner=False, ttl=600)
def run_query(query: str) -> pd.DataFrame:
    conn = get_snowflake_conn()
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetch_pandas_all()

# --- FILTER LOGIC ---
@st.cache_data(show_spinner=False)
def get_filters():
    states = run_query("SELECT DISTINCT customer_state FROM dim_customers WHERE customer_state IS NOT NULL ORDER BY 1")['CUSTOMER_STATE'].tolist()
    years = run_query("SELECT DISTINCT year FROM dim_date WHERE year IS NOT NULL ORDER BY 1 DESC")['YEAR'].tolist()
    categories = run_query("SELECT DISTINCT category_name FROM dim_products WHERE category_name IS NOT NULL ORDER BY 1")['CATEGORY_NAME'].tolist()
    statuses = run_query("SELECT DISTINCT order_status FROM fact_orders WHERE order_status IS NOT NULL ORDER BY 1")['ORDER_STATUS'].tolist()
    return states, categories, years, statuses

def build_where_clause(state, year, category, status):
    conditions = ["1=1"]
    if state != "All": conditions.append(f"c.customer_state = '{state}'")
    if year != "All": conditions.append(f"d.year = {year}")
    if category != "All": conditions.append(f"p.category_name = '{category}'")
    if status != "All": conditions.append(f"o.order_status = '{status}'")
    return " AND ".join(conditions)

# --- UI SETUP ---
dark_mode = st.sidebar.toggle("Dark mode", value=False)
tokens = theme_tokens(dark_mode)
inject_css(tokens)

states, categories, years, statuses = get_filters()

st.markdown(
    """
    <div class="hero">
        <h1>Geocart Intelligence Command Center</h1>
        <p>Enterprise performance, customer behavior, and operational health anomalies.</p>
    </div>
    """, unsafe_allow_html=True,
)

st.sidebar.markdown("### Strategic Filters")
selected_state = st.sidebar.selectbox("Region / State", ["All"] + states)
selected_year = st.sidebar.selectbox("Year", ["All"] + [str(y) for y in years])
selected_category = st.sidebar.selectbox("Product Category", ["All"] + categories)
selected_status = st.sidebar.selectbox("Campaign Proxy (Status)", ["All"] + statuses)

where_clause = build_where_clause(selected_state, selected_year, selected_category, selected_status)

# --- MAIN DASHBOARD QUERIES ---
# 1. KPIs
kpi_query = f"""
    SELECT 
        SUM(s.price + s.freight_value) as TOTAL_REVENUE,
        COUNT(DISTINCT s.order_id) as TOTAL_ORDERS,
        COUNT(DISTINCT c.customer_unique_id) as TOTAL_CUSTOMERS,
        AVG(o.delivery_days) as AVG_DELIVERY_DAYS,
        AVG(o.avg_review_score) as AVG_REVIEW_SCORE,
        (COUNT(DISTINCT CASE WHEN o.order_status = 'delivered' THEN o.order_id END) * 100.0) / NULLIF(COUNT(DISTINCT o.order_id), 0) as CONVERSION_RATE,
        (COUNT(DISTINCT CASE WHEN c.customer_segment IN ('High Value', 'Repeat') THEN c.customer_unique_id END) * 100.0) / NULLIF(COUNT(DISTINCT c.customer_unique_id), 0) as REPEAT_RATE
    FROM fact_sales s
    JOIN fact_orders o ON s.order_id = o.order_id
    JOIN dim_customers c ON s.customer_id = c.customer_id
    JOIN dim_products p ON s.product_id = p.product_id
    JOIN dim_date d ON s.date_key = d.date_key
    WHERE {where_clause}
"""
kpis = run_query(kpi_query).iloc[0]

kpi_cols = st.columns(6)
kpi_cols[0].metric("Total Revenue", compact_currency(kpis["TOTAL_REVENUE"]))
kpi_cols[1].metric("Total Orders", compact_number(kpis["TOTAL_ORDERS"]))
kpi_cols[2].metric("Conversion (Delivery) Rate", compact_percent(kpis["CONVERSION_RATE"]))
kpi_cols[3].metric("Repeat Customer %", compact_percent(kpis["REPEAT_RATE"]))
kpi_cols[4].metric("Avg Review Score", f"{kpis['AVG_REVIEW_SCORE']:.1f}" if pd.notna(kpis['AVG_REVIEW_SCORE']) else "0.0")
kpi_cols[5].metric("Avg Delivery Days", f"{kpis['AVG_DELIVERY_DAYS']:.1f}" if pd.notna(kpis['AVG_DELIVERY_DAYS']) else "0.0")

tabs = st.tabs(["Executive Overview", "Product & Customer", "Anomalies & Alerts"])

# --- TAB 1: EXECUTIVE OVERVIEW ---
with tabs[0]:
    col1, col2 = st.columns([1.5, 1])
    
    with col1:
        # Trend Chart
        trend_query = f"""
            SELECT d.year, d.month, SUM(s.price + s.freight_value) as REVENUE, COUNT(DISTINCT s.order_id) as ORDERS
            FROM fact_sales s JOIN dim_date d ON s.date_key = d.date_key JOIN dim_customers c ON s.customer_id = c.customer_id JOIN dim_products p ON s.product_id = p.product_id JOIN fact_orders o ON s.order_id = o.order_id
            WHERE {where_clause} GROUP BY d.year, d.month ORDER BY d.year, d.month
        """
        trend_df = run_query(trend_query)
        if not trend_df.empty:
            trend_df["MONTH_STR"] = trend_df["YEAR"].astype(str) + "-" + trend_df["MONTH"].astype(str).str.zfill(2)
            fig_trend = go.Figure()
            fig_trend.add_trace(go.Scatter(x=trend_df["MONTH_STR"], y=trend_df["REVENUE"], mode="lines+markers", name="Revenue", line=dict(color=tokens["accent"], width=3), yaxis="y1"))
            fig_trend.add_trace(go.Bar(x=trend_df["MONTH_STR"], y=trend_df["ORDERS"], name="Orders", marker_color=tokens["accent_alt"], opacity=0.45, yaxis="y2"))
            fig_trend.update_layout(template=tokens["plotly"], title="Revenue vs Orders Trend", height=400, yaxis=dict(title="Revenue"), yaxis2=dict(title="Orders", overlaying="y", side="right"), legend=dict(orientation="h", y=1.08))
            st.plotly_chart(fig_trend, use_container_width=True)

    with col2:
        # Comparison YoY
        yoy_query = f"""
            SELECT d.year, d.month, SUM(s.price + s.freight_value) as REVENUE
            FROM fact_sales s JOIN dim_date d ON s.date_key = d.date_key JOIN dim_customers c ON s.customer_id = c.customer_id JOIN dim_products p ON s.product_id = p.product_id JOIN fact_orders o ON s.order_id = o.order_id
            WHERE {where_clause.replace(f"d.year = {selected_year}", "1=1") if selected_year != 'All' else where_clause}
            GROUP BY d.year, d.month ORDER BY d.month, d.year DESC
        """
        yoy_df = run_query(yoy_query)
        if not yoy_df.empty:
            fig_compare = px.line(yoy_df, x="MONTH", y="REVENUE", color="YEAR", template=tokens["plotly"], title="YoY Revenue Comparison", color_discrete_sequence=[tokens["accent"], tokens["accent_alt"], "#f59e0b"])
            st.plotly_chart(fig_compare, use_container_width=True)

# --- TAB 2: PRODUCT & CUSTOMER ---
with tabs[1]:
    col1, col2 = st.columns(2)
    with col1:
        # Revenue by Category
        cat_query = f"""
            SELECT p.category_name, SUM(s.price + s.freight_value) as REVENUE
            FROM fact_sales s JOIN dim_products p ON s.product_id = p.product_id JOIN dim_customers c ON s.customer_id = c.customer_id JOIN dim_date d ON s.date_key = d.date_key JOIN fact_orders o ON s.order_id = o.order_id
            WHERE {where_clause} AND p.category_name IS NOT NULL GROUP BY p.category_name ORDER BY REVENUE DESC LIMIT 10
        """
        cat_df = run_query(cat_query)
        fig_cat = px.bar(cat_df, x="REVENUE", y="CATEGORY_NAME", orientation='h', template=tokens["plotly"], title="Top 10 Categories by Revenue", color="REVENUE", color_continuous_scale="Tealgrn")
        fig_cat.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_cat, use_container_width=True)

    with col2:
        # Customer Segments
        seg_query = f"""
            SELECT c.customer_segment, COUNT(DISTINCT c.customer_unique_id) as CUSTOMERS
            FROM dim_customers c JOIN fact_sales s ON c.customer_id = s.customer_id JOIN dim_products p ON s.product_id = p.product_id JOIN dim_date d ON s.date_key = d.date_key JOIN fact_orders o ON s.order_id = o.order_id
            WHERE {where_clause} GROUP BY c.customer_segment
        """
        seg_df = run_query(seg_query)
        fig_seg = px.pie(seg_df, values="CUSTOMERS", names="CUSTOMER_SEGMENT", template=tokens["plotly"], title="Customer Segmentation", hole=0.4, color_discrete_sequence=[tokens["accent"], tokens["accent_alt"], "#f59e0b"])
        st.plotly_chart(fig_seg, use_container_width=True)

# --- TAB 3: ANOMALIES & ALERTS ---
with tabs[2]:
    st.markdown("### Operational Alerts & Funnel Analysis")
    col1, col2 = st.columns([1, 1.5])
    
    with col1:
        # Funnel
        funnel_query = f"""
            SELECT 
                COUNT(DISTINCT s.order_id) as "1_CREATED",
                COUNT(DISTINCT CASE WHEN o.total_payment_value > 0 THEN o.order_id END) as "2_PAID",
                COUNT(DISTINCT CASE WHEN o.delivery_days IS NOT NULL THEN o.order_id END) as "3_DELIVERED"
            FROM fact_sales s JOIN fact_orders o ON s.order_id = o.order_id JOIN dim_customers c ON s.customer_id = c.customer_id JOIN dim_products p ON s.product_id = p.product_id JOIN dim_date d ON s.date_key = d.date_key
            WHERE {where_clause}
        """
        funnel_res = run_query(funnel_query).iloc[0]
        funnel_stages = pd.DataFrame({"Stage": ["Orders Created", "Payments Cleared", "Orders Delivered"], "Value": [funnel_res["1_CREATED"], funnel_res["2_PAID"], funnel_res["3_DELIVERED"]]})
        
        fig_funnel = go.Figure(go.Funnel(y=funnel_stages["Stage"], x=funnel_stages["Value"], textinfo="value+percent initial", marker=dict(color=[tokens["accent"], tokens["accent_alt"], "#84cc16"])))
        fig_funnel.update_layout(template=tokens["plotly"], title="Logistical Conversion Funnel", height=350)
        st.plotly_chart(fig_funnel, use_container_width=True)

    with col2:
        # Anomaly Detection Queries
        anomaly_freight_query = f"""
            SELECT COUNT(s.order_id) as HIGH_FREIGHT 
            FROM fact_sales s 
            JOIN dim_customers c ON s.customer_id = c.customer_id 
            JOIN dim_products p ON s.product_id = p.product_id 
            JOIN dim_date d ON s.date_key = d.date_key 
            JOIN fact_orders o ON s.order_id = o.order_id
            WHERE {where_clause} AND freight_value > (price * 0.5)
        """
        high_freight = run_query(anomaly_freight_query).iloc[0]["HIGH_FREIGHT"]
        
        anomaly_delay_query = f"""
            SELECT COUNT(o.order_id) as SEVERE_DELAYS 
            FROM fact_orders o 
            JOIN fact_sales s ON o.order_id = s.order_id 
            JOIN dim_customers c ON o.customer_id = c.customer_id 
            JOIN dim_products p ON s.product_id = p.product_id 
            JOIN dim_date d ON s.date_key = d.date_key
            WHERE {where_clause} AND delivery_days > 21
        """
        severe_delays = run_query(anomaly_delay_query).iloc[0]["SEVERE_DELAYS"]

        st.markdown(f"""
        <div class="alert-card" style="margin-bottom: 1rem;">
            <h4>⚠️ Freight Outliers Detected</h4>
            <p><strong>{int(high_freight):,} items</strong> were flagged where shipping costs exceeded 50% of the item's purchase price. Investigate logistical contracts in impacted regions.</p>
        </div>
        """, unsafe_allow_html=True)

        st.markdown(f"""
        <div class="alert-card">
            <h4>🚨 Severe SLA Breaches</h4>
            <p><strong>{int(severe_delays):,} orders</strong> took longer than 21 days to deliver. Check the Conversion Funnel drop-off to correlate with fulfillment centers.</p>
        </div>
        """, unsafe_allow_html=True)