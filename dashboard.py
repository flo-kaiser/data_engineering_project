import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime

# --- Page Configuration ---
st.set_page_config(
    page_title="Gold Intelligence Dashboard",
    page_icon="🏆",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Styling ---
st.markdown("""
    <style>
    .main {
        background-color: #f5f5f5;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    </style>
    """, unsafe_allow_html=True)

# --- Data Loading ---
@st.cache_data
def load_data():
    # Try environment variable first, then fallback to common local paths
    db_path = os.getenv('DUCKDB_PATH')
    if not db_path:
        paths_to_try = [
            'gold_dbt/data/gold_market_local.duckdb',
            'gold_dbt/data/gold_market.duckdb'
        ]
        for p in paths_to_try:
            if os.path.exists(p):
                db_path = p
                break
        
    if not db_path:
        db_path = 'gold_dbt/data/gold_market.duckdb'

    con = duckdb.connect(db_path)
    
    # DEBUG INFO (Visible in Streamlit if it fails)
    tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").df()
    if 'stg_gold_prices' not in tables['table_name'].values:
        available_tables = ", ".join(tables['table_name'].tolist())
        raise ValueError(f"Database '{db_path}' found, but table 'stg_gold_prices' is missing. Available tables in 'main': {available_tables}")

    # Load Daily Gold Prices
    df_daily = con.execute("SELECT * FROM stg_gold_prices ORDER BY price_date ASC").df()
    df_daily['price_date'] = pd.to_datetime(df_daily['price_date'])

    # Load Market Summary (Monthly)
    df_summary = con.execute("SELECT * FROM main.fct_market_summary ORDER BY month ASC").df()
    df_summary['month'] = pd.to_datetime(df_summary['month'])
    
    # Load Valuation Index
    df_valuation = con.execute("SELECT * FROM main.fct_gold_valuation_index ORDER BY month ASC").df()
    df_valuation['month'] = pd.to_datetime(df_valuation['month'])

    # Load Macro Drivers
    df_drivers = con.execute("SELECT * FROM main.fct_gold_market_drivers ORDER BY market_month ASC").df()
    df_drivers['market_month'] = pd.to_datetime(df_drivers['market_month'])

    # Load Mining Data
    df_mining = con.execute("SELECT * FROM main.fct_mining_vs_price ORDER BY market_year ASC").df()
    
    con.close()
    return df_daily, df_summary, df_valuation, df_drivers, df_mining

try:
    df_daily, df_summary, df_valuation, df_drivers, df_mining = load_data()
    
    # --- Sidebar ---
    st.sidebar.title("🏆 Gold Intelligence")
    st.sidebar.markdown("Enterprise Market Framework")
    st.sidebar.divider()
    
    # Date Filter - Now using daily resolution for the max date
    min_date = df_daily['price_date'].min().date()
    max_date = df_daily['price_date'].max().date()
    
    start_date, end_date = st.sidebar.slider(
        "Select Time Period",
        min_value=min_date,
        max_value=max_date,
        value=(min_date, max_date)
    )
    
    # Apply Filtering
    df_daily_filtered = df_daily[
        (df_daily['price_date'].dt.date >= start_date) & 
        (df_daily['price_date'].dt.date <= end_date)
    ]

    df_filtered = df_summary[
        (df_summary['month'].dt.date >= start_date) & 
        (df_summary['month'].dt.date <= end_date)
    ]
    
    df_val_filtered = df_valuation[
        (df_valuation['month'].dt.date >= start_date) & 
        (df_valuation['month'].dt.date <= end_date)
    ]

    # --- Main Dashboard ---
    st.title("🏆 Gold Market Intelligence Dashboard")
    
    tab1, tab2, tab3 = st.tabs(["📊 Market Overview", "📈 Macro Drivers", "⚒️ Supply & Demand"])

    with tab1:
        # Top Metrics - Decoupled to show latest available for EACH metric
        col1, col2, col3, col4 = st.columns(4)
        
        # 1. Gold Price (Latest ACTUAL daily price)
        latest_price_row = df_daily.iloc[-1]
        # Previous price for delta (yesterday)
        prev_price_row = df_daily.iloc[-2] if len(df_daily) > 1 else latest_price_row
        price_delta = ((latest_price_row['price_usd_per_oz'] - prev_price_row['price_usd_per_oz']) / prev_price_row['price_usd_per_oz']) * 100
        col1.metric("Gold Price (Spot/oz)", f"${latest_price_row['price_usd_per_oz']:,.2f}", f"{price_delta:.1f}%")
        
        # 2. 12M Correlation (Latest available monthly)
        df_corr_latest = df_summary.dropna(subset=['rolling_corr_12m'])
        latest_corr = df_corr_latest.iloc[-1]['rolling_corr_12m'] if not df_corr_latest.empty else 0
        col2.metric("12M Correlation", f"{latest_corr:.2f}")
        
        # 3. Valuation Score (Latest available monthly)
        df_val_latest = df_valuation.dropna(subset=['valuation_score'])
        latest_val = df_val_latest.iloc[-1]['valuation_score'] if not df_val_latest.empty else 0
        col3.metric("Valuation Score", f"{latest_val:.1f}")
        
        # 4. Global Reserves (Latest available monthly)
        df_res_latest = df_summary.dropna(subset=['total_gold_reserves_tonnes'])
        latest_res = df_res_latest.iloc[-1]['total_gold_reserves_tonnes'] if not df_res_latest.empty else 0
        col4.metric("Global Reserves (t)", f"{latest_res:,.0f}")

        st.divider()
        c1, c2 = st.columns(2)
        with c1:
            # Using DAILY filtered data for higher resolution price trend
            fig_price = px.line(df_daily_filtered, x='price_date', y='price_usd_per_oz', title='Gold Daily Spot Price Trend')
            st.plotly_chart(fig_price, width='stretch')
        with c2:
            fig_val = px.area(df_val_filtered, x='month', y='valuation_score', title='Gold Valuation Index (Monthly)')
            st.plotly_chart(fig_val, width='stretch')

    with tab2:
        st.subheader("Macro-Economic Impact Factors")
        col_m1, col_m2 = st.columns(2)
        
        with col_m1:
            # ETF Chart: Filter to start at 2005
            df_etf = df_drivers[df_drivers['market_month'] >= '2005-01-01']
            fig_etf = px.bar(df_etf, x='market_month', y='total_etf_flow_usd_mn', title='Gold Market Activity Proxy (ETFs since 2005)')
            st.plotly_chart(fig_etf, width='stretch')
        
        with col_m2:
            # DXY Chart: Filter to start at 1971
            df_dxy = df_drivers[df_drivers['market_month'] >= '1971-01-01']
            fig_dxy = px.line(df_dxy, x='market_month', y='avg_dxy', title='US Dollar Index (DXY) Trend since 1971')
            st.plotly_chart(fig_dxy, width='stretch')

    with tab3:
        st.subheader("Global Supply Fundamentals")
        fig_mining = go.Figure()
        fig_mining.add_trace(go.Bar(x=df_mining['market_year'], y=df_mining['total_mining_production_tonnes'], name="Mining Production (t)"))
        fig_mining.add_trace(go.Scatter(x=df_mining['market_year'], y=df_mining['avg_gold_price_usd'], name="Avg Price (R)", yaxis="y2"))
        
        fig_mining.update_layout(
            title="Mining Production vs. Gold Price",
            yaxis=dict(title="Tonnes"),
            yaxis2=dict(title="Price (USD)", overlaying="y", side="right"),
            legend=dict(x=0.01, y=0.99)
        )
        st.plotly_chart(fig_mining, width='stretch')

    # Data Deep Dive
    with st.expander("🔍 View Raw Daily Analytical Data"):
        st.dataframe(df_daily_filtered.sort_values('price_date', ascending=False), width='stretch')

    # Ingestion Metadata / Lineage
    with st.expander("⛓️ Data Lineage & Freshness (Bronze Layer)"):
        con = duckdb.connect(db_path)
        df_meta = con.execute("SELECT * FROM bronze.ingestion_metadata ORDER BY last_updated DESC").df()
        con.close()
        
        st.markdown("### Latest Ingestion Jobs")
        st.dataframe(df_meta, width='stretch')
        
        # Simple health check based on metadata
        failed_jobs = df_meta[df_meta['status'] == 'FAILED']
        if not failed_jobs.empty:
            st.warning(f"Found {len(failed_jobs)} failed ingestion jobs. Check logs for details.")
        else:
            st.success("All recent ingestion jobs completed successfully.")

except Exception as e:
    st.error(f"Error loading dashboard: {str(e)}")
    st.info("Make sure the pipeline has been run successfully: `python main.py`")
    st.exception(e)

st.divider()
st.caption("GIF - Gold Intelligence Framework | Professional API Dashboard")
