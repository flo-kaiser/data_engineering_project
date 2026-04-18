import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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
    con = duckdb.connect('gold_dbt/data/gold_market.duckdb')
    
    # Load Market Summary
    df_summary = con.execute("SELECT * FROM main.fct_market_summary ORDER BY month ASC").df()
    
    # Load Valuation Index
    df_valuation = con.execute("SELECT * FROM main.fct_gold_valuation_index ORDER BY month ASC").df()
    
    con.close()
    return df_summary, df_valuation

try:
    df_summary, df_valuation = load_data()
    
    # --- Sidebar ---
    st.sidebar.title("🏆 Gold Intelligence")
    st.sidebar.markdown("Enterprise Market Framework")
    st.sidebar.divider()
    
    last_updated = df_summary['month'].max().strftime('%B %Y')
    st.sidebar.success(f"Data updated: {last_updated}")
    
    st.sidebar.subheader("Settings")
    date_range = st.sidebar.slider(
        "Select Date Range",
        min_value=df_summary['month'].min().to_pydatetime(),
        max_value=df_summary['month'].max().to_pydatetime(),
        value=(df_summary['month'].min().to_pydatetime(), df_summary['month'].max().to_pydatetime())
    )
    
    st.sidebar.divider()
    st.sidebar.subheader("Export Data")
    csv = df_summary.to_csv(index=False).encode('utf-8')
    st.sidebar.download_button(
        label="📥 Download Full Dataset (CSV)",
        data=csv,
        file_name='gold_intelligence_report.csv',
        mime='text/csv',
    )
    
    # Filter Data
    mask = (df_summary['month'] >= date_range[0]) & (df_summary['month'] <= date_range[1])
    df_filtered = df_summary.loc[mask]
    df_val_filtered = df_valuation[(df_valuation['month'] >= date_range[0]) & (df_valuation['month'] <= date_range[1])]

    # --- Main Dashboard ---
    st.title("🏆 Gold Market Intelligence Dashboard")
    st.markdown("Automated Analysis of Global Macro-Economic Drivers")
    
    # Top Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    latest = df_summary.iloc[-1]
    prev = df_summary.iloc[-2]
    
    price_delta = ((latest['avg_gold_price_usd'] - prev['avg_gold_price_usd']) / prev['avg_gold_price_usd']) * 100
    
    col1.metric("Gold Price (USD/oz)", f"${latest['avg_gold_price_usd']:,.2f}", f"{price_delta:.1f}%", help="Monthly average spot price in USD.")
    col2.metric("12M Correlation", f"{latest['rolling_corr_12m']:.2f}", help="Pearson correlation between Gold and 10Y Real Rates. Negative correlation is typical for 'Safe Haven' behavior.")
    
    latest_val = df_valuation.iloc[-1]
    prev_val = df_valuation.iloc[-2]
    val_delta = latest_val['valuation_score'] - prev_val['valuation_score']
    
    col3.metric("Valuation Score", f"{latest_val['valuation_score']:.1f}", f"{val_delta:.1f}", help="Composite index (0-100) based on Reserves, FX, and Interest Rate Correlation.")
    col4.metric("Global Reserves (t)", f"{latest['total_gold_reserves_tonnes']:,.0f}", help="Total official gold holdings by central banks in metric tonnes.")

    st.divider()

    # Charts Row 1
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("Gold Price Trend (USD)")
        fig_price = px.line(df_filtered, x='month', y='avg_gold_price_usd', 
                           title='Monthly Average Gold Spot Price',
                           labels={'avg_gold_price_usd': 'Price (USD)', 'month': 'Month'})
        fig_price.update_traces(line_color='#FFD700', line_width=3)
        st.plotly_chart(fig_price, use_container_width=True)

    with c2:
        st.subheader("Gold Valuation Index")
        fig_val = px.area(df_val_filtered, x='month', y='valuation_score', 
                         title='Composite Valuation Score (0-100)',
                         labels={'valuation_score': 'Score', 'month': 'Month'})
        fig_val.update_traces(fillcolor='rgba(255, 215, 0, 0.3)', line_color='#DAA520')
        st.plotly_chart(fig_val, use_container_width=True)

    # Charts Row 2
    st.subheader("Drivers & Correlation")
    
    # Dual Axis Chart for Correlation vs Price
    fig_corr = go.Figure()
    fig_corr.add_trace(go.Scatter(x=df_filtered['month'], y=df_filtered['avg_gold_price_usd'],
                                 name="Gold Price (L)", yaxis="y1", line=dict(color="#FFD700")))
    fig_corr.add_trace(go.Scatter(x=df_filtered['month'], y=df_filtered['rolling_corr_12m'],
                                 name="12M Correlation (R)", yaxis="y2", line=dict(color="#4682B4", dash='dot')))
    
    fig_corr.update_layout(
        title="Gold Price vs. Real Rate Correlation",
        yaxis=dict(title="Gold Price (USD)"),
        yaxis2=dict(title="Correlation", overlaying="y", side="right", range=[-1, 1]),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    st.plotly_chart(fig_corr, use_container_width=True)

    # Data Deep Dive
    with st.expander("🔍 View Raw Analytical Data"):
        st.dataframe(df_filtered.sort_values('month', ascending=False), use_container_width=True)

except Exception as e:
    st.error(f"Error loading dashboard: {str(e)}")
    st.info("Make sure the pipeline has been run successfully: `python main.py`")

st.divider()
st.caption("GIF - Gold Intelligence Framework | Professional Edition")
