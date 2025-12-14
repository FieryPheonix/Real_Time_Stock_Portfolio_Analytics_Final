import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import time
from datetime import datetime, timedelta

# ==========================================
# CONFIGURATION & SETUP
# ==========================================
st.set_page_config(page_title="Real-Time Stock Analytics", layout="wide", page_icon="📈")

# Database Connection
DB_URL = 'postgresql://root:root@pgdatabase:5432/stockanalytics'

@st.cache_resource
def get_engine():
    return create_engine(DB_URL)

engine = get_engine()

def load_data(table_name):
    try:
        return pd.read_sql(f"SELECT * FROM {table_name}", engine)
    except Exception as e:
        return pd.DataFrame()

# ==========================================
# SIDEBAR FILTERS
# ==========================================
st.sidebar.title("📊 Dashboard Settings")
st.sidebar.markdown("---")

# 1. Date Range Filter
st.sidebar.subheader("📅 Date Range")
today = datetime.today()
last_week = today - timedelta(days=7)
date_range = st.sidebar.date_input("Select Range", [last_week, today])

# 2. Global Filters (Ticker, Sector, Customer Type)
st.sidebar.subheader("🔍 Global Filters")
# Load reference data for filters
df_ref_ticker = load_data("spark_analytics_1")
df_ref_sector = load_data("spark_analytics_2")
df_ref_cust = load_data("spark_sql_2")

# Ticker Filter
all_tickers = df_ref_ticker['stock_ticker'].unique().tolist() if not df_ref_ticker.empty else []
selected_tickers = st.sidebar.multiselect("Stock Ticker", all_tickers, default=all_tickers[:5])

# Sector Filter
all_sectors = df_ref_sector['stock_sector'].unique().tolist() if not df_ref_sector.empty else []
selected_sectors = st.sidebar.multiselect("Sector", all_sectors, default=all_sectors)

# Customer Type Filter
all_cust_types = df_ref_cust['customer_account_type'].unique().tolist() if not df_ref_cust.empty else []
selected_cust_types = st.sidebar.multiselect("Customer Type", all_cust_types, default=all_cust_types)

st.sidebar.markdown("---")
refresh_rate = st.sidebar.slider("Refresh Rate (seconds)", 5, 60, 30)
auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh", value=True)

if st.sidebar.button("🔄 Hard Refresh Data (Database)"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("### Navigation")
view_mode = st.sidebar.radio("Select View", ["Core Visualizations", "Advanced Visualizations", "Detailed Analysis"])

# Helper function to filter data
def filter_dataframe(df, col_ticker=None, col_sector=None, col_cust_type=None):
    if df.empty: return df
    if col_ticker and col_ticker in df.columns and selected_tickers:
        df = df[df[col_ticker].isin(selected_tickers)]
    if col_sector and col_sector in df.columns and selected_sectors:
        df = df[df[col_sector].isin(selected_sectors)]
    if col_cust_type and col_cust_type in df.columns and selected_cust_types:
        df = df[df[col_cust_type].isin(selected_cust_types)]
    return df

# ==========================================
# CORE VISUALIZATIONS
# ==========================================
if view_mode == "Core Visualizations":
    st.title("📉 Core Visualizations")
    st.markdown("---")
    
    # Load & Filter Data
    df_vol_ticker = filter_dataframe(load_data("spark_analytics_1"), col_ticker='stock_ticker')
    df_price_sector = filter_dataframe(load_data("spark_analytics_2"), col_sector='stock_sector')
    df_trans_type = load_data("spark_analytics_3") # No filters applicable usually
    df_daily_activity = load_data("spark_analytics_5") 
    df_cust_dist = filter_dataframe(load_data("spark_sql_2"), col_cust_type='customer_account_type')
    df_top_cust = load_data("spark_analytics_4")

    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("1. Trading Volume by Stock Ticker")
        if not df_vol_ticker.empty:
            fig1 = px.bar(df_vol_ticker, x='stock_ticker', y='total_trading_volume', 
                          title="Volume per Ticker", color='total_trading_volume')
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("No data available for selected filters.")

    with col2:
        st.subheader("2. Stock Price Trends by Sector")
        if not df_price_sector.empty:
            fig2 = px.bar(df_price_sector, x='stock_sector', y='average_price', 
                          title="Average Price by Sector", color='stock_sector')
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No data available for selected filters.")

    st.markdown("---")
    col3, col4 = st.columns(2)
    
    with col3:
        st.subheader("3. Buy vs Sell Transactions")
        if not df_trans_type.empty:
            fig3 = px.pie(df_trans_type, names='transaction_type', values='count', 
                          title="Buy vs Sell", hole=0.4)
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("No data available.")

    with col4:
        st.subheader("4. Trading Activity by Day of Week")
        if not df_daily_activity.empty:
            days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            df_daily_activity['day_name'] = pd.Categorical(df_daily_activity['day_name'], categories=days_order, ordered=True)
            df_daily_activity.sort_values('day_name', inplace=True)
            fig4 = px.line(df_daily_activity, x='day_name', y='total_amount', markers=True,
                           title="Weekly Activity Trend")
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info("No data available.")

    st.markdown("---")
    col5, col6 = st.columns(2)
    
    with col5:
        st.subheader("5. Customer Transaction Distribution")
        if not df_cust_dist.empty:
            fig5 = px.box(df_cust_dist, x='customer_account_type', y='avg_trade_amount', 
                          title="Trade Amount Distribution by Type", points="all")
            st.plotly_chart(fig5, use_container_width=True)
        else:
            st.info("No data available.")

    with col6:
        st.subheader("6. Top 10 Customers by Trade Amount")
        if not df_top_cust.empty:
            fig6 = px.bar(df_top_cust.head(10), x='customer_id', y='transaction_count',
                          title="Top Active Customers")
            st.plotly_chart(fig6, use_container_width=True)
        else:
            st.info("No data available.")

# ==========================================
# ADVANCED VISUALIZATIONS
# ==========================================
elif view_mode == "Advanced Visualizations":
    st.header("🚀 Advanced Visualizations")
    st.markdown("---")

    # Load Data
    df_vol_ticker = filter_dataframe(load_data("spark_analytics_1"), col_ticker='stock_ticker')
    df_price_sector = filter_dataframe(load_data("spark_analytics_2"), col_sector='stock_sector')
    df_trans_type = load_data("spark_analytics_3")
    
    st.subheader("📊 2. Portfolio Performance Metrics")
    kpi1, kpi2, kpi3 = st.columns(3)
    
    total_vol = df_vol_ticker['total_trading_volume'].sum() if not df_vol_ticker.empty else 0
    avg_price = df_price_sector['average_price'].mean() if not df_price_sector.empty else 0
    total_trans = df_trans_type['count'].sum() if not df_trans_type.empty else 0

    kpi1.metric("Total Market Volume", f"${total_vol:,.0f}")
    kpi2.metric("Avg Sector Price", f"${avg_price:,.2f}")
    kpi3.metric("Weekend Transactions", f"{total_trans:,}")
    
    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("🏢 3. Sector Comparison")
        if not df_price_sector.empty:
            fig7 = px.funnel(df_price_sector, x='average_price', y='stock_sector', 
                             title="Sector Price Hierarchy", template="plotly_white")
            st.plotly_chart(fig7, use_container_width=True)
        else:
            st.info("No Sector data available.")

    with col2:
        st.subheader("📅 4. Holiday vs Non-Holiday Patterns")
        df_sql_holiday = load_data("spark_sql_3")
        if not df_sql_holiday.empty:
            df_sql_holiday['is_holiday_str'] = df_sql_holiday['is_holiday'].map({True: 'Holiday', False: 'Regular Day'})
            fig8 = px.bar(df_sql_holiday, x='is_holiday_str', y='transaction_count', color='is_holiday_str',
                          title="Holiday Trading Volume", template="plotly_white")
            st.plotly_chart(fig8, use_container_width=True)
        else:
            st.info("No Holiday data available.")

    st.markdown("---")
    st.subheader("📡 1. Real-time Streaming Monitor & 💧 5. Liquidity Tier Analysis")
    
    try:
        DATA_PATH = '/opt/airflow/data/visualization/dashboard_data.csv'
        df_raw = pd.read_csv(DATA_PATH)
        
        # Apply Sidebar Filters to Streaming Data
        if selected_tickers:
            df_raw = df_raw[df_raw['stock_ticker'].isin(selected_tickers)]
        if selected_sectors:
            df_raw = df_raw[df_raw['stock_sector'].isin(selected_sectors)]
            
        col_mon, col_liq = st.columns([2, 1])
        
        with col_mon:
            st.markdown(f"**Live Transactions ({len(df_raw)} rows)** - *Filtered*")
            st.dataframe(df_raw.sort_index(ascending=False).head(100), height=400, use_container_width=True)
            
            # EXPORT FUNCTIONALITY (Bonus)
            st.markdown("### Export")
            csv_data = df_raw.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download Filtered Data (CSV)", data=csv_data, file_name="filtered_stock_data.csv", mime="text/csv")
            st.caption("Use the camera icon in charts to download as PNG.")
            
        with col_liq:
            def classify_liquidity(qty):
                if qty > 5000: return 'High'
                elif qty > 1000: return 'Medium'
                else: return 'Low'
            
            df_raw['Liquidity'] = df_raw['quantity'].apply(classify_liquidity)
            liq_counts = df_raw['Liquidity'].value_counts().reset_index()
            liq_counts.columns = ['Tier', 'Count']
            
            fig9 = px.pie(liq_counts, values='Count', names='Tier', title="Liquidity Tier Distribution", hole=0.3)
            st.plotly_chart(fig9, use_container_width=True)
            
    except Exception as e:
        st.warning("Streaming data file not found or empty.")

# ==========================================
# DETAILED ANALYSIS
# ==========================================
elif view_mode == "Detailed Analysis":
    st.header("Deep Dive Analysis")
    st.dataframe(load_data("spark_sql_1"), use_container_width=True)

if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()
