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
all_tickers = sorted(df_ref_ticker['stock_ticker'].unique().tolist()) if not df_ref_ticker.empty else []
selected_tickers = st.sidebar.multiselect("Stock Ticker", all_tickers, default=all_tickers)

# Sector Filter
all_sectors = df_ref_sector['stock_sector'].unique().tolist() if not df_ref_sector.empty else []
selected_sectors = st.sidebar.multiselect("Sector", all_sectors, default=all_sectors)

# Customer Type Filter
all_cust_types = df_ref_cust['customer_account_type'].unique().tolist() if not df_ref_cust.empty else []
selected_cust_types = st.sidebar.multiselect("Customer Type", all_cust_types, default=all_cust_types)

st.sidebar.markdown("---")
# DEBUG: Check values
# st.sidebar.write("SQL Cust Types:", all_cust_types)
# st.sidebar.write("Selected:", selected_cust_types)

refresh_rate = st.sidebar.slider("Refresh Rate (seconds)", 5, 60, 30)
auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh", value=True)

if st.sidebar.button("🔄 Hard Refresh Data (Database)"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("### Navigation")
view_mode = st.sidebar.radio("Select View", [
    "Core Visualizations", 
    "Real-time Streaming Monitor",
    "Sector Comparison Dashboard", 
])

# Helper function to filter data
def filter_dataframe(df, col_ticker=None, col_sector=None, col_cust_type=None, date_col=None):
    if df.empty: return df
    if col_ticker and col_ticker in df.columns and selected_tickers:
        df = df[df[col_ticker].isin(selected_tickers)]
    if col_sector and col_sector in df.columns and selected_sectors:
        df = df[df[col_sector].isin(selected_sectors)]
    if col_cust_type and col_cust_type in df.columns and selected_cust_types:
        df = df[df[col_cust_type].isin(selected_cust_types)]
    
    # Apply Date Filter
    if date_col and date_col in df.columns and date_range:
        try:
            # Ensure it's datetime
            if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            
            # Handle Single Date vs Range
            start_date = pd.to_datetime(date_range[0])
            end_date = pd.to_datetime(date_range[1]) if len(date_range) > 1 else start_date
            
            df = df[
                (df[date_col].dt.date >= start_date.date()) & 
                (df[date_col].dt.date <= end_date.date())
            ]
        except Exception:
            pass # Ignore date filter errors
            
    return df

# ==========================================
# CENTRALIZED DATA LOADING (CSV)
# ==========================================
@st.cache_data
def load_and_process_full_data():
    csv_path = '/opt/airflow/data/streaming_dataset/full_stocks.csv'
    try:
        # Load ALL needed columns at once to handle filters and charts
        # Note: stock_sector is one-hot encoded in this CSV
        cols = [
            'stock_ticker', 'total_trade_amount', 'day_name', 'customer_id', 'timestamp', 'stock_price',
            'transaction_type_BUY', 'transaction_type_SELL',
            'customer_account_type_Institutional', 'customer_account_type_Retail',
            'stock_sector_Consumer', 'stock_sector_Energy', 'stock_sector_Finance', 
            'stock_sector_Healthcare', 'stock_sector_Technology' 
        ]
        # Force string types for IDs to match SQL/Filter types (which are often strings in Streamlit)
        dtype_map = {'stock_ticker': str, 'customer_id': str}
        
        df = pd.read_csv(csv_path, usecols=cols, dtype=dtype_map)
        
        # 0. Format Ticker to match SQL Filter Format (e.g. 1 -> STK001)
        # SQL uses 'STK' + 3-digit zero-padded ID. CSV has raw int ID.
        def format_ticker(val):
             try:
                 return f"STK{int(val):03d}"
             except:
                 return str(val)
        df['stock_ticker'] = df['stock_ticker'].apply(format_ticker)
        
        # Helper to safely check Truthy values (1, True, 'True')
        def is_true(val):
            return val == 1 or val == True or str(val).lower() == 'true'

        # 1. Reverse One-Hot for Customer Type
        def get_cust_type(row):
            if is_true(row.get('customer_account_type_Institutional')): return 'Institutional'
            elif is_true(row.get('customer_account_type_Retail')): return 'Retail'
            return 'Unknown'
        df['customer_account_type'] = df.apply(get_cust_type, axis=1)

        # 2. Reverse One-Hot for Transaction Type
        def get_trans_type(row):
            if is_true(row.get('transaction_type_BUY')): return 'BUY'
            elif is_true(row.get('transaction_type_SELL')): return 'SELL'
            return 'Unknown'
        df['transaction_type'] = df.apply(get_trans_type, axis=1)

        # 3. Reverse One-Hot for Stock Sector
        def get_sector(row):
            if is_true(row.get('stock_sector_Consumer')): return 'Consumer'
            elif is_true(row.get('stock_sector_Energy')): return 'Energy'
            elif is_true(row.get('stock_sector_Finance')): return 'Finance'
            elif is_true(row.get('stock_sector_Healthcare')): return 'Healthcare'
            elif is_true(row.get('stock_sector_Technology')): return 'Technology'
            return 'Unknown'
        df['stock_sector'] = df.apply(get_sector, axis=1)
        
        # 4. Process Day Name
        day_map = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday", 4: "Friday", 5: "Saturday", 6: "Sunday"}
        df['day_name'] = pd.to_numeric(df['day_name'], errors='coerce')
        df['day_name_str'] = df['day_name'].map(day_map)

        return df

    except Exception as e:
        st.error(f"Global Data Load Error: {e}")
        return pd.DataFrame()

# Load and Filter Global Data ONCE
df_full_raw = load_and_process_full_data()
df_full_filtered = filter_dataframe(df_full_raw.copy(), col_ticker='stock_ticker', col_sector='stock_sector', col_cust_type='customer_account_type', date_col='timestamp')


# ==========================================
# CORE VISUALIZATIONS
# ==========================================
if view_mode == "Core Visualizations":
    st.title("📉 Core Visualizations")
    st.markdown("---")
    
    # Calculated Metrics from Filtered Data (Replaces Static SQL)
    
    # 1. Trading Volume by Stock Ticker
    if not df_full_filtered.empty:
        df_vol_ticker = df_full_filtered.groupby('stock_ticker')['total_trade_amount'].sum().reset_index()
        df_vol_ticker.columns = ['stock_ticker', 'total_trading_volume']
    else:
        df_vol_ticker = pd.DataFrame()

    # 2. Stock Price Trends by Sector (Average Price)
    if not df_full_filtered.empty:
        df_price_sector = df_full_filtered.groupby('stock_sector')['stock_price'].mean().reset_index()
        df_price_sector.columns = ['stock_sector', 'average_price']
    else:
        df_price_sector = pd.DataFrame()
    
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
        st.subheader("3. Buy vs Sell Transactions (Overall)")
        
        if not df_full_filtered.empty:
            # Use the pre-calculated 'transaction_type' or sum the columns from filtered data
            # Since we have reversed one-hot, we can just value_counts or sum bools.
            # Let's count rows per type from our new column for simplicity
            df_buy_sell = df_full_filtered['transaction_type'].value_counts().reset_index()
            df_buy_sell.columns = ['transaction_type', 'count']
            
            if not df_buy_sell.empty:
                fig3 = px.pie(df_buy_sell, names='transaction_type', values='count', 
                              title="Buy vs Sell (All Time)", hole=0.4)
                st.plotly_chart(fig3, use_container_width=True)
            else:
                 st.info("No BUY/SELL data found in filtered set.")
        else:
            st.info("No data available (check filters).")

    with col4:
        st.subheader("4. Trading Activity by Day of Week")
        
        if not df_full_filtered.empty:
            # Count transactions per day from filtered data
            df_counts = df_full_filtered['day_name_str'].value_counts().reset_index()
            df_counts.columns = ['day_name_str', 'total_activity']
            
            days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            
            if not df_counts.empty:
               fig4 = px.bar(df_counts, x='day_name_str', y='total_activity',
                              title="Daily Trading Activity (Count)",
                              category_orders={'day_name_str': days_order})
               st.plotly_chart(fig4, use_container_width=True)
            else:
               st.info("No daily data available.")
        else:
            st.info("No data available.")

    st.markdown("---")
    col5, col6 = st.columns(2)
    
    with col5:
        st.subheader("5. Customer Transaction Distribution")
        
        if not df_full_filtered.empty:
             # Chart 5: Distribution of Trade Amounts
             fig5 = px.box(df_full_filtered, x='customer_account_type', y='total_trade_amount', 
                           title="Trade Amount Distribution", points="outliers")
             st.plotly_chart(fig5, use_container_width=True)
        else:
             st.info("No customer data available.")

    with col6:
        st.subheader("6. Top 10 Customers by Trade Amount")
        
        if not df_full_filtered.empty:
             # Group by Customer ID and Sum Trade Amount
             df_top_10 = df_full_filtered.groupby('customer_id')['total_trade_amount'].sum().reset_index()
             # Sort descending for vertical bar (largest on left)
             df_top_10.sort_values('total_trade_amount', ascending=False, inplace=True)
             df_top_10 = df_top_10.head(10)
             
             # Convert ID to string for categorical axis
             df_top_10['customer_id'] = df_top_10['customer_id'].astype(str)

             fig6 = px.bar(df_top_10, x='customer_id', y='total_trade_amount',
                           title="Top 10 Customers (Volume)",
                           text_auto='.2s')
             fig6.update_layout(xaxis_title="Customer ID", yaxis_title="Total Trade Amount")
             st.plotly_chart(fig6, use_container_width=True)
        else:
             st.info("Customer data not available.")

# ==========================================
# 1. REAL-TIME STREAMING MONITOR
# ==========================================
elif view_mode == "Real-time Streaming Monitor":
    st.header("📡 Real-time Streaming Data Monitor")
    st.markdown("---")
    
    try:
        DATA_PATH = '/opt/airflow/data/visualization/dashboard_data.csv'
        if pd.io.common.file_exists(DATA_PATH):
            df_raw = pd.read_csv(DATA_PATH)
            
            # Apply Sidebar Filters to Streaming Data
            if selected_tickers:
                df_raw = df_raw[df_raw['stock_ticker'].isin(selected_tickers)]
            if selected_sectors:
                df_raw = df_raw[df_raw['stock_sector'].isin(selected_sectors)]
            if selected_cust_types:
                df_raw = df_raw[df_raw['customer_account_type'].isin(selected_cust_types)]
            
            # Apply Date Filter
            if date_range:
                try:
                    df_raw['timestamp_dt'] = pd.to_datetime(df_raw['timestamp'])
                    start_date = pd.to_datetime(date_range[0])
                    # Handle case where user picks one day or a range
                    end_date = pd.to_datetime(date_range[1]) if len(date_range) > 1 else start_date
                    
                    df_raw = df_raw[
                        (df_raw['timestamp_dt'].dt.date >= start_date.date()) & 
                        (df_raw['timestamp_dt'].dt.date <= end_date.date())
                    ]
                except Exception as e:
                    # If date parsing fails, just ignore date filter to avoid crashing
                    pass

            col_mon, col_liq = st.columns([2, 1])
            
            with col_mon:
                st.markdown(f"**Live Transactions ({len(df_raw)} rows)** - *Filtered*")
                st.dataframe(df_raw.sort_index(ascending=False).head(100), height=400, use_container_width=True)
                
                # EXPORT FUNCTIONALITY
                st.markdown("### Export")
                csv_data = df_raw.to_csv(index=False).encode('utf-8')
                st.download_button("📥 Download Filtered Data (CSV)", data=csv_data, file_name="filtered_stock_data.csv", mime="text/csv")
                
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

        else:
            st.warning("Streaming data file not found yet. Waiting for Spark job...")
    except Exception as e:
        st.error(f"Error reading streaming data: {e}")

# ==========================================
# 2. SECTOR COMPARISON DASHBOARD
# ==========================================
elif view_mode == "Sector Comparison Dashboard":
    st.header("🏢 Sector Comparison Dashboard")
    st.markdown("---")
    
    # Calculate Sector Metrics from Filtered Global Data
    if not df_full_filtered.empty:
        df_price_sector = df_full_filtered.groupby('stock_sector')['stock_price'].mean().reset_index()
        df_price_sector.columns = ['stock_sector', 'average_price']
    else:
        df_price_sector = pd.DataFrame()
    
    if not df_price_sector.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Sector Price Hierarchy")
            fig7 = px.funnel(df_price_sector, x='average_price', y='stock_sector', 
                             title="Avg Price by Sector", template="plotly_white")
            st.plotly_chart(fig7, use_container_width=True)
        
        with col2:
            st.subheader("Sector Comparison Bar")
            fig_sec = px.bar(df_price_sector, x='average_price', y='stock_sector', orientation='h',
                             title="Sector Average Price Comparison", color='stock_sector')
            st.plotly_chart(fig_sec, use_container_width=True)
    else:
        st.info("No Sector data available.")


# ==========================================
# DETAILED ANALYSIS
# ==========================================
elif view_mode == "Detailed Analysis":
    st.header("Deep Dive Analysis")
    st.dataframe(load_data("spark_sql_1"), use_container_width=True)

if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()
