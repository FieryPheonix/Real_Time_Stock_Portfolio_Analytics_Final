# Real-Time Stock Portfolio Analytics - Visualization Guide

## 1. Accessing the Dashboard
The dashboard is built with **Streamlit** and is orchestrated by Airflow. 

- **URL**: [http://localhost:8501](http://localhost:8501)
- **Status**: It starts automatically as part of the Airflow DAG (`start_visualization_service` task).
- **Manual Start**: If you need to restart it manually inside the container:
  ```bash
  docker exec -it airflow-worker bash
  streamlit run /opt/airflow/scripts/dashboard.py --server.port 8501 --server.address 0.0.0.0
  ```

## 2. Dashboard Features
The dashboard is organized into navigable sections in the sidebar:

### A. Core Visualizations (Refreshed Daily)
1. **Trading Volume by Ticker**: Bar chart of total volume.
2. **Price Trends by Sector**: Average stock price per sector.
3. **Buy vs Sell Transactions**: Pie chart of transaction types.
4. **Trading Activity by Day**: Operations count by day of the week.
5. **Customer Trade Distribution**: Box plots of trade amounts by customer type.
6. **Top 10 Customers**: Vertical bar chart of highest volume traders.

### B. Real-time Streaming Monitor (Live)
- **Live Data**: Shows the last 100 transactions streaming from Kafka (simulated).
- **Filters**: Filter by Ticker and Sector.
- **Export**: Download the filtered streaming data as CSV.
- **Liquidity Tier**: Real-time classification of trades (High/Medium/Low qty).

### C. Sector Comparison Dashboard
- **Hierarchy**: Funnel chart of sector performance.
- **Comparison**: Horizontal bar chart for easy comparison.

### D. Holiday vs Non-Holiday Patterns
- **Impact Analysis**: Comparative view of transaction volume on Holidays vs Regular days.

## 3. Filters
- **Stock Ticker**: Multi-select (Defaults to All).
- **Sector**: Filter specific industries.
- **Customer Type**: Retail vs Institutional.
- **Auto-Refresh**: Toggle to pause/resume live updates (default 30s).
