# Real-Time Stock Portfolio Analytics Pipeline

This project implements an end-to-end data engineering pipeline featuring real-time Kafka streaming, Spark analytics, Airflow orchestration, and an AI-powered Interactive Dashboard.

---

## Architecture


---

## How to Run the Pipeline

### 1. Prerequisites
- **Docker & Docker Compose** installed.
- **HUGGINGFACEHUB_API_TOKEN** set in your `.env` file (for the AI Agent).

### 2. Start Services
Run the following command in the project root to build and start the entire stack:
```bash
docker-compose up -d --build
```
*Wait a few minutes for all services (Airflow, Kafka, Spark, Postgres) to initialized.*

### 3. Access Airflow UI
Manage your pipeline DAGs here.
- **URL**: [http://localhost:8080](http://localhost:8080)
- **Credentials**: `airflow` / `airflow`
- **Action**: Locate the DAG `stock_portfolio_pipeline_datafrogs` and toggle it **ON**.

---

## How to View the Dashboard
The dashboard provides real-time visualization, historical analytics, and the AI Analyst interface.

- **URL**: [http://localhost:8501](http://localhost:8501)
- **Features**:
    - **Core Visualizations**: Volume, Price Trends, Top Customers.
    - **Real-Time Monitor**: Live streaming table updates.
    - **Sector Dashboard**: Comparative analysis of sectors.
    - **Holiday Analysis**: Holiday vs Regular day trading patterns.
    - **AI Analyst**: Interactive Chat-to-SQL interface.

---

## How to Test the AI Agent

You can interact with the AI Agent in two ways:

### A. Interactive Mode (UI - Recommended)
1. Open the Dashboard at [http://localhost:8501](http://localhost:8501).
2. Select **"AI Analyst 🤖"** from the sidebar navigation.
3. Type a natural language question in the chat box.
   - *Example: "What was the total trading volume for Technology stocks in 2024?"*
   - *Example: "Show me the top 5 customers by trade amount."*
4. The Agent will:
   - Generate the correct SQL query.
   - Execute it against the dataset.
   - Display the results in a table.
   - Provide a **"Download CSV"** button for the results.

### B. Autonomous Mode (Airflow)
1. Edit the file `agents/user_query.txt` and write your question there.
2. Trigger the Airflow DAG.
3. The `process_with_ai_agent` task will read the file, process it, and save logs to `agents/AGENT_LOGS.JSON`.

---

## Project Structure
- **`dags/`**: Contains the Airflow DAG definition (`airflow.py`) and the Agent Module (`ai_agent.py`).
- **`scripts/`**: Contains the Streamlit Dashboard (`dashboard.py`) and Kafka Producer.
- **`data/`**: Source datasets.
- **`agents/`**: Logs and inputs for the AI Agent.