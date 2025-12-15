# Real-Time Stock Portfolio Analytics Pipeline (Milestone 3)

## Project Overview
This project implements an end-to-end data engineering pipeline using Apache Airflow, Spark, Kafka, and PostgreSQL. It features real-time data streaming, batch processing, and an interactive Streamlit dashboard.

## Architecture
1.  **Ingestion**: Kafka Producer streams data from CSV.
2.  **Orchestration**: Airflow DAG `stock_portfolio_pipeline_datafrogs` manages the workflow.
3.  **Processing**:
    *   **Spark**: Performs complex aggregations and analytics.
    *   **Pandas**: Handles data cleaning and integration.
4.  **Storage**: PostgreSQL (`stockanalytics` database).
5.  **Visualization**: Streamlit Dashboard.
6.  **AI Agent**: GenAI agent converts natural language queries to SQL.

## How to Run

1.  **Start Services**:
    ```bash
    docker-compose up -d --build
    ```
    *Note: A restart is required if you are adding the `agents` volume for the first time.*

2.  **Access Airflow**:
    *   URL: [http://localhost:8080](http://localhost:8080)
    *   User/Pass: `airflow` / `airflow`
    *   Trigger the DAG `stock_portfolio_pipeline_datafrogs`.

3.  **Access Dashboard**:
    *   URL: [http://localhost:8501](http://localhost:8501)
    *   (See `VISUALIZATION.md` for details).

4.  **AI Agent Usage**:
    *   Place your question in `agents/user_query.txt`.
    *   The DAG's `process_with_ai_agent` task will read this file.
    *   Check `agents/AGENT_LOGS.JSON` for the generated SQL response.
    *   *(Requires `HUGGINGFACEHUB_API_TOKEN` in environment for real generation, otherwise uses mock).*

## Project Structure
- `dags/airflow.py`: Main DAG definition and task logic.
- `scripts/dashboard.py`: Streamlit dashboard application.
- `scripts/start_kafka_producer.py`: Kafka producer.
- `agents/`: Directory for AI Agent inputs/logs.
- `docker-compose.yaml`: Infrastructure definition.

## Testing
- **Pipeline**: Verify all tasks turn green in Airflow Grid View.
- **Data**: Check PostgreSQL tables `spark_analytics_X` and `spark_sql_X`.
- **Visualization**: Verify charts load and filters work in Streamlit.