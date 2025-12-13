# How to run the pipeline
- Note that it you have to start by having your terminal be in this project's folder then:

1. In the terminal write: `docker compose up --build`
2. In the browser type: `localhost:8080`
3. The last step will prompt an airflow login page, login using: `username: airflow`, `password: airflow`
4. After logging in, you'll see a DAG named stock_portfolio_pipeline_datafrogs, press on the run button located at the right of it to run the pipeline
5. (Optional) To see the graph, press on stock_portfolio_pipeline_datafrogs to see more information, then on the right menu choose graphs to see the full execution graph
`