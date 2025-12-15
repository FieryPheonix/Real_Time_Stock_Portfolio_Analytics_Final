import os
import time
import json
import pandas as pd
import sqlite3
from datetime import datetime
from huggingface_hub import InferenceClient
from dotenv import load_dotenv

load_dotenv()

# Constants for Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'streaming_dataset')
AGENTS_DIR = os.path.join(BASE_DIR, 'agents')
QUERY_FILE = os.path.join(AGENTS_DIR, 'user_query.txt')
LOG_FILE = os.path.join(AGENTS_DIR, 'AGENT_LOGS.JSON')
CSV_FILE = os.path.join(DATA_DIR, 'full_stocks.csv')
OUTPUT_DIR = os.path.join(AGENTS_DIR, 'outputs')

# Ensure output dir exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

class StockAgent:
    def __init__(self):
        self.api_key = os.getenv("HUGGINGFACEHUB_API_TOKEN")
        if not self.api_key:
            raise ValueError("HUGGINGFACEHUB_API_TOKEN not found in environment variables. Please check your .env file.")
        self.client = InferenceClient(token=self.api_key)
        self.models = [
            "Qwen/Qwen2.5-Coder-32B-Instruct",
            "meta-llama/Meta-Llama-3-8B-Instruct",
            "HuggingFaceH4/zephyr-7b-beta"
        ]

    # --- TOOLS ---
    def tool_read_query_file(self):
        """Tool to read the user query from a text file."""
        try:
            with open(QUERY_FILE, 'r') as f:
                query = f.read().strip()
            print(f"[Tool] Read Query from file: {query}")
            return query
        except FileNotFoundError:
            print(f"[Tool] Error: {QUERY_FILE} not found.")
            return None

    def tool_execute_sql(self, sql_query):
        """Tool to execute SQL query on the CSV data."""
        print(f"[Tool] Executing SQL: {sql_query}")
        try:
            conn = sqlite3.connect(':memory:')
            if os.path.exists(CSV_FILE):
                # Load Data (using chunks if large, but full for now as per logic)
                df = pd.read_csv(CSV_FILE)
                # Quick Clean: Ensure column names are clean
                df.columns = [c.replace(' ', '_') for c in df.columns]
                df.to_sql('full_stocks', conn, index=False, if_exists='replace')
                
                result_df = pd.read_sql_query(sql_query, conn)
                return result_df
            else:
                return pd.DataFrame({"error": ["Data file not found"]})
        except Exception as e:
            print(f"[Tool] SQL Execution Error: {e}")
            return pd.DataFrame({"error": [str(e)]})

    def tool_save_csv(self, df, prefix="agent_output"):
        """Tool to save the resulting dataframe to a CSV file."""
        if df is None or df.empty or "error" in df.columns:
            print("[Tool] No valid data to save.")
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{prefix}_{timestamp}.csv"
        filepath = os.path.join(OUTPUT_DIR, filename)
        try:
            df.to_csv(filepath, index=False)
            print(f"[Tool] Saved output to {filepath}")
            return filepath
        except Exception as e:
            print(f"[Tool] Save Error: {e}")
            return None

    def _get_schema(self):
        """Helper to get schema for the prompt."""
        try:
            if os.path.exists(CSV_FILE):
                df = pd.read_csv(CSV_FILE, nrows=1)
                return ", ".join([c.replace(' ', '_') for c in df.columns])
        except:
            pass
        return "Unknown Schema"

    def _generate_sql(self, user_query, schema):
        """Internal helper to call LLM."""
        current_date = datetime.now().strftime('%Y-%m-%d')
        prompt = f"""
        You are an expert Data Analyst Agent.
        Rules:
        1. Parse the User Question: "{user_query}"
        2. Schema: table 'full_stocks' with columns: {schema}
        3. Context: Today is {current_date}.
        4. task: Generate a SQL (SQLite) query to answer the question.
        5. CRITICAL: The data is One-Hot Encoded.
           - There is NO 'stock_sector' column. 
           - Columns are 'stock_sector_Technology', 'stock_sector_Energy', etc. (values 0 or 1).
           - To filter: WHERE stock_sector_Technology = 1
           - To GROUP BY SECTOR: You MUST use a CASE statement:
             CASE 
               WHEN stock_sector_Technology = 1 THEN 'Technology'
               WHEN stock_sector_Energy = 1 THEN 'Energy'
               ...
             END AS sector
        6. Return ONLY valid SQL. No markdown.
        7. If you are asked something irrelevant, say that this is out of scope
        
        SQL:
        """
        for model in self.models:
            try:
                response = self.client.chat_completion(
                    messages=[{"role": "user", "content": prompt}],
                    model=model, max_tokens=300, temperature=0.1
                )
                raw_content = response.choices[0].message.content
                print(f"[DEBUG] Model {model} raw response: {raw_content}")
                
                sql = raw_content.replace("```sql", "").replace("```", "").strip()
                if "SELECT" in sql.upper():
                    return sql
                else:
                    print(f"[DEBUG] Model {model} response did not contain SELECT. Retrying...")
            except Exception as e:
                print(f"[DEBUG] Model {model} failed: {e}")
                continue
        return None

    def run(self, input_query=None):
        """
        Main Agent Loop.
        If input_query is provided (UI mode), uses it.
        Otherwise, reads from file (Autonomous mode).
        """
        # 1. Acquire Input
        if input_query:
            query = input_query
            print(f"[Agent] Received Interactive Query: {query}")
        else:
            print("[Agent] No input provided. Invoking 'read_query_file' tool...")
            query = self.tool_read_query_file()
        
        if not query:
            return {"status": "No query found"}

        # 2. Generate SQL
        schema = self._get_schema()
        sql_query = self._generate_sql(query, schema)
        
        if not sql_query:
            # LOG FAILURE
            failure_log = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "query": query,
                "error": "Failed to generate SQL",
                "schema_used": schema
            }
            self._log_transaction(failure_log)
            print("[Agent] Failed to generate SQL. See logs.")
            return {"status": "Failed to generate SQL"}

        # 3. Execute Tool
        result_df = self.tool_execute_sql(sql_query)
        result_list = result_df.to_dict(orient='records')

        # 4. Save Tool
        saved_path = self.tool_save_csv(result_df)

        # 5. Log
        log_entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "user_query": query,
            "agent_response": sql_query,
            "result_summary": f"{len(result_list)} rows returned",
            "saved_file": saved_path
        }
        self._log_transaction(log_entry)
        
        return {
            "query": query,
            "sql": sql_query,
            "data": result_df,
            "saved_path": saved_path
        }

    def _log_transaction(self, entry):
        try:
            logs = []
            if os.path.exists(LOG_FILE):
                with open(LOG_FILE, 'r') as f:
                    logs = json.load(f)
            logs.append(entry)
            with open(LOG_FILE, 'w') as f:
                json.dump(logs, f, indent=4)
        except Exception as e:
            print(f"Log Error: {e}")

# Maintain backward compatibility for Airflow
def process_with_ai_agent(filename=None):
    # We ignore filename here because the class uses the constant CSV_FILE, 
    # which points to the same location (/opt/airflow/data/streaming_dataset/full_stocks.csv)
    # If the filename passed is different, we should technically update it, but for now strict consistency is fine.
    agent = StockAgent()
    agent.run()

