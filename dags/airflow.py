import numpy as np
from sklearn import preprocessing
from sqlalchemy import create_engine,text
from typing import Dict
import pandas as pd
import ast
import copy

from kafka import KafkaProducer
from kafka import KafkaConsumer

import json
import time

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, desc
from pyspark.sql import functions as F

import os
import sys


# steps to run
# 1. docker compose up
# 2. run in browser: localhost:8080
# 3. username: airflow, password: airflow

################################ 1) Stage 1 ##################################################################################################

# def extract_clean(filename):
#     df = pd.read_csv(filename)
#     df = clean_missing(df)
#     df.to_csv('/opt/airflow/data/titanic_clean.csv',index=False)
#     print('loaded after cleaned_datasets succesfully')
#
# def clean_missing(df):
#     df = impute_mean(df,'Age')
#     df = impute_arbitrary(df,'Cabin','Missing')
#     df = cca(df,'Embarked')
#     return df


# 1) Data cleaned_datasets
def clean_missing_values(daily_trade_prices):
    # use daily trade prices file
    """
    Imputes missing values in specified stock price columns using the median.

    This function modifies the 'daily_trade_prices' table by filling Nulls in
    stock columns that showed missing data in the original notebook analysis.
    """
    df_prices = pd.read_csv(daily_trade_prices)
    df_cleaned = df_prices.copy()

    # Columns identified as having missing values (from STK001, STK004, STK009, STK012, STK019, STK020)
    imputation_cols = df_cleaned.columns[df_prices.isnull().any()].tolist()
    print(imputation_cols)
    #imputation_cols = [
    #    "STK001", "STK004", "STK009",
    #    "STK012", "STK019", "STK020"
    #]

    print("\n--- 1. Cleaning Daily Trade Prices (Median Imputation) ---")

    medians = []
    for col in imputation_cols:
        if col in df_cleaned.columns:
            median_val = df_cleaned[col].median()
            medians.append(median_val)
            print("before:",df_cleaned[col])
            df_cleaned[col] = df_cleaned[col].fillna(value=median_val)
            print("after:",df_cleaned[col])

    if df_cleaned.isnull().any().any():
        print("Note: Other stock columns (like STK002, STK003 etc.) had no initial missing values.")
    else:
        print("All specified missing values have been successfully imputed.")

    df_cleaned.to_csv('/opt/airflow/data/cleaned_datasets/daily_trade_prices.csv', index=False)

    # save imputation values
    imputation_df = pd.DataFrame({
        "imputation_cols": imputation_cols,
        "imputation_values": medians
    })
    imputation_df.to_csv("/opt/airflow/data/lookup_tables/imputation_values.csv", index=False)

# 2) Handle outliers
def detect_iqr_outliers(df, columns=None):
    """
    Detects and prints the percentage of outliers using the IQR method for each given column.
    If no columns are specified, all numeric columns are used.
    """
    outlier_percentage = []
    if columns is None:
        columns = df.select_dtypes(include=['number']).columns  # auto-select numeric columns

    for col in columns:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1

        mask = (df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR))
        outliers = df[mask]

        print(f"{col}: {len(outliers)/len(df)*100:.3f}% outliers")
        outlier_percentage.append(len(outliers)/len(df)*100)
    return outlier_percentage

def detect_outliers(daily_trade_prices,customers,date,stock,trades):

    print("\n--- 2. Handling Outliers ---")
    """
    Handles outliers from the detect_iqr_outliers function using log transform only if the outlier percentage is more than 10%.
    If no columns are specified, no numeric columns are used.
    """
    # 1. get the columns that might have outliers

    daily_trade_prices_df = pd.read_csv(daily_trade_prices)
    df_daily_trade_prices_cleaned = daily_trade_prices_df.copy()
    daily_trade_prices_columns = ['STK001','STK002', 'STK003', 'STK004', 'STK005', 'STK006', 'STK007', 'STK008', 'STK009', 'STK010', 'STK011', 'STK012', 'STK013','STK014', 'STK015', 'STK016', 'STK017', 'STK018', 'STK019', 'STK020']

    trades_df = pd.read_csv(trades)
    df_trade = trades_df.copy()
    trade_columns = ['quantity', 'average_trade_size', 'cumulative_portfolio_value']


    # 2. Calculate the outlier percentage for each column
    daily_trade_prices_outlier_list = detect_iqr_outliers(df_daily_trade_prices_cleaned,daily_trade_prices_columns)
    trade_outlier_list = detect_iqr_outliers(df_trade,trade_columns)

    # 3. only log_transform the columns with outlier percentage > 10%
    # for df_daily_trade_prices_cleaned
    for column, outlier_percent in zip(daily_trade_prices_columns, daily_trade_prices_outlier_list):
        if outlier_percent > 10:
            if((df_daily_trade_prices_cleaned[column] == 0).any()):
                df_daily_trade_prices_cleaned[column] = np.log(df_daily_trade_prices_cleaned[column]+1)  # log transform
            else:
                df_daily_trade_prices_cleaned[column] = np.log(df_daily_trade_prices_cleaned[column]) #log transform

   #for trade
    for column, outlier_percent in zip(trade_columns, trade_outlier_list):
        if outlier_percent > 10:
            if((df_trade[column] == 0).any()):
                df_trade[column] = np.log(df_trade[column]+1)  # log transform
            else:
                df_trade[column] = np.log(df_trade[column]) #log transform

    customers_df = pd.read_csv(customers)
    date_df = pd.read_csv(date)
    stock_df = pd.read_csv(stock)

    df_daily_trade_prices_cleaned.to_csv('/opt/airflow/data/cleaned_datasets/daily_trade_prices.csv', index=False)
    customers_df.to_csv('/opt/airflow/data/cleaned_datasets/dim_customer.csv', index=False)
    date_df.to_csv('/opt/airflow/data/cleaned_datasets/dim_date.csv', index=False)
    stock_df.to_csv('/opt/airflow/data/cleaned_datasets/dim_stock.csv', index=False)
    df_trade.to_csv('/opt/airflow/data/cleaned_datasets/trades.csv', index=False)


# 3) Merge
def integrate_datasets(daily_trade_prices,customers,date,stock,trades):

    """
    Merges all processed datasets starting from the 'trades' table and
    calculates the final 'total_trade_amount'.
    """
    print("\n--- 3. Merging Datasets ---")

    # Access cleaned and raw dataframes
    trades_df = pd.read_csv(trades)
    customers_df = pd.read_csv(customers)
    date_df = pd.read_csv(date)
    stock_df = pd.read_csv(stock)
    daily_trade_prices_df = pd.read_csv(daily_trade_prices)

    df_trade = trades_df.copy()
    df_customers = customers_df.copy()
    df_date = date_df.copy()
    df_stock = stock_df.copy()
    df_daily_trade_prices_cleaned = daily_trade_prices_df.copy()

    # 4.1. Clean trade (remove 'cumulative_portfolio_value')
    merged_df = df_trade.drop(['cumulative_portfolio_value'], axis=1)

    # 4.2. Merge with customers
    merged_df = merged_df.merge(df_customers, on='customer_id', how='left')
    merged_df = merged_df.drop(['customer_key', 'avg_trade_size_baseline'], axis=1)
    merged_df = merged_df.rename(columns={'account_type': 'customer_account_type'})

    # 4.3. Merge with date
    merged_df = merged_df.merge(df_date, left_on='timestamp', right_on='date', how='left')
    merged_df = merged_df.drop([
        'date_key', 'date', 'day', 'month', 'month_name', 'quarter', 'year', 'day_of_week'
    ], axis=1)

    # 4.4. Merge with stock
    merged_df = merged_df.merge(df_stock, on='stock_ticker', how='left')
    merged_df = merged_df.drop(['stock_key', 'company_name'], axis=1)
    merged_df = merged_df.rename(columns={
        'liquidity_tier': 'stock_liquidity_tier',
        'sector': 'stock_sector',
        'industry': 'stock_industry'
    })

    # 4.5. Merge with daily trade prices (pivot prices long first)
    df_prices_long = df_daily_trade_prices_cleaned.melt(
        id_vars=['date'],
        var_name='stock_ticker',
        value_name='stock_price'
    )
    # The merge key is ['timestamp', 'stock_ticker'] in merged_df and ['date', 'stock_ticker'] in df_prices_long
    merged_df = merged_df.merge(
        df_prices_long,
        left_on=['timestamp', 'stock_ticker'],
        right_on=['date', 'stock_ticker'],
        how='left'
    )
    merged_df = merged_df.drop(['date'], axis=1)  # Drop redundant 'date' column from prices

    # 4.6. Calculate final derived column
    merged_df['total_trade_amount'] = merged_df['stock_price'] * merged_df['quantity']

    print(f" Final merged DataFrame created with {len(merged_df)} rows and {len(merged_df.columns)} columns.")
    merged_df.to_csv('/opt/airflow/data/merged_dataset/merged_dataset.csv', index=False)

# 4) Load to postgres
def create_database():
    try:
        # Connect to PostgreSQL server using the default 'postgres' database
        conn = psycopg2.connect(
            host='pgdatabase',
            user='root',
            password='root',
            database='postgres',  # Connect to default postgres database first
            port=5432
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'stockanalytics'")
        exists = cursor.fetchone()

        if not exists:
            # Create database
            cursor.execute('CREATE DATABASE stockanalytics')
            print("Database 'stockanalytics' created successfully")
        else:
            print("Database 'stockanalytics' already exists")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error creating database: {e}")

def load_to_postgres(merged_path):
    merged_df = pd.read_csv(merged_path)
    create_database()

    # postgresql://username:password@container_name:port/database_name
    engine = create_engine('postgresql://root:root@pgdatabase:5432/stockanalytics')

    try:
        # Test connection
        with engine.connect() as conn:
            print('Connected to Database')

        # Write to database
        print('Writing cleaned dataset to database')
        merged_df.to_sql('stockanalytics', con=engine, if_exists='replace', index=False)
        print('Done writing to database')

    except ValueError as vx:
        print(f'ValueError: {vx}')
    except Exception as ex:
        print(f'Error: {ex}')
    finally:
        engine.dispose()

################################ 2) Stage 2 ##################################################################################################

# 1) Extract Streaming data
def prepare_streaming_data(merged_path):
    df = pd.read_csv(merged_path)
    print("dataframe length", len(df))

    streaming_table = df.sample(frac=0.05, random_state=42)
    print("streaming length", len(streaming_table))

    remaining_table = df.drop(streaming_table.index)
    print("remaining encoded length", len(remaining_table))

    # extract unique day names for label encoding
    day_names = remaining_table["day_name"].unique().tolist()
    stock_ticker_names = sorted(remaining_table["stock_ticker"].unique().tolist())

    streaming_table.to_csv("/opt/airflow/data/streaming_dataset/to_be_streamed.csv", index=False)
    remaining_table.to_csv('/opt/airflow/data/encoded_dataset/to_be_encoded.csv', index=False)

    pd.DataFrame({"day": day_names}).to_csv('/opt/airflow/data/lookup_tables/label_encoding_day_values.csv', index=False)
    pd.DataFrame({"stock_ticker": stock_ticker_names}).to_csv('/opt/airflow/data/lookup_tables/label_encoding_stock_values.csv', index=False)


# 2) Encoding
def create_lookup_table(columns,operator, original_values, new_values):
    lookup_table = pd.DataFrame({
        "Column Name": columns,
        "Original Value": original_values,
        f"{operator} Value": new_values,
    })

    return lookup_table

def encode_categorical_data(to_be_encoded_path,imputation_values_path,day_names_path,stock_ticker_names_path):

    # 1) Generate imputation lookup table

    csv_data = pd.read_csv(imputation_values_path)
    imputation_cols = []
    imputation_values = []

    for _, row in csv_data.iterrows():
        imputation_cols.append(row["imputation_cols"])
        imputation_values.append(row["imputation_values"])

    null_list = [None] * len(imputation_values)
    lookup_table_imputation = create_lookup_table(imputation_cols, "Imputed", null_list, imputation_values)

    lookup_table_imputation.to_csv("/opt/airflow/data/lookup_tables/imputation_lookup_table.csv", index=False)


    # 2) Apply encoding to all categorical columns in the remaining 95%
    df = pd.read_csv(to_be_encoded_path)

    to_be_label_encoded_columns = ['day_name', 'stock_ticker', 'is_weekend', 'is_holiday']
    to_be_one_hot_encoded_columns = ['transaction_type', 'customer_account_type', 'stock_sector', 'stock_industry']

    #one hot encode
    encoded_df = pd.get_dummies(df, columns=to_be_one_hot_encoded_columns)

    # label encode
    custom_orders = {
        'stock_ticker': ['STK001', 'STK002', 'STK003', 'STK004', 'STK005', 'STK006',
       'STK007', 'STK008', 'STK009', 'STK010', 'STK011', 'STK012',
       'STK013', 'STK014', 'STK015', 'STK016', 'STK017', 'STK018',
       'STK019', 'STK020'],
        'day_name' : ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'],
        'is_weekend': [False, True],
        'is_holiday': [False, True]
    }

    for col, order in custom_orders.items():
        encoded_df[col] = pd.Categorical(encoded_df[col], categories=order, ordered=True)
        encoded_df[col] = encoded_df[col].cat.codes

    encoded_df.to_csv("/opt/airflow/data/encoded_dataset/encoded_partial_df.csv", index=False)
    encoded_df.to_csv("/opt/airflow/data/streaming_dataset/full_stocks.csv", index=False)

    # 3) save encoding into lookup tables
    # one hot encoded data
    mapping = {}
    for col in to_be_one_hot_encoded_columns:
        # dummy columns always start with "col_"
        mapped_cols = [c for c in encoded_df.columns if c.startswith(col + "_")]
        mapping[col] = mapped_cols

    lookup_table_one_hot_encoding = pd.DataFrame([
        {"original_column": k, "new_columns": ",".join(v)}
        for k, v in mapping.items()
    ])

    lookup_table_one_hot_encoding.to_csv("/opt/airflow/data/lookup_tables/one_hot_encoding_lookup_table.csv", index=False)

    # label encoded data
    day_names = pd.read_csv(day_names_path)
    stock_ticker_names = pd.read_csv(stock_ticker_names_path)

    day_values = day_names["day"].tolist()
    ticker_values = stock_ticker_names["stock_ticker"].tolist()

    unique_values_names = list(range(len(day_names)))
    unique_values_stock_ticker = list(range(len(stock_ticker_names)))
    lookup_table_label_encoding = create_lookup_table(to_be_label_encoded_columns, "Encoded",
                                                      [day_values, ticker_values, [False, True], [False, True]],
                                                      [unique_values_names, unique_values_stock_ticker, [0, 1], [0, 1]])
    lookup_table_label_encoding.to_csv("/opt/airflow/data/lookup_tables/label_encoding_lookup_table.csv", index=False)
    print(lookup_table_label_encoding)

################################ 3) Stage 3 ##################################################################################################
# 1) Kafka producer
# def start_kafka_producer(to_be_streamed_path):
#     producer = KafkaProducer(
#         bootstrap_servers='localhost:9092',
#         value_serializer=lambda v: json.dumps(v).encode('utf-8')
#     )
#
#     csv_data = pd.read_csv(to_be_streamed_path)
#
#     print("Sending data to Kafka...")
#
#     for index, row in csv_data.iterrows():
#         message = row.to_dict()
#         # message['timestamp'] = pd.Timestamp.now().isoformat()
#         producer.send('55_0593_Topic', value=message)
#         print(f"Sent: {message}")
#         time.sleep(0.3)
#
#     producer.send('55_0593_Topic', value='EOS')
#     print("Sent EOS message.")
#
#     producer.close()

# 2) Kafka consumer
def extract_one_hot_encoded_columns(df_one_hot):
    new_columns = []
    for _, row in df_one_hot.iterrows():
        new_row = row["new_columns"].split(",")
        new_columns.extend(new_row)
    return new_columns

def extract_label_encoded_columns(label_df):
    mappings = []  # list of dictionaries

    for _, row in label_df.iterrows():
        original_values = ast.literal_eval(row["Original Value"])
        encoded_values = ast.literal_eval(row["Encoded Value"])

        # create dict via zip
        mapping_dict = dict(zip(original_values, encoded_values))

        # append dictionary to output list
        mappings.append(mapping_dict)

    return mappings

def process_stream(message):
    # extract one hot encoded columns and label encoding
    label_df = pd.read_csv("/opt/airflow/data/lookup_tables/label_encoding_lookup_table.csv")
    label_mappings = extract_label_encoded_columns(label_df)
    print(label_mappings)

    df_one_hot = pd.read_csv("/opt/airflow/data/lookup_tables/one_hot_encoding_lookup_table.csv")
    original_one_hot_encoded_cols = df_one_hot["original_column"].tolist()
    one_hot_expected_columns = extract_one_hot_encoded_columns(df_one_hot)
    print(one_hot_expected_columns)



    existing_df = pd.read_csv("/opt/airflow/data/streaming_dataset/full_stocks.csv")
    # a) label encode
    day = message["day_name"]
    message["day_name"] = label_mappings[0].get(day)

    stock_ticker = message["stock_ticker"]
    #print("before:",message["stock_ticker"])
    message["stock_ticker"] = label_mappings[1].get(stock_ticker)
    #print("mapping", label_mappings[1])
    #print(".get", label_mappings[1].get(stock_ticker))
    #print("after:",message["stock_ticker"])
    print("DEBUG inside: ", len(existing_df))
    print("DEBUG inside: ", existing_df['transaction_id'].duplicated().any())

    is_weekend = message["is_weekend"]
    message["is_weekend"] = label_mappings[2].get(is_weekend)

    is_holiday = message["is_holiday"]
    message["is_holiday"] = label_mappings[3].get(is_holiday)

    # b) one hot encode
    df_message = pd.DataFrame([message])
    message_cleaned = pd.get_dummies(df_message, columns=original_one_hot_encoded_cols)

    for col in one_hot_expected_columns:
        if col not in message_cleaned.columns:
            message_cleaned[col] = False

    # c) append each column to encoded_data as full_stocks.csv
    new_df = pd.concat([existing_df, message_cleaned], ignore_index=True)
    new_df.to_csv("/opt/airflow/data/streaming_dataset/full_stocks.csv", index=False)
    return message_cleaned

def consume_and_process_stream():

    # initial_df = pd.read_csv('/opt/airflow/data/encoded_dataset/encoded_partial_df.csv')
    # initial_df.to_csv("/opt/airflow/data/streaming_dataset/full_stocks.csv", index=False)
    # print("DEBUG: ", len(initial_df))
    # 1. Initialize Kafka consumer
    kafka_consumer = KafkaConsumer(
        '55_0593_Topic',
        bootstrap_servers='kafka:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Listening for messages in '55_0593_Topic'...")

    for message in kafka_consumer:
        print(f"Received: {message.value}")

        # check if stopper received
        if message.value == 'EOS':
            print("EOS received, stopping consumer.")
            break

        # encode received message
        print(type(message.value))
        message_cleaned = process_stream(message.value)
        print(f"Encoded it to: {message_cleaned}")
        print('-' * 50)

    print("Closing consumer...")
    kafka_consumer.close()


# 3) Load to postgres

def save_final_to_postgres(fully_encoded_path):
    encoded_df = pd.read_csv(fully_encoded_path)
    create_database()

    # postgresql://username:password@container_name:port/database_name
    engine = create_engine('postgresql://root:root@pgdatabase:5432/stockanalytics')

    try:
        # Test connection
        with engine.connect() as conn:
            print('Connected to Database')

        # Write to database
        print('Writing cleaned dataset to database')
        encoded_df.to_sql('stockanalytics_encoded', con=engine, if_exists='replace', index=False)
        print('Done writing to database')

    except ValueError as vx:
        print(f'ValueError: {vx}')
    except Exception as ex:
        print(f'Error: {ex}')
    finally:
        engine.dispose()


################################ 4) Stage 4,5 & 6 ##################################################################################################

def initialize_spark_session():
    # Stop any existing SparkContext
    try:
        sc = SparkContext.getOrCreate()
        if not sc._jsc.sc().isStopped():
            sc.stop()
    except:
        pass

    # Create a fresh SparkSession
    spark = SparkSession.builder \
        .appName("M3_SPARK_APP_TEAM_NAME") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

def run_spark_analytics(file_path: str):

    print("=== Starting Spark Analytics Task ===")

    # ==========================================
    # INITIALIZE SPARK SESSION
    # ==========================================
    try:
        from pyspark import SparkContext
        # Stop any existing SparkContext
        try:
            sc = SparkContext.getOrCreate()
            if not sc._jsc.sc().isStopped():
                sc.stop()
                print("Stopped existing SparkContext.")
        except:
            pass

        # Create a fresh SparkSession
        print("Creating SparkSession...")
        spark = SparkSession.builder \
            .appName("M3_SPARK_APP_TEAM_NAME") \
            .master("spark://spark-master:7077") \
            .config("spark.driver.host", "airflow-worker") \
            .getOrCreate()
        
        print("SparkSession created successfully.")
    except Exception as e:
        print(f"ERROR: Failed to create SparkSession: {e}")
        sys.exit(1)

    try:
        # ==========================================
        # CHECK FILE PATH
        # ==========================================
        print(f"Checking for file at: {file_path}")
        if not os.path.exists(file_path):
            print("ERROR: File not found!")
            sys.exit(1)
        print("SUCCESS: File found.")

        # ==========================================
        # LOAD CSV
        # ==========================================
        try:
            df = spark.read.csv(file_path, header=True, inferSchema=True)
            print("CSV loaded successfully, showing first 5 rows:")
            df.show(5)
        except Exception as e:
            print(f"ERROR: Failed to read CSV: {e}")
            sys.exit(1)

        # ==========================================
        # HELPER FUNCTION TO SAVE TO POSTGRES
        # ==========================================
        def save_spark_to_postgres(spark_df, table_name):
            print(f"Saving '{table_name}' to Postgres...")

            create_database()

            pandas_df = spark_df.toPandas()

            engine = create_engine(
                "postgresql://root:root@pgdatabase:5432/stockanalytics",
                future=True
            )

            try:
                # ---- DROP TABLE in AUTOCOMMIT (IMPORTANT)
                with engine.connect() as conn:
                    conn.execution_options(isolation_level="AUTOCOMMIT")
                    conn.execute(
                        text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
                    )

                # ---- CREATE TABLE in a NEW transaction
                pandas_df.to_sql(
                    table_name,
                    con=engine,
                    if_exists="replace",
                    index=False,
                    method="multi"
                )

                print(f"Saved '{table_name}' successfully.")

            except Exception as e:
                print(f"Error saving '{table_name}': {e}")

            finally:
                engine.dispose()



        # ==========================================
        # DATA CLEANING / TRANSFORMATION
        # ==========================================
        df_cleaned = df.withColumn(
            "transaction_type", 
            F.when(F.col("transaction_type_BUY") == True, "BUY")
             .when(F.col("transaction_type_SELL") == True, "SELL")
             .otherwise("Unknown")
        ).withColumn(
            "customer_account_type",
            F.when(F.col("customer_account_type_Institutional") == True, "Institutional")
             .when(F.col("customer_account_type_Retail") == True, "Retail")
             .otherwise("Unknown")
        ).withColumn(
            "stock_sector",
            F.when(F.col("stock_sector_Consumer") == True, "Consumer")
             .when(F.col("stock_sector_Energy") == True, "Energy")
             .when(F.col("stock_sector_Finance") == True, "Finance")
             .when(F.col("stock_sector_Healthcare") == True, "Healthcare")
             .when(F.col("stock_sector_Technology") == True, "Technology")
             .otherwise("Other")
        ).withColumn(
            "stock_ticker", 
            F.concat(F.lit("STK"), F.lpad(F.col("stock_ticker") + 1, 3, "0"))
        )

        # ==========================================
        # SPARK FUNCTIONS ANALYSIS
        # ==========================================
        df_q1 = df_cleaned.groupBy("stock_ticker").agg(F.sum("quantity").alias("total_trading_volume")).orderBy("stock_ticker")
        df_q1.show(); save_spark_to_postgres(df_q1, "spark_analytics_1")

        df_q2 = df_cleaned.groupBy("stock_sector").agg(F.avg("stock_price").alias("average_price"))
        df_q2.show(); save_spark_to_postgres(df_q2, "spark_analytics_2")

        df_q3 = df_cleaned.filter(F.col("is_weekend") == 1).groupBy("transaction_type").count()
        df_q3.show(); save_spark_to_postgres(df_q3, "spark_analytics_3")

        df_q4 = df_cleaned.groupBy("customer_id").agg(F.count("transaction_id").alias("transaction_count")).filter(F.col("transaction_count") > 10)
        df_q4.show(); save_spark_to_postgres(df_q4, "spark_analytics_4")

        df_q5 = df_cleaned.groupBy("day_name").agg(F.sum("total_trade_amount").alias("total_amount")).orderBy(F.col("total_amount").desc())
        df_q5.show(); save_spark_to_postgres(df_q5, "spark_analytics_5")

        # ==========================================
        # SPARK SQL ANALYSIS
        # ==========================================
        df_cleaned.createOrReplaceTempView("transactions")

        sql_queries = {
            "spark_sql_1": """
                SELECT stock_ticker, SUM(quantity) as total_quantity
                FROM transactions
                GROUP BY stock_ticker
                ORDER BY total_quantity DESC
                LIMIT 5
            """,
            "spark_sql_2": """
                SELECT customer_account_type, AVG(total_trade_amount) as avg_trade_amount
                FROM transactions
                GROUP BY customer_account_type
            """,
            "spark_sql_3": """
                SELECT is_holiday, COUNT(*) as transaction_count
                FROM transactions
                GROUP BY is_holiday
            """,
            "spark_sql_4": """
                SELECT stock_sector, SUM(quantity) as total_volume
                FROM transactions
                WHERE is_weekend = 1
                GROUP BY stock_sector
                ORDER BY total_volume DESC
            """,
            "spark_sql_5": """
                SELECT stock_liquidity_tier, transaction_type, CAST(SUM(total_trade_amount) AS DECIMAL(20, 2)) as total_amount
                FROM transactions
                GROUP BY stock_liquidity_tier, transaction_type
                ORDER BY stock_liquidity_tier, transaction_type
            """
        }

        for table_name, query in sql_queries.items():
            df_sql = spark.sql(query)
            df_sql.show()
            save_spark_to_postgres(df_sql, table_name)

        print("=== Spark Analytics Task Completed ===")

    except Exception as e:
        print(f"ERROR: An unexpected error occurred during Spark analytics: {e}")

    finally:
        print("Stopping SparkSession...")
        try:
            spark.stop()
            print("SparkSession stopped successfully.")
        except Exception as e:
            print(f"WARNING: Failed to stop SparkSession: {e}")


# 2) Visualization
def prepare_visualization(file_path):
    print("=== Starting Prepare Visualization Task ===")
    
    output_path = '/opt/airflow/data/visualization/dashboard_data.csv'
    
    # Check input file
    print(f"Reading from: {file_path}")
    if not os.path.exists(file_path):
        print(f"ERROR: Input file not found at {file_path}")
        # Create empty dummy file to prevent downstream failure if spark failed to write
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        pd.DataFrame().to_csv(output_path)
        return

    df = pd.read_csv(file_path)
    print(f"Loaded {len(df)} rows from {file_path}")

    # ==============================
    # 1. Reverse Label Encoding
    # ==============================
    try:
        label_lookup_path = "/opt/airflow/data/lookup_tables/label_encoding_lookup_table.csv"
        if os.path.exists(label_lookup_path):
            label_df = pd.read_csv(label_lookup_path)
            # Use the existing helper function
            label_mappings = extract_label_encoded_columns(label_df)
            # label_mappings is list of dicts: [day_map, ticker_map, weekend_map, holiday_map]
            
            # 0: day_name (Reverse mapping)
            day_map = {v: k for k, v in label_mappings[0].items()}
            # get() is safer than direct map for partial matches
            df['day_name'] = df['day_name'].map(day_map).fillna(df['day_name'])
            
            # 1: stock_ticker
            ticker_map = {v: k for k, v in label_mappings[1].items()}
            df['stock_ticker'] = df['stock_ticker'].map(ticker_map).fillna(df['stock_ticker'])
            
            # 2: is_weekend
            weekend_map = {v: k for k, v in label_mappings[2].items()}
            df['is_weekend'] = df['is_weekend'].map(weekend_map).fillna(df['is_weekend'])
            
            # 3: is_holiday
            holiday_map = {v: k for k, v in label_mappings[3].items()}
            df['is_holiday'] = df['is_holiday'].map(holiday_map).fillna(df['is_holiday'])
            
            print("Reversed label encoding successfully.")
        else:
            print("WARNING: Label lookup table not found. Skipping label decoding.")
            
    except Exception as e:
        print(f"ERROR during label decoding: {e}")

    # ==============================
    # 2. Reverse One-Hot Encoding
    # ==============================
    one_hot_groups = ['transaction_type', 'customer_account_type', 'stock_sector', 'stock_industry']
    
    for group in one_hot_groups:
        # Find cols belonging to this group
        group_cols = [c for c in df.columns if c.startswith(f"{group}_")]
        if group_cols:
            # Logic: For each row, find column where value is True/1, get its name, strip prefix
            # We use idxmax for speed on axis 1
            # Filter to relevant cols
            subset = df[group_cols]
            # idxmax returns the column name with the max value (1/True)
            # We then strip the prefix
            df[group] = subset.idxmax(axis=1).astype(str).str.replace(f"{group}_", "")
            
            # Drop the dummy cols
            df.drop(columns=group_cols, inplace=True)
            print(f"Reversed one-hot encoding for {group}")

    # ==============================
    # 3. Save
    # ==============================
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Saved prepared data to {output_path}")


# 3) AI agent (Imported from separate module)
def process_with_ai_agent(filename):
    try:
        from ai_agent import process_with_ai_agent as run_agent
        run_agent(filename)
    except ImportError as e:
        print(f"Could not import AI Agent module: {e}")
    except Exception as e:
        print(f"Error running AI Agent: {e}")

################################ 2) DAG ##################################################################################################
# Define the DAG -> direced acyclic graph
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_retry': False,
    'retries': 1
}


with DAG(
    'stock_portfolio_pipeline_datafrogs',
    default_args=default_args,
    description='End-to-end stock portfolio analytics pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['data-engineering', 'stocks', 'analytics'],

)as dag:
    ############ Stage 1 ######################
    clean_missing_values_task = PythonOperator(
        task_id = 'clean_missing_values',
        python_callable = clean_missing_values,
        op_kwargs = {
            'daily_trade_prices': '/opt/airflow/data/initial_datasets/daily_trade_prices.csv'
        }
    )

    detect_outliers_task = PythonOperator(
        task_id = 'detect_outliers',
        python_callable = detect_outliers,
        op_kwargs = {
            'daily_trade_prices': '/opt/airflow/data/cleaned_datasets/daily_trade_prices.csv',
            'customers': '/opt/airflow/data/initial_datasets/dim_customer.csv',
            'date': '/opt/airflow/data/initial_datasets/dim_date.csv',
            'stock': '/opt/airflow/data/initial_datasets/dim_stock.csv',
            'trades': '/opt/airflow/data/initial_datasets/trades.csv'
        }
    )

    integrate_datasets_task = PythonOperator(
        task_id='integrate_datasets',
        python_callable=integrate_datasets,
        op_kwargs={
            'daily_trade_prices': '/opt/airflow/data/cleaned_datasets/daily_trade_prices.csv',
            'customers': '/opt/airflow/data/cleaned_datasets/dim_customer.csv',
            'date': '/opt/airflow/data/cleaned_datasets/dim_date.csv',
            'stock': '/opt/airflow/data/cleaned_datasets/dim_stock.csv',
            'trades': '/opt/airflow/data/cleaned_datasets/trades.csv'
        }
    )

    load_to_postgres_task = PythonOperator(
        task_id = 'load_to_postgres',
        python_callable = load_to_postgres,
        op_kwargs = {
            'merged_path': '/opt/airflow/data/merged_dataset/merged_dataset.csv'
        }
    )

    clean_missing_values_task >> detect_outliers_task >> integrate_datasets_task >> load_to_postgres_task

    ############ Stage 2 ######################

    prepare_streaming_data_task = PythonOperator(
        task_id='prepare_streaming_data',
        python_callable=prepare_streaming_data,
        op_kwargs={
            'merged_path': '/opt/airflow/data/merged_dataset/merged_dataset.csv'
        }
    )

    encode_categorical_data_task = PythonOperator(
        task_id='encode_categorical_data',
        python_callable=encode_categorical_data,
        op_kwargs={
            'to_be_encoded_path': '/opt/airflow/data/encoded_dataset/to_be_encoded.csv',
            'imputation_values_path': '/opt/airflow/data/lookup_tables/imputation_values.csv',
            'day_names_path': '/opt/airflow/data/lookup_tables/label_encoding_day_values.csv',
            'stock_ticker_names_path': '/opt/airflow/data/lookup_tables/label_encoding_stock_values.csv'
        }
    )

    prepare_streaming_data_task >> encode_categorical_data_task

    ############ Stage 3 ######################

    start_kafka_producer_task = BashOperator(
        task_id='start_kafka_producer',
        bash_command='python3 /opt/airflow/scripts/start_kafka_producer.py'
        # params={
        #     'to_be_streamed_path': "/opt/airflow/data/streaming_dataset/to_be_streamed.csv"
        # }
    )

    consume_and_process_stream_task = PythonOperator(
        task_id='consume_and_process_stream',
        python_callable=consume_and_process_stream
    )

    save_final_to_postgres_task = PythonOperator(
        task_id='save_final_to_postgres',
        python_callable=save_final_to_postgres,
        op_kwargs={
            'fully_encoded_path': '/opt/airflow/data/streaming_dataset/full_stocks.csv'
        }
    )
    start_kafka_producer_task >> consume_and_process_stream_task >> save_final_to_postgres_task

    ############ Stage 4 ######################
    initialize_spark_session_task = PythonOperator(
        task_id='initialize_spark_session',
        python_callable=initialize_spark_session,
    )

    run_spark_analytics_task = PythonOperator(
        task_id='run_spark_analytics',
        python_callable=run_spark_analytics,
        op_kwargs={
            'file_path': '/opt/airflow/data/streaming_dataset/full_stocks.csv'  # <-- path inside container
        }
    )

    prepare_visualization_task = PythonOperator(
        task_id='prepare_visualization',
        python_callable=prepare_visualization,
        op_kwargs={
            'file_path': '/opt/airflow/data/streaming_dataset/full_stocks.csv'
        }
    )

    start_visualization_service_task = BashOperator(
        task_id='start_visualization_service',
        bash_command='streamlit run /opt/airflow/scripts/dashboard.py --server.port 8501 --server.address 0.0.0.0'
    )
    
    process_with_ai_agent_task = PythonOperator(
        task_id='process_with_ai_agent',
        python_callable=process_with_ai_agent,
        op_kwargs={
            'filename': ''#fill in its path
        }
    )
    
    initialize_spark_session_task >> run_spark_analytics_task
    
    run_spark_analytics_task >> prepare_visualization_task >> start_visualization_service_task
    run_spark_analytics_task >> process_with_ai_agent_task

