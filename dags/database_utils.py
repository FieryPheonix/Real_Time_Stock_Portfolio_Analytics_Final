import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

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
