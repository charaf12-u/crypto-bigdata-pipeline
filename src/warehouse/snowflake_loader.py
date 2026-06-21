from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from config.settings import (
    GOLD_BUCKET,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_ROLE,
)
from src.utils.minio_client import get_minio_client
from src.utils.logger import auth_logger, pipeline_logger


TABLES = [
    "dim_crypto",
    "dim_date",
    "dim_time",
    "fact_crypto_market",
]


def get_snowflake_connection():
    try:
        auth_logger.info("Initializing connection to Snowflake cloud database...")
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE,
        )
        auth_logger.info("Snowflake connection established successfully.")
        return conn
    except Exception as e:
        auth_logger.error(f"Failed to authenticate/connect to Snowflake: {e}", exc_info=True)
        raise e


def list_gold_table_paths(table_name):
    try:
        client = get_minio_client()
        pipeline_logger.info(f"Listing Gold table paths for table '{table_name}' in bucket: {GOLD_BUCKET}...")
        response = client.list_objects_v2(Bucket=GOLD_BUCKET)

        paths = []
        for obj in response.get("Contents", []):
            key = obj["Key"]
            if key.endswith(f"{table_name}.parquet"):
                paths.append(key)

        pipeline_logger.info(f"Found {len(paths)} Parquet paths for '{table_name}'.")
        return sorted(paths)
    except Exception as e:
        pipeline_logger.error(f"Failed to list objects in Gold bucket: {e}", exc_info=True)
        raise e


def read_gold_table_from_minio(table_name):
    try:
        client = get_minio_client()
        paths = list_gold_table_paths(table_name)

        if not paths:
            raise FileNotFoundError(f"No parquet files found for {table_name}")

        dataframes = []
        for path in paths:
            pipeline_logger.info(f"Reading Gold table: {GOLD_BUCKET}/{path}...")
            response = client.get_object(
                Bucket=GOLD_BUCKET,
                Key=path,
            )

            data = response["Body"].read()
            df = pd.read_parquet(BytesIO(data), engine="pyarrow")
            dataframes.append(df)

        result = pd.concat(dataframes, ignore_index=True)

        if table_name == "dim_crypto":
            result = result.drop_duplicates(subset=["crypto_key"])

        if table_name == "dim_date":
            result = result.drop_duplicates(subset=["date_key"])

        if table_name == "dim_time":
            result = result.drop_duplicates(subset=["time_key"])

        if table_name == "fact_crypto_market":
            result = result.reset_index(drop=True)
            result["fact_id"] = range(1, len(result) + 1)

        pipeline_logger.info(f"Loaded Gold table '{table_name}' with {len(result)} records.")
        return result
    except Exception as e:
        pipeline_logger.error(f"Failed to read/concatenate Gold table '{table_name}': {e}", exc_info=True)
        raise e


# --> execute sql
def execute_sql(cursor, sql):
    try:
        cursor.execute(sql)
    except Exception as e:
        pipeline_logger.error(f"SQL execution error for query '{sql}': {e}", exc_info=True)
        raise e


# --> create database schema and tables
def create_database_schema_and_tables(conn):
    try:
        pipeline_logger.info("Setting up Snowflake database, schema and tables (DDL execution)...")
        cursor = conn.cursor()

        execute_sql(cursor, f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE};")
        execute_sql(cursor, f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA};")
        execute_sql(cursor, f"USE DATABASE {SNOWFLAKE_DATABASE};")
        execute_sql(cursor, f"USE SCHEMA {SNOWFLAKE_SCHEMA};")

        execute_sql(cursor, """
            CREATE TABLE IF NOT EXISTS DIM_CRYPTO (
                CRYPTO_KEY INTEGER PRIMARY KEY,
                COIN_ID VARCHAR,
                SYMBOL VARCHAR,
                NAME VARCHAR,
                MARKET_CAP_RANK INTEGER
            );
        """)

        execute_sql(cursor, """
            CREATE TABLE IF NOT EXISTS DIM_DATE (
                DATE_KEY INTEGER PRIMARY KEY,
                FULL_DATE DATE,
                DAY INTEGER,
                MONTH INTEGER,
                YEAR INTEGER,
                QUARTER INTEGER
            );
        """)

        execute_sql(cursor, """
            CREATE TABLE IF NOT EXISTS DIM_TIME (
                TIME_KEY INTEGER PRIMARY KEY,
                FULL_TIME TIME,
                HOUR INTEGER,
                MINUTE INTEGER,
                SECOND INTEGER
            );
        """)

        execute_sql(cursor, """
            CREATE TABLE IF NOT EXISTS FACT_CRYPTO_MARKET (
                FACT_ID INTEGER PRIMARY KEY,
                CRYPTO_KEY INTEGER,
                DATE_KEY INTEGER,
                TIME_KEY INTEGER,
                CURRENT_PRICE FLOAT,
                HIGH_24h FLOAT,
                LOW_24H FLOAT,
                TOTAL_VOLUME FLOAT,
                MARKET_CAP FLOAT,
                PRICE_CHANGE_24H FLOAT,
                PRICE_CHANGE_PERCENTAGE_24H FLOAT,
                FOREIGN KEY (CRYPTO_KEY) REFERENCES DIM_CRYPTO(CRYPTO_KEY),
                FOREIGN KEY (DATE_KEY) REFERENCES DIM_DATE(DATE_KEY),
                FOREIGN KEY (TIME_KEY) REFERENCES DIM_TIME(TIME_KEY)
            );
        """)

        cursor.close()
        pipeline_logger.info("Snowflake database, schema and tables verified/created successfully.")
    except Exception as e:
        pipeline_logger.error(f"Failed to create Snowflake tables: {e}", exc_info=True)
        raise e

# --> truncate tables
def truncate_tables(conn):
    try:
        pipeline_logger.info("Truncating Snowflake tables to prepare for bulk reload...")
        cursor = conn.cursor()

        execute_sql(cursor, "TRUNCATE TABLE IF EXISTS FACT_CRYPTO_MARKET;")
        execute_sql(cursor, "TRUNCATE TABLE IF EXISTS DIM_CRYPTO;")
        execute_sql(cursor, "TRUNCATE TABLE IF EXISTS DIM_DATE;")
        execute_sql(cursor, "TRUNCATE TABLE IF EXISTS DIM_TIME;")

        cursor.close()
        pipeline_logger.info("Snowflake tables truncated successfully.")
    except Exception as e:
        pipeline_logger.error(f"Failed to truncate Snowflake tables: {e}", exc_info=True)
        raise e

# --> fix dim_date types
def fix_dim_date_types(dim_date):
    try:
        dim_date = dim_date.copy()
        dim_date["full_date"] = pd.to_datetime(
            dim_date["full_date"],
            errors="coerce",
        ).dt.strftime("%Y-%m-%d")
        return dim_date
    except Exception as e:
        pipeline_logger.error(f"Failed to fix DIM_DATE types: {e}", exc_info=True)
        raise e


# --> fix dim_time types
def fix_dim_time_types(dim_time):
    try:
        dim_time = dim_time.copy()
        dim_time["full_time"] = dim_time["full_time"].astype(str)
        return dim_time
    except Exception as e:
        pipeline_logger.error(f"Failed to fix DIM_TIME types: {e}", exc_info=True)
        raise e


# --> prepare dataframe for snowflake
def prepare_dataframe_for_snowflake(df):
    df = df.copy()
    df.columns = [col.upper() for col in df.columns]
    return df

# --> load table to snowflake
def load_table_to_snowflake(conn, table_name, df):
    try:
        pipeline_logger.info(f"Loading {len(df)} rows into Snowflake table: {table_name.upper()}...")
        df = prepare_dataframe_for_snowflake(df)

        success, nchunks, nrows, output = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name.upper(),
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            overwrite=False,
        )

        if not success:
            raise Exception(f"Failed to load table {table_name} (write_pandas returned success=False)")

        pipeline_logger.info(f"Successfully loaded {nrows} rows into {table_name.upper()} ({nchunks} chunks).")
    except Exception as e:
        pipeline_logger.error(f"Failed to load table '{table_name}' into Snowflake: {e}", exc_info=True)
        raise e

# --> validate snowflake load
def validate_snowflake_load(conn):
    try:
        pipeline_logger.info("Validating counts of records loaded into Snowflake...")
        cursor = conn.cursor()

        for table in TABLES:
            cursor.execute(f"SELECT COUNT(*) FROM {table.upper()};")
            count = cursor.fetchone()[0]
            pipeline_logger.info(f"Snowflake Table {table.upper()} row count: {count}")

        cursor.close()
        pipeline_logger.info("Snowflake bulk load validation completed.")
    except Exception as e:
        pipeline_logger.error(f"Failed to validate Snowflake load: {e}", exc_info=True)
        raise e


# --> run snowflake loading
def run_snowflake_loading():
    conn = None

    try:
        pipeline_logger.info("Starting Snowflake bulk loader pipeline...")

        conn = get_snowflake_connection()

        create_database_schema_and_tables(conn)
        truncate_tables(conn)

        dim_crypto = read_gold_table_from_minio("dim_crypto")
        dim_date = read_gold_table_from_minio("dim_date")
        dim_time = read_gold_table_from_minio("dim_time")
        fact_crypto_market = read_gold_table_from_minio("fact_crypto_market")

        dim_date = fix_dim_date_types(dim_date)
        dim_time = fix_dim_time_types(dim_time)

        load_table_to_snowflake(conn, "dim_crypto", dim_crypto)
        load_table_to_snowflake(conn, "dim_date", dim_date)
        load_table_to_snowflake(conn, "dim_time", dim_time)
        load_table_to_snowflake(conn, "fact_crypto_market", fact_crypto_market)

        validate_snowflake_load(conn)

        pipeline_logger.info("Snowflake loading pipeline completed successfully.")

    except Exception as error:
        pipeline_logger.error(f"Snowflake loading pipeline failed: {error}", exc_info=True)
        raise error

    finally:
        if conn:
            try:
                conn.close()
                auth_logger.info("Closed Snowflake database connection.")
            except Exception as e:
                auth_logger.error(f"Error closing Snowflake connection: {e}")