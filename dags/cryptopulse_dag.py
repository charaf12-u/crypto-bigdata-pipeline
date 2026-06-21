from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingestion.bronze_ingestion import run_bronze_ingestion
from src.transformation.silver_transformation import run_silver_transformation
from src.modeling.gold_modeling import run_gold_modeling
from src.warehouse.snowflake_loader import run_snowflake_loading


default_args = {
    "owner": "cryptopulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="cryptopulse_pipeline_dag",
    description="Daily CryptoPulse Big Data pipeline",
    default_args=default_args,
    start_date=datetime(2026, 6, 17),
    schedule="@daily",
    catchup=False,
    tags=["cryptopulse", "minio", "snowflake", "crypto"],
) as dag:

    ingest_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=run_bronze_ingestion,
    )

    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=run_silver_transformation,
    )

    build_gold_model = PythonOperator(
        task_id="build_gold_model",
        python_callable=run_gold_modeling,
    )

    load_snowflake = PythonOperator(
        task_id="load_snowflake",
        python_callable=run_snowflake_loading,
    )

    ingest_bronze >> transform_silver >> build_gold_model >> load_snowflake