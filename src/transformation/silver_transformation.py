import json
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd

from config.settings import BRONZE_BUCKET, SILVER_BUCKET
from src.utils.minio_client import get_minio_client, create_bucket_if_not_exists
from src.utils.logger import pipeline_logger


REQUIRED_COLUMNS = [
    "id",
    "symbol",
    "name",
    "market_cap_rank",
    "current_price",
    "high_24h",
    "low_24h",
    "total_volume",
    "market_cap",
    "price_change_24h",
    "price_change_percentage_24h",
]

# --> build bronze path
def build_bronze_path():
    today = datetime.now(timezone.utc)
    return f"{today.year}/{today.month:02d}/{today.day:02d}/raw.json"

# --> build silver path
def build_silver_path():
    today = datetime.now(timezone.utc)
    return f"{today.year}/{today.month:02d}/{today.day:02d}/market_data.parquet"


# --> read bronze json from minio
def read_bronze_json_from_minio():
    try:
        client = get_minio_client()
        object_path = build_bronze_path()

        pipeline_logger.info(f"Reading raw Bronze JSON from MinIO: {BRONZE_BUCKET}/{object_path}...")
        response = client.get_object(
            Bucket=BRONZE_BUCKET,
            Key=object_path
        )

        raw_data = response["Body"].read().decode("utf-8")
        pipeline_logger.info("Successfully loaded raw Bronze JSON.")
        return json.loads(raw_data)
    except Exception as e:
        pipeline_logger.error(f"Failed to read Bronze JSON from MinIO: {e}", exc_info=True)
        raise e


# --> normalize columns
def normalize_columns(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )
    return df


# --> clean crypto data
def clean_crypto_data(data):
    try:
        pipeline_logger.info("Starting cleaning and normalization of cryptocurrency data...")
        df = pd.DataFrame(data)

        df = normalize_columns(df)

        df = df[REQUIRED_COLUMNS].copy()

        df["collected_at"] = datetime.now(timezone.utc)

        numeric_columns = [
            "market_cap_rank",
            "current_price",
            "high_24h",
            "low_24h",
            "total_volume",
            "market_cap",
            "price_change_24h",
            "price_change_percentage_24h",
        ]

        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["id"] = df["id"].astype(str)
        df["symbol"] = df["symbol"].astype(str).str.upper()
        df["name"] = df["name"].astype(str)

        initial_len = len(df)
        df = df.drop_duplicates(subset=["id"])
        df = df.dropna(subset=["id", "symbol", "name", "current_price"])
        cleaned_len = len(df)

        pipeline_logger.info(f"Cleaned data: dropped {initial_len - cleaned_len} rows. Cleaned dataset has {cleaned_len} registers.")
        return df
    except Exception as e:
        pipeline_logger.error(f"Failed to clean cryptocurrency data: {e}", exc_info=True)
        raise e


# --> upload silver parquet to minio
def upload_silver_parquet_to_minio(df):
    try:
        client = get_minio_client()
        create_bucket_if_not_exists(client, SILVER_BUCKET)

        object_path = build_silver_path()

        pipeline_logger.info(f"Writing and uploading cleaned DataFrame as Parquet to Silver layer: {SILVER_BUCKET}/{object_path}...")
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        client.put_object(
            Bucket=SILVER_BUCKET,
            Key=object_path,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )

        pipeline_logger.info(f"Silver Parquet uploaded successfully: {SILVER_BUCKET}/{object_path}")
    except Exception as e:
        pipeline_logger.error(f"Failed to upload Parquet to Silver: {e}", exc_info=True)
        raise e


# --> run silver transformation
def run_silver_transformation():
    try:
        pipeline_logger.info("Starting Silver transformation process...")

        data = read_bronze_json_from_minio()
        df = clean_crypto_data(data)
        upload_silver_parquet_to_minio(df)

        pipeline_logger.info("Silver transformation completed successfully.")

    except Exception as error:
        pipeline_logger.error(f"Silver transformation failed: {error}", exc_info=True)
        raise error