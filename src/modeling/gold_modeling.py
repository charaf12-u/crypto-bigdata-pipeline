from datetime import datetime, timezone
from io import BytesIO

import pandas as pd

from config.settings import SILVER_BUCKET, GOLD_BUCKET
from src.utils.minio_client import get_minio_client, create_bucket_if_not_exists
from src.utils.logger import pipeline_logger

# --> build silver path
def build_silver_path():
    today = datetime.now(timezone.utc)
    return f"{today.year}/{today.month:02d}/{today.day:02d}/market_data.parquet"


# --> build gold path
def build_gold_path(table_name):
    today = datetime.now(timezone.utc)
    return f"{today.year}/{today.month:02d}/{today.day:02d}/{table_name}.parquet"


# --> read silver parquet from minio
def read_silver_parquet_from_minio():
    try:
        client = get_minio_client()
        object_path = build_silver_path()

        pipeline_logger.info(f"Reading Silver Parquet from MinIO: {SILVER_BUCKET}/{object_path}...")
        response = client.get_object(
            Bucket=SILVER_BUCKET,
            Key=object_path
        )

        data = response["Body"].read()
        pipeline_logger.info("Successfully read Silver Parquet.")
        return pd.read_parquet(BytesIO(data), engine="pyarrow")
    except Exception as e:
        pipeline_logger.error(f"Failed to read Silver Parquet: {e}", exc_info=True)
        raise e


# --> build dim_crypto
def build_dim_crypto(df):
    try:
        pipeline_logger.info("Building DIM_CRYPTO dimension table...")
        dim_crypto = df[
            [
                "id",
                "symbol",
                "name",
                "market_cap_rank",
            ]
        ].drop_duplicates(subset=["id"]).copy()

        dim_crypto = dim_crypto.sort_values("market_cap_rank", na_position="last")
        dim_crypto.insert(0, "crypto_key", range(1, len(dim_crypto) + 1))

        dim_crypto = dim_crypto.rename(
            columns={
                "id": "coin_id",
            }
        )

        result = dim_crypto[
            [
                "crypto_key",
                "coin_id",
                "symbol",
                "name",
                "market_cap_rank",
            ]
        ]
        pipeline_logger.info(f"DIM_CRYPTO built successfully: {len(result)} rows.")
        return result
    except Exception as e:
        pipeline_logger.error(f"Failed to build DIM_CRYPTO: {e}", exc_info=True)
        raise e


# --> build dim_date
def build_dim_date(df):
    try:
        pipeline_logger.info("Building DIM_DATE dimension table...")
        dates = pd.to_datetime(df["collected_at"]).dt.date.drop_duplicates()

        dim_date = pd.DataFrame({"full_date": dates})
        dim_date["full_date"] = pd.to_datetime(dim_date["full_date"])

        dim_date["date_key"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
        dim_date["day"] = dim_date["full_date"].dt.day
        dim_date["month"] = dim_date["full_date"].dt.month
        dim_date["year"] = dim_date["full_date"].dt.year
        dim_date["quarter"] = dim_date["full_date"].dt.quarter

        result = dim_date[
            [
                "date_key",
                "full_date",
                "day",
                "month",
                "year",
                "quarter",
            ]
        ]
        pipeline_logger.info(f"DIM_DATE built successfully: {len(result)} rows.")
        return result
    except Exception as e:
        pipeline_logger.error(f"Failed to build DIM_DATE: {e}", exc_info=True)
        raise e


# --> build dim_time
def build_dim_time(df):
    try:
        pipeline_logger.info("Building DIM_TIME dimension table...")
        collected_at = pd.to_datetime(df["collected_at"])

        times = collected_at.dt.strftime("%H:%M:%S").drop_duplicates()

        dim_time = pd.DataFrame({"full_time": times})
        dim_time["time_key"] = dim_time["full_time"].str.replace(":", "").astype(int)
        dim_time["hour"] = dim_time["full_time"].str.slice(0, 2).astype(int)
        dim_time["minute"] = dim_time["full_time"].str.slice(3, 5).astype(int)
        dim_time["second"] = dim_time["full_time"].str.slice(6, 8).astype(int)

        result = dim_time[
            [
                "time_key",
                "full_time",
                "hour",
                "minute",
                "second",
            ]
        ]
        pipeline_logger.info(f"DIM_TIME built successfully: {len(result)} rows.")
        return result
    except Exception as e:
        pipeline_logger.error(f"Failed to build DIM_TIME: {e}", exc_info=True)
        raise e


# --> build fact_crypto_market
def build_fact_crypto_market(df, dim_crypto):
    try:
        pipeline_logger.info("Building FACT_CRYPTO_MARKET fact table...")
        fact = df.copy()

        fact["collected_at"] = pd.to_datetime(fact["collected_at"])

        fact["date_key"] = fact["collected_at"].dt.strftime("%Y%m%d").astype(int)
        fact["time_key"] = fact["collected_at"].dt.strftime("%H%M%S").astype(int)

        fact = fact.merge(
            dim_crypto[["crypto_key", "coin_id"]],
            left_on="id",
            right_on="coin_id",
            how="left",
        )

        fact.insert(0, "fact_id", range(1, len(fact) + 1))

        result = fact[
            [
                "fact_id",
                "crypto_key",
                "date_key",
                "time_key",
                "current_price",
                "high_24h",
                "low_24h",
                "total_volume",
                "market_cap",
                "price_change_24h",
                "price_change_percentage_24h",
            ]
        ]
        pipeline_logger.info(f"FACT_CRYPTO_MARKET built successfully: {len(result)} rows.")
        return result
    except Exception as e:
        pipeline_logger.error(f"Failed to build FACT_CRYPTO_MARKET: {e}", exc_info=True)
        raise e


# --> validate referential integrity
def validate_referential_integrity(fact, dim_crypto, dim_date, dim_time):
    try:
        pipeline_logger.info("Validating referential integrity...")
        missing_crypto_keys = fact["crypto_key"].isna().sum()

        fact_date_keys = set(fact["date_key"].unique())
        dim_date_keys = set(dim_date["date_key"].unique())

        fact_time_keys = set(fact["time_key"].unique())
        dim_time_keys = set(dim_time["time_key"].unique())

        missing_date_keys = fact_date_keys - dim_date_keys
        missing_time_keys = fact_time_keys - dim_time_keys

        if missing_crypto_keys > 0:
            raise ValueError(f"Referential integrity failed: {missing_crypto_keys} crypto_key values are missing.")

        if missing_date_keys:
            raise ValueError(f"Referential integrity failed: missing date keys {missing_date_keys}")

        if missing_time_keys:
            raise ValueError(f"Referential integrity failed: missing time keys {missing_time_keys}")

        pipeline_logger.info("Referential integrity validated successfully.")
    except Exception as e:
        pipeline_logger.error(f"Referential integrity validation failed: {e}", exc_info=True)
        raise e


# --> upload dataframe to gold
def upload_dataframe_to_gold(df, table_name):
    try:
        client = get_minio_client()
        create_bucket_if_not_exists(client, GOLD_BUCKET)

        object_path = build_gold_path(table_name)

        pipeline_logger.info(f"Uploading Gold dimensional model table to MinIO: {GOLD_BUCKET}/{object_path}...")
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        client.put_object(
            Bucket=GOLD_BUCKET,
            Key=object_path,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )

        pipeline_logger.info(f"Gold table uploaded successfully: {GOLD_BUCKET}/{object_path}")
    except Exception as e:
        pipeline_logger.error(f"Failed to upload Gold table {table_name} to MinIO: {e}", exc_info=True)
        raise e


# --> run gold modeling
def run_gold_modeling():
    try:
        pipeline_logger.info("Starting Gold dimensional modeling process...")

        silver_df = read_silver_parquet_from_minio()

        dim_crypto = build_dim_crypto(silver_df)
        dim_date = build_dim_date(silver_df)
        dim_time = build_dim_time(silver_df)
        fact_crypto_market = build_fact_crypto_market(silver_df, dim_crypto)

        validate_referential_integrity(
            fact_crypto_market,
            dim_crypto,
            dim_date,
            dim_time,
        )

        upload_dataframe_to_gold(dim_crypto, "dim_crypto")
        upload_dataframe_to_gold(dim_date, "dim_date")
        upload_dataframe_to_gold(dim_time, "dim_time")
        upload_dataframe_to_gold(fact_crypto_market, "fact_crypto_market")

        pipeline_logger.info("Gold dimensional modeling completed successfully.")

    except Exception as error:
        pipeline_logger.error(f"Gold dimensional modeling failed: {error}", exc_info=True)
        raise error