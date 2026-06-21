import json
from datetime import datetime, timezone
import requests

from config.settings import (
    COINGECKO_BASE_URL,
    COINGECKO_ENDPOINT,
    COINGECKO_CURRENCY,
    COINGECKO_PER_PAGE,
    COINGECKO_PAGE,
    COINGECKO_TIMEOUT,
    BRONZE_BUCKET,
)

from src.utils.minio_client import get_minio_client, create_bucket_if_not_exists
from src.utils.logger import pipeline_logger

# --> fetch crypto market data from coingcko api
def fetch_crypto_market_data():
    url = f"{COINGECKO_BASE_URL}{COINGECKO_ENDPOINT}"

    params = {
        "vs_currency": COINGECKO_CURRENCY,
        "order": "market_cap_desc",
        "per_page": COINGECKO_PER_PAGE,
        "page": COINGECKO_PAGE,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }

    try:
        pipeline_logger.info(f"Fetching cryptocurrency market data from CoinGecko API: {url}...")
        response = requests.get(url, params=params, timeout=COINGECKO_TIMEOUT)
        response.raise_for_status()
        pipeline_logger.info("Successfully fetched market data from CoinGecko API.")
        return response.json()
    except Exception as e:
        pipeline_logger.error(f"Failed to fetch market data from CoinGecko: {e}", exc_info=True)
        raise e


# --> build bronze path
def build_bronze_path():
    today = datetime.now(timezone.utc)
    return f"{today.year}/{today.month:02d}/{today.day:02d}/raw.json"

# --> upload raw json
def upload_raw_json(data):
    try:
        client = get_minio_client()
        create_bucket_if_not_exists(client, BRONZE_BUCKET)

        object_path = build_bronze_path()
        raw_json = json.dumps(data, ensure_ascii=False, indent=2)

        pipeline_logger.info(f"Uploading raw JSON to Bronze bucket: {BRONZE_BUCKET}/{object_path}...")
        client.put_object(
            Bucket=BRONZE_BUCKET,
            Key=object_path,
            Body=raw_json.encode("utf-8"),
            ContentType="application/json",
        )
        pipeline_logger.info(f"Uploaded raw JSON to Bronze: {BRONZE_BUCKET}/{object_path}")
    except Exception as e:
        pipeline_logger.error(f"Failed to upload raw JSON to Bronze layer: {e}", exc_info=True)
        raise e

# --> run bronze ingestion
def run_bronze_ingestion():
    try:
        pipeline_logger.info("Starting Bronze ingestion...")
        data = fetch_crypto_market_data()
        upload_raw_json(data)
        pipeline_logger.info("Bronze ingestion completed successfully.")

    except requests.exceptions.Timeout as error:
        pipeline_logger.error(f"Timeout error: CoinGecko API did not respond. Details: {error}")
        raise error

    except requests.exceptions.HTTPError as error:
        pipeline_logger.error(f"HTTP error occurred during ingestion: {error}")
        raise error

    except requests.exceptions.RequestException as error:
        pipeline_logger.error(f"Connection error occurred during ingestion: {error}")
        raise error

    except Exception as error:
        pipeline_logger.error(f"Unexpected error in Bronze ingestion pipeline: {error}", exc_info=True)
        raise error