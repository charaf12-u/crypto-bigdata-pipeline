import boto3
from botocore.exceptions import ClientError

from config.settings import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
)
from src.utils.logger import auth_logger


# --> get minio client
def get_minio_client():
    try:
        auth_logger.info("Initializing MinIO client connection...")
        client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )
        auth_logger.info("MinIO client connection initialized successfully.")
        return client
    except Exception as e:
        auth_logger.error(f"Failed to authenticate/connect to MinIO: {e}", exc_info=True)
        raise e


# --> create bucket if not exists
def create_bucket_if_not_exists(client, bucket_name):
    try:
        client.head_bucket(Bucket=bucket_name)
        auth_logger.info(f"Bucket already exists: {bucket_name}")

    except ClientError:
        try:
            auth_logger.info(f"Bucket not found. Creating bucket: {bucket_name}...")
            client.create_bucket(Bucket=bucket_name)
            auth_logger.info(f"Bucket created: {bucket_name}")
        except Exception as e:
            auth_logger.error(f"Failed to create MinIO bucket {bucket_name}: {e}", exc_info=True)
            raise e
    except Exception as e:
        auth_logger.error(f"Error checking bucket existence for {bucket_name}: {e}", exc_info=True)
        raise e