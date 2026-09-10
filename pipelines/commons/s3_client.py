import boto3
from pipelines.commons.env_loader import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

def get_s3_client():
    endpoint = MINIO_ENDPOINT if MINIO_ENDPOINT.startswith("http") else f"http://{MINIO_ENDPOINT}"
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )