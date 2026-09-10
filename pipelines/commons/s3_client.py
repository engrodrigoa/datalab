import boto3
from botocore.exceptions import ClientError
from pipelines.commons.env_loader import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

def get_s3_client():
    endpoint = MINIO_ENDPOINT if MINIO_ENDPOINT.startswith("http") else f"http://{MINIO_ENDPOINT}"
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

def ensure_buckets_exist(bucket_names: list[str]):
    s3 = get_s3_client()
    for bucket in bucket_names:
        try:
            s3.head_bucket(Bucket=bucket)
            print(f">>> [SKIP] bucket já existe: {bucket}")
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                s3.create_bucket(Bucket=bucket)
                print(f">>>>> [SUCCESS] bucket criado: {bucket}")
            else:
                raise