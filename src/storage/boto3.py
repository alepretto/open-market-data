import os

import boto3
import dotenv

dotenv.load_dotenv()

endpoint = os.getenv("MINIO_ENDPOINT", "").strip()
if not endpoint.startswith("http"):
    endpoint = f"http://{endpoint}"


s3_client = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    region_name=os.getenv("MINIO_REGION"),
)
