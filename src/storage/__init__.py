import os
from io import BytesIO

import dotenv
import polars as pl

from src.storage.boto3 import s3_client

dotenv.load_dotenv()


def load_parquet(key: str):

    bucket = os.getenv("BUCKET", "")

    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()

    return pl.read_parquet(BytesIO(body))
