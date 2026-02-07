from pathlib import Path

from minio import Minio


class MinioClient:
    def __init__(
        self, endpoint: str, access_key: str, secret_key: str, secure: bool = False
    ) -> None:

        self.client = Minio(
            endpoint, access_key=access_key, secret_key=secret_key, secure=secure
        )

    def upload_file(self, bucket: str, object: str, file_path: str):
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        self.client.fget_object(bucket, object, file_path)
