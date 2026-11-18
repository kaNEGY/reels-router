"""S3-совместимое хранение: блокирующий клиент вызывается вне event loop."""

import asyncio
import hashlib
from uuid import uuid4

import boto3


class Storage:
    def __init__(self, settings):
        self.bucket = settings.s3_bucket
        self.client = boto3.client("s3", endpoint_url=settings.s3_endpoint_url, region_name=settings.s3_region,
                                   aws_access_key_id=settings.s3_access_key_id,
                                   aws_secret_access_key=settings.s3_secret_access_key)

    async def upload(self, job_id, video_id, stage_id, files):
        attempt = uuid4()
        records = []
        for kind, path in files.items():
            object_id = uuid4()
            object_key = f"jobs/{job_id}/{video_id}/{stage_id}/{attempt}/{path.name}"
            digest = await asyncio.to_thread(self._hash, path)
            await asyncio.to_thread(self.client.upload_file, str(path), self.bucket, object_key)
            records.append({"id": object_id, "kind": kind, "bucket": self.bucket,
                            "object_key": object_key, "size_bytes": path.stat().st_size, "sha256": digest})
        return records

    async def download(self, bucket, key, path):
        await asyncio.to_thread(self.client.download_file, bucket, key, str(path))

    @staticmethod
    def _hash(path):
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
