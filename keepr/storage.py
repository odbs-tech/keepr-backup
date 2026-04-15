from __future__ import annotations

from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig

from keepr.config import KeeprConfig, S3Config
from keepr import output


def upload_to_s3(s3_config: S3Config, local_path: Path, s3_key: str) -> None:
    client = _get_s3_client(s3_config)
    full_key = f"{s3_config.prefix}{s3_key}" if s3_config.prefix else s3_key

    output.info(f"Uploading to S3: {s3_config.bucket}/{full_key}")
    client.upload_file(str(local_path), s3_config.bucket, full_key)
    output.success("S3 upload complete.")


def download_from_s3(s3_config: S3Config, s3_key: str, local_path: Path) -> None:
    client = _get_s3_client(s3_config)
    full_key = f"{s3_config.prefix}{s3_key}" if s3_config.prefix else s3_key

    local_path.parent.mkdir(parents=True, exist_ok=True)
    output.info(f"Downloading from S3: {s3_config.bucket}/{full_key}")
    client.download_file(s3_config.bucket, full_key, str(local_path))
    output.success("S3 download complete.")


def delete_from_s3(s3_config: S3Config, s3_key: str) -> None:
    client = _get_s3_client(s3_config)
    full_key = f"{s3_config.prefix}{s3_key}" if s3_config.prefix else s3_key

    client.delete_object(Bucket=s3_config.bucket, Key=full_key)


def _get_s3_client(s3_config: S3Config):
    kwargs = {"region_name": s3_config.region}
    if s3_config.endpoint_url:
        kwargs["endpoint_url"] = s3_config.endpoint_url
    if s3_config.access_key_id and s3_config.secret_access_key:
        kwargs["aws_access_key_id"] = s3_config.access_key_id
        kwargs["aws_secret_access_key"] = s3_config.secret_access_key
    return boto3.client("s3", **kwargs)
