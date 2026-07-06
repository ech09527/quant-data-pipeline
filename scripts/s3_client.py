"""MinIO (S3 兼容) 客户端工具。"""

from __future__ import annotations

import fnmatch
from urllib.parse import urlparse

import boto3
from botocore.config import Config


def parse_boto_endpoint(endpoint: str) -> str:
    normalized = endpoint if "://" in endpoint else f"https://{endpoint}"
    parsed = urlparse(normalized)
    if not parsed.hostname:
        raise ValueError(f"无法解析 MINIO_ENDPOINT: {endpoint}")

    endpoint_url = f"{parsed.scheme}://{parsed.hostname}"
    if parsed.port:
        endpoint_url = f"{endpoint_url}:{parsed.port}"
    return endpoint_url


def glob_list_prefix(glob_pattern: str) -> str:
    wildcard_index = glob_pattern.find("*")
    if wildcard_index == -1:
        if "/" in glob_pattern:
            return glob_pattern.rsplit("/", 1)[0] + "/"
        return ""
    return glob_pattern[:wildcard_index]


def build_s3_client(
    *,
    endpoint: str,
    access_key: str,
    secret_key: str,
    region: str,
) -> boto3.client:
    return boto3.client(
        "s3",
        endpoint_url=parse_boto_endpoint(endpoint),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 10, "mode": "adaptive"},
            connect_timeout=60,
            read_timeout=300,
        ),
    )


def list_matching_keys(
    client: boto3.client,
    *,
    bucket: str,
    glob_pattern: str,
) -> list[str]:
    prefix = glob_list_prefix(glob_pattern)
    keys: list[str] = []

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if fnmatch.fnmatch(key, glob_pattern):
                keys.append(key)

    keys.sort()
    return keys


def download_objects(
    client: boto3.client,
    *,
    bucket: str,
    keys: list[str],
    staging_dir,
) -> None:
    from pathlib import Path

    staging_root = Path(staging_dir)
    for key in keys:
        local_path = staging_root / key
        local_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"下载: s3://{bucket}/{key} -> {local_path}")
        client.download_file(bucket, key, str(local_path))


def format_bytes(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KB"
    if size < 1024 * 1024 * 1024:
        return f"{size / (1024 * 1024):.1f} MB"
    return f"{size / (1024 * 1024 * 1024):.2f} GB"


def inspect_matches(
    client: boto3.client,
    *,
    bucket: str,
    glob_pattern: str,
) -> list[str]:
    keys = list_matching_keys(client, bucket=bucket, glob_pattern=glob_pattern)
    if not keys:
        print("未匹配到任何对象")
        return keys

    total_size = 0
    print(f"匹配对象数: {len(keys)}")
    for key in keys:
        head = client.head_object(Bucket=bucket, Key=key)
        size = int(head.get("ContentLength", 0))
        total_size += size
        print(f"  - s3://{bucket}/{key} ({format_bytes(size)})")
    print(f"总大小: {format_bytes(total_size)}")
    return keys
