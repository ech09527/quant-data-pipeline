#!/usr/bin/env python3
"""从 MinIO (S3 兼容) 下载数据并上传到 Kaggle 数据集。"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import boto3
from botocore.config import Config

if TYPE_CHECKING:
    from kaggle.api.kaggle_api_extended import KaggleApi


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"缺少环境变量: {name}", file=sys.stderr)
        sys.exit(1)
    return value


def configure_kaggle_auth() -> str:
    """配置 Kaggle 认证，优先使用 Access Token。"""
    api_token = os.environ.get("KAGGLE_API_TOKEN", "").strip()
    legacy_key = os.environ.get("KAGGLE_KEY", "").strip()
    username = os.environ.get("KAGGLE_USERNAME", "").strip()

    if not api_token and legacy_key.startswith("KGAT_"):
        os.environ["KAGGLE_API_TOKEN"] = legacy_key
        api_token = legacy_key
        print(
            "提示: 检测到 KAGGLE_KEY 为 Access Token，请改用 KAGGLE_API_TOKEN",
            file=sys.stderr,
        )

    if api_token:
        return "access_token"

    if username and legacy_key:
        return "legacy"

    print(
        "缺少 Kaggle 认证，请配置以下任一方式：\n"
        "  1. KAGGLE_API_TOKEN=<Access Token>\n"
        "  2. KAGGLE_USERNAME + KAGGLE_KEY=<Legacy API Key>",
        file=sys.stderr,
    )
    sys.exit(1)


def get_kaggle_api() -> KaggleApi:
    auth_method = configure_kaggle_auth()
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()
    print(f"Kaggle 认证成功 ({auth_method})")
    return api


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
    staging_dir: Path,
) -> None:
    for key in keys:
        local_path = staging_dir / key
        local_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"下载: s3://{bucket}/{key} -> {local_path}")
        client.download_file(bucket, key, str(local_path))


def write_dataset_metadata(
    staging_dir: Path,
    *,
    dataset_slug: str,
    title: str,
    license_name: str,
) -> Path:
    metadata_path = staging_dir / "dataset-metadata.json"
    metadata = {
        "title": title,
        "id": dataset_slug,
        "licenses": [{"name": license_name}],
    }
    metadata_path.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return metadata_path


def ensure_kaggle_metadata(
    api: KaggleApi,
    staging_dir: Path,
    *,
    dataset_slug: str,
    title: str,
    license_name: str,
    create_new: bool,
) -> None:
    metadata_path = staging_dir / "dataset-metadata.json"
    if create_new:
        write_dataset_metadata(
            staging_dir,
            dataset_slug=dataset_slug,
            title=title,
            license_name=license_name,
        )
        print(f"已写入新建数据集元数据: {metadata_path}")
        return

    try:
        api.dataset_metadata(dataset_slug, path=str(staging_dir))
        print(f"已同步 Kaggle 数据集元数据: {metadata_path}")
    except Exception as exc:
        print(
            f"无法获取已有数据集元数据 ({dataset_slug}): {exc}",
            file=sys.stderr,
        )
        print("将使用本地元数据模板继续上传", file=sys.stderr)
        write_dataset_metadata(
            staging_dir,
            dataset_slug=dataset_slug,
            title=title,
            license_name=license_name,
        )


def upload_to_kaggle(
    api: KaggleApi,
    staging_dir: Path,
    *,
    create_new: bool,
    version_notes: str,
    dir_mode: str,
    public: bool,
) -> None:
    folder = str(staging_dir)
    if create_new:
        print(f"创建 Kaggle 数据集: {folder}")
        api.dataset_create_new(
            folder,
            public=public,
            convert_to_csv=False,
            dir_mode=dir_mode,
        )
        return

    print(f"上传 Kaggle 数据集新版本: {folder}")
    api.dataset_create_version(
        folder,
        version_notes,
        convert_to_csv=False,
        dir_mode=dir_mode,
    )


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


def main() -> None:
    parser = argparse.ArgumentParser(description="将 MinIO 上的数据上传到 Kaggle 数据集")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅列出匹配对象，不下载或上传",
    )
    parser.add_argument(
        "--create-new",
        action="store_true",
        help="创建新的 Kaggle 数据集（默认上传新版本）",
    )
    parser.add_argument(
        "--staging-dir",
        default=os.environ.get("KAGGLE_STAGING_DIR", "/tmp/kaggle-staging"),
        help="本地下载与上传暂存目录",
    )
    parser.add_argument(
        "--dir-mode",
        choices=("zip", "tar"),
        default=os.environ.get("KAGGLE_DIR_MODE", "zip"),
        help="Kaggle 上传打包方式，默认 zip",
    )
    parser.add_argument(
        "--public",
        action="store_true",
        default=os.environ.get("KAGGLE_PUBLIC", "").strip().lower() in {"1", "true", "yes"},
        help="创建新数据集时设为公开（仅 --create-new 生效）",
    )
    args = parser.parse_args()

    endpoint = require_env("MINIO_ENDPOINT")
    access_key = require_env("MINIO_ACCESS_KEY")
    secret_key = require_env("MINIO_SECRET_KEY")
    region = os.environ.get("MINIO_REGION", "us-east-1").strip() or "us-east-1"
    input_bucket = require_env("INPUT_BUCKET")
    input_glob = require_env("INPUT_GLOB")
    dataset_slug = require_env("KAGGLE_DATASET")
    version_notes = os.environ.get("VERSION_NOTES", "Uploaded from S3 via quant-data-pipeline").strip()
    dataset_title = os.environ.get(
        "KAGGLE_DATASET_TITLE",
        dataset_slug.split("/", 1)[-1].replace("-", " ").title(),
    ).strip()
    license_name = os.environ.get("KAGGLE_LICENSE", "CC0-1.0").strip() or "CC0-1.0"

    configure_kaggle_auth()

    client = build_s3_client(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
    )

    print(f"输入: s3://{input_bucket}/{input_glob}")
    print(f"目标数据集: {dataset_slug}")

    keys = inspect_matches(client, bucket=input_bucket, glob_pattern=input_glob)
    if args.dry_run or not keys:
        if args.dry_run:
            print("dry-run 完成，未下载或上传")
        return

    staging_dir = Path(args.staging_dir)
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    try:
        download_started = time.monotonic()
        download_objects(client, bucket=input_bucket, keys=keys, staging_dir=staging_dir)
        download_seconds = time.monotonic() - download_started
        print(f"下载完成，耗时 {download_seconds:.1f}s")

        api = get_kaggle_api()

        ensure_kaggle_metadata(
            api,
            staging_dir,
            dataset_slug=dataset_slug,
            title=dataset_title,
            license_name=license_name,
            create_new=args.create_new,
        )

        upload_started = time.monotonic()
        upload_to_kaggle(
            api,
            staging_dir,
            create_new=args.create_new,
            version_notes=version_notes,
            dir_mode=args.dir_mode,
            public=args.public,
        )
        upload_seconds = time.monotonic() - upload_started
        print(f"Kaggle 上传完成，耗时 {upload_seconds:.1f}s")
    finally:
        if staging_dir.exists():
            shutil.rmtree(staging_dir)
            print(f"已清理暂存目录: {staging_dir}")

    print("传输完成")


if __name__ == "__main__":
    main()
