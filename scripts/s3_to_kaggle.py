#!/usr/bin/env python3
"""从 MinIO (S3 兼容) 下载数据并上传到 Kaggle 数据集。"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from kaggle_publish import configure_kaggle_auth, publish_staging_dir
from s3_client import build_s3_client, download_objects, inspect_matches


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"缺少环境变量: {name}", file=sys.stderr)
        sys.exit(1)
    return value


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

        upload_started = time.monotonic()
        publish_staging_dir(
            staging_dir,
            dataset_slug=dataset_slug,
            version_notes=version_notes,
            dataset_title=dataset_title,
            license_name=license_name,
            create_new=args.create_new,
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
