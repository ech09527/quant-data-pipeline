#!/usr/bin/env python3
"""使用 DuckDB 从 MinIO (S3 兼容) 合并多个 Parquet 文件为一个。"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

import boto3
import duckdb
from boto3.exceptions import S3UploadFailedError
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

from kaggle_publish import configure_kaggle_auth, publish_staging_dir
from s3_client import parse_boto_endpoint


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"缺少环境变量: {name}", file=sys.stderr)
        sys.exit(1)
    return value


def parse_duckdb_endpoint(endpoint: str) -> tuple[str, bool]:
    normalized = endpoint if "://" in endpoint else f"https://{endpoint}"
    parsed = urlparse(normalized)
    host = parsed.hostname
    if not host:
        raise ValueError(f"无法解析 MINIO_ENDPOINT: {endpoint}")

    if parsed.port:
        host = f"{host}:{parsed.port}"

    use_ssl = parsed.scheme != "http"
    return host, use_ssl


def sql_literal(value: str) -> str:
    return value.replace("'", "''")


def configure_s3(
    con: duckdb.DuckDBPyConnection,
    *,
    endpoint: str,
    access_key: str,
    secret_key: str,
    region: str,
) -> None:
    host, use_ssl = parse_duckdb_endpoint(endpoint)

    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{sql_literal(host)}';")
    con.execute(f"SET s3_access_key_id='{sql_literal(access_key)}';")
    con.execute(f"SET s3_secret_access_key='{sql_literal(secret_key)}';")
    con.execute(f"SET s3_region='{sql_literal(region)}';")
    con.execute("SET s3_url_style='path';")
    con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'};")


def build_s3_uri(bucket: str, key: str) -> str:
    return f"s3://{bucket.strip('/')}/{key.lstrip('/')}"


SYMBOL_FROM_FILENAME_REGEX = r"/([^/]+)/[^/]+\.parquet$"


def source_has_column(
    con: duckdb.DuckDBPyConnection,
    source_uri: str,
    column_name: str,
) -> bool:
    columns = con.execute(
        f"""
        DESCRIBE
        SELECT * FROM read_parquet('{sql_literal(source_uri)}', union_by_name=true)
        LIMIT 0;
        """
    ).fetchall()
    return any(row[0] == column_name for row in columns)


def build_merge_select_sql(
    con: duckdb.DuckDBPyConnection,
    source_uri: str,
    *,
    symbol_from_path: bool,
) -> str:
    literal_uri = sql_literal(source_uri)
    if not symbol_from_path:
        return f"SELECT * FROM read_parquet('{literal_uri}', union_by_name=true)"

    has_symbol = source_has_column(con, source_uri, "symbol")
    symbol_expr = (
        "COALESCE("
        "NULLIF(CAST(t.symbol AS VARCHAR), ''), "
        f"regexp_extract(t.filename, '{SYMBOL_FROM_FILENAME_REGEX}', 1)"
        ")"
        if has_symbol
        else f"regexp_extract(t.filename, '{SYMBOL_FROM_FILENAME_REGEX}', 1)"
    )
    exclude_cols = ["filename"]
    if has_symbol:
        exclude_cols.append("symbol")

    return f"""
        SELECT
            t.* EXCLUDE ({", ".join(exclude_cols)}),
            {symbol_expr} AS symbol
        FROM (
            SELECT *
            FROM read_parquet('{literal_uri}', union_by_name=true, filename=true)
        ) AS t
    """


def merge_to_local(
    con: duckdb.DuckDBPyConnection,
    *,
    source_uri: str,
    local_path: Path,
    temp_directory: str,
    memory_limit: str,
    symbol_from_path: bool,
) -> None:
    con.execute(f"SET temp_directory='{sql_literal(temp_directory)}';")
    con.execute(f"SET memory_limit='{sql_literal(memory_limit)}';")

    local_path.parent.mkdir(parents=True, exist_ok=True)
    select_sql = build_merge_select_sql(
        con,
        source_uri,
        symbol_from_path=symbol_from_path,
    )
    con.execute(
        f"""
        COPY (
            {select_sql}
        ) TO '{sql_literal(str(local_path))}' (FORMAT PARQUET, COMPRESSION SNAPPY);
        """
    )


def is_gateway_timeout(exc: BaseException) -> bool:
    if isinstance(exc, ClientError):
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        return code == "524" or status == 524
    return "524" in str(exc)


def upload_multipart(
    *,
    local_path: Path,
    endpoint: str,
    access_key: str,
    secret_key: str,
    region: str,
    bucket: str,
    key: str,
    chunk_size: int,
    max_concurrency: int,
    max_retries: int,
) -> None:
    client = boto3.client(
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
    transfer_config = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,
        multipart_chunksize=chunk_size,
        max_concurrency=max_concurrency,
        use_threads=max_concurrency > 1,
    )

    size_mb = local_path.stat().st_size / (1024 * 1024)
    object_key = key.lstrip("/")
    print(
        f"分段上传: s3://{bucket}/{object_key} "
        f"({size_mb:.1f} MB, chunk={chunk_size // (1024 * 1024)}MB, "
        f"concurrency={max_concurrency})"
    )

    for attempt in range(1, max_retries + 1):
        try:
            upload_started = time.monotonic()
            client.upload_file(
                str(local_path),
                bucket,
                object_key,
                Config=transfer_config,
            )
            upload_seconds = time.monotonic() - upload_started
            print(f"上传完成，耗时 {upload_seconds:.1f}s")
            return
        except (S3UploadFailedError, ClientError) as exc:
            if not is_gateway_timeout(exc) or attempt == max_retries:
                raise
            wait_seconds = min(60, 2**attempt)
            print(
                f"网关超时 (attempt {attempt}/{max_retries}): {exc}; "
                f"{wait_seconds}s 后重试..."
            )
            time.sleep(wait_seconds)


def inspect_source(
    con: duckdb.DuckDBPyConnection,
    source_uri: str,
    *,
    symbol_from_path: bool,
) -> None:
    file_count = con.execute(
        f"SELECT count(*) FROM glob('{sql_literal(source_uri)}');"
    ).fetchone()[0]
    select_sql = build_merge_select_sql(
        con,
        source_uri,
        symbol_from_path=symbol_from_path,
    )
    row_count = con.execute(f"SELECT count(*) FROM ({select_sql}) AS merged;").fetchone()[0]
    print(f"匹配文件数: {file_count}")
    print(f"总行数: {row_count}")
    if symbol_from_path:
        symbol_count = con.execute(
            f"SELECT count(DISTINCT symbol) FROM ({select_sql}) AS merged "
            "WHERE symbol IS NOT NULL AND symbol <> '';"
        ).fetchone()[0]
        empty_symbol_count = con.execute(
            f"SELECT count(*) FROM ({select_sql}) AS merged "
            "WHERE symbol IS NULL OR symbol = '';"
        ).fetchone()[0]
        print(f"symbol 列: 从文件路径提取，distinct={symbol_count}")
        if empty_symbol_count:
            print(f"警告: 有 {empty_symbol_count} 行未能解析 symbol")


def main() -> None:
    parser = argparse.ArgumentParser(description="合并 MinIO 上的 Parquet 文件")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅统计匹配文件数和行数，不执行合并",
    )
    parser.add_argument(
        "--temp-directory",
        default=os.environ.get("DUCKDB_TEMP_DIRECTORY", "/tmp/duckdb"),
        help="DuckDB 临时目录，用于磁盘溢写",
    )
    parser.add_argument(
        "--memory-limit",
        default=os.environ.get("DUCKDB_MEMORY_LIMIT", "4GB"),
        help="DuckDB 内存上限，超出部分落盘",
    )
    parser.add_argument(
        "--multipart-chunk-size",
        type=int,
        default=int(os.environ.get("MULTIPART_CHUNK_SIZE", str(8 * 1024 * 1024))),
        help="S3 分段上传每段大小（字节），默认 8MB",
    )
    parser.add_argument(
        "--multipart-concurrency",
        type=int,
        default=int(os.environ.get("MULTIPART_CONCURRENCY", "2")),
        help="S3 分段上传并发数，默认 2",
    )
    parser.add_argument(
        "--upload-max-retries",
        type=int,
        default=int(os.environ.get("UPLOAD_MAX_RETRIES", "5")),
        help="网关超时后的整次上传重试次数，默认 5",
    )
    symbol_from_path_default = os.environ.get("SYMBOL_FROM_PATH", "true").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    parser.add_argument(
        "--symbol-from-path",
        dest="symbol_from_path",
        action="store_true",
        default=symbol_from_path_default,
        help="从 Parquet 文件路径提取 symbol 列（默认开启）",
    )
    parser.add_argument(
        "--no-symbol-from-path",
        dest="symbol_from_path",
        action="store_false",
        help="不提取 symbol，仅合并原始列",
    )
    publish_default = os.environ.get("PUBLISH_TO_KAGGLE", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    parser.add_argument(
        "--publish-to-kaggle",
        action="store_true",
        default=publish_default,
        help="合并并上传 MinIO 后，直接发布到 Kaggle（复用本地文件，无需二次下载）",
    )
    parser.add_argument(
        "--no-publish-to-kaggle",
        dest="publish_to_kaggle",
        action="store_false",
        help="仅合并并写回 MinIO，不上传 Kaggle",
    )
    parser.add_argument(
        "--kaggle-staging-dir",
        default=os.environ.get("KAGGLE_STAGING_DIR", "/tmp/kaggle-staging"),
        help="Kaggle 上传暂存目录",
    )
    parser.add_argument(
        "--kaggle-dir-mode",
        choices=("zip", "tar"),
        default=os.environ.get("KAGGLE_DIR_MODE", "zip"),
        help="Kaggle 上传打包方式，默认 zip",
    )
    parser.add_argument(
        "--kaggle-create-new",
        action="store_true",
        default=os.environ.get("KAGGLE_CREATE_NEW", "").strip().lower()
        in {"1", "true", "yes"},
        help="创建新的 Kaggle 数据集（默认上传新版本）",
    )
    parser.add_argument(
        "--kaggle-public",
        action="store_true",
        default=os.environ.get("KAGGLE_PUBLIC", "").strip().lower() in {"1", "true", "yes"},
        help="创建新数据集时设为公开（仅 --kaggle-create-new 生效）",
    )
    args = parser.parse_args()

    endpoint = require_env("MINIO_ENDPOINT")
    access_key = require_env("MINIO_ACCESS_KEY")
    secret_key = require_env("MINIO_SECRET_KEY")
    region = os.environ.get("MINIO_REGION", "us-east-1").strip() or "us-east-1"
    input_bucket = require_env("INPUT_BUCKET")
    output_bucket = os.environ.get("OUTPUT_BUCKET", input_bucket).strip() or input_bucket
    input_glob = require_env("INPUT_GLOB")
    output_path = require_env("OUTPUT_PATH")

    source_uri = build_s3_uri(input_bucket, input_glob)
    dest_uri = build_s3_uri(output_bucket, output_path)
    local_output = Path(args.temp_directory) / Path(output_path).name

    Path(args.temp_directory).mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    configure_s3(
        con,
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
    )

    print(f"输入: {source_uri}")
    print(f"symbol 提取: {'开启（从路径）' if args.symbol_from_path else '关闭'}")
    if args.dry_run:
        inspect_source(con, source_uri, symbol_from_path=args.symbol_from_path)
        print("dry-run 完成，未写入输出文件")
        return

    print(f"输出: {dest_uri}")
    print(f"本地临时文件: {local_output}")
    try:
        merge_started = time.monotonic()
        merge_to_local(
            con,
            source_uri=source_uri,
            local_path=local_output,
            temp_directory=args.temp_directory,
            memory_limit=args.memory_limit,
            symbol_from_path=args.symbol_from_path,
        )
        merge_seconds = time.monotonic() - merge_started
        size_mb = local_output.stat().st_size / (1024 * 1024)
        print(f"合并完成，耗时 {merge_seconds:.1f}s，文件大小 {size_mb:.1f} MB")
        upload_multipart(
            local_path=local_output,
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            region=region,
            bucket=output_bucket,
            key=output_path,
            chunk_size=args.multipart_chunk_size,
            max_concurrency=args.multipart_concurrency,
            max_retries=args.upload_max_retries,
        )

        if args.publish_to_kaggle:
            dataset_slug = os.environ.get("KAGGLE_DATASET", "").strip()
            if not dataset_slug:
                print("缺少 KAGGLE_DATASET，无法发布到 Kaggle", file=sys.stderr)
                sys.exit(1)

            version_notes = os.environ.get(
                "VERSION_NOTES",
                "Merged parquet with symbol column from file path",
            ).strip()
            dataset_title = os.environ.get(
                "KAGGLE_DATASET_TITLE",
                dataset_slug.split("/", 1)[-1].replace("-", " ").title(),
            ).strip()
            license_name = os.environ.get("KAGGLE_LICENSE", "CC0-1.0").strip() or "CC0-1.0"

            configure_kaggle_auth()

            staging_dir = Path(args.kaggle_staging_dir)
            if staging_dir.exists():
                shutil.rmtree(staging_dir)
            staging_dir.mkdir(parents=True, exist_ok=True)
            staged_file = staging_dir / output_path.lstrip("/")
            staged_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_output, staged_file)
            print(f"Kaggle 暂存: {staged_file}")

            publish_started = time.monotonic()
            publish_staging_dir(
                staging_dir,
                dataset_slug=dataset_slug,
                version_notes=version_notes,
                dataset_title=dataset_title,
                license_name=license_name,
                create_new=args.kaggle_create_new,
                dir_mode=args.kaggle_dir_mode,
                public=args.kaggle_public,
            )
            publish_seconds = time.monotonic() - publish_started
            print(f"Kaggle 发布完成，耗时 {publish_seconds:.1f}s")

            shutil.rmtree(staging_dir)
            print(f"已清理 Kaggle 暂存目录: {staging_dir}")
    finally:
        if local_output.exists():
            local_output.unlink()
            print(f"已删除本地临时文件: {local_output}")

    print("管道任务完成")


if __name__ == "__main__":
    main()
