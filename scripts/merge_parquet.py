#!/usr/bin/env python3
"""使用 DuckDB 从 MinIO (S3 兼容) 合并多个 Parquet 文件为一个。"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

import duckdb


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"缺少环境变量: {name}", file=sys.stderr)
        sys.exit(1)
    return value


def parse_endpoint(endpoint: str) -> tuple[str, bool]:
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
    host, use_ssl = parse_endpoint(endpoint)

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


def merge_parquet(
    con: duckdb.DuckDBPyConnection,
  *,
  source_uri: str,
  dest_uri: str,
  temp_directory: str,
  memory_limit: str,
) -> None:
    con.execute(f"SET temp_directory='{sql_literal(temp_directory)}';")
    con.execute(f"SET memory_limit='{sql_literal(memory_limit)}';")

    con.execute(
        f"""
        COPY (
            SELECT * FROM read_parquet('{sql_literal(source_uri)}', union_by_name=true)
        ) TO '{sql_literal(dest_uri)}' (FORMAT PARQUET, COMPRESSION SNAPPY);
        """
    )


def inspect_source(con: duckdb.DuckDBPyConnection, source_uri: str) -> None:
    file_count = con.execute(
        f"SELECT count(*) FROM glob('{sql_literal(source_uri)}');"
    ).fetchone()[0]
    row_count = con.execute(
        f"SELECT count(*) FROM read_parquet('{sql_literal(source_uri)}', union_by_name=true);"
    ).fetchone()[0]
    print(f"匹配文件数: {file_count}")
    print(f"总行数: {row_count}")


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
    if args.dry_run:
        inspect_source(con, source_uri)
        print("dry-run 完成，未写入输出文件")
        return

    print(f"输出: {dest_uri}")
    merge_parquet(
        con,
        source_uri=source_uri,
        dest_uri=dest_uri,
        temp_directory=args.temp_directory,
        memory_limit=args.memory_limit,
    )
    print("合并完成")


if __name__ == "__main__":
    main()
