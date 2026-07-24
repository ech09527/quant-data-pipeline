#!/usr/bin/env python3
"""合并本地 Parquet 文件，执行异常值检测，并可选上传 Kaggle。

数据流：本地读取已下载的 parquet → 合并 → 异常值处理 → 写本地 parquet → 上传 Kaggle
"""

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

import pandas as pd

from kaggle_publish import configure_kaggle_auth, publish_staging_dir
from outlier_detection import clean_kline_outliers


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"缺少环境变量: {name}", file=sys.stderr)
        sys.exit(1)
    return value


def merge_local_parquets(
    input_dir: str,
    output_path: Path,
    *,
    symbol_from_path: bool = True,
) -> int:
    """读取目录下所有 parquet 文件并合并为一个 DataFrame。

    Returns:
        合并后的总行数
    """
    input_path = Path(input_dir)
    if not input_path.exists():
        raise FileNotFoundError(f"输入目录不存在: {input_dir}")

    parquet_files = sorted(input_path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"未找到 parquet 文件: {input_dir}")

    print(f"发现 {len(parquet_files)} 个 parquet 文件")

    frames = []
    for pf in parquet_files:
        df = pd.read_parquet(pf)
        if symbol_from_path:
            # Extract symbol from parent directory name or filename stem
            # Expected structure: <dir>/<symbol>/.../<file>.parquet
            # or <dir>/<symbol>-<interval>.parquet
            parts = pf.relative_to(input_path).parts
            if len(parts) >= 2:
                symbol = parts[0]  # First directory component
            else:
                symbol = pf.stem.split("-")[0] if "-" in pf.stem else pf.stem
            if "symbol" not in df.columns:
                df["symbol"] = symbol
            else:
                # Fill empty symbol values from path
                mask = df["symbol"].isna() | (df["symbol"] == "")
                df.loc[mask, "symbol"] = symbol
        frames.append(df)

    merged = pd.concat(frames, ignore_index=True)
    print(f"合并完成: {len(merged)} 行")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(output_path, compression="snappy", index=False)
    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"写入: {output_path} ({size_mb:.1f} MB)")

    return len(merged)


def main() -> None:
    parser = argparse.ArgumentParser(description="合并本地 Parquet 文件并执行异常值检测")
    parser.add_argument(
        "--input-dir",
        default=os.environ.get("INPUT_DIR", "./data/raw"),
        help="包含待合并 parquet 文件的本地目录",
    )
    parser.add_argument(
        "--output-path",
        default=os.environ.get("OUTPUT_PATH", "./data/merged/output.parquet"),
        help="合并后输出文件路径",
    )
    parser.add_argument(
        "--skip-outlier-detection",
        action="store_true",
        default=False,
        help="跳过异常值检测步骤",
    )
    parser.add_argument(
        "--price-jump-threshold",
        type=float,
        default=float(os.environ.get("PRICE_JUMP_THRESHOLD", "0.10")),
        help="价格跳变阈值（默认 0.10 即 10%%）",
    )
    parser.add_argument(
        "--interval-ms",
        type=int,
        default=int(os.environ.get("INTERVAL_MS", "3600000")),
        help="K线间隔毫秒数（默认 3600000 即 1h）",
    )
    symbol_from_path_default = os.environ.get("SYMBOL_FROM_PATH", "true").strip().lower() in {
        "1", "true", "yes",
    }
    parser.add_argument(
        "--symbol-from-path",
        dest="symbol_from_path",
        action="store_true",
        default=symbol_from_path_default,
        help="从文件路径提取 symbol 列（默认开启）",
    )
    parser.add_argument(
        "--no-symbol-from-path",
        dest="symbol_from_path",
        action="store_false",
        help="不从路径提取 symbol",
    )
    publish_default = os.environ.get("PUBLISH_TO_KAGGLE", "").strip().lower() in {
        "1", "true", "yes",
    }
    parser.add_argument(
        "--publish-to-kaggle",
        action="store_true",
        default=publish_default,
        help="合并后发布到 Kaggle",
    )
    parser.add_argument(
        "--no-publish-to-kaggle",
        dest="publish_to_kaggle",
        action="store_false",
        help="仅合并，不上传 Kaggle",
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

    output_path = Path(args.output_path)

    # Step 1: Merge local parquets
    print(f"=== 步骤 1: 合并本地 Parquet ===")
    print(f"输入目录: {args.input_dir}")
    print(f"输出文件: {output_path}")
    print(f"symbol 提取: {'开启' if args.symbol_from_path else '关闭'}")

    merge_started = time.monotonic()
    total_rows = merge_local_parquets(
        args.input_dir,
        output_path,
        symbol_from_path=args.symbol_from_path,
    )
    merge_seconds = time.monotonic() - merge_started
    print(f"合并耗时: {merge_seconds:.1f}s\n")

    # Step 2: Outlier detection
    if not args.skip_outlier_detection:
        print(f"=== 步骤 2: 异常值检测 ===")
        print(f"价格跳变阈值: {args.price_jump_threshold:.0%}")
        print(f"K线间隔: {args.interval_ms}ms")

        outlier_started = time.monotonic()
        df = pd.read_parquet(output_path)
        cleaned_df, report = clean_kline_outliers(
            df,
            price_jump_threshold=args.price_jump_threshold,
            interval_ms=args.interval_ms,
        )
        print(report.summary())

        # Write cleaned data back
        cleaned_df.to_parquet(output_path, compression="snappy", index=False)
        size_mb = output_path.stat().st_size / (1024 * 1024)
        outlier_seconds = time.monotonic() - outlier_started
        print(f"清洗后文件大小: {size_mb:.1f} MB")
        print(f"异常值检测耗时: {outlier_seconds:.1f}s\n")
    else:
        print("=== 步骤 2: 异常值检测 (已跳过) ===\n")

    # Step 3: Upload to Kaggle
    if args.publish_to_kaggle:
        print(f"=== 步骤 3: 上传 Kaggle ===")
        dataset_slug = os.environ.get("KAGGLE_DATASET", "").strip()
        if not dataset_slug:
            print("缺少 KAGGLE_DATASET，无法发布到 Kaggle", file=sys.stderr)
            sys.exit(1)

        version_notes = os.environ.get(
            "VERSION_NOTES",
            "Merged parquet with outlier detection",
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

        staged_file = staging_dir / output_path.name
        shutil.copy2(output_path, staged_file)
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
        print(f"已清理 Kaggle 暂存目录: {staging_dir}\n")
    else:
        print("=== 步骤 3: 上传 Kaggle (已跳过) ===\n")

    print("管道任务完成")


if __name__ == "__main__":
    main()
