#!/usr/bin/env python3
"""下载 Binance UM USDT 永续资金费率（Vision 月度 ZIP），合并为单个 Parquet 并可选发布到 Kaggle。"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from io import BytesIO
from pathlib import Path
from typing import Iterator
from urllib.parse import urlsplit

import httpx
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from kaggle_publish import configure_kaggle_auth, publish_staging_dir

logger = logging.getLogger(__name__)

BINANCE_FAPI_BASE = "https://fapi.binance.com"
BINANCE_VISION_BASE = "https://data.binance.vision"
VISION_FUNDING_PREFIX = "data/futures/um/monthly/fundingRate"
FUNDING_COLUMNS = ["calc_time", "funding_interval_hours", "last_funding_rate"]
DEFAULT_OUTPUT_PATH = "binance/futures/um/fundingRate.parquet"


@dataclass(frozen=True)
class SymbolInfo:
    symbol: str
    onboard_year: int
    onboard_month: int


def _proxy_from_env(key: str) -> str | None:
    value = os.environ.get(key, "").strip()
    return value or None


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.environ.get(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes"}


def _previous_complete_month(today: date | None = None) -> tuple[int, int]:
    today = today or date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def parse_year_month(value: str) -> tuple[int, int]:
    text = value.strip()
    year_s, month_s = text.split("-", 1)
    year, month = int(year_s), int(month_s)
    if not (1 <= month <= 12):
        raise ValueError(f"非法月份: {value}")
    return year, month


def iter_months(start: date, end: date) -> Iterator[tuple[int, int]]:
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        yield year, month
        month += 1
        if month > 12:
            month = 1
            year += 1


def funding_zip_url(symbol: str, year: int, month: int) -> str:
    ym = f"{year:04d}-{month:02d}"
    filename = f"{symbol}-fundingRate-{ym}.zip"
    return f"{BINANCE_VISION_BASE}/{VISION_FUNDING_PREFIX}/{symbol}/{filename}"


@contextmanager
def fapi_client(*, proxy: str | None, timeout: float = 60) -> Iterator[httpx.Client]:
    kwargs: dict = {"timeout": timeout}
    if proxy:
        kwargs["proxy"] = proxy
    with httpx.Client(**kwargs) as client:
        yield client


@contextmanager
def vision_client(
    *,
    proxy: str | None,
    timeout: float = 120,
    follow_redirects: bool = True,
) -> Iterator[httpx.Client]:
    kwargs: dict = {"timeout": timeout, "follow_redirects": follow_redirects}
    if proxy:
        kwargs["proxy"] = proxy
    with httpx.Client(**kwargs) as client:
        yield client


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30))
def fetch_usdt_perpetual_symbol_info(*, fapi_proxy: str | None = None) -> list[SymbolInfo]:
    url = f"{BINANCE_FAPI_BASE}/fapi/v1/exchangeInfo"
    with fapi_client(proxy=fapi_proxy) as client:
        response = client.get(url)
        response.raise_for_status()
        payload = response.json()

    symbols: list[SymbolInfo] = []
    for item in payload.get("symbols", []):
        if (
            item.get("contractType") == "PERPETUAL"
            and item.get("quoteAsset") == "USDT"
            and item.get("status") == "TRADING"
        ):
            onboard_ms = item.get("onboardDate")
            if onboard_ms is None:
                logger.warning("Symbol %s missing onboardDate, skipping", item.get("symbol"))
                continue
            dt = datetime.fromtimestamp(int(onboard_ms) / 1000, tz=UTC)
            symbols.append(
                SymbolInfo(symbol=item["symbol"], onboard_year=dt.year, onboard_month=dt.month)
            )

    symbols.sort(key=lambda s: s.symbol)
    logger.info("Fetched %d USDT perpetual symbols", len(symbols))
    return symbols


def resolve_symbols(
    requested: list[str] | None,
    *,
    fapi_proxy: str | None,
) -> dict[str, tuple[int, int]]:
    all_info = fetch_usdt_perpetual_symbol_info(fapi_proxy=fapi_proxy)
    by_symbol = {info.symbol: (info.onboard_year, info.onboard_month) for info in all_info}
    if not requested:
        return by_symbol

    missing = [symbol for symbol in requested if symbol not in by_symbol]
    if missing:
        raise ValueError(f"未知或非 USDT 永续交易中合约: {', '.join(missing)}")
    return {symbol: by_symbol[symbol] for symbol in requested}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30))
def download_funding_zip(
    symbol: str,
    year: int,
    month: int,
    dest: Path,
    *,
    vision_proxy: str | None,
) -> bool:
    """下载月度 ZIP。存在返回 True；404 返回 False。"""
    url = funding_zip_url(symbol, year, month)
    dest.parent.mkdir(parents=True, exist_ok=True)

    with vision_client(proxy=vision_proxy) as client:
        with client.stream("GET", url) as response:
            if response.status_code == 404:
                logger.debug("Not found: %s", url)
                return False
            if response.status_code != 200:
                raise RuntimeError(f"HTTP {response.status_code} for {url}")
            with dest.open("wb") as handle:
                for chunk in response.iter_bytes(chunk_size=256 * 1024):
                    handle.write(chunk)

    logger.info("Downloaded %s", url)
    return True


def zip_to_frame(zip_path: Path, symbol: str) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [name for name in zf.namelist() if name.endswith(".csv")]
        if not csv_names:
            raise ValueError(f"No CSV in {zip_path}")
        with zf.open(csv_names[0]) as raw:
            csv_bytes = raw.read()

    frame = pd.read_csv(BytesIO(csv_bytes), header=None, names=FUNDING_COLUMNS)
    frame["symbol"] = symbol
    frame["calc_time"] = pd.to_numeric(frame["calc_time"], errors="coerce").astype("Int64")
    frame["funding_interval_hours"] = pd.to_numeric(
        frame["funding_interval_hours"], errors="coerce"
    ).astype("Int64")
    frame["last_funding_rate"] = pd.to_numeric(frame["last_funding_rate"], errors="coerce")
    return frame[["symbol", *FUNDING_COLUMNS]]


@dataclass
class DownloadResult:
    symbol: str
    year: int
    month: int
    status: str
    path: Path | None = None
    detail: str = ""


def _download_one(
    symbol: str,
    year: int,
    month: int,
    work_dir: Path,
    vision_proxy: str | None,
) -> DownloadResult:
    zip_path = work_dir / "zip" / symbol / f"{symbol}-fundingRate-{year:04d}-{month:02d}.zip"
    try:
        ok = download_funding_zip(symbol, year, month, zip_path, vision_proxy=vision_proxy)
        if not ok:
            return DownloadResult(symbol, year, month, "not_found")
        return DownloadResult(symbol, year, month, "ok", zip_path)
    except Exception as exc:  # noqa: BLE001 — collect per-task failures
        logger.exception("Failed %s %04d-%02d", symbol, year, month)
        return DownloadResult(symbol, year, month, "failed", detail=str(exc))


def download_and_merge(
    symbol_starts: dict[str, tuple[int, int]],
    *,
    start_override: tuple[int, int] | None,
    end_year: int,
    end_month: int,
    work_dir: Path,
    vision_proxy: str | None,
    max_workers: int,
) -> pd.DataFrame:
    end = date(end_year, end_month, 1)
    tasks: list[tuple[str, int, int]] = []
    for symbol, (onboard_year, onboard_month) in symbol_starts.items():
        if start_override:
            start_year, start_month = start_override
        else:
            start_year, start_month = onboard_year, onboard_month
        start = date(start_year, start_month, 1)
        if start > end:
            logger.warning(
                "Skipping %s: start %04d-%02d after end %04d-%02d",
                symbol,
                start_year,
                start_month,
                end_year,
                end_month,
            )
            continue
        for year, month in iter_months(start, end):
            tasks.append((symbol, year, month))

    logger.info(
        "Downloading %d task(s) for %d symbol(s), end %04d-%02d",
        len(tasks),
        len(symbol_starts),
        end_year,
        end_month,
    )

    results: list[DownloadResult] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_download_one, symbol, year, month, work_dir, vision_proxy): (
                symbol,
                year,
                month,
            )
            for symbol, year, month in tasks
        }
        for future in as_completed(futures):
            results.append(future.result())

    ok_paths = [item.path for item in results if item.status == "ok" and item.path is not None]
    failed = [item for item in results if item.status == "failed"]
    not_found = sum(1 for item in results if item.status == "not_found")
    logger.info(
        "Download done: ok=%d not_found=%d failed=%d",
        len(ok_paths),
        not_found,
        len(failed),
    )
    if failed:
        sample = ", ".join(
            f"{item.symbol}-{item.year:04d}-{item.month:02d}" for item in failed[:5]
        )
        raise RuntimeError(f"{len(failed)} download(s) failed, e.g. {sample}")
    if not ok_paths:
        raise RuntimeError("没有下载到任何 fundingRate 数据")

    frames: list[pd.DataFrame] = []
    for item in results:
        if item.status != "ok" or item.path is None:
            continue
        frames.append(zip_to_frame(item.path, item.symbol))

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.sort_values(["symbol", "calc_time"], kind="mergesort").reset_index(drop=True)
    return merged


def write_parquet(frame: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info("Wrote %s (%d rows, %.2f MB)", output_path, len(frame), size_mb)


def publish_to_kaggle(
    local_parquet: Path,
    *,
    dataset_slug: str,
    relative_output: str,
    version_notes: str,
    dataset_title: str,
    license_name: str,
    create_new: bool,
    dir_mode: str,
    public: bool,
    staging_dir: Path,
    preserve_existing: bool,
) -> None:
    configure_kaggle_auth()
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    if preserve_existing and not create_new:
        from kaggle.api.kaggle_api_extended import KaggleApi

        api = KaggleApi()
        api.authenticate()
        download_dir = staging_dir / "_download"
        download_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Downloading existing Kaggle dataset files: %s", dataset_slug)
        api.dataset_download_files(dataset_slug, path=str(download_dir), quiet=False, unzip=True)
        for path in download_dir.rglob("*"):
            if path.is_file() and path.name != "dataset-metadata.json":
                rel = path.relative_to(download_dir)
                dest = staging_dir / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(path), str(dest))
        shutil.rmtree(download_dir, ignore_errors=True)

    staged = staging_dir / relative_output.lstrip("/")
    staged.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(local_parquet, staged)
    logger.info("Kaggle staging file: %s", staged)

    publish_staging_dir(
        staging_dir,
        dataset_slug=dataset_slug,
        version_notes=version_notes,
        dataset_title=dataset_title,
        license_name=license_name,
        create_new=create_new,
        dir_mode=dir_mode,
        public=public,
    )
    shutil.rmtree(staging_dir)
    logger.info("Cleaned Kaggle staging dir: %s", staging_dir)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download Binance UM USDT fundingRate, merge to one Parquet, optionally publish to Kaggle",
    )
    parser.add_argument(
        "--symbols",
        default=os.environ.get("SYMBOLS", "").strip(),
        help="Comma-separated symbols (default: all USDT perpetual)",
    )
    parser.add_argument(
        "--start-month",
        default=os.environ.get("START_MONTH", "").strip(),
        help="YYYY-MM override start for all symbols",
    )
    parser.add_argument(
        "--end-month",
        default=os.environ.get("END_MONTH", "").strip(),
        help="YYYY-MM inclusive end (default: previous complete month)",
    )
    parser.add_argument(
        "--output",
        default=os.environ.get("OUTPUT_PATH", DEFAULT_OUTPUT_PATH).strip() or DEFAULT_OUTPUT_PATH,
        help=f"Relative output path inside work/kaggle staging (default: {DEFAULT_OUTPUT_PATH})",
    )
    parser.add_argument(
        "--work-dir",
        default=os.environ.get("WORK_DIR", "").strip(),
        help="Working directory (default: temp dir)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=int(os.environ.get("MAX_WORKERS", "8")),
    )
    parser.add_argument(
        "--publish-to-kaggle",
        action=argparse.BooleanOptionalAction,
        default=_bool_env("PUBLISH_TO_KAGGLE", False),
    )
    parser.add_argument(
        "--preserve-existing-dataset",
        action=argparse.BooleanOptionalAction,
        default=_bool_env("PRESERVE_EXISTING_DATASET", True),
        help="When updating Kaggle dataset, keep existing files and add/overwrite fundingRate",
    )
    parser.add_argument(
        "--kaggle-staging-dir",
        default=os.environ.get("KAGGLE_STAGING_DIR", "/tmp/kaggle-staging-funding"),
    )
    parser.add_argument(
        "--kaggle-dir-mode",
        choices=["zip", "tar"],
        default=os.environ.get("KAGGLE_DIR_MODE", "zip"),
    )
    parser.add_argument(
        "--kaggle-create-new",
        action=argparse.BooleanOptionalAction,
        default=_bool_env("KAGGLE_CREATE_NEW", False),
    )
    parser.add_argument(
        "--kaggle-public",
        action=argparse.BooleanOptionalAction,
        default=_bool_env("KAGGLE_PUBLIC", False),
    )
    parser.add_argument(
        "--symbols-only",
        action="store_true",
        help="Print USDT perpetual symbols and exit",
    )
    return parser


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    args = build_parser().parse_args()

    fapi_proxy = _proxy_from_env("FAPI_HTTP_PROXY")
    vision_proxy = _proxy_from_env("VISION_HTTP_PROXY")
    logger.info(
        "Proxies: FAPI=%s VISION=%s",
        urlsplit(fapi_proxy).hostname if fapi_proxy else None,
        urlsplit(vision_proxy).hostname if vision_proxy else None,
    )

    if args.symbols_only:
        for info in fetch_usdt_perpetual_symbol_info(fapi_proxy=fapi_proxy):
            print(info.symbol)
        return

    requested = [s.strip().upper() for s in args.symbols.split(",") if s.strip()] or None
    symbol_starts = resolve_symbols(requested, fapi_proxy=fapi_proxy)

    if args.end_month:
        end_year, end_month = parse_year_month(args.end_month)
    else:
        end_year, end_month = _previous_complete_month()

    start_override = parse_year_month(args.start_month) if args.start_month else None

    own_work_dir = not args.work_dir
    work_dir = Path(args.work_dir) if args.work_dir else Path(tempfile.mkdtemp(prefix="funding-rate-"))
    work_dir.mkdir(parents=True, exist_ok=True)
    local_output = work_dir / "out" / Path(args.output).name

    try:
        frame = download_and_merge(
            symbol_starts,
            start_override=start_override,
            end_year=end_year,
            end_month=end_month,
            work_dir=work_dir,
            vision_proxy=vision_proxy,
            max_workers=max(1, args.max_workers),
        )
        write_parquet(frame, local_output)

        print("--- preview ---")
        print(frame.head(10).to_string(index=False))
        print(
            f"symbols={frame['symbol'].nunique()} rows={len(frame)} "
            f"calc_time=[{frame['calc_time'].min()}, {frame['calc_time'].max()}]"
        )

        if args.publish_to_kaggle:
            dataset_slug = os.environ.get("KAGGLE_DATASET", "").strip()
            if not dataset_slug:
                print("缺少 KAGGLE_DATASET，无法发布到 Kaggle", file=sys.stderr)
                sys.exit(1)
            version_notes = os.environ.get(
                "VERSION_NOTES",
                "Add Binance UM USDT perpetual fundingRate parquet",
            ).strip()
            dataset_title = os.environ.get(
                "KAGGLE_DATASET_TITLE",
                dataset_slug.split("/", 1)[-1].replace("-", " ").title(),
            ).strip()
            license_name = os.environ.get("KAGGLE_LICENSE", "CC0-1.0").strip() or "CC0-1.0"
            publish_to_kaggle(
                local_output,
                dataset_slug=dataset_slug,
                relative_output=args.output,
                version_notes=version_notes,
                dataset_title=dataset_title,
                license_name=license_name,
                create_new=args.kaggle_create_new,
                dir_mode=args.kaggle_dir_mode,
                public=args.kaggle_public,
                staging_dir=Path(args.kaggle_staging_dir),
                preserve_existing=args.preserve_existing_dataset,
            )
    finally:
        if own_work_dir and work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)
            logger.info("Cleaned work dir: %s", work_dir)

    print("done")


if __name__ == "__main__":
    main()
