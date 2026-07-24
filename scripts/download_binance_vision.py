#!/usr/bin/env python3
"""Download Binance Vision kline ZIP archives, extract CSV to Parquet (per-symbol).

Data flow: fapi.binance.com (symbol list) -> data.binance.vision (ZIP download) -> local Parquet
Output directory structure: <output-dir>/<symbol>/<symbol>-<interval>-<YYYY-MM>.parquet

Proxies:
  FAPI_HTTP_PROXY  - required for fapi.binance.com
  VISION_HTTP_PROXY - optional, can accelerate data.binance.vision downloads
"""

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

logger = logging.getLogger(__name__)

BINANCE_FAPI_BASE = "https://fapi.binance.com"
BINANCE_VISION_BASE = "https://data.binance.vision"
VISION_KLINE_PREFIX = "data/futures/um/monthly/klines"

KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
]


@dataclass(frozen=True)
class SymbolInfo:
    symbol: str
    onboard_year: int
    onboard_month: int


def _proxy_from_env(key: str) -> str | None:
    value = os.environ.get(key, "").strip()
    return value or None


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
        raise ValueError(f"Invalid month: {value}")
    return year, month


def iter_months(start: date, end: date) -> Iterator[tuple[int, int]]:
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        yield year, month
        month += 1
        if month > 12:
            month = 1
            year += 1


def kline_zip_url(symbol: str, interval: str, year: int, month: int) -> str:
    ym = f"{year:04d}-{month:02d}"
    filename = f"{symbol}-{interval}-{ym}.zip"
    return f"{BINANCE_VISION_BASE}/{VISION_KLINE_PREFIX}/{symbol}/{interval}/{filename}"


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
def fetch_usdt_perpetual_symbols(*, fapi_proxy: str | None = None) -> list[SymbolInfo]:
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
    all_info = fetch_usdt_perpetual_symbols(fapi_proxy=fapi_proxy)
    by_symbol = {info.symbol: (info.onboard_year, info.onboard_month) for info in all_info}
    if not requested:
        return by_symbol

    missing = [symbol for symbol in requested if symbol not in by_symbol]
    if missing:
        raise ValueError(f"Unknown or non-USDT perpetual symbols: {', '.join(missing)}")
    return {symbol: by_symbol[symbol] for symbol in requested}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30))
def download_kline_zip(
    symbol: str,
    interval: str,
    year: int,
    month: int,
    dest: Path,
    *,
    vision_proxy: str | None,
) -> bool:
    """Download monthly kline ZIP. Returns True if downloaded, False if 404."""
    url = kline_zip_url(symbol, interval, year, month)
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


def zip_to_parquet(zip_path: Path, parquet_path: Path) -> int:
    """Extract CSV from ZIP and write Parquet. Returns row count."""
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            raise ValueError(f"No CSV found in {zip_path}")
        with zf.open(csv_names[0]) as raw:
            csv_bytes = raw.read()

    df = pd.read_csv(BytesIO(csv_bytes), header=None, names=KLINE_COLUMNS)

    int_cols = ["open_time", "close_time", "count"]
    float_cols = [
        "open", "high", "low", "close",
        "volume", "quote_volume",
        "taker_buy_volume", "taker_buy_quote_volume",
        "ignore",
    ]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_path, engine="pyarrow", compression="snappy", index=False)
    logger.info("Converted %s -> %s (%d rows)", zip_path.name, parquet_path.name, len(df))
    return len(df)


@dataclass
class DownloadResult:
    symbol: str
    year: int
    month: int
    status: str  # ok, not_found, failed, skipped
    parquet_path: Path | None = None
    detail: str = ""


def _download_and_convert_one(
    symbol: str,
    interval: str,
    year: int,
    month: int,
    output_dir: Path,
    temp_dir: Path,
    vision_proxy: str | None,
    skip_existing: bool,
) -> DownloadResult:
    ym = f"{year:04d}-{month:02d}"
    parquet_path = output_dir / symbol / f"{symbol}-{interval}-{ym}.parquet"

    if skip_existing and parquet_path.exists():
        logger.debug("Skipping existing: %s", parquet_path)
        return DownloadResult(symbol, year, month, "skipped", parquet_path)

    zip_path = temp_dir / "zip" / symbol / f"{symbol}-{interval}-{ym}.zip"
    try:
        ok = download_kline_zip(symbol, interval, year, month, zip_path, vision_proxy=vision_proxy)
        if not ok:
            return DownloadResult(symbol, year, month, "not_found")
        zip_to_parquet(zip_path, parquet_path)
        return DownloadResult(symbol, year, month, "ok", parquet_path)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed %s %s", symbol, ym)
        return DownloadResult(symbol, year, month, "failed", detail=str(exc))


def download_all(
    symbol_starts: dict[str, tuple[int, int]],
    *,
    interval: str,
    start_override: tuple[int, int] | None,
    end_year: int,
    end_month: int,
    output_dir: Path,
    temp_dir: Path,
    vision_proxy: str | None,
    max_workers: int,
    skip_existing: bool,
) -> list[DownloadResult]:
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
                symbol, start_year, start_month, end_year, end_month,
            )
            continue
        for year, month in iter_months(start, end):
            tasks.append((symbol, year, month))

    logger.info(
        "Downloading %d task(s) for %d symbol(s), interval=%s, end=%04d-%02d",
        len(tasks), len(symbol_starts), interval, end_year, end_month,
    )

    results: list[DownloadResult] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _download_and_convert_one,
                symbol, interval, year, month,
                output_dir, temp_dir, vision_proxy, skip_existing,
            ): (symbol, year, month)
            for symbol, year, month in tasks
        }
        for future in as_completed(futures):
            results.append(future.result())

    ok_count = sum(1 for r in results if r.status == "ok")
    skipped_count = sum(1 for r in results if r.status == "skipped")
    not_found_count = sum(1 for r in results if r.status == "not_found")
    failed = [r for r in results if r.status == "failed"]
    logger.info(
        "Download done: ok=%d skipped=%d not_found=%d failed=%d",
        ok_count, skipped_count, not_found_count, len(failed),
    )
    if failed:
        sample = ", ".join(f"{r.symbol}-{r.year:04d}-{r.month:02d}" for r in failed[:5])
        raise RuntimeError(f"{len(failed)} download(s) failed, e.g. {sample}")

    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download Binance Vision USDT perpetual klines to local Parquet files",
    )
    parser.add_argument(
        "--symbols",
        default=os.environ.get("SYMBOLS", "").strip(),
        help="Comma-separated symbols (default: all USDT perpetual)",
    )
    parser.add_argument(
        "--interval",
        default=os.environ.get("INTERVAL", "1h").strip(),
        help="Kline interval: 1h, 4h, 1d, etc. (default: 1h)",
    )
    parser.add_argument(
        "--start-month",
        default=os.environ.get("START_MONTH", "").strip(),
        help="Start month YYYY-MM (default: per-symbol onboard date)",
    )
    parser.add_argument(
        "--end-month",
        default=os.environ.get("END_MONTH", "").strip(),
        help="End month YYYY-MM inclusive (default: previous complete month)",
    )
    parser.add_argument(
        "--output-dir",
        default=os.environ.get("OUTPUT_DIR", "/tmp/binance-klines").strip(),
        help="Output directory for Parquet files (default: /tmp/binance-klines)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=int(os.environ.get("MAX_WORKERS", "8")),
        help="Concurrent download workers (default: 8)",
    )
    parser.add_argument(
        "--skip-existing",
        action=argparse.BooleanOptionalAction,
        default=os.environ.get("SKIP_EXISTING", "true").strip().lower() in {"1", "true", "yes"},
        help="Skip already-downloaded Parquet files (default: true)",
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
        for info in fetch_usdt_perpetual_symbols(fapi_proxy=fapi_proxy):
            print(info.symbol)
        return

    requested = [s.strip().upper() for s in args.symbols.split(",") if s.strip()] or None
    symbol_starts = resolve_symbols(requested, fapi_proxy=fapi_proxy)

    if args.end_month:
        end_year, end_month = parse_year_month(args.end_month)
    else:
        end_year, end_month = _previous_complete_month()

    start_override = parse_year_month(args.start_month) if args.start_month else None

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    temp_dir = Path(tempfile.mkdtemp(prefix="binance-vision-dl-"))

    try:
        results = download_all(
            symbol_starts,
            interval=args.interval,
            start_override=start_override,
            end_year=end_year,
            end_month=end_month,
            output_dir=output_dir,
            temp_dir=temp_dir,
            vision_proxy=vision_proxy,
            max_workers=max(1, args.max_workers),
            skip_existing=args.skip_existing,
        )

        parquet_files = sorted(
            str(r.parquet_path) for r in results
            if r.status in ("ok", "skipped") and r.parquet_path is not None
        )
        print(f"\n=== Download Summary ===")
        print(f"Output dir: {output_dir}")
        print(f"Parquet files: {len(parquet_files)}")
        print(f"Interval: {args.interval}")
        print(f"Symbols: {len(symbol_starts)}")

        # Print file list for downstream consumption
        if parquet_files:
            print(f"\n--- File List ---")
            for pf in parquet_files:
                print(pf)

    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
            logger.info("Cleaned temp dir: %s", temp_dir)

    print("\ndone")


if __name__ == "__main__":
    main()
