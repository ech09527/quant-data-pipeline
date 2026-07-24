#!/usr/bin/env python3
"""K线异常值检测与清洗模块。

针对合约1h K线数据的量化因子标准异常值处理：
- OHLC一致性校验（自动修正）
- 价格跳变检测（标记，按 symbol 分组计算）
- 成交量异常（标记）
- 时间戳连续性检查（标记，按 symbol 分组计算）
- 重复时间戳去重（按 symbol + open_time 联合键）
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades",
    "taker_buy_base_volume", "taker_buy_quote_volume", "ignore", "symbol",
]


@dataclass
class OutlierReport:
    """异常值检测报告。"""
    total_rows: int = 0
    ohlc_inconsistent_count: int = 0
    ohlc_corrected_count: int = 0
    price_jump_count: int = 0
    zero_volume_count: int = 0
    extreme_volume_count: int = 0
    missing_timestamp_count: int = 0
    duplicate_timestamp_count: int = 0
    duplicates_removed: int = 0
    price_jump_indices: List[int] = field(default_factory=list)
    zero_volume_indices: List[int] = field(default_factory=list)
    extreme_volume_indices: List[int] = field(default_factory=list)
    missing_timestamps: List[int] = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            f"=== 异常值检测报告 ===",
            f"总行数: {self.total_rows}",
            f"OHLC不一致: {self.ohlc_inconsistent_count} (修正: {self.ohlc_corrected_count})",
            f"价格跳变(>阈值): {self.price_jump_count}",
            f"零成交量: {self.zero_volume_count}",
            f"极端放量(>5σ): {self.extreme_volume_count}",
            f"缺失时间点: {self.missing_timestamp_count}",
            f"重复时间戳: {self.duplicate_timestamp_count} (已去重: {self.duplicates_removed})",
        ]
        return "\n".join(lines)


def _fix_ohlc_consistency(df: pd.DataFrame, report: OutlierReport) -> pd.DataFrame:
    """修正OHLC一致性：确保 high>=low, high>=open/close, low<=open/close。

    同时从原始四价计算 corrected_high 和 corrected_low，避免级联覆盖风险。
    """
    df = df.copy()

    mask_high_lt_low = df["high"] < df["low"]
    mask_high_lt_open = df["high"] < df["open"]
    mask_high_lt_close = df["high"] < df["close"]
    mask_low_gt_open = df["low"] > df["open"]
    mask_low_gt_close = df["low"] > df["close"]

    inconsistent_mask = (
        mask_high_lt_low | mask_high_lt_open | mask_high_lt_close |
        mask_low_gt_open | mask_low_gt_close
    )
    report.ohlc_inconsistent_count = int(inconsistent_mask.sum())

    if report.ohlc_inconsistent_count == 0:
        return df

    # Compute corrected values from original four prices simultaneously
    # to avoid cascade overwrite issues
    ohlc_cols = ["open", "high", "low", "close"]
    corrected_high = df.loc[inconsistent_mask, ohlc_cols].max(axis=1)
    corrected_low = df.loc[inconsistent_mask, ohlc_cols].min(axis=1)

    df.loc[inconsistent_mask, "high"] = corrected_high
    df.loc[inconsistent_mask, "low"] = corrected_low

    report.ohlc_corrected_count = int(inconsistent_mask.sum())
    logger.info(f"OHLC修正: {report.ohlc_corrected_count} 行")
    return df


def _detect_price_jumps(
    df: pd.DataFrame, report: OutlierReport, threshold: float = 0.10
) -> pd.DataFrame:
    """检测相邻时间点价格变化超阈值的行（标记但不删除）。

    按 symbol 分组计算 pct_change，避免跨 symbol 边界产生虚假跳变。
    """
    df = df.copy()

    if len(df) < 2:
        df["_price_jump"] = 0
        return df

    # Sort within each symbol group by open_time
    df = df.sort_values(["symbol", "open_time"]).reset_index(drop=True)

    # Per-symbol pct_change to avoid cross-symbol false jumps
    price_change = df.groupby("symbol", sort=False)["close"].pct_change().abs()
    jump_mask = price_change > threshold
    jump_mask = jump_mask.fillna(False)

    report.price_jump_count = int(jump_mask.sum())
    report.price_jump_indices = df.index[jump_mask].tolist()

    if report.price_jump_count > 0:
        logger.warning(f"检测到 {report.price_jump_count} 个价格跳变点 (阈值={threshold:.0%})")

    df["_price_jump"] = jump_mask.astype(int)
    return df


def _detect_volume_anomalies(df: pd.DataFrame, report: OutlierReport) -> pd.DataFrame:
    """检测成交量异常：零成交量和极端放量(>5σ)。"""
    df = df.copy()

    zero_vol_mask = df["volume"] == 0
    report.zero_volume_count = int(zero_vol_mask.sum())
    report.zero_volume_indices = df.index[zero_vol_mask].tolist()

    non_zero_volume = df.loc[~zero_vol_mask, "volume"]
    if len(non_zero_volume) > 1:
        mean_vol = non_zero_volume.mean()
        std_vol = non_zero_volume.std()
        if std_vol > 0:
            extreme_mask = (df["volume"] - mean_vol).abs() > (5 * std_vol)
            extreme_mask = extreme_mask & ~zero_vol_mask
            report.extreme_volume_count = int(extreme_mask.sum())
            report.extreme_volume_indices = df.index[extreme_mask].tolist()
        else:
            report.extreme_volume_count = 0
    else:
        report.extreme_volume_count = 0

    df["_zero_volume"] = zero_vol_mask.astype(int)
    df["_extreme_volume"] = 0
    if report.extreme_volume_count > 0:
        df.loc[report.extreme_volume_indices, "_extreme_volume"] = 1

    if report.zero_volume_count > 0:
        logger.warning(f"检测到 {report.zero_volume_count} 个零成交量行")
    if report.extreme_volume_count > 0:
        logger.warning(f"检测到 {report.extreme_volume_count} 个极端放量行")

    return df


def _check_timestamp_continuity(
    df: pd.DataFrame, report: OutlierReport, interval_ms: int = 3600000
) -> pd.DataFrame:
    """检测缺失的时间点（1h = 3600000ms）。

    按 symbol 分组检测时间间隔，避免跨 symbol 边界的虚假 gap。
    """
    df = df.copy()

    if len(df) < 2:
        df["_gap_after"] = 0
        return df

    df = df.sort_values(["symbol", "open_time"]).reset_index(drop=True)

    # Per-symbol time diff
    time_diffs = df.groupby("symbol", sort=False)["open_time"].diff()
    expected_diff = interval_ms

    gap_mask = time_diffs > expected_diff
    gap_mask = gap_mask.fillna(False)

    missing_count = 0
    missing_timestamps = []

    for idx in df.index[gap_mask]:
        curr_symbol = df.loc[idx, "symbol"]
        curr_time = df.loc[idx, "open_time"]
        # Find previous row with same symbol
        prev_rows = df.loc[:idx - 1]
        prev_same = prev_rows[prev_rows["symbol"] == curr_symbol]
        if len(prev_same) > 0:
            prev_time = prev_same.iloc[-1]["open_time"]
            gap_size = curr_time - prev_time
            n_missing = int(gap_size // expected_diff) - 1
            if n_missing > 0:
                missing_count += n_missing
                missing_timestamps.append(int(prev_time + expected_diff))

    report.missing_timestamp_count = missing_count
    report.missing_timestamps = missing_timestamps

    if missing_count > 0:
        logger.warning(f"检测到 {missing_count} 个缺失时间点")

    df["_gap_after"] = gap_mask.astype(int)
    return df


def _remove_duplicate_timestamps(df: pd.DataFrame, report: OutlierReport) -> pd.DataFrame:
    """去除重复时间戳，保留第一条。

    使用 (symbol, open_time) 作为联合去重键，避免不同 symbol 相同时间戳被误删。
    """
    before_count = len(df)

    # Use symbol + open_time as composite key for deduplication
    if "symbol" in df.columns:
        dedup_subset = ["symbol", "open_time"]
    else:
        dedup_subset = ["open_time"]

    df = df.drop_duplicates(subset=dedup_subset, keep="first").reset_index(drop=True)
    after_count = len(df)

    report.duplicate_timestamp_count = before_count - after_count
    report.duplicates_removed = report.duplicate_timestamp_count

    if report.duplicate_timestamp_count > 0:
        logger.info(f"去重: 移除 {report.duplicate_timestamp_count} 条重复时间戳记录")

    return df


def clean_kline_outliers(
    df: pd.DataFrame,
    *,
    price_jump_threshold: float = 0.10,
    interval_ms: int = 3600000,
) -> Tuple[pd.DataFrame, OutlierReport]:
    """K线异常值清洗主函数。

    Args:
        df: 原始K线DataFrame，需包含KLINE_COLUMNS中的列
        price_jump_threshold: 价格跳变阈值，默认10%
        interval_ms: K线间隔毫秒数，默认3600000(1h)

    Returns:
        (清洗后DataFrame, OutlierReport)
        返回的 DataFrame 会追加以下标记列:
        - _price_jump: 价格跳变标记 (0/1)
        - _zero_volume: 零成交量标记 (0/1)
        - _extreme_volume: 极端放量标记 (0/1)
        - _gap_after: 时间戳间断标记 (0/1)
    """
    report = OutlierReport(total_rows=len(df))

    if df.empty:
        logger.warning("输入DataFrame为空")
        return df, report

    required = {"open_time", "open", "high", "low", "close", "volume"}
    missing_cols = required - set(df.columns)
    if missing_cols:
        raise ValueError(f"缺少必要列: {missing_cols}")

    # Step 1: Remove duplicate timestamps
    df = _remove_duplicate_timestamps(df, report)

    # Step 2: Fix OHLC consistency
    df = _fix_ohlc_consistency(df, report)

    # Step 3: Detect price jumps
    df = _detect_price_jumps(df, report, threshold=price_jump_threshold)

    # Step 4: Detect volume anomalies
    df = _detect_volume_anomalies(df, report)

    # Step 5: Check timestamp continuity
    df = _check_timestamp_continuity(df, report, interval_ms=interval_ms)

    # Update total rows after cleaning
    report.total_rows = len(df)

    logger.info(f"清洗完成: {report.summary()}")
    return df, report
