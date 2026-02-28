from __future__ import annotations

from datetime import date
from typing import Iterable

import numpy as np
import pandas as pd


def asof_align_to_spine(spine_dates: pd.Series, frame: pd.DataFrame) -> pd.DataFrame:
    """Aligns each symbol's daily series to KRX spine using backward as-of semantics."""
    left = pd.DataFrame({"trade_date": pd.to_datetime(spine_dates)}).sort_values("trade_date")

    if frame.empty:
        left["close"] = np.nan
        left["volume"] = np.nan
        left["price_date_latest"] = pd.NaT
        return left

    source = frame[["trade_date", "close", "volume"]].copy().sort_values("trade_date")
    source["price_date_latest"] = source["trade_date"]

    merged = pd.merge_asof(left, source, on="trade_date", direction="backward")
    return merged


def normalize_base100(frame: pd.DataFrame, start_date: date) -> pd.DataFrame:
    normalized = pd.DataFrame(index=frame.index)
    start_ts = pd.Timestamp(start_date)

    for column in frame.columns:
        series = pd.to_numeric(frame[column], errors="coerce")
        valid = series.loc[series.index >= start_ts].dropna()
        if valid.empty:
            normalized[column] = np.nan
            continue

        base_value = valid.iloc[0]
        if base_value == 0:
            normalized[column] = np.nan
            continue

        normalized[column] = (series / base_value) * 100.0

    return normalized


def latest_pct_change(series: pd.Series) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if len(values) < 2:
        return 0.0

    previous = values.iloc[-2]
    current = values.iloc[-1]
    if previous == 0:
        return 0.0
    return float((current / previous) - 1.0)


def latest_diff(series: pd.Series) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if len(values) < 2:
        return 0.0
    return float(values.iloc[-1] - values.iloc[-2])


def rolling_volatility(series: pd.Series, window: int = 20) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if len(values) < window + 1:
        return 0.0

    returns = values.pct_change().dropna()
    if len(returns) < window:
        return 0.0

    return float(returns.tail(window).std() * np.sqrt(252))


def classify_state_from_score(score: float) -> tuple[str, str]:
    if score >= 1.0:
        return ("강세", "bullish")
    if score <= -1.0:
        return ("약세", "bearish")
    return ("중립", "neutral")


def sum_last_value(values: Iterable[float | None]) -> float:
    total = 0.0
    for value in values:
        if value is None:
            continue
        total += float(value)
    return total
