from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pandas as pd
import yfinance as yf

from pipeline.models import DailyMacroRecord, DailyPriceRecord
from pipeline.providers.interfaces import MacroProvider, PriceProvider


class YFinanceProvider(PriceProvider, MacroProvider):
    """Overseas price and macro proxy provider via yfinance."""

    def fetch_daily_prices(
        self, symbols: list[str], start_date: date, end_date: date
    ) -> list[DailyPriceRecord]:
        records: list[DailyPriceRecord] = []
        end_exclusive = end_date + timedelta(days=1)

        for symbol in symbols:
            frame = yf.download(
                symbol,
                start=start_date.isoformat(),
                end=end_exclusive.isoformat(),
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
            )
            normalized = _normalize_ohlcv_frame(frame)
            if normalized.empty:
                continue

            for trade_date, row in normalized.iterrows():
                day = trade_date.date()
                close = _to_float(row.get("Close"))
                if close is None:
                    continue

                records.append(
                    DailyPriceRecord(
                        symbol=symbol,
                        trade_date=day,
                        open=_to_float(row.get("Open")),
                        high=_to_float(row.get("High")),
                        low=_to_float(row.get("Low")),
                        close=close,
                        volume=_to_float(row.get("Volume")),
                        source="yfinance",
                        price_date_actual=day,
                    )
                )

        return records

    def fetch_daily_macro(
        self, metrics: list[dict[str, str]], start_date: date, end_date: date
    ) -> list[DailyMacroRecord]:
        records: list[DailyMacroRecord] = []
        end_exclusive = end_date + timedelta(days=1)

        for metric in metrics:
            ticker = metric["ticker"]
            metric_code = metric["metric_code"]
            unit = metric["unit"]

            frame = yf.download(
                ticker,
                start=start_date.isoformat(),
                end=end_exclusive.isoformat(),
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
            )
            normalized = _normalize_ohlcv_frame(frame)
            if normalized.empty:
                continue

            for trade_date, row in normalized.iterrows():
                close = _to_float(row.get("Close"))
                if close is None:
                    continue

                records.append(
                    DailyMacroRecord(
                        metric_code=metric_code,
                        trade_date=trade_date.date(),
                        value=close,
                        unit=unit,
                        source="yfinance",
                    )
                )

        return records


def _normalize_ohlcv_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    normalized = frame.copy()
    if isinstance(normalized.columns, pd.MultiIndex):
        normalized.columns = normalized.columns.get_level_values(0)

    if not isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index)

    normalized = normalized.sort_index()
    return normalized


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if pd.isna(value):
        return None
    return float(value)
