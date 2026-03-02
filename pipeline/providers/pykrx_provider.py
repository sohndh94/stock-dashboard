from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any

import pandas as pd
from pykrx import stock

from pipeline.models import DailyFlowRecord, DailyPriceRecord
from pipeline.providers.interfaces import FlowProvider, PriceProvider

logger = logging.getLogger(__name__)


class PykrxProvider(PriceProvider, FlowProvider):
    """KRX data provider using pykrx."""

    STOCK_TICKER_MAP = {
        "207940.KS": "207940",
        "068270.KS": "068270",
    }

    INDEX_CODE_MAP = {
        "KOSPI": "1001",
        "KOSPI200_HEALTHCARE": "1160",
    }
    # Fallback when KRX index endpoint is unstable.
    # TIGER 200 Health Care ETF is used as a free proxy for index direction.
    INDEX_PROXY_ETF_MAP = {
        "KOSPI200_HEALTHCARE": "227540",
    }

    STOCK_FLOW_TARGETS = {
        "207940": "207940.KS",
        "068270": "068270.KS",
    }

    INVESTOR_MAP = {
        "외국인합계": "foreign",
        "기관합계": "institution",
        "개인": "retail",
        "기타법인": "other",
        "기타": "other",
    }

    PRICE_COLUMN_MAP = {
        "시가": "open",
        "고가": "high",
        "저가": "low",
        "종가": "close",
        "거래량": "volume",
    }

    def _fmt(self, value: date) -> str:
        return value.strftime("%Y%m%d")

    def _retry_krx_call(self, call_name: str, func, *args, **kwargs) -> pd.DataFrame:
        """Retries flaky KRX endpoints and returns empty DataFrame if all retries fail."""
        delays = [2, 5, 10, 20]
        last_error: Exception | None = None
        for attempt in range(1, len(delays) + 2):
            try:
                frame = func(*args, **kwargs)
                if isinstance(frame, pd.DataFrame):
                    return frame
                return pd.DataFrame()
            except Exception as exc:  # noqa: BLE001 - external API failures are non-deterministic
                last_error = exc
                error_msg = self._safe_error_message(exc)
                if attempt <= len(delays):
                    delay = delays[attempt - 1]
                    logger.warning(
                        "KRX call failed (%s, attempt %s): %s. retrying in %ss",
                        call_name,
                        attempt,
                        error_msg,
                        delay,
                    )
                    time.sleep(delay)
                else:
                    break

        logger.error(
            "KRX call failed (%s) after retries: %s",
            call_name,
            self._safe_error_message(last_error),
        )
        return pd.DataFrame()

    def _safe_error_message(self, exc: Exception | None) -> str:
        if exc is None:
            return "unknown error"
        try:
            return str(exc)
        except Exception:  # noqa: BLE001
            return repr(exc)

    def _normalize_price_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame

        renamed = frame.rename(columns=self.PRICE_COLUMN_MAP)
        for column in ["open", "high", "low", "close", "volume"]:
            if column not in renamed.columns:
                renamed[column] = None

        return renamed[["open", "high", "low", "close", "volume"]].copy()

    def fetch_daily_prices(
        self, symbols: list[str] | list[dict[str, str]], start_date: date, end_date: date
    ) -> list[DailyPriceRecord]:
        results: list[DailyPriceRecord] = []
        start_raw = self._fmt(start_date)
        end_raw = self._fmt(end_date)

        for target in symbols:
            if isinstance(target, dict):
                symbol = target["symbol"]
                provider_symbol = target.get("provider_symbol", symbol)
                asset_type = target.get("asset_type")
            else:
                symbol = target
                provider_symbol = ""
                asset_type = None

            frame = pd.DataFrame()
            source_name = "pykrx"
            if asset_type == "stock" or symbol in self.STOCK_TICKER_MAP:
                ticker = provider_symbol or self.STOCK_TICKER_MAP.get(symbol, symbol.replace(".KS", ""))
                frame = self._retry_krx_call(
                    f"get_market_ohlcv_by_date:{symbol}",
                    stock.get_market_ohlcv_by_date,
                    start_raw,
                    end_raw,
                    ticker,
                )
            elif asset_type == "index" or symbol in self.INDEX_CODE_MAP:
                index_code = provider_symbol or self.INDEX_CODE_MAP.get(symbol, "")
                if not index_code:
                    continue
                frame = self._retry_krx_call(
                    f"get_index_ohlcv_by_date:{symbol}",
                    stock.get_index_ohlcv_by_date,
                    start_raw,
                    end_raw,
                    index_code,
                )
                if frame.empty and symbol in self.INDEX_PROXY_ETF_MAP:
                    proxy_ticker = self.INDEX_PROXY_ETF_MAP[symbol]
                    logger.warning(
                        "Using ETF proxy %s for %s due to missing index OHLCV.",
                        proxy_ticker,
                        symbol,
                    )
                    source_name = "pykrx_proxy_etf"
                    frame = self._retry_krx_call(
                        f"get_market_ohlcv_by_date:proxy:{symbol}",
                        stock.get_market_ohlcv_by_date,
                        start_raw,
                        end_raw,
                        proxy_ticker,
                    )

            if frame.empty:
                continue

            normalized = self._normalize_price_frame(frame)
            for trade_date, row in normalized.iterrows():
                trade_day = trade_date.date()
                close = row.get("close")
                if close is None or pd.isna(close):
                    continue

                results.append(
                    DailyPriceRecord(
                        symbol=symbol,
                        trade_date=trade_day,
                        open=_to_float(row.get("open")),
                        high=_to_float(row.get("high")),
                        low=_to_float(row.get("low")),
                        close=float(close),
                        volume=_to_float(row.get("volume")),
                        source=source_name,
                        price_date_actual=trade_day,
                    )
                )

        return results

    def fetch_daily_flows(
        self, start_date: date, end_date: date, symbols: list[str] | None = None
    ) -> list[DailyFlowRecord]:
        results: list[DailyFlowRecord] = []
        start_raw = self._fmt(start_date)
        end_raw = self._fmt(end_date)

        spine = self._retry_krx_call(
            "get_index_ohlcv_by_date:KOSPI:flow_spine",
            stock.get_index_ohlcv_by_date,
            start_raw,
            end_raw,
            self.INDEX_CODE_MAP["KOSPI"],
        )
        if spine.empty:
            return results

        for trade_date in spine.index:
            day = trade_date.date()
            day_raw = self._fmt(day)

            market_flows = self._safe_trading_value_by_investor(day_raw, day_raw, "KOSPI")
            if market_flows is not None:
                results.extend(
                    self._frame_to_flows(
                        frame=market_flows,
                        target_type="market",
                        target_code="KOSPI",
                        trade_date=day,
                    )
                )

            for ticker, symbol in self.STOCK_FLOW_TARGETS.items():
                if symbols and symbol not in symbols:
                    continue
                stock_flows = self._safe_trading_value_by_investor(day_raw, day_raw, ticker)
                if stock_flows is None:
                    continue
                results.extend(
                    self._frame_to_flows(
                        frame=stock_flows,
                        target_type="stock",
                        target_code=symbol,
                        trade_date=day,
                    )
                )

        return results

    def _safe_trading_value_by_investor(
        self, start_raw: str, end_raw: str, target: str
    ) -> pd.DataFrame | None:
        frame = self._retry_krx_call(
            f"get_market_trading_value_by_investor:{target}",
            stock.get_market_trading_value_by_investor,
            start_raw,
            end_raw,
            target,
        )
        if frame.empty:
            return None
        return frame

    def _frame_to_flows(
        self,
        frame: pd.DataFrame,
        target_type: str,
        target_code: str,
        trade_date: date,
    ) -> list[DailyFlowRecord]:
        if frame.empty:
            return []

        records: list[DailyFlowRecord] = []
        for investor_name, row in frame.iterrows():
            investor_type = self.INVESTOR_MAP.get(str(investor_name))
            if investor_type is None:
                continue

            buy_value = _to_float(row.get("매수"))
            sell_value = _to_float(row.get("매도"))
            if buy_value is None:
                buy_value = 0.0
            if sell_value is None:
                sell_value = 0.0

            records.append(
                DailyFlowRecord(
                    target_type=target_type,
                    target_code=target_code,
                    trade_date=trade_date,
                    investor_type=investor_type,
                    buy_value_krw=float(buy_value),
                    sell_value_krw=float(sell_value),
                    source="pykrx",
                )
            )

        return records


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if pd.isna(value):
        return None
    return float(value)
