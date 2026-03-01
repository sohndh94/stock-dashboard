from __future__ import annotations

from datetime import date

from pipeline.config import (
    INSTRUMENT_DEFINITIONS,
    KRX_PRICE_SYMBOLS,
    MACRO_METRICS,
    YF_PRICE_SYMBOLS,
)
from pipeline.models import DailyPriceRecord, InstrumentRecord
from pipeline.providers.pykrx_provider import PykrxProvider
from pipeline.providers.yfinance_provider import YFinanceProvider
from pipeline.repository import SupabaseRepository


def collect_ingest_window(
    repository: SupabaseRepository,
    start_date: date,
    end_date: date,
) -> dict:
    instruments = [InstrumentRecord(**row) for row in INSTRUMENT_DEFINITIONS]
    repository.upsert_instruments(instruments)

    pykrx = PykrxProvider()
    yfinance = YFinanceProvider()

    price_records = []
    krx_prices = pykrx.fetch_daily_prices(KRX_PRICE_SYMBOLS, start_date, end_date)
    price_records.extend(krx_prices)
    price_records.extend(yfinance.fetch_daily_prices(YF_PRICE_SYMBOLS, start_date, end_date))

    # Fallback for KOSPI when KRX index endpoint is temporarily unstable.
    has_kospi = any(record.symbol == "KOSPI" for record in krx_prices)
    if not has_kospi:
        kospi_proxy = yfinance.fetch_daily_prices(["^KS11"], start_date, end_date)
        for record in kospi_proxy:
            price_records.append(
                DailyPriceRecord(
                    symbol="KOSPI",
                    trade_date=record.trade_date,
                    open=record.open,
                    high=record.high,
                    low=record.low,
                    close=record.close,
                    volume=record.volume,
                    source="yfinance",
                    price_date_actual=record.price_date_actual,
                )
            )

    flow_records = pykrx.fetch_daily_flows(
        start_date,
        end_date,
        symbols=["207940.KS", "068270.KS"],
    )

    macro_records = yfinance.fetch_daily_macro(MACRO_METRICS, start_date, end_date)

    repository.upsert_daily_prices(price_records)
    repository.upsert_daily_flows(flow_records)
    repository.upsert_daily_macro(macro_records)

    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "price_rows": len(price_records),
        "flow_rows": len(flow_records),
        "macro_rows": len(macro_records),
    }
