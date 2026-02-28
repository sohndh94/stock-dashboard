from __future__ import annotations

from datetime import date

from pipeline.config import (
    INSTRUMENT_DEFINITIONS,
    KRX_PRICE_SYMBOLS,
    MACRO_METRICS,
    YF_PRICE_SYMBOLS,
)
from pipeline.models import InstrumentRecord
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
    price_records.extend(pykrx.fetch_daily_prices(KRX_PRICE_SYMBOLS, start_date, end_date))
    price_records.extend(yfinance.fetch_daily_prices(YF_PRICE_SYMBOLS, start_date, end_date))

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
