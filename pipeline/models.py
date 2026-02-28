from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any


@dataclass(frozen=True)
class InstrumentRecord:
    symbol: str
    name: str
    category: str
    asset_type: str
    market: str
    currency: str
    is_active: bool = True


@dataclass(frozen=True)
class DailyPriceRecord:
    symbol: str
    trade_date: date
    close: float
    source: str
    price_date_actual: date
    open: float | None = None
    high: float | None = None
    low: float | None = None
    volume: float | None = None


@dataclass(frozen=True)
class DailyFlowRecord:
    target_type: str
    target_code: str
    trade_date: date
    investor_type: str
    buy_value_krw: float
    sell_value_krw: float
    source: str


@dataclass(frozen=True)
class DailyMacroRecord:
    metric_code: str
    trade_date: date
    value: float
    unit: str
    source: str


@dataclass(frozen=True)
class ReportSectionRecord:
    report_date: date
    section_key: str
    title_ko: str
    title_en: str
    analysis_ko: str
    analysis_en: str
    chart_key: str
    as_of_date: date
    input_snapshot_json: dict[str, Any]


@dataclass(frozen=True)
class ReportRecord:
    report_date: date
    cutoff_kst: str
    status: str
    notes: str | None
