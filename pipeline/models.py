from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any


@dataclass(frozen=True)
class InstrumentRecord:
    symbol: str
    name: str
    asset_type: str
    market: str
    currency: str
    provider: str
    provider_symbol: str
    category: str | None = None
    name_ko: str | None = None
    display_order: int = 1000
    is_compare_default: bool = False
    is_active: bool = True


@dataclass(frozen=True)
class InstrumentGroupRecord:
    group_key: str
    name: str
    purpose: str | None
    is_active: bool = True


@dataclass(frozen=True)
class InstrumentGroupMemberRecord:
    group_key: str
    symbol: str
    weight: float = 1.0
    role: str = "member"


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
class DailyCompanyMetricRecord:
    symbol: str
    trade_date: date
    market_cap: float | None
    shares_outstanding: float | None
    source: str


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
class DailyIssueEventRecord:
    issue_id: str
    trade_date: date
    section_key: str
    symbol: str
    source_name: str
    source_tier: int
    title: str
    summary: str | None
    url: str
    published_at_kst: datetime
    language: str
    topic_tags: list[str]
    sentiment: float | None
    relevance_score: float
    is_same_day: bool


@dataclass(frozen=True)
class SectionEvidenceRecord:
    report_date: date
    section_key: str
    rank: int
    issue_id: str | None
    evidence_type: str
    weight: float
    reason: str


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
    analysis_steps_ko: list[str] | None = None
    analysis_steps_en: list[str] | None = None
    evidence_count: int = 0
    confidence_score: float = 0.0


@dataclass(frozen=True)
class ReportRecord:
    report_date: date
    cutoff_kst: str
    status: str
    notes: str | None
