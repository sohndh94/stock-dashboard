from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from urllib.parse import urlparse

from dotenv import load_dotenv

from pipeline.universe_registry import symbols_by_group, symbols_by_provider

load_dotenv()


@dataclass(frozen=True)
class AppConfig:
    supabase_url: str
    supabase_service_role_key: str
    report_start_date: date
    report_cutoff_kst: str
    report_generate_kst: str
    strict_issue_cutoff_kst: str
    dart_api_key: str | None
    ecos_api_key: str | None
    fred_api_key: str | None
    naver_client_id: str | None
    naver_client_secret: str | None
    alphavantage_api_key: str | None
    llm_enabled: bool
    openai_api_key: str | None
    openai_model: str


def load_config() -> AppConfig:
    supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    if not supabase_url:
        raise RuntimeError("SUPABASE_URL (or NEXT_PUBLIC_SUPABASE_URL) is required")
    supabase_url = supabase_url.strip()
    parsed = urlparse(supabase_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise RuntimeError(
            "SUPABASE_URL is invalid. Expected format: https://<project-ref>.supabase.co"
        )

    service_role = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not service_role:
        raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY is required")
    service_role = service_role.strip()

    start_date_raw = os.getenv("REPORT_START_DATE", "2025-07-01")
    year, month, day = [int(part) for part in start_date_raw.split("-")]
    llm_enabled = os.getenv("LLM_ENABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    return AppConfig(
        supabase_url=supabase_url,
        supabase_service_role_key=service_role,
        report_start_date=date(year, month, day),
        report_cutoff_kst=os.getenv("REPORT_CUTOFF_KST", "16:00"),
        report_generate_kst=os.getenv("REPORT_GENERATE_KST", "16:03"),
        strict_issue_cutoff_kst=os.getenv("STRICT_ISSUE_CUTOFF_KST", "16:03"),
        dart_api_key=(os.getenv("DART_API_KEY") or "").strip() or None,
        ecos_api_key=(os.getenv("ECOS_API_KEY") or "").strip() or None,
        fred_api_key=(os.getenv("FRED_API_KEY") or "").strip() or None,
        naver_client_id=(os.getenv("NAVER_CLIENT_ID") or "").strip() or None,
        naver_client_secret=(os.getenv("NAVER_CLIENT_SECRET") or "").strip() or None,
        alphavantage_api_key=(os.getenv("ALPHAVANTAGE_API_KEY") or "").strip() or None,
        llm_enabled=llm_enabled,
        openai_api_key=(os.getenv("OPENAI_API_KEY") or "").strip() or None,
        openai_model=os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip(),
    )


def _symbols_for_provider(provider: str) -> list[str]:
    return [item["symbol"] for item in symbols_by_provider(provider)]


def get_required_report_symbols() -> list[str]:
    return symbols_by_group("report_required")


def get_report_bio_global_symbols() -> list[str]:
    return symbols_by_group("report_bio_global")


def get_issue_watch_symbols() -> list[str]:
    return symbols_by_group("issue_watchlist")


def get_compare_default_symbols() -> list[str]:
    symbols = []
    for item in symbols_by_group("compare"):
        symbols.append(item)
    return symbols


KRX_PRICE_SYMBOLS = _symbols_for_provider("pykrx")
YF_PRICE_SYMBOLS = _symbols_for_provider("yfinance")
REQUIRED_REPORT_SYMBOLS = get_required_report_symbols()

MACRO_METRICS = [
    {
        "metric_code": "USDKRW",
        "ticker": "KRW=X",
        "fred_series_id": "DEXKOUS",
        "unit": "KRW",
        "ecos_stat_code": os.getenv("ECOS_USDKRW_STAT_CODE"),
        "ecos_item_code1": os.getenv("ECOS_USDKRW_ITEM_CODE1"),
    },
    {
        "metric_code": "US10Y",
        "ticker": "^TNX",
        "fred_series_id": "DGS10",
        "unit": "%",
        "ecos_stat_code": os.getenv("ECOS_US10Y_STAT_CODE"),
        "ecos_item_code1": os.getenv("ECOS_US10Y_ITEM_CODE1"),
    },
]

SECTION_NEWS_QUERIES = {
    "kospi": ["코스피 장마감", "코스피 수급", "원달러 환율 마감"],
    "bio": ["글로벌 제약", "바이오 산업", "CDMO 시장"],
    "samsung_bio": ["삼성바이오로직스", "Samsung Biologics", "207940"],
}

ALPHAVANTAGE_SECTION_TOPICS = {
    "kospi": "financial_markets",
    "bio": "biotechnology",
    "samsung_bio": "biotechnology",
}

SECTION_TITLES = {
    "kospi": {"ko": "코스피 시장", "en": "KOSPI Market"},
    "bio": {"ko": "바이오 산업", "en": "Bio Industry"},
    "samsung_bio": {"ko": "삼성바이오로직스", "en": "Samsung Biologics"},
}
