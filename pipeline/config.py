from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class AppConfig:
    supabase_url: str
    supabase_service_role_key: str
    report_start_date: date
    report_cutoff_kst: str


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

    return AppConfig(
        supabase_url=supabase_url,
        supabase_service_role_key=service_role,
        report_start_date=date(year, month, day),
        report_cutoff_kst=os.getenv("REPORT_CUTOFF_KST", "16:00"),
    )


INSTRUMENT_DEFINITIONS = [
    {
        "symbol": "207940.KS",
        "name": "Samsung Biologics",
        "category": "CDMO",
        "asset_type": "stock",
        "market": "KRX",
        "currency": "KRW",
    },
    {
        "symbol": "068270.KS",
        "name": "Celltrion",
        "category": "CDMO",
        "asset_type": "stock",
        "market": "KRX",
        "currency": "KRW",
    },
    {
        "symbol": "2269.HK",
        "name": "Wuxi Biologics",
        "category": "CDMO",
        "asset_type": "stock",
        "market": "HKEX",
        "currency": "HKD",
    },
    {
        "symbol": "LONN.SW",
        "name": "Lonza",
        "category": "CDMO",
        "asset_type": "stock",
        "market": "SIX",
        "currency": "CHF",
    },
    {
        "symbol": "KOSPI",
        "name": "KOSPI",
        "category": "Market",
        "asset_type": "index",
        "market": "KRX",
        "currency": "KRW",
    },
    {
        "symbol": "KOSPI200_HEALTHCARE",
        "name": "KOSPI200 Health Care",
        "category": "Sector",
        "asset_type": "index",
        "market": "KRX",
        "currency": "KRW",
    },
]

KRX_PRICE_SYMBOLS = [
    "207940.KS",
    "068270.KS",
    "KOSPI",
    "KOSPI200_HEALTHCARE",
]

YF_PRICE_SYMBOLS = ["2269.HK", "LONN.SW"]

MACRO_METRICS = [
    {"metric_code": "USDKRW", "ticker": "KRW=X", "unit": "KRW"},
    {"metric_code": "US10Y", "ticker": "^TNX", "unit": "%"},
]

SECTION_TITLES = {
    "kospi": {"ko": "코스피 시장", "en": "KOSPI Market"},
    "bio": {"ko": "바이오 산업", "en": "Bio Industry"},
    "samsung_bio": {"ko": "삼성바이오로직스", "en": "Samsung Biologics"},
}

REQUIRED_REPORT_SYMBOLS = [
    "KOSPI",
    "KOSPI200_HEALTHCARE",
    "207940.KS",
    "068270.KS",
    "2269.HK",
    "LONN.SW",
]
