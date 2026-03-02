from datetime import date

from pipeline.reporting.report_v2 import (
    _build_section_analysis,
    _build_section_steps,
    _evidence_type,
    _parse_hhmm,
)


def test_parse_hhmm_uses_trade_date_and_time():
    dt = _parse_hhmm(date(2026, 3, 2), "16:03")
    assert dt.year == 2026
    assert dt.month == 3
    assert dt.day == 2
    assert dt.hour == 16
    assert dt.minute == 3


def test_evidence_type_mapping():
    assert _evidence_type("open_dart") == "disclosure"
    assert _evidence_type("pykrx_flow") == "flow"
    assert _evidence_type("macro_official") == "macro"
    assert _evidence_type("naver_news") == "news"


def test_build_section_analysis_uses_flow_direction_consistently():
    metrics = {
        "kospi_return": 0.01,
        "kospi_foreign_flow": -100_000_000_000,
        "usdkrw_change": 0.001,
        "us10y_bp_change": 2.5,
        "kospi_score": -1.3,
        "healthcare_excess_return": 0.0,
        "bio_proxy_foreign_flow": 0.0,
        "bio_score": 0.0,
        "samsung_return": 0.0,
        "samsung_vs_peer": 0.0,
        "samsung_foreign_flow": 0.0,
        "samsung_institution_flow": 0.0,
        "samsung_score": 0.0,
    }
    ko, en = _build_section_analysis("kospi", metrics, "이슈", "Issue")
    assert "순매도" in ko
    assert "net selling" in en


def test_build_steps_has_three_entries():
    metrics = {
        "kospi_return": 0.01,
        "kospi_vol20": 0.12,
        "kospi_foreign_flow": 0.0,
        "healthcare_excess_return": 0.0,
        "bio_proxy_foreign_flow": 0.0,
        "samsung_return": 0.0,
        "samsung_vs_peer": 0.0,
        "samsung_foreign_flow": 0.0,
        "samsung_institution_flow": 0.0,
    }
    ko_steps, en_steps = _build_section_steps("kospi", metrics, "이슈", "Issue")
    assert len(ko_steps) == 3
    assert len(en_steps) == 3
