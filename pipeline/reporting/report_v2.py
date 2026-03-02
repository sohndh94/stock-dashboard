from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from pipeline.config import (
    SECTION_TITLES,
    get_report_bio_global_symbols,
    get_required_report_symbols,
    load_config,
)
from pipeline.models import (
    DailyIssueEventRecord,
    ReportRecord,
    ReportSectionRecord,
    SectionEvidenceRecord,
)
from pipeline.providers.utils import make_issue_id
from pipeline.reporting.analysis import (
    asof_align_to_spine,
    classify_state_from_score,
    latest_diff,
    latest_pct_change,
    normalize_base100,
    rolling_volatility,
)
from pipeline.reporting.llm_enhancer import enhance_bilingual_analysis
if TYPE_CHECKING:
    from pipeline.repository import SupabaseRepository

KST = ZoneInfo("Asia/Seoul")
DEFAULT_CORE_PEER_SYMBOLS = ["207940.KS", "068270.KS", "2269.HK", "LONN.SW"]
SECTION_KEYS = ["kospi", "bio", "samsung_bio"]
SECTION_SERIES = {
    "kospi": ["KOSPI", "KOSPI200_HEALTHCARE", "207940.KS", "BIO_PEER_AVG"],
    "bio": [
        "KOSPI200_HEALTHCARE",
        "KOSPI",
        "207940.KS",
        "068270.KS",
        "2269.HK",
        "LONN.SW",
    ],
    "samsung_bio": ["207940.KS", "BIO_PEER_AVG", "KOSPI", "KOSPI200_HEALTHCARE"],
}


def _fmt_pct(value: float) -> str:
    return f"{value * 100:+.2f}%"


def _fmt_pct_point(value: float) -> str:
    return f"{value * 100:+.2f}%p"


def _fmt_bp(value: float) -> str:
    return f"{value:+.1f}bp"


def _fmt_eok(value: float) -> str:
    return f"{value / 100_000_000:+,.0f}억원"


def _fmt_flow_direction_ko(value: float) -> str:
    if value > 0:
        return f"순매수 {_fmt_eok(value)}"
    if value < 0:
        return f"순매도 {_fmt_eok(value)}"
    return "중립(0억원)"


def _fmt_flow_direction_en(value: float) -> str:
    if value > 0:
        return f"net buying {_fmt_eok(value)}"
    if value < 0:
        return f"net selling {_fmt_eok(value)}"
    return "flat (KRW 0)"


def _flow_on_date(frame: pd.DataFrame, report_date: date, investor_type: str) -> float:
    if frame.empty:
        return 0.0

    on_date = frame[frame["trade_date"] == pd.Timestamp(report_date)]
    on_date = on_date[on_date["investor_type"] == investor_type]
    if on_date.empty:
        return 0.0
    return float(on_date.iloc[-1]["net_value_krw"])


def _has_flow_on_date(frame: pd.DataFrame, report_date: date, investor_type: str) -> bool:
    if frame.empty:
        return False
    on_date = frame[frame["trade_date"] == pd.Timestamp(report_date)]
    on_date = on_date[on_date["investor_type"] == investor_type]
    return not on_date.empty


def _parse_hhmm(trade_date: date, hhmm: str) -> datetime:
    hh, mm = [int(part) for part in hhmm.split(":")]
    return datetime(trade_date.year, trade_date.month, trade_date.day, hh, mm, tzinfo=KST)


def _evidence_type(source_name: str) -> str:
    source_lower = source_name.lower()
    if "dart" in source_lower or "disclosure" in source_lower:
        return "disclosure"
    if "flow" in source_lower:
        return "flow"
    if "macro" in source_lower or "fred" in source_lower or "ecos" in source_lower:
        return "macro"
    return "news"


def _section_symbol(section_key: str) -> str:
    if section_key == "samsung_bio":
        return "207940.KS"
    if section_key == "bio":
        return "KOSPI200_HEALTHCARE"
    return "KOSPI"


def _macro_source_has_official(frame: pd.DataFrame, report_date: date) -> bool:
    if frame.empty:
        return False

    on_date = frame[frame["trade_date"] == pd.Timestamp(report_date)]
    if on_date.empty or "source" not in on_date.columns:
        return False

    sources = {str(item).lower() for item in on_date["source"].dropna().tolist()}
    return bool(sources.intersection({"ecos", "fred"}))


def _build_section_steps(
    section_key: str,
    metrics: dict[str, float],
    top_issue_title_ko: str,
    top_issue_title_en: str,
) -> tuple[list[str], list[str]]:
    if section_key == "kospi":
        steps_ko = [
            f"Step 1: 코스피 당일 수익률은 {_fmt_pct(metrics['kospi_return'])}이고 20일 변동성은 {metrics['kospi_vol20']:.2%}입니다.",
            f"Step 2: 외국인 수급은 {_fmt_flow_direction_ko(metrics['kospi_foreign_flow'])}로 마감했습니다.",
            f"Step 3: 당일 주요 이슈는 '{top_issue_title_ko}' 입니다.",
        ]
        steps_en = [
            f"Step 1: KOSPI daily return was {_fmt_pct(metrics['kospi_return'])} with 20-day volatility at {metrics['kospi_vol20']:.2%}.",
            f"Step 2: Foreign flow closed at {_fmt_flow_direction_en(metrics['kospi_foreign_flow'])}.",
            f"Step 3: Key same-day issue was '{top_issue_title_en}'.",
        ]
        return steps_ko, steps_en

    if section_key == "bio":
        steps_ko = [
            f"Step 1: 코스피200 헬스케어의 코스피 대비 초과수익은 {_fmt_pct_point(metrics['healthcare_excess_return'])}입니다.",
            f"Step 2: 국내 바이오 대표(삼성바이오+셀트리온) 외국인 수급은 {_fmt_flow_direction_ko(metrics['bio_proxy_foreign_flow'])}입니다.",
            f"Step 3: 당일 바이오 이슈는 '{top_issue_title_ko}' 입니다.",
        ]
        steps_en = [
            f"Step 1: KOSPI200 Health Care excess return vs KOSPI was {_fmt_pct_point(metrics['healthcare_excess_return'])}.",
            f"Step 2: Foreign flow proxy for Samsung Biologics + Celltrion was {_fmt_flow_direction_en(metrics['bio_proxy_foreign_flow'])}.",
            f"Step 3: Key same-day bio issue was '{top_issue_title_en}'.",
        ]
        return steps_ko, steps_en

    steps_ko = [
        f"Step 1: 삼성바이오로직스 당일 수익률은 {_fmt_pct(metrics['samsung_return'])}, 피어 대비 상대성과는 {_fmt_pct_point(metrics['samsung_vs_peer'])}입니다.",
        f"Step 2: 외국인/기관 수급은 각각 {_fmt_flow_direction_ko(metrics['samsung_foreign_flow'])}, {_fmt_flow_direction_ko(metrics['samsung_institution_flow'])}입니다.",
        f"Step 3: 당일 삼성바이오로직스 이슈는 '{top_issue_title_ko}' 입니다.",
    ]
    steps_en = [
        f"Step 1: Samsung Biologics daily return was {_fmt_pct(metrics['samsung_return'])} and relative performance vs peers was {_fmt_pct_point(metrics['samsung_vs_peer'])}.",
        f"Step 2: Foreign/institutional flows were {_fmt_flow_direction_en(metrics['samsung_foreign_flow'])} and {_fmt_flow_direction_en(metrics['samsung_institution_flow'])}.",
        f"Step 3: Key same-day Samsung Biologics issue was '{top_issue_title_en}'.",
    ]
    return steps_ko, steps_en


def _build_section_analysis(
    section_key: str,
    metrics: dict[str, float],
    top_issue_title_ko: str,
    top_issue_title_en: str,
) -> tuple[str, str]:
    if section_key == "kospi":
        kospi_state_ko, kospi_state_en = classify_state_from_score(metrics["kospi_score"])
        analysis_ko = (
            f"코스피는 당일 {_fmt_pct(metrics['kospi_return'])}로 {kospi_state_ko} 흐름이었고, 외국인 수급은 {_fmt_flow_direction_ko(metrics['kospi_foreign_flow'])}였습니다. "
            f"원/달러 {_fmt_pct(metrics['usdkrw_change'])}, 미10년물 {_fmt_bp(metrics['us10y_bp_change'])} 변동과 '{top_issue_title_ko}' 이슈가 동시 반영됐습니다."
        )
        analysis_en = (
            f"KOSPI closed {_fmt_pct(metrics['kospi_return'])} with a {kospi_state_en} tone, while foreign flow ended at {_fmt_flow_direction_en(metrics['kospi_foreign_flow'])}. "
            f"USD/KRW {_fmt_pct(metrics['usdkrw_change'])}, U.S.10Y {_fmt_bp(metrics['us10y_bp_change'])}, and '{top_issue_title_en}' shaped same-day risk tone."
        )
        return analysis_ko, analysis_en

    if section_key == "bio":
        bio_state_ko, bio_state_en = classify_state_from_score(metrics["bio_score"])
        analysis_ko = (
            f"바이오 섹터는 코스피 대비 {_fmt_pct_point(metrics['healthcare_excess_return'])}의 상대성과로 {bio_state_ko} 구간이며, 국내 대표 수급은 {_fmt_flow_direction_ko(metrics['bio_proxy_foreign_flow'])}였습니다. "
            f"당일 핵심 이슈는 '{top_issue_title_ko}' 입니다."
        )
        analysis_en = (
            f"Bio sector showed {_fmt_pct_point(metrics['healthcare_excess_return'])} excess return versus KOSPI with a {bio_state_en} tone, while domestic leader flow proxy was {_fmt_flow_direction_en(metrics['bio_proxy_foreign_flow'])}. "
            f"The key same-day issue was '{top_issue_title_en}'."
        )
        return analysis_ko, analysis_en

    sb_state_ko, sb_state_en = classify_state_from_score(metrics["samsung_score"])
    analysis_ko = (
        f"삼성바이오로직스는 당일 {_fmt_pct(metrics['samsung_return'])}, 피어 대비 {_fmt_pct_point(metrics['samsung_vs_peer'])}로 {sb_state_ko} 흐름이었고, "
        f"외국인/기관 수급은 {_fmt_flow_direction_ko(metrics['samsung_foreign_flow'])}, {_fmt_flow_direction_ko(metrics['samsung_institution_flow'])}였습니다. "
        f"관련 당일 이슈는 '{top_issue_title_ko}' 입니다."
    )
    analysis_en = (
        f"Samsung Biologics moved {_fmt_pct(metrics['samsung_return'])} with {_fmt_pct_point(metrics['samsung_vs_peer'])} relative performance versus peers in a {sb_state_en} zone. "
        f"Foreign/institutional flows ended at {_fmt_flow_direction_en(metrics['samsung_foreign_flow'])} and {_fmt_flow_direction_en(metrics['samsung_institution_flow'])}, with '{top_issue_title_en}' as the key issue."
    )
    return analysis_ko, analysis_en


def _build_synthetic_issue_events(
    *,
    report_date: date,
    cutoff: datetime,
    metrics: dict[str, float],
) -> list[DailyIssueEventRecord]:
    flow_url = "https://data.krx.co.kr/"
    fred_usdkrw = "https://fred.stlouisfed.org/series/DEXKOUS"
    fred_us10y = "https://fred.stlouisfed.org/series/DGS10"

    events: list[DailyIssueEventRecord] = []
    for section_key in SECTION_KEYS:
        flow_title = f"{SECTION_TITLES[section_key]['ko']} 당일 수급 데이터"
        flow_summary = "당일 수급 방향과 강도에 기반한 공식 수치 근거"
        flow_issue_id = make_issue_id("pykrx_flow", section_key, flow_url, cutoff)
        events.append(
            DailyIssueEventRecord(
                issue_id=flow_issue_id,
                trade_date=report_date,
                section_key=section_key,
                symbol=_section_symbol(section_key),
                source_name="pykrx_flow",
                source_tier=1,
                title=flow_title,
                summary=flow_summary,
                url=flow_url,
                published_at_kst=cutoff,
                language="ko",
                topic_tags=["flow", "official"],
                sentiment=None,
                relevance_score=0.92,
                is_same_day=True,
            )
        )

        macro_issue_id = make_issue_id("macro_official", section_key, fred_us10y, cutoff)
        macro_title = "거시 공식 지표(환율/금리)"
        macro_summary = (
            f"USDKRW {_fmt_pct(metrics['usdkrw_change'])}, US10Y {_fmt_bp(metrics['us10y_bp_change'])}"
        )
        events.append(
            DailyIssueEventRecord(
                issue_id=macro_issue_id,
                trade_date=report_date,
                section_key=section_key,
                symbol="US10Y",
                source_name="macro_official",
                source_tier=1,
                title=macro_title,
                summary=macro_summary,
                url=fred_usdkrw if section_key == "kospi" else fred_us10y,
                published_at_kst=cutoff,
                language="en",
                topic_tags=["macro", "official"],
                sentiment=None,
                relevance_score=0.88,
                is_same_day=True,
            )
        )

    return events


def _select_section_evidence(
    repository: SupabaseRepository,
    report_date: date,
    section_key: str,
    cutoff: datetime,
) -> tuple[list[SectionEvidenceRecord], pd.DataFrame, float]:
    issues = repository.get_issue_events(report_date, section_key, cutoff.isoformat())
    if issues.empty:
        return [], issues, 0.0

    score = (
        pd.to_numeric(issues["relevance_score"], errors="coerce").fillna(0.0)
        + (4 - pd.to_numeric(issues["source_tier"], errors="coerce").fillna(3.0)) * 0.2
        + pd.to_numeric(issues["sentiment"], errors="coerce").abs().fillna(0.0) * 0.05
    )
    issues = issues.assign(_score=score)
    issues = issues.sort_values(["_score", "published_at_kst"], ascending=[False, False])

    selected = issues.head(3).copy()
    evidences: list[SectionEvidenceRecord] = []
    for idx, (_, row) in enumerate(selected.iterrows(), start=1):
        evidences.append(
            SectionEvidenceRecord(
                report_date=report_date,
                section_key=section_key,
                rank=idx,
                issue_id=str(row["issue_id"]),
                evidence_type=_evidence_type(str(row["source_name"])),
                weight=float(row["_score"]),
                reason=f"{row['source_name']}: {row['title']}",
            )
        )

    confidence = float(np.clip(selected["_score"].mean() if not selected.empty else 0.0, 0.0, 1.0))
    return evidences, selected, confidence


def generate_report_v2(repository: SupabaseRepository) -> dict:
    config = load_config()
    report_date = repository.get_latest_trade_date("KOSPI")
    if not report_date:
        raise RuntimeError("No KOSPI data found. Run backfill/daily ingest first.")

    cutoff = _parse_hhmm(report_date, config.strict_issue_cutoff_kst)

    required_symbols = get_required_report_symbols() or [
        "KOSPI",
        "KOSPI200_HEALTHCARE",
        "207940.KS",
        "068270.KS",
        "2269.HK",
        "LONN.SW",
    ]
    global_bio_symbols = get_report_bio_global_symbols() or DEFAULT_CORE_PEER_SYMBOLS
    core_peer_symbols = repository.get_group_symbols("bio_peer_core") or DEFAULT_CORE_PEER_SYMBOLS
    all_symbols = sorted(set(required_symbols + global_bio_symbols))

    price_frames = {
        symbol: repository.get_price_history(symbol, config.report_start_date, report_date)
        for symbol in all_symbols
    }

    kospi_frame = price_frames["KOSPI"]
    if kospi_frame.empty:
        raise RuntimeError("KOSPI frame is empty.")

    spine_dates = kospi_frame["trade_date"].sort_values().drop_duplicates()
    report_ts = pd.Timestamp(report_date)
    aligned = {
        symbol: asof_align_to_spine(spine_dates, frame)
        for symbol, frame in price_frames.items()
    }

    missing_symbols: list[str] = []
    for symbol in required_symbols:
        frame = aligned.get(symbol)
        if frame is None:
            missing_symbols.append(symbol)
            continue
        indexed = frame.set_index("trade_date")
        close_value = indexed.loc[report_ts, "close"] if report_ts in indexed.index else float("nan")
        if pd.isna(close_value):
            missing_symbols.append(symbol)

    if len(missing_symbols) > 1:
        raise RuntimeError(f"Too many missing symbols on report date: {', '.join(missing_symbols)}")

    close_matrix = pd.DataFrame(index=spine_dates)
    for symbol, frame in aligned.items():
        indexed = frame.set_index("trade_date")
        close_matrix[symbol] = indexed["close"]

    available_core_peer = [symbol for symbol in core_peer_symbols if symbol in close_matrix.columns]
    if not available_core_peer:
        available_core_peer = [symbol for symbol in DEFAULT_CORE_PEER_SYMBOLS if symbol in close_matrix.columns]
    close_matrix["BIO_PEER_AVG"] = close_matrix[available_core_peer].mean(axis=1, skipna=True)

    available_global = [symbol for symbol in global_bio_symbols if symbol in close_matrix.columns]
    if available_global:
        close_matrix["BIO_GLOBAL_AVG"] = close_matrix[available_global].mean(axis=1, skipna=True)
    else:
        close_matrix["BIO_GLOBAL_AVG"] = close_matrix["BIO_PEER_AVG"]
    base100 = normalize_base100(close_matrix, config.report_start_date)

    kospi_return = latest_pct_change(close_matrix["KOSPI"])
    healthcare_return = latest_pct_change(close_matrix["KOSPI200_HEALTHCARE"])
    samsung_return = latest_pct_change(close_matrix["207940.KS"])
    peer_return = latest_pct_change(close_matrix["BIO_PEER_AVG"])
    global_return = latest_pct_change(close_matrix["BIO_GLOBAL_AVG"])

    healthcare_excess = healthcare_return - kospi_return
    bio_basket_relative = global_return - kospi_return
    samsung_vs_peer = samsung_return - peer_return

    kospi_flow = repository.get_flow_history("market", "KOSPI", config.report_start_date, report_date)
    sbio_flow = repository.get_flow_history("stock", "207940.KS", config.report_start_date, report_date)
    celltrion_flow = repository.get_flow_history("stock", "068270.KS", config.report_start_date, report_date)

    kospi_foreign_flow = _flow_on_date(kospi_flow, report_date, "foreign")
    samsung_foreign_flow = _flow_on_date(sbio_flow, report_date, "foreign")
    samsung_institution_flow = _flow_on_date(sbio_flow, report_date, "institution")
    bio_proxy_foreign_flow = samsung_foreign_flow + _flow_on_date(celltrion_flow, report_date, "foreign")

    samsung_trade_value = (price_frames["207940.KS"]["close"] * price_frames["207940.KS"]["volume"]).dropna()
    samsung_avg_trade_value_20 = float(samsung_trade_value.tail(20).mean()) if not samsung_trade_value.empty else 0.0
    samsung_flow_strength = 0.0
    if samsung_avg_trade_value_20 > 0:
        samsung_flow_strength = (samsung_foreign_flow + samsung_institution_flow) / samsung_avg_trade_value_20

    usdkrw = repository.get_macro_history("USDKRW", config.report_start_date, report_date)
    us10y = repository.get_macro_history("US10Y", config.report_start_date, report_date)
    usdkrw_change = latest_pct_change(usdkrw["value"]) if not usdkrw.empty else 0.0
    us10y_bp_change = (latest_diff(us10y["value"]) * 100) if not us10y.empty else 0.0

    kospi_score = (kospi_return * 120) + (kospi_foreign_flow / 100_000_000_000 * 0.4) - (usdkrw_change * 25)
    bio_score = (healthcare_excess * 120) + (bio_basket_relative * 100) + (bio_proxy_foreign_flow / 100_000_000_000 * 0.2)
    samsung_score = (samsung_return * 100) + (samsung_vs_peer * 120) + (samsung_flow_strength * 8)

    metrics = {
        "report_date": report_date.isoformat(),
        "missing_symbols": missing_symbols,
        "kospi_return": kospi_return,
        "healthcare_excess_return": healthcare_excess,
        "samsung_return": samsung_return,
        "samsung_vs_peer": samsung_vs_peer,
        "bio_basket_relative": bio_basket_relative,
        "global_bio_relative": global_return - kospi_return,
        "kospi_foreign_flow": kospi_foreign_flow,
        "bio_proxy_foreign_flow": bio_proxy_foreign_flow,
        "samsung_foreign_flow": samsung_foreign_flow,
        "samsung_institution_flow": samsung_institution_flow,
        "samsung_flow_strength": samsung_flow_strength,
        "usdkrw_change": usdkrw_change,
        "us10y_bp_change": us10y_bp_change,
        "kospi_vol20": rolling_volatility(close_matrix["KOSPI"]),
        "samsung_vol20": rolling_volatility(close_matrix["207940.KS"]),
        "kospi_score": kospi_score,
        "bio_score": bio_score,
        "samsung_score": samsung_score,
    }

    synthetic_events = _build_synthetic_issue_events(report_date=report_date, cutoff=cutoff, metrics=metrics)
    repository.upsert_daily_issue_events(synthetic_events)

    partial_reasons: list[str] = []
    has_official_macro = _macro_source_has_official(usdkrw, report_date) and _macro_source_has_official(us10y, report_date)
    if not has_official_macro:
        partial_reasons.append("official macro source missing on report date")

    has_kospi_flow = _has_flow_on_date(kospi_flow, report_date, "foreign")
    has_samsung_flow = _has_flow_on_date(sbio_flow, report_date, "foreign")
    if not has_kospi_flow or not has_samsung_flow:
        partial_reasons.append("official flow source missing on report date")

    samsung_issues = repository.get_issue_events(report_date, "samsung_bio", cutoff.isoformat())
    has_open_dart = False
    if not samsung_issues.empty:
        has_open_dart = bool(
            samsung_issues["source_name"].astype(str).str.lower().str.contains("open_dart").any()
        )
    if not has_open_dart:
        partial_reasons.append("open_dart evidence missing for samsung_bio")

    section_records: list[ReportSectionRecord] = []
    section_evidences: list[SectionEvidenceRecord] = []

    for section_key in SECTION_KEYS:
        evidences, selected_issues, confidence = _select_section_evidence(
            repository=repository,
            report_date=report_date,
            section_key=section_key,
            cutoff=cutoff,
        )

        evidence_count = len(evidences)
        if evidence_count < 2:
            partial_reasons.append(f"evidence<2:{section_key}")

        top_issue_ko = "근거 부족"
        top_issue_en = "insufficient evidence"
        if not selected_issues.empty:
            top_issue_ko = str(selected_issues.iloc[0].get("title") or top_issue_ko)
            top_issue_en = top_issue_ko

        steps_ko, steps_en = _build_section_steps(section_key, metrics, top_issue_ko, top_issue_en)
        analysis_ko, analysis_en = _build_section_analysis(section_key, metrics, top_issue_ko, top_issue_en)

        enhanced = enhance_bilingual_analysis(
            enabled=config.llm_enabled,
            api_key=config.openai_api_key,
            model=config.openai_model,
            section_key=section_key,
            analysis_ko=analysis_ko,
            analysis_en=analysis_en,
            steps_ko=steps_ko,
            steps_en=steps_en,
        )
        if enhanced:
            analysis_ko, analysis_en = enhanced

        if evidence_count < 2:
            analysis_ko = "근거 링크가 2건 미만이어서 당일 해석 문장을 보류했습니다."
            analysis_en = "Interpretation was withheld because fewer than two evidence links were available."

        section_snapshot = {
            "metrics": metrics,
            "series_latest_base100": {
                series: float(base100.loc[report_ts, series])
                if report_ts in base100.index and series in base100.columns and pd.notna(base100.loc[report_ts, series])
                else None
                for series in SECTION_SERIES[section_key]
            },
            "cutoff_kst": cutoff.isoformat(),
            "selected_issue_ids": [str(row.get("issue_id")) for _, row in selected_issues.iterrows()] if not selected_issues.empty else [],
        }

        section_records.append(
            ReportSectionRecord(
                report_date=report_date,
                section_key=section_key,
                title_ko=SECTION_TITLES[section_key]["ko"],
                title_en=SECTION_TITLES[section_key]["en"],
                analysis_ko=analysis_ko,
                analysis_en=analysis_en,
                chart_key=section_key,
                as_of_date=report_date,
                input_snapshot_json=section_snapshot,
                analysis_steps_ko=steps_ko,
                analysis_steps_en=steps_en,
                evidence_count=evidence_count,
                confidence_score=confidence,
            )
        )

        repository.delete_section_evidence(report_date, section_key)
        section_evidences.extend(evidences)

    if missing_symbols:
        partial_reasons.append(f"missing_symbols:{','.join(missing_symbols)}")

    status = "partial" if partial_reasons else "complete"
    notes = "; ".join(partial_reasons) if partial_reasons else None

    report_record = ReportRecord(
        report_date=report_date,
        cutoff_kst=config.report_cutoff_kst,
        status=status,
        notes=notes,
    )

    repository.upsert_report(report_record, section_records)
    repository.upsert_section_evidence(section_evidences)

    return {
        "status": status,
        "report_date": report_date.isoformat(),
        "section_count": len(section_records),
        "evidence_count_total": len(section_evidences),
        "partial_reasons": partial_reasons,
    }
