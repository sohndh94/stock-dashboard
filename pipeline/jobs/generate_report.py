from __future__ import annotations

from datetime import date

import pandas as pd

from pipeline.config import REQUIRED_REPORT_SYMBOLS, SECTION_TITLES, load_config
from pipeline.jobs.common import (
    common_arg_parser,
    parse_delay_seconds,
    run_with_retries,
    setup_logging,
)
from pipeline.models import ReportRecord, ReportSectionRecord
from pipeline.reporting.analysis import (
    asof_align_to_spine,
    classify_state_from_score,
    latest_diff,
    latest_pct_change,
    normalize_base100,
    rolling_volatility,
)
from pipeline.repository import SupabaseRepository

PEER_SYMBOLS = ["207940.KS", "068270.KS", "2269.HK", "LONN.SW"]
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


def _macro_impact_label(usdkrw_change: float, us10y_bp_change: float) -> tuple[str, str]:
    if usdkrw_change >= 0.003 and us10y_bp_change >= 3:
        return ("부담", "a headwind")
    if usdkrw_change <= -0.003 and us10y_bp_change <= -3:
        return ("우호", "supportive")
    return ("중립", "mixed")


def _flow_on_date(frame: pd.DataFrame, report_date: date, investor_type: str) -> float:
    if frame.empty:
        return 0.0

    on_date = frame[frame["trade_date"] == pd.Timestamp(report_date)]
    on_date = on_date[on_date["investor_type"] == investor_type]
    if on_date.empty:
        return 0.0
    return float(on_date.iloc[-1]["net_value_krw"])


def _build_analyses(metrics: dict[str, float]) -> dict[str, tuple[str, str]]:
    kospi_state_ko, kospi_state_en = classify_state_from_score(metrics["kospi_score"])
    bio_state_ko, bio_state_en = classify_state_from_score(metrics["bio_score"])
    sbio_state_ko, sbio_state_en = classify_state_from_score(metrics["samsung_score"])
    macro_impact_ko, macro_impact_en = _macro_impact_label(
        metrics["usdkrw_change"], metrics["us10y_bp_change"]
    )

    kospi_ko = (
        f"코스피는 전일 대비 {_fmt_pct(metrics['kospi_return'])}로 {kospi_state_ko} 흐름이며, "
        f"외국인 순매수는 {_fmt_eok(metrics['kospi_foreign_flow'])}입니다. "
        f"원/달러는 {_fmt_pct(metrics['usdkrw_change'])}, 미국 10년물은 {_fmt_bp(metrics['us10y_bp_change'])} 변해 "
        f"거시 변수는 당일 시장에 {macro_impact_ko}으로 작용했습니다."
    )
    kospi_en = (
        f"KOSPI moved {_fmt_pct(metrics['kospi_return'])} day-over-day with a {kospi_state_en} tone, "
        f"while foreign net flow was {_fmt_eok(metrics['kospi_foreign_flow'])}. "
        f"USD/KRW changed {_fmt_pct(metrics['usdkrw_change'])} and U.S. 10Y moved {_fmt_bp(metrics['us10y_bp_change'])}, "
        f"so macro conditions acted as {macro_impact_en} for risk appetite."
    )

    bio_ko = (
        f"코스피200 헬스케어는 코스피 대비 {_fmt_pct_point(metrics['healthcare_excess_return'])} 초과성과를 보이며 {bio_state_ko} 톤입니다. "
        f"바이오 바스켓(삼성바이오·셀트리온·우시·론자)의 상대강도는 {_fmt_pct_point(metrics['bio_basket_relative'])}, "
        f"국내 대표 종목(삼성바이오+셀트리온) 외국인 순매수는 {_fmt_eok(metrics['bio_proxy_foreign_flow'])}로 집계됐습니다."
    )
    bio_en = (
        f"KOSPI200 Health Care outperformed KOSPI by {_fmt_pct_point(metrics['healthcare_excess_return'])} with a {bio_state_en} tone. "
        f"Relative strength of the bio basket (Samsung Biologics, Celltrion, Wuxi Biologics, Lonza) was {_fmt_pct_point(metrics['bio_basket_relative'])}, "
        f"and foreign net flow proxy for domestic leaders was {_fmt_eok(metrics['bio_proxy_foreign_flow'])}."
    )

    samsung_ko = (
        f"삼성바이오로직스는 전일 대비 {_fmt_pct(metrics['samsung_return'])}로 피어 평균 대비 {_fmt_pct_point(metrics['samsung_vs_peer'])} 상대성과를 기록해 {sbio_state_ko} 구간입니다. "
        f"외국인/기관 순매수는 각각 {_fmt_eok(metrics['samsung_foreign_flow'])}, {_fmt_eok(metrics['samsung_institution_flow'])}이며, "
        f"20일 평균 거래대금 대비 수급 강도는 {metrics['samsung_flow_strength']:+.2%}입니다."
    )
    samsung_en = (
        f"Samsung Biologics returned {_fmt_pct(metrics['samsung_return'])} and outperformed peer average by {_fmt_pct_point(metrics['samsung_vs_peer'])}, "
        f"placing it in a {sbio_state_en} zone. "
        f"Foreign/institutional net flows were {_fmt_eok(metrics['samsung_foreign_flow'])} and {_fmt_eok(metrics['samsung_institution_flow'])}, "
        f"with flow intensity at {metrics['samsung_flow_strength']:+.2%} of 20-day average traded value."
    )

    return {
        "kospi": (kospi_ko, kospi_en),
        "bio": (bio_ko, bio_en),
        "samsung_bio": (samsung_ko, samsung_en),
    }


def generate_report(repository: SupabaseRepository) -> dict:
    config = load_config()

    report_date = repository.get_latest_trade_date("KOSPI")
    if not report_date:
        raise RuntimeError("No KOSPI data found. Run backfill/daily ingest first.")

    price_frames = {
        symbol: repository.get_price_history(symbol, config.report_start_date, report_date)
        for symbol in REQUIRED_REPORT_SYMBOLS
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
    for symbol, frame in aligned.items():
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

    close_matrix["BIO_PEER_AVG"] = close_matrix[PEER_SYMBOLS].mean(axis=1, skipna=True)
    base100 = normalize_base100(close_matrix, config.report_start_date)

    kospi_return = latest_pct_change(close_matrix["KOSPI"])
    healthcare_return = latest_pct_change(close_matrix["KOSPI200_HEALTHCARE"])
    samsung_return = latest_pct_change(close_matrix["207940.KS"])
    peer_return = latest_pct_change(close_matrix["BIO_PEER_AVG"])

    healthcare_excess = healthcare_return - kospi_return
    bio_basket_relative = peer_return - kospi_return
    samsung_vs_peer = samsung_return - peer_return

    kospi_flow = repository.get_flow_history("market", "KOSPI", config.report_start_date, report_date)
    sbio_flow = repository.get_flow_history("stock", "207940.KS", config.report_start_date, report_date)
    celltrion_flow = repository.get_flow_history("stock", "068270.KS", config.report_start_date, report_date)

    kospi_foreign_flow = _flow_on_date(kospi_flow, report_date, "foreign")
    samsung_foreign_flow = _flow_on_date(sbio_flow, report_date, "foreign")
    samsung_institution_flow = _flow_on_date(sbio_flow, report_date, "institution")
    bio_proxy_foreign_flow = samsung_foreign_flow + _flow_on_date(
        celltrion_flow, report_date, "foreign"
    )

    samsung_trade_value = (
        price_frames["207940.KS"]["close"] * price_frames["207940.KS"]["volume"]
    ).dropna()
    samsung_avg_trade_value_20 = (
        float(samsung_trade_value.tail(20).mean()) if not samsung_trade_value.empty else 0.0
    )
    samsung_flow_strength = 0.0
    if samsung_avg_trade_value_20 > 0:
        samsung_flow_strength = (
            samsung_foreign_flow + samsung_institution_flow
        ) / samsung_avg_trade_value_20

    usdkrw = repository.get_macro_history("USDKRW", config.report_start_date, report_date)
    us10y = repository.get_macro_history("US10Y", config.report_start_date, report_date)

    usdkrw_change = latest_pct_change(usdkrw["value"]) if not usdkrw.empty else 0.0
    us10y_bp_change = (latest_diff(us10y["value"]) * 100) if not us10y.empty else 0.0

    kospi_score = (
        (kospi_return * 120)
        + (kospi_foreign_flow / 100_000_000_000 * 0.4)
        - (usdkrw_change * 25)
    )
    bio_score = (
        (healthcare_excess * 120)
        + (bio_basket_relative * 100)
        + (bio_proxy_foreign_flow / 100_000_000_000 * 0.2)
    )
    samsung_score = (
        (samsung_return * 100)
        + (samsung_vs_peer * 120)
        + (samsung_flow_strength * 8)
    )

    metrics = {
        "report_date": report_date.isoformat(),
        "missing_symbols": missing_symbols,
        "kospi_return": kospi_return,
        "healthcare_excess_return": healthcare_excess,
        "samsung_return": samsung_return,
        "samsung_vs_peer": samsung_vs_peer,
        "bio_basket_relative": bio_basket_relative,
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

    analyses = _build_analyses(metrics)

    section_records: list[ReportSectionRecord] = []
    for section_key in ["kospi", "bio", "samsung_bio"]:
        analysis_ko, analysis_en = analyses[section_key]
        section_snapshot = {
            "metrics": metrics,
            "series_latest_base100": {
                series: float(base100.loc[report_ts, series])
                if report_ts in base100.index and series in base100.columns and pd.notna(base100.loc[report_ts, series])
                else None
                for series in SECTION_SERIES[section_key]
            },
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
            )
        )

    status = "partial" if len(missing_symbols) == 1 else "complete"
    notes = None
    if missing_symbols:
        notes = f"Missing symbols on report date: {', '.join(missing_symbols)}"

    report_record = ReportRecord(
        report_date=report_date,
        cutoff_kst=config.report_cutoff_kst,
        status=status,
        notes=notes,
    )

    repository.upsert_report(report_record, section_records)

    metrics_for_job = {
        "status": status,
        "report_date": report_date.isoformat(),
        "section_count": len(section_records),
        "missing_symbols": missing_symbols,
    }
    return metrics_for_job


def main() -> None:
    setup_logging()
    parser = common_arg_parser("Generate daily bilingual report")
    args = parser.parse_args()

    config = load_config()
    repository = SupabaseRepository(config.supabase_url, config.supabase_service_role_key)
    retry_delays = parse_delay_seconds(args.retry_delays, args.retries)

    def run_once() -> dict:
        return generate_report(repository)

    try:
        metrics = run_with_retries(run_once, retries=args.retries, delay_seconds=retry_delays)
        status = metrics.get("status", "success")
        repository.insert_job_run("generate_report", status, metrics=metrics)
    except Exception as exc:
        repository.insert_job_run(
            "generate_report",
            "failed",
            metrics={},
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    main()
