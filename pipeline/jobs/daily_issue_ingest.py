from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

from pipeline.config import SECTION_NEWS_QUERIES, get_issue_watch_symbols, load_config
from pipeline.jobs.common import (
    common_arg_parser,
    kst_now,
    parse_delay_seconds,
    run_with_retries,
    setup_logging,
)
from pipeline.models import DailyIssueEventRecord
from pipeline.providers.alpha_vantage_provider import AlphaVantageNewsProvider
from pipeline.providers.naver_news_provider import NaverNewsProvider
from pipeline.providers.open_dart_provider import OpenDartProvider
from pipeline.repository import SupabaseRepository

KST = ZoneInfo("Asia/Seoul")


def _cutoff_datetime(trade_date: date, cutoff_hhmm: str) -> datetime:
    hh, mm = [int(part) for part in cutoff_hhmm.split(":")]
    return datetime(trade_date.year, trade_date.month, trade_date.day, hh, mm, tzinfo=KST)


def ingest_daily_issues(repository: SupabaseRepository) -> dict:
    config = load_config()

    trade_date = repository.get_latest_trade_date("KOSPI") or kst_now().date()
    cutoff = _cutoff_datetime(trade_date, config.strict_issue_cutoff_kst)

    providers = [
        (
            "open_dart",
            OpenDartProvider(config.dart_api_key),
            bool(config.dart_api_key),
        ),
        (
            "naver_news",
            NaverNewsProvider(config.naver_client_id, config.naver_client_secret),
            bool(config.naver_client_id and config.naver_client_secret),
        ),
        (
            "alpha_vantage",
            AlphaVantageNewsProvider(config.alphavantage_api_key),
            bool(config.alphavantage_api_key),
        ),
    ]

    collected: list[DailyIssueEventRecord] = []
    metrics: dict[str, int | str] = {
        "trade_date": trade_date.isoformat(),
        "cutoff_kst": cutoff.isoformat(),
        "total_events": 0,
    }

    for source_name, provider, is_configured in providers:
        if not is_configured:
            repository.insert_source_fetch_run(
                source_name=source_name,
                status="skipped",
                metrics={"reason": "missing_api_credentials", "trade_date": trade_date.isoformat()},
            )
            metrics[f"{source_name}_events"] = 0
            continue

        try:
            rows = provider.fetch_daily_issues(
                query_date=trade_date,
                cutoff_kst=cutoff,
                section_queries=SECTION_NEWS_QUERIES,
                symbols=get_issue_watch_symbols(),
            )
            rows = [
                row
                for row in rows
                if row.trade_date == trade_date
                and row.published_at_kst.date() == trade_date
                and row.published_at_kst <= cutoff
            ]

            repository.insert_source_fetch_run(
                source_name=source_name,
                status="success",
                metrics={"event_count": len(rows), "trade_date": trade_date.isoformat()},
            )
            metrics[f"{source_name}_events"] = len(rows)
            collected.extend(rows)
        except Exception as exc:
            repository.insert_source_fetch_run(
                source_name=source_name,
                status="failed",
                metrics={"trade_date": trade_date.isoformat()},
                error_message=str(exc),
            )
            metrics[f"{source_name}_events"] = 0

    deduped: dict[str, DailyIssueEventRecord] = {}
    for row in collected:
        deduped[row.issue_id] = row

    rows_to_save = list(deduped.values())
    repository.upsert_daily_issue_events(rows_to_save)

    metrics["total_events"] = len(rows_to_save)
    return metrics


def main() -> None:
    setup_logging()
    parser = common_arg_parser("Daily issue ingest for report evidence")
    args = parser.parse_args()

    config = load_config()
    repository = SupabaseRepository(config.supabase_url, config.supabase_service_role_key)
    retry_delays = parse_delay_seconds(args.retry_delays, args.retries)

    def run_once() -> dict:
        return ingest_daily_issues(repository)

    try:
        metrics = run_with_retries(run_once, retries=args.retries, delay_seconds=retry_delays)
        repository.insert_job_run("daily_issue_ingest", "success", metrics=metrics)
    except Exception as exc:
        repository.insert_job_run(
            "daily_issue_ingest",
            "failed",
            metrics={},
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    main()
