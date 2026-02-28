from __future__ import annotations

from datetime import date

from pipeline.config import load_config
from pipeline.jobs.common import (
    common_arg_parser,
    kst_now,
    parse_delay_seconds,
    run_with_retries,
    setup_logging,
)
from pipeline.jobs.ingest_shared import collect_ingest_window
from pipeline.repository import SupabaseRepository


def _parse_iso_date(raw: str) -> date:
    return date.fromisoformat(raw)


def main() -> None:
    setup_logging()
    parser = common_arg_parser("Backfill historical data for stock dashboard")
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    args = parser.parse_args()

    config = load_config()
    repository = SupabaseRepository(config.supabase_url, config.supabase_service_role_key)

    start_date = _parse_iso_date(args.start) if args.start else config.report_start_date
    end_date = _parse_iso_date(args.end) if args.end else kst_now().date()
    retry_delays = parse_delay_seconds(args.retry_delays, args.retries)

    def run_once() -> dict:
        return collect_ingest_window(repository, start_date, end_date)

    try:
        metrics = run_with_retries(run_once, retries=args.retries, delay_seconds=retry_delays)
        repository.insert_job_run("backfill", "success", metrics=metrics)
    except Exception as exc:
        repository.insert_job_run(
            "backfill",
            "failed",
            metrics={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    main()
