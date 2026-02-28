from __future__ import annotations

from datetime import timedelta

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


def main() -> None:
    setup_logging()
    parser = common_arg_parser("Daily ingest for stock dashboard")
    parser.add_argument("--lookback-days", type=int, default=7)
    args = parser.parse_args()

    config = load_config()
    repository = SupabaseRepository(config.supabase_url, config.supabase_service_role_key)

    end_date = kst_now().date()
    start_date = end_date - timedelta(days=args.lookback_days)
    retry_delays = parse_delay_seconds(args.retry_delays, args.retries)

    def run_once() -> dict:
        return collect_ingest_window(repository, start_date, end_date)

    try:
        metrics = run_with_retries(run_once, retries=args.retries, delay_seconds=retry_delays)
        repository.insert_job_run("daily_ingest", "success", metrics=metrics)
    except Exception as exc:
        repository.insert_job_run(
            "daily_ingest",
            "failed",
            metrics={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    main()
