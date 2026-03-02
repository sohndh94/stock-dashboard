from __future__ import annotations

from pipeline.config import load_config
from pipeline.jobs.common import (
    common_arg_parser,
    parse_delay_seconds,
    run_with_retries,
    setup_logging,
)
from pipeline.reporting.report_v2 import generate_report_v2
from pipeline.repository import SupabaseRepository


def _job_status_from_report_status(report_status: str | None) -> str:
    normalized = (report_status or "").strip().lower()
    if normalized == "partial":
        return "partial"
    if normalized in {"complete", "success"}:
        return "success"
    return "success"


def main() -> None:
    setup_logging()
    parser = common_arg_parser("Generate v2 daily bilingual report with evidence")
    args = parser.parse_args()

    config = load_config()
    repository = SupabaseRepository(config.supabase_url, config.supabase_service_role_key)
    retry_delays = parse_delay_seconds(args.retry_delays, args.retries)

    def run_once() -> dict:
        return generate_report_v2(repository)

    try:
        metrics = run_with_retries(run_once, retries=args.retries, delay_seconds=retry_delays)
        status = _job_status_from_report_status(metrics.get("status"))
        repository.insert_job_run("generate_report_v2", status, metrics=metrics)
    except Exception as exc:
        repository.insert_job_run(
            "generate_report_v2",
            "failed",
            metrics={},
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    main()
