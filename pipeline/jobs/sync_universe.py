from __future__ import annotations

from pipeline.config import load_config
from pipeline.jobs.common import (
    common_arg_parser,
    parse_delay_seconds,
    run_with_retries,
    setup_logging,
)
from pipeline.repository import SupabaseRepository
from pipeline.universe_registry import (
    build_group_member_records,
    build_group_records,
    build_instrument_records,
)


def sync_universe(repository: SupabaseRepository) -> dict:
    instruments = build_instrument_records()
    groups = build_group_records()
    members = build_group_member_records()

    repository.upsert_instruments(instruments)
    repository.upsert_instrument_groups(groups)
    repository.replace_group_members(members)

    return {
        "instrument_count": len(instruments),
        "group_count": len(groups),
        "group_member_count": len(members),
    }


def main() -> None:
    setup_logging()
    parser = common_arg_parser("Sync instrument universe from registry")
    args = parser.parse_args()

    config = load_config()
    repository = SupabaseRepository(config.supabase_url, config.supabase_service_role_key)
    retry_delays = parse_delay_seconds(args.retry_delays, args.retries)

    def run_once() -> dict:
        return sync_universe(repository)

    try:
        metrics = run_with_retries(run_once, retries=args.retries, delay_seconds=retry_delays)
        repository.insert_job_run("sync_universe", "success", metrics=metrics)
    except Exception as exc:
        repository.insert_job_run(
            "sync_universe",
            "failed",
            metrics={},
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    main()
