from __future__ import annotations

import argparse
import logging
import time
from collections.abc import Callable
from datetime import datetime

import pytz

logger = logging.getLogger("pipeline")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )


def kst_now() -> datetime:
    timezone = pytz.timezone("Asia/Seoul")
    return datetime.now(timezone)


def parse_delay_seconds(raw: str | None, retries: int) -> list[int]:
    if raw:
        delays = [int(value.strip()) for value in raw.split(",") if value.strip()]
        if len(delays) >= retries:
            return delays[:retries]
        if delays:
            while len(delays) < retries:
                delays.append(delays[-1])
            return delays

    defaults = [60, 180]
    if retries <= len(defaults):
        return defaults[:retries]

    while len(defaults) < retries:
        defaults.append(defaults[-1])
    return defaults


def run_with_retries(
    run: Callable[[], dict],
    retries: int,
    delay_seconds: list[int],
) -> dict:
    attempts = retries + 1
    for attempt in range(1, attempts + 1):
        try:
            return run()
        except Exception as exc:
            if attempt == attempts:
                raise
            sleep_for = delay_seconds[attempt - 1]
            logger.warning(
                "Attempt %s/%s failed: %s. Retrying in %ss",
                attempt,
                attempts,
                exc,
                sleep_for,
            )
            time.sleep(sleep_for)

    return {}


def common_arg_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument(
        "--retry-delays",
        type=str,
        default="60,180",
        help="comma-separated seconds between retries",
    )
    return parser
