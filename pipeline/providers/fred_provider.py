from __future__ import annotations

from datetime import date, datetime

import requests

from pipeline.models import DailyMacroRecord
from pipeline.providers.interfaces import MacroProvider


class FredProvider(MacroProvider):
    """Macro provider backed by FRED official API."""

    ENDPOINT = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(self, api_key: str | None):
        self.api_key = api_key

    def fetch_daily_macro(
        self, metrics: list[dict[str, str]], start_date: date, end_date: date
    ) -> list[DailyMacroRecord]:
        if not self.api_key:
            return []

        records: list[DailyMacroRecord] = []
        for metric in metrics:
            series_id = metric.get("fred_series_id")
            if not series_id:
                continue

            response = requests.get(
                self.ENDPOINT,
                params={
                    "api_key": self.api_key,
                    "file_type": "json",
                    "series_id": series_id,
                    "observation_start": start_date.isoformat(),
                    "observation_end": end_date.isoformat(),
                    "sort_order": "asc",
                },
                timeout=20,
            )
            response.raise_for_status()
            payload = response.json()

            for item in payload.get("observations", []) or []:
                value_raw = item.get("value")
                if value_raw in {None, "."}:
                    continue
                try:
                    value = float(value_raw)
                except (TypeError, ValueError):
                    continue

                records.append(
                    DailyMacroRecord(
                        metric_code=metric["metric_code"],
                        trade_date=datetime.strptime(item["date"], "%Y-%m-%d").date(),
                        value=value,
                        unit=metric["unit"],
                        source="fred",
                    )
                )

        return records
