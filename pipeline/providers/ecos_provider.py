from __future__ import annotations

from datetime import date, datetime

import requests

from pipeline.models import DailyMacroRecord
from pipeline.providers.interfaces import MacroProvider


class EcosProvider(MacroProvider):
    """Bank of Korea ECOS macro provider."""

    BASE = "https://ecos.bok.or.kr/api/StatisticSearch"

    def __init__(self, api_key: str | None):
        self.api_key = api_key

    def fetch_daily_macro(
        self, metrics: list[dict[str, str]], start_date: date, end_date: date
    ) -> list[DailyMacroRecord]:
        if not self.api_key:
            return []

        records: list[DailyMacroRecord] = []
        start = start_date.strftime("%Y%m%d")
        end = end_date.strftime("%Y%m%d")

        for metric in metrics:
            stat_code = metric.get("ecos_stat_code")
            item_code1 = metric.get("ecos_item_code1")
            if not stat_code or not item_code1:
                continue

            url = (
                f"{self.BASE}/{self.api_key}/json/kr/1/1000/"
                f"{stat_code}/D/{start}/{end}/{item_code1}"
            )
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            payload = response.json()
            search = payload.get("StatisticSearch") or {}
            rows = search.get("row") or []

            for row in rows:
                time_raw = str(row.get("TIME") or "")
                value_raw = row.get("DATA_VALUE")
                if len(time_raw) != 8 or value_raw in {None, ""}:
                    continue
                try:
                    trade_date = datetime.strptime(time_raw, "%Y%m%d").date()
                    value = float(value_raw)
                except (TypeError, ValueError):
                    continue

                records.append(
                    DailyMacroRecord(
                        metric_code=metric["metric_code"],
                        trade_date=trade_date,
                        value=value,
                        unit=metric["unit"],
                        source="ecos",
                    )
                )

        return records
