from __future__ import annotations

from dataclasses import asdict
from datetime import date
from typing import Any

import pandas as pd
from supabase import Client, create_client

from pipeline.models import (
    DailyFlowRecord,
    DailyMacroRecord,
    DailyPriceRecord,
    InstrumentRecord,
    ReportRecord,
    ReportSectionRecord,
)


class SupabaseRepository:
    def __init__(self, url: str, service_role_key: str):
        self.client: Client = create_client(url, service_role_key)

    def _chunk(self, rows: list[dict[str, Any]], size: int = 500):
        for index in range(0, len(rows), size):
            yield rows[index : index + size]

    def upsert_instruments(self, rows: list[InstrumentRecord]) -> None:
        payload = [asdict(row) for row in rows]
        if not payload:
            return
        for batch in self._chunk(payload):
            self.client.table("instruments").upsert(batch, on_conflict="symbol").execute()

    def get_instrument_map(self, symbols: list[str] | None = None) -> dict[str, str]:
        query = self.client.table("instruments").select("instrument_id,symbol")
        if symbols:
            query = query.in_("symbol", symbols)
        response = query.execute()
        rows = response.data or []
        return {row["symbol"]: row["instrument_id"] for row in rows}

    def upsert_daily_prices(self, rows: list[DailyPriceRecord]) -> None:
        if not rows:
            return

        symbols = sorted({row.symbol for row in rows})
        instrument_map = self.get_instrument_map(symbols)

        payload: list[dict[str, Any]] = []
        for row in rows:
            instrument_id = instrument_map.get(row.symbol)
            if not instrument_id:
                continue
            payload.append(
                {
                    "instrument_id": instrument_id,
                    "trade_date": row.trade_date.isoformat(),
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "volume": row.volume,
                    "source": row.source,
                    "price_date_actual": row.price_date_actual.isoformat(),
                }
            )

        for batch in self._chunk(payload):
            self.client.table("daily_prices").upsert(
                batch,
                on_conflict="instrument_id,trade_date",
            ).execute()

    def upsert_daily_flows(self, rows: list[DailyFlowRecord]) -> None:
        payload = [
            {
                "target_type": row.target_type,
                "target_code": row.target_code,
                "trade_date": row.trade_date.isoformat(),
                "investor_type": row.investor_type,
                "buy_value_krw": row.buy_value_krw,
                "sell_value_krw": row.sell_value_krw,
                "source": row.source,
            }
            for row in rows
        ]

        for batch in self._chunk(payload):
            self.client.table("daily_flows").upsert(
                batch,
                on_conflict="target_type,target_code,trade_date,investor_type",
            ).execute()

    def upsert_daily_macro(self, rows: list[DailyMacroRecord]) -> None:
        payload = [
            {
                "metric_code": row.metric_code,
                "trade_date": row.trade_date.isoformat(),
                "value": row.value,
                "unit": row.unit,
                "source": row.source,
            }
            for row in rows
        ]

        for batch in self._chunk(payload):
            self.client.table("daily_macro").upsert(
                batch,
                on_conflict="metric_code,trade_date",
            ).execute()

    def upsert_report(
        self, report: ReportRecord, sections: list[ReportSectionRecord]
    ) -> None:
        self.client.table("daily_reports").upsert(
            [
                {
                    "report_date": report.report_date.isoformat(),
                    "cutoff_kst": report.cutoff_kst,
                    "status": report.status,
                    "notes": report.notes,
                }
            ],
            on_conflict="report_date",
        ).execute()

        section_payload = [
            {
                "report_date": section.report_date.isoformat(),
                "section_key": section.section_key,
                "title_ko": section.title_ko,
                "title_en": section.title_en,
                "analysis_ko": section.analysis_ko,
                "analysis_en": section.analysis_en,
                "chart_key": section.chart_key,
                "as_of_date": section.as_of_date.isoformat(),
                "input_snapshot_json": section.input_snapshot_json,
            }
            for section in sections
        ]

        for batch in self._chunk(section_payload):
            self.client.table("daily_report_sections").upsert(
                batch,
                on_conflict="report_date,section_key",
            ).execute()

    def insert_job_run(
        self,
        job_name: str,
        status: str,
        metrics: dict[str, Any] | None = None,
        error_message: str | None = None,
    ) -> None:
        self.client.table("job_runs").insert(
            {
                "job_name": job_name,
                "status": status,
                "error_message": error_message,
                "metrics_json": metrics or {},
            }
        ).execute()

    def get_latest_trade_date(self, symbol: str) -> date | None:
        instrument_map = self.get_instrument_map([symbol])
        instrument_id = instrument_map.get(symbol)
        if not instrument_id:
            return None

        response = (
            self.client.table("daily_prices")
            .select("trade_date")
            .eq("instrument_id", instrument_id)
            .order("trade_date", desc=True)
            .limit(1)
            .execute()
        )

        row = (response.data or [None])[0]
        if not row:
            return None
        return date.fromisoformat(row["trade_date"])

    def get_price_history(
        self,
        symbol: str,
        start_date: date,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        instrument_map = self.get_instrument_map([symbol])
        instrument_id = instrument_map.get(symbol)
        if not instrument_id:
            return pd.DataFrame(columns=["trade_date", "close", "volume", "price_date_actual"])

        query = (
            self.client.table("daily_prices")
            .select("trade_date,close,volume,price_date_actual")
            .eq("instrument_id", instrument_id)
            .gte("trade_date", start_date.isoformat())
        )
        if end_date:
            query = query.lte("trade_date", end_date.isoformat())

        response = query.order("trade_date", desc=False).execute()
        frame = pd.DataFrame(response.data or [])
        if frame.empty:
            return pd.DataFrame(columns=["trade_date", "close", "volume", "price_date_actual"])

        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
        frame["price_date_actual"] = pd.to_datetime(frame["price_date_actual"])
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
        return frame.sort_values("trade_date")

    def get_flow_history(
        self,
        target_type: str,
        target_code: str,
        start_date: date,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        query = (
            self.client.table("daily_flows")
            .select("trade_date,investor_type,buy_value_krw,sell_value_krw,net_value_krw")
            .eq("target_type", target_type)
            .eq("target_code", target_code)
            .gte("trade_date", start_date.isoformat())
            .order("trade_date", desc=False)
        )
        if end_date:
            query = query.lte("trade_date", end_date.isoformat())

        response = query.execute()
        frame = pd.DataFrame(response.data or [])
        if frame.empty:
            return pd.DataFrame(
                columns=[
                    "trade_date",
                    "investor_type",
                    "buy_value_krw",
                    "sell_value_krw",
                    "net_value_krw",
                ]
            )

        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
        for column in ["buy_value_krw", "sell_value_krw", "net_value_krw"]:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)

        return frame

    def get_macro_history(
        self,
        metric_code: str,
        start_date: date,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        query = (
            self.client.table("daily_macro")
            .select("trade_date,value")
            .eq("metric_code", metric_code)
            .gte("trade_date", start_date.isoformat())
            .order("trade_date", desc=False)
        )
        if end_date:
            query = query.lte("trade_date", end_date.isoformat())

        response = query.execute()
        frame = pd.DataFrame(response.data or [])
        if frame.empty:
            return pd.DataFrame(columns=["trade_date", "value"])

        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
        return frame
