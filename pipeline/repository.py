from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
from supabase import Client, create_client

from pipeline.models import (
    DailyCompanyMetricRecord,
    DailyFlowRecord,
    DailyIssueEventRecord,
    DailyMacroRecord,
    DailyPriceRecord,
    InstrumentGroupMemberRecord,
    InstrumentGroupRecord,
    InstrumentRecord,
    ReportRecord,
    ReportSectionRecord,
    SectionEvidenceRecord,
)


class SupabaseRepository:
    def __init__(self, url: str, service_role_key: str):
        self.client: Client = create_client(url, service_role_key)

    def _chunk(self, rows: list[dict[str, Any]], size: int = 500):
        for index in range(0, len(rows), size):
            yield rows[index : index + size]

    def upsert_instruments(self, rows: list[InstrumentRecord]) -> None:
        payload = [
            {
                "symbol": row.symbol,
                "name": row.name,
                "name_ko": row.name_ko,
                "category": row.category,
                "asset_type": row.asset_type,
                "market": row.market,
                "currency": row.currency,
                "provider": row.provider,
                "provider_symbol": row.provider_symbol,
                "display_order": row.display_order,
                "is_compare_default": row.is_compare_default,
                "is_active": row.is_active,
            }
            for row in rows
        ]
        if not payload:
            return
        for batch in self._chunk(payload):
            self.client.table("instruments").upsert(batch, on_conflict="symbol").execute()

    def upsert_instrument_groups(self, rows: list[InstrumentGroupRecord]) -> None:
        payload = [
            {
                "group_key": row.group_key,
                "name": row.name,
                "purpose": row.purpose,
                "is_active": row.is_active,
            }
            for row in rows
        ]
        if not payload:
            return
        for batch in self._chunk(payload):
            self.client.table("instrument_groups").upsert(
                batch,
                on_conflict="group_key",
            ).execute()

    def replace_group_members(self, rows: list[InstrumentGroupMemberRecord]) -> None:
        if not rows:
            return

        group_keys = sorted({row.group_key for row in rows})
        instrument_map = self.get_instrument_map()

        for group_key in group_keys:
            (
                self.client.table("instrument_group_members")
                .delete()
                .eq("group_key", group_key)
                .execute()
            )

        payload: list[dict[str, Any]] = []
        for row in rows:
            instrument_id = instrument_map.get(row.symbol)
            if not instrument_id:
                continue
            payload.append(
                {
                    "group_key": row.group_key,
                    "instrument_id": instrument_id,
                    "weight": row.weight,
                    "role": row.role,
                }
            )

        for batch in self._chunk(payload):
            self.client.table("instrument_group_members").insert(batch).execute()

    def get_instrument_map(self, symbols: list[str] | None = None) -> dict[str, str]:
        query = self.client.table("instruments").select("instrument_id,symbol")
        if symbols:
            query = query.in_("symbol", symbols)
        response = query.execute()
        rows = response.data or []
        return {row["symbol"]: row["instrument_id"] for row in rows}

    def get_active_instruments(
        self,
        provider: str | None = None,
        asset_type: str | None = None,
    ) -> list[dict[str, Any]]:
        query = (
            self.client.table("instruments")
            .select(
                "symbol,name,name_ko,category,asset_type,market,currency,"
                "provider,provider_symbol,display_order,is_compare_default,is_active"
            )
            .eq("is_active", True)
            .order("display_order", desc=False)
        )
        if provider:
            query = query.eq("provider", provider)
        if asset_type:
            query = query.eq("asset_type", asset_type)

        response = query.execute()
        return list(response.data or [])

    def get_group_symbols(self, group_key: str) -> list[str]:
        response = (
            self.client.table("instrument_group_members")
            .select("instrument_id,instruments(symbol,is_active)")
            .eq("group_key", group_key)
            .order("instrument_id", desc=False)
            .execute()
        )

        symbols: list[str] = []
        for row in response.data or []:
            instrument = row.get("instruments")
            if not instrument:
                continue
            if isinstance(instrument, list):
                if not instrument:
                    continue
                item = instrument[0]
            else:
                item = instrument
            if not item.get("is_active", True):
                continue
            symbol = item.get("symbol")
            if symbol:
                symbols.append(symbol)
        return symbols

    def get_compare_default_symbols(self) -> list[str]:
        response = (
            self.client.table("instruments")
            .select("symbol")
            .eq("is_active", True)
            .eq("is_compare_default", True)
            .order("display_order", desc=False)
            .execute()
        )
        return [row["symbol"] for row in (response.data or [])]

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

    def upsert_daily_company_metrics(self, rows: list[DailyCompanyMetricRecord]) -> None:
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
                    "market_cap": row.market_cap,
                    "shares_outstanding": row.shares_outstanding,
                    "source": row.source,
                }
            )

        for batch in self._chunk(payload):
            self.client.table("daily_company_metrics").upsert(
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

    def upsert_daily_issue_events(self, rows: list[DailyIssueEventRecord]) -> None:
        payload = [
            {
                "issue_id": row.issue_id,
                "trade_date": row.trade_date.isoformat(),
                "section_key": row.section_key,
                "symbol": row.symbol,
                "source_name": row.source_name,
                "source_tier": row.source_tier,
                "title": row.title,
                "summary": row.summary,
                "url": row.url,
                "published_at_kst": row.published_at_kst.isoformat(),
                "language": row.language,
                "topic_tags": row.topic_tags,
                "sentiment": row.sentiment,
                "relevance_score": row.relevance_score,
                "is_same_day": row.is_same_day,
            }
            for row in rows
        ]

        if not payload:
            return

        for batch in self._chunk(payload):
            self.client.table("daily_issue_events").upsert(
                batch,
                on_conflict="issue_id",
            ).execute()

    def upsert_section_evidence(self, rows: list[SectionEvidenceRecord]) -> None:
        payload = [
            {
                "report_date": row.report_date.isoformat(),
                "section_key": row.section_key,
                "rank": row.rank,
                "issue_id": row.issue_id,
                "evidence_type": row.evidence_type,
                "weight": row.weight,
                "reason": row.reason,
            }
            for row in rows
        ]

        if not payload:
            return

        for batch in self._chunk(payload):
            self.client.table("daily_section_evidence").upsert(
                batch,
                on_conflict="report_date,section_key,rank",
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
                "analysis_steps_ko": section.analysis_steps_ko or [],
                "analysis_steps_en": section.analysis_steps_en or [],
                "chart_key": section.chart_key,
                "as_of_date": section.as_of_date.isoformat(),
                "evidence_count": section.evidence_count,
                "confidence_score": section.confidence_score,
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

    def insert_source_fetch_run(
        self,
        source_name: str,
        status: str,
        metrics: dict[str, Any] | None = None,
        http_status: int | None = None,
        error_message: str | None = None,
    ) -> None:
        try:
            self.client.table("source_fetch_runs").insert(
                {
                    "source_name": source_name,
                    "status": status,
                    "http_status": http_status,
                    "error_message": error_message,
                    "metrics_json": metrics or {},
                }
            ).execute()
        except Exception:
            return

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

    def get_issue_events(
        self,
        trade_date: date,
        section_key: str,
        cutoff_kst_iso: str,
    ) -> pd.DataFrame:
        response = (
            self.client.table("daily_issue_events")
            .select(
                "issue_id,trade_date,section_key,symbol,source_name,source_tier,title,summary,url,"
                "published_at_kst,language,topic_tags,sentiment,relevance_score,is_same_day"
            )
            .eq("trade_date", trade_date.isoformat())
            .eq("section_key", section_key)
            .eq("is_same_day", True)
            .lte("published_at_kst", cutoff_kst_iso)
            .order("published_at_kst", desc=True)
            .execute()
        )
        frame = pd.DataFrame(response.data or [])
        if frame.empty:
            return pd.DataFrame(
                columns=[
                    "issue_id",
                    "trade_date",
                    "section_key",
                    "symbol",
                    "source_name",
                    "source_tier",
                    "title",
                    "summary",
                    "url",
                    "published_at_kst",
                    "language",
                    "topic_tags",
                    "sentiment",
                    "relevance_score",
                    "is_same_day",
                ]
            )

        frame["published_at_kst"] = pd.to_datetime(
            frame["published_at_kst"], errors="coerce"
        )
        frame["source_tier"] = pd.to_numeric(frame["source_tier"], errors="coerce")
        frame["relevance_score"] = pd.to_numeric(
            frame["relevance_score"], errors="coerce"
        ).fillna(0.0)
        frame["sentiment"] = pd.to_numeric(frame["sentiment"], errors="coerce")
        return frame

    def delete_section_evidence(self, report_date: date, section_key: str) -> None:
        (
            self.client.table("daily_section_evidence")
            .delete()
            .eq("report_date", report_date.isoformat())
            .eq("section_key", section_key)
            .execute()
        )

    def get_macro_history(
        self,
        metric_code: str,
        start_date: date,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        query = (
            self.client.table("daily_macro")
            .select("trade_date,value,source")
            .eq("metric_code", metric_code)
            .gte("trade_date", start_date.isoformat())
            .order("trade_date", desc=False)
        )
        if end_date:
            query = query.lte("trade_date", end_date.isoformat())

        response = query.execute()
        frame = pd.DataFrame(response.data or [])
        if frame.empty:
            return pd.DataFrame(columns=["trade_date", "value", "source"])

        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
        return frame
