from __future__ import annotations

from datetime import date, datetime

import requests

from pipeline.models import DailyIssueEventRecord
from pipeline.providers.interfaces import IssueProvider
from pipeline.providers.utils import KST, kst_datetime_on_day, make_issue_id


class OpenDartProvider(IssueProvider):
    """Official disclosure source via OpenDART list API."""

    ENDPOINT = "https://opendart.fss.or.kr/api/list.json"

    def __init__(self, api_key: str | None):
        self.api_key = api_key

    def fetch_daily_issues(
        self,
        query_date: date,
        cutoff_kst: datetime,
        section_queries: dict[str, list[str]],
        symbols: list[str],
    ) -> list[DailyIssueEventRecord]:
        if not self.api_key:
            return []

        day_raw = query_date.strftime("%Y%m%d")
        response = requests.get(
            self.ENDPOINT,
            params={
                "crtfc_key": self.api_key,
                "bgn_de": day_raw,
                "end_de": day_raw,
                "page_count": 100,
            },
            timeout=20,
        )
        response.raise_for_status()

        payload = response.json()
        if payload.get("status") not in {"000", "013"}:
            message = payload.get("message", "OpenDART API error")
            raise RuntimeError(f"OpenDART error: {message}")

        items = payload.get("list", []) or []
        results: list[DailyIssueEventRecord] = []
        for item in items:
            stock_code = str(item.get("stock_code") or "").strip()
            corp_name = str(item.get("corp_name") or "")
            if stock_code != "207940" and "삼성바이오" not in corp_name:
                continue

            receipt_no = str(item.get("rcept_no") or "").strip()
            if not receipt_no:
                continue

            published = self._parse_dart_time(item.get("rcept_dt"), query_date)
            if published.date() != query_date or published > cutoff_kst:
                continue

            url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={receipt_no}"
            title = str(item.get("report_nm") or "공시")
            issue_id = make_issue_id("open_dart", "samsung_bio", url, published)

            results.append(
                DailyIssueEventRecord(
                    issue_id=issue_id,
                    trade_date=query_date,
                    section_key="samsung_bio",
                    symbol="207940.KS",
                    source_name="open_dart",
                    source_tier=1,
                    title=title,
                    summary=f"공시 접수번호 {receipt_no}",
                    url=url,
                    published_at_kst=published,
                    language="ko",
                    topic_tags=["disclosure", "official"],
                    sentiment=None,
                    relevance_score=1.0,
                    is_same_day=True,
                )
            )

        return results

    def _parse_dart_time(self, value: object, fallback_date: date) -> datetime:
        raw = str(value or "").strip()
        if len(raw) == 8 and raw.isdigit():
            day = date(int(raw[0:4]), int(raw[4:6]), int(raw[6:8]))
            return kst_datetime_on_day(day, 16, 0)
        return kst_datetime_on_day(fallback_date, 16, 0).astimezone(KST)
