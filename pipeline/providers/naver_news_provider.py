from __future__ import annotations

from datetime import date, datetime
from email.utils import parsedate_to_datetime

import requests

from pipeline.models import DailyIssueEventRecord
from pipeline.providers.interfaces import IssueProvider
from pipeline.providers.utils import KST, make_issue_id, strip_html


class NaverNewsProvider(IssueProvider):
    """Korean news provider using Naver Search OpenAPI."""

    ENDPOINT = "https://openapi.naver.com/v1/search/news.json"

    def __init__(self, client_id: str | None, client_secret: str | None):
        self.client_id = client_id
        self.client_secret = client_secret

    def fetch_daily_issues(
        self,
        query_date: date,
        cutoff_kst: datetime,
        section_queries: dict[str, list[str]],
        symbols: list[str],
    ) -> list[DailyIssueEventRecord]:
        if not self.client_id or not self.client_secret:
            return []

        headers = {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
        }

        results: list[DailyIssueEventRecord] = []
        seen: set[str] = set()

        for section_key, queries in section_queries.items():
            for query in queries:
                response = requests.get(
                    self.ENDPOINT,
                    headers=headers,
                    params={
                        "query": query,
                        "display": 10,
                        "sort": "date",
                    },
                    timeout=20,
                )
                response.raise_for_status()

                payload = response.json()
                for item in payload.get("items", []) or []:
                    url = str(item.get("originallink") or item.get("link") or "").strip()
                    if not url or url in seen:
                        continue

                    published = self._parse_pub_date(item.get("pubDate"))
                    if published is None:
                        continue
                    if published.date() != query_date or published > cutoff_kst:
                        continue

                    seen.add(url)
                    title = strip_html(str(item.get("title") or ""))
                    summary = strip_html(str(item.get("description") or ""))
                    symbol = self._symbol_for_section(section_key)
                    issue_id = make_issue_id("naver_news", section_key, url, published)

                    results.append(
                        DailyIssueEventRecord(
                            issue_id=issue_id,
                            trade_date=query_date,
                            section_key=section_key,
                            symbol=symbol,
                            source_name="naver_news",
                            source_tier=2,
                            title=title or query,
                            summary=summary or None,
                            url=url,
                            published_at_kst=published,
                            language="ko",
                            topic_tags=["news", query],
                            sentiment=None,
                            relevance_score=0.7,
                            is_same_day=True,
                        )
                    )

        return results

    def _parse_pub_date(self, value: object) -> datetime | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        parsed = parsedate_to_datetime(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=KST)
        return parsed.astimezone(KST)

    def _symbol_for_section(self, section_key: str) -> str:
        if section_key == "samsung_bio":
            return "207940.KS"
        if section_key == "bio":
            return "KOSPI200_HEALTHCARE"
        return "KOSPI"
