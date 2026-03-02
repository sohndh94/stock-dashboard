from __future__ import annotations

from datetime import date, datetime

import requests

from pipeline.config import ALPHAVANTAGE_SECTION_TOPICS
from pipeline.models import DailyIssueEventRecord
from pipeline.providers.interfaces import IssueProvider
from pipeline.providers.utils import KST, make_issue_id


class AlphaVantageNewsProvider(IssueProvider):
    """Global news fallback via Alpha Vantage NEWS_SENTIMENT."""

    ENDPOINT = "https://www.alphavantage.co/query"

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

        time_from = query_date.strftime("%Y%m%dT0000")
        time_to = cutoff_kst.strftime("%Y%m%dT%H%M")

        results: list[DailyIssueEventRecord] = []
        seen: set[str] = set()

        for section_key, topic in ALPHAVANTAGE_SECTION_TOPICS.items():
            response = requests.get(
                self.ENDPOINT,
                params={
                    "function": "NEWS_SENTIMENT",
                    "topics": topic,
                    "sort": "LATEST",
                    "limit": 20,
                    "time_from": time_from,
                    "time_to": time_to,
                    "apikey": self.api_key,
                },
                timeout=25,
            )
            response.raise_for_status()

            payload = response.json()
            for item in payload.get("feed", []) or []:
                url = str(item.get("url") or "").strip()
                if not url or url in seen:
                    continue

                published = self._parse_time(item.get("time_published"))
                if published is None:
                    continue
                if published.date() != query_date or published > cutoff_kst:
                    continue

                title = str(item.get("title") or "").strip()
                summary = str(item.get("summary") or "").strip() or None
                sentiment = _to_float(item.get("overall_sentiment_score"))
                issue_id = make_issue_id("alpha_vantage", section_key, url, published)

                seen.add(url)
                results.append(
                    DailyIssueEventRecord(
                        issue_id=issue_id,
                        trade_date=query_date,
                        section_key=section_key,
                        symbol=self._symbol_for_section(section_key),
                        source_name="alpha_vantage",
                        source_tier=3,
                        title=title or topic,
                        summary=summary,
                        url=url,
                        published_at_kst=published,
                        language="en",
                        topic_tags=["news", topic],
                        sentiment=sentiment,
                        relevance_score=0.55,
                        is_same_day=True,
                    )
                )

        return results

    def _parse_time(self, raw_value: object) -> datetime | None:
        raw = str(raw_value or "").strip()
        if len(raw) < 15:
            return None

        try:
            dt = datetime.strptime(raw[:15], "%Y%m%dT%H%M%S")
        except ValueError:
            return None

        return dt.replace(tzinfo=KST)

    def _symbol_for_section(self, section_key: str) -> str:
        if section_key == "samsung_bio":
            return "207940.KS"
        if section_key == "bio":
            return "KOSPI200_HEALTHCARE"
        return "KOSPI"


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
