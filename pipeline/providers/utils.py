from __future__ import annotations

import html
import re
import uuid
from datetime import date, datetime
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")


def make_issue_id(
    source_name: str,
    section_key: str,
    url: str,
    published_at_kst: datetime,
) -> str:
    seed = f"{source_name}|{section_key}|{url}|{published_at_kst.isoformat()}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


def strip_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", "", value)
    return html.unescape(text).strip()


def kst_datetime_on_day(
    query_date: date,
    hour: int,
    minute: int,
    second: int = 0,
) -> datetime:
    return datetime(
        query_date.year,
        query_date.month,
        query_date.day,
        hour,
        minute,
        second,
        tzinfo=KST,
    )
