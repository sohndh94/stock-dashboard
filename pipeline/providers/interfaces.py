from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

from pipeline.models import DailyFlowRecord, DailyMacroRecord, DailyPriceRecord


class PriceProvider(ABC):
    @abstractmethod
    def fetch_daily_prices(
        self, symbols: list[str], start_date: date, end_date: date
    ) -> list[DailyPriceRecord]:
        raise NotImplementedError


class FlowProvider(ABC):
    @abstractmethod
    def fetch_daily_flows(
        self, start_date: date, end_date: date, symbols: list[str] | None = None
    ) -> list[DailyFlowRecord]:
        raise NotImplementedError


class MacroProvider(ABC):
    @abstractmethod
    def fetch_daily_macro(
        self, metrics: list[dict[str, str]], start_date: date, end_date: date
    ) -> list[DailyMacroRecord]:
        raise NotImplementedError


class IssueProvider(ABC):
    @abstractmethod
    def fetch_daily_issues(
        self, query_date: date, symbols: list[str]
    ) -> list[dict[str, str]]:
        raise NotImplementedError
