from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from pipeline.models import InstrumentGroupMemberRecord, InstrumentGroupRecord, InstrumentRecord

_REGISTRY_PATH = Path(__file__).resolve().parents[1] / "config" / "universe.json"


def registry_path() -> Path:
    return _REGISTRY_PATH


@lru_cache(maxsize=1)
def load_universe_registry() -> dict[str, Any]:
    with registry_path().open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if "instruments" not in payload or not isinstance(payload["instruments"], list):
        raise RuntimeError("Invalid registry: instruments list is required")
    if "groups" not in payload or not isinstance(payload["groups"], list):
        raise RuntimeError("Invalid registry: groups list is required")

    return payload


def clear_registry_cache() -> None:
    load_universe_registry.cache_clear()


def registry_instruments() -> list[dict[str, Any]]:
    payload = load_universe_registry()
    return list(payload.get("instruments", []))


def registry_groups() -> list[dict[str, Any]]:
    payload = load_universe_registry()
    return list(payload.get("groups", []))


def build_instrument_records() -> list[InstrumentRecord]:
    records: list[InstrumentRecord] = []
    for instrument in registry_instruments():
        records.append(
            InstrumentRecord(
                symbol=instrument["symbol"],
                name=instrument["name"],
                name_ko=instrument.get("name_ko"),
                category=instrument.get("category"),
                asset_type=instrument["asset_type"],
                market=instrument["market"],
                currency=instrument["currency"],
                provider=instrument.get("provider", "yfinance"),
                provider_symbol=instrument.get("provider_symbol", instrument["symbol"]),
                display_order=int(instrument.get("display_order", 1000)),
                is_compare_default=bool(instrument.get("is_compare_default", False)),
                is_active=bool(instrument.get("is_active", True)),
            )
        )
    return records


def build_group_records() -> list[InstrumentGroupRecord]:
    records: list[InstrumentGroupRecord] = []
    for group in registry_groups():
        records.append(
            InstrumentGroupRecord(
                group_key=group["group_key"],
                name=group["name"],
                purpose=group.get("purpose"),
                is_active=bool(group.get("is_active", True)),
            )
        )
    return records


def build_group_member_records() -> list[InstrumentGroupMemberRecord]:
    records: list[InstrumentGroupMemberRecord] = []
    for instrument in registry_instruments():
        symbol = instrument["symbol"]
        for group_key in instrument.get("groups", []):
            records.append(
                InstrumentGroupMemberRecord(
                    group_key=group_key,
                    symbol=symbol,
                    weight=1.0,
                    role="member",
                )
            )
    return records


def symbols_by_provider(provider: str, *, active_only: bool = True) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for instrument in registry_instruments():
        if instrument.get("provider") != provider:
            continue
        if active_only and not instrument.get("is_active", True):
            continue
        items.append(
            {
                "symbol": instrument["symbol"],
                "provider_symbol": instrument.get("provider_symbol", instrument["symbol"]),
                "asset_type": instrument["asset_type"],
            }
        )
    return items


def symbols_by_group(group_key: str, *, active_only: bool = True) -> list[str]:
    symbols: list[str] = []
    for instrument in registry_instruments():
        if group_key not in instrument.get("groups", []):
            continue
        if active_only and not instrument.get("is_active", True):
            continue
        symbols.append(instrument["symbol"])
    return symbols
