from __future__ import annotations

from datetime import date

from pipeline.config import MACRO_METRICS, load_config
from pipeline.providers.ecos_provider import EcosProvider
from pipeline.providers.fred_provider import FredProvider
from pipeline.providers.pykrx_provider import PykrxProvider
from pipeline.providers.yfinance_provider import YFinanceProvider
from pipeline.repository import SupabaseRepository
from pipeline.universe_registry import (
    build_group_member_records,
    build_group_records,
    build_instrument_records,
)


def _ensure_universe_seeded(repository: SupabaseRepository) -> None:
    instruments = repository.get_active_instruments()
    if instruments:
        return

    instrument_records = build_instrument_records()
    group_records = build_group_records()
    group_members = build_group_member_records()

    repository.upsert_instruments(instrument_records)
    repository.upsert_instrument_groups(group_records)
    repository.replace_group_members(group_members)


def _to_targets(rows: list[dict]) -> list[dict[str, str]]:
    targets: list[dict[str, str]] = []
    for row in rows:
        targets.append(
            {
                "symbol": row["symbol"],
                "provider_symbol": row.get("provider_symbol") or row["symbol"],
                "asset_type": row["asset_type"],
            }
        )
    return targets


def collect_ingest_window(
    repository: SupabaseRepository,
    start_date: date,
    end_date: date,
) -> dict:
    config = load_config()
    _ensure_universe_seeded(repository)

    pykrx_rows = repository.get_active_instruments(provider="pykrx")
    yf_rows = repository.get_active_instruments(provider="yfinance")

    pykrx_targets = _to_targets(pykrx_rows)
    yf_targets = _to_targets(yf_rows)

    pykrx = PykrxProvider()
    yfinance = YFinanceProvider()

    price_records = []
    krx_prices = pykrx.fetch_daily_prices(pykrx_targets, start_date, end_date)
    price_records.extend(krx_prices)
    price_records.extend(yfinance.fetch_daily_prices(yf_targets, start_date, end_date))

    has_kospi = any(record.symbol == "KOSPI" for record in krx_prices)
    if not has_kospi:
        kospi_proxy = yfinance.fetch_daily_prices(
            [{"symbol": "KOSPI", "provider_symbol": "^KS11", "asset_type": "index"}],
            start_date,
            end_date,
        )
        for record in kospi_proxy:
            price_records.append(record)

    flow_symbols = repository.get_group_symbols("flow_watchlist")
    flow_records = pykrx.fetch_daily_flows(
        start_date,
        end_date,
        symbols=flow_symbols or ["207940.KS", "068270.KS"],
    )

    ecos = EcosProvider(config.ecos_api_key)
    fred = FredProvider(config.fred_api_key)
    macro_records: list = []
    macro_runs: dict[str, int] = {}

    try:
        yf_macro = yfinance.fetch_daily_macro(MACRO_METRICS, start_date, end_date)
        macro_records.extend(yf_macro)
        macro_runs["yfinance"] = len(yf_macro)
        repository.insert_source_fetch_run(
            source_name="yfinance_macro",
            status="success",
            metrics={"rows": len(yf_macro)},
        )
    except Exception as exc:
        macro_runs["yfinance"] = 0
        repository.insert_source_fetch_run(
            source_name="yfinance_macro",
            status="failed",
            error_message=str(exc),
            metrics={},
        )

    if config.fred_api_key:
        try:
            fred_macro = fred.fetch_daily_macro(MACRO_METRICS, start_date, end_date)
            macro_records.extend(fred_macro)
            macro_runs["fred"] = len(fred_macro)
            repository.insert_source_fetch_run(
                source_name="fred_macro",
                status="success",
                metrics={"rows": len(fred_macro)},
            )
        except Exception as exc:
            macro_runs["fred"] = 0
            repository.insert_source_fetch_run(
                source_name="fred_macro",
                status="failed",
                error_message=str(exc),
                metrics={},
            )
    else:
        macro_runs["fred"] = 0
        repository.insert_source_fetch_run(
            source_name="fred_macro",
            status="skipped",
            metrics={"reason": "missing_api_key"},
        )

    if config.ecos_api_key:
        try:
            ecos_macro = ecos.fetch_daily_macro(MACRO_METRICS, start_date, end_date)
            macro_records.extend(ecos_macro)
            macro_runs["ecos"] = len(ecos_macro)
            repository.insert_source_fetch_run(
                source_name="ecos_macro",
                status="success",
                metrics={"rows": len(ecos_macro)},
            )
        except Exception as exc:
            macro_runs["ecos"] = 0
            repository.insert_source_fetch_run(
                source_name="ecos_macro",
                status="failed",
                error_message=str(exc),
                metrics={},
            )
    else:
        macro_runs["ecos"] = 0
        repository.insert_source_fetch_run(
            source_name="ecos_macro",
            status="skipped",
            metrics={"reason": "missing_api_key"},
        )

    stock_metric_targets = [
        {"symbol": row["symbol"], "provider_symbol": row["symbol"]}
        for row in repository.get_active_instruments(asset_type="stock")
    ]
    company_metrics = yfinance.fetch_company_metrics(stock_metric_targets, end_date)
    repository.upsert_daily_company_metrics(company_metrics)

    macro_records = _select_preferred_macro_source(macro_records)

    repository.upsert_daily_prices(price_records)
    repository.upsert_daily_flows(flow_records)
    repository.upsert_daily_macro(macro_records)

    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "price_rows": len(price_records),
        "flow_rows": len(flow_records),
        "macro_rows": len(macro_records),
        "company_metric_rows": len(company_metrics),
        "macro_source_rows": macro_runs,
    }


def _select_preferred_macro_source(records: list) -> list:
    source_priority = {"yfinance": 1, "fred": 2, "ecos": 3}
    selected = {}
    for row in records:
        key = (row.metric_code, row.trade_date)
        current = selected.get(key)
        if not current:
            selected[key] = row
            continue

        current_pri = source_priority.get(current.source, 0)
        next_pri = source_priority.get(row.source, 0)
        if next_pri >= current_pri:
            selected[key] = row

    return list(selected.values())
