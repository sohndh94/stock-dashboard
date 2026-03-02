"use client";

import { useEffect, useMemo, useState } from "react";

import PriceChart from "@/app/components/PriceChart";
import {
  CompareChartResponse,
  CompanyDetailChartResponse,
  CompanySnapshotResponse,
  UniverseItem
} from "@/lib/types";

const BASE_START_DATE = "2025-07-01";

type DetailRange = "3M" | "1Y";
type DetailMode = "PRICE" | "BASE100";

interface CompareDashboardProps {
  initialData: CompareChartResponse;
  universe: UniverseItem[];
  initialActiveSymbol: string;
  initialSnapshot: CompanySnapshotResponse | null;
  initialDetailChart: CompanyDetailChartResponse;
}

function toIsoDateUtc(date: Date): string {
  const yyyy = date.getUTCFullYear();
  const mm = String(date.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(date.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function getTodayKstIso(): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Seoul",
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).formatToParts(new Date());

  const year = parts.find((part) => part.type === "year")?.value;
  const month = parts.find((part) => part.type === "month")?.value;
  const day = parts.find((part) => part.type === "day")?.value;

  if (!year || !month || !day) {
    return toIsoDateUtc(new Date());
  }

  return `${year}-${month}-${day}`;
}

function shiftMonths(baseIsoDate: string, deltaMonths: number): string {
  const [year, month, day] = baseIsoDate.split("-").map(Number);
  const base = new Date(Date.UTC(year, month - 1, day));
  base.setUTCMonth(base.getUTCMonth() + deltaMonths);
  return toIsoDateUtc(base);
}

function compactNumber(value: number | null): string {
  if (value === null || Number.isNaN(value)) {
    return "N/A";
  }

  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 2
  }).format(value);
}

function pctText(value: number | null): string {
  if (value === null || Number.isNaN(value)) {
    return "N/A";
  }

  const sign = value >= 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

export default function CompareDashboard({
  initialData,
  universe,
  initialActiveSymbol,
  initialSnapshot,
  initialDetailChart
}: CompareDashboardProps) {
  const [query, setQuery] = useState("");
  const [selectedSymbols, setSelectedSymbols] = useState<string[]>(
    initialData.selected_symbols
  );
  const [from, setFrom] = useState(initialData.from);
  const [to, setTo] = useState(initialData.to);

  const [activeSymbol, setActiveSymbol] = useState(initialActiveSymbol);
  const [snapshot, setSnapshot] = useState<CompanySnapshotResponse | null>(
    initialSnapshot
  );
  const [detailChart, setDetailChart] =
    useState<CompanyDetailChartResponse>(initialDetailChart);

  const [detailRange, setDetailRange] = useState<DetailRange>("1Y");
  const [detailMode, setDetailMode] = useState<DetailMode>("PRICE");

  const [chartData, setChartData] = useState<CompareChartResponse>(initialData);
  const [isCompareLoading, setIsCompareLoading] = useState(false);
  const [isDetailLoading, setIsDetailLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const optionBySymbol = useMemo(
    () => new Map(universe.map((option) => [option.symbol, option])),
    [universe]
  );

  const filteredUniverse = useMemo(() => {
    const keyword = query.trim().toLowerCase();
    if (!keyword) {
      return universe;
    }
    return universe.filter((item) => {
      return (
        item.symbol.toLowerCase().includes(keyword) ||
        item.name.toLowerCase().includes(keyword) ||
        (item.name_ko ?? "").toLowerCase().includes(keyword)
      );
    });
  }, [query, universe]);

  useEffect(() => {
    let canceled = false;

    async function loadCompareChart() {
      if (selectedSymbols.length === 0) {
        return;
      }

      setIsCompareLoading(true);
      setError(null);

      const params = new URLSearchParams({
        symbols: selectedSymbols.join(","),
        from,
        to
      });

      try {
        const response = await fetch(`/api/v1/charts/compare?${params.toString()}`, {
          cache: "no-store"
        });

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const payload = (await response.json()) as CompareChartResponse;
        if (!canceled) {
          setChartData(payload);
        }
      } catch (loadError) {
        if (!canceled) {
          setError(
            loadError instanceof Error
              ? `비교 차트 로딩 실패: ${loadError.message}`
              : "비교 차트 로딩 실패"
          );
        }
      } finally {
        if (!canceled) {
          setIsCompareLoading(false);
        }
      }
    }

    loadCompareChart();

    return () => {
      canceled = true;
    };
  }, [selectedSymbols, from, to]);

  useEffect(() => {
    let canceled = false;

    async function loadDetail() {
      setIsDetailLoading(true);

      try {
        const today = getTodayKstIso();
        const rangeFrom =
          detailRange === "3M"
            ? shiftMonths(today, -3)
            : shiftMonths(today, -12);

        const [snapshotRes, chartRes] = await Promise.all([
          fetch(
            `/api/v1/company/${encodeURIComponent(activeSymbol)}/snapshot?date=${today}`,
            { cache: "no-store" }
          ),
          fetch(
            `/api/v1/company/${encodeURIComponent(
              activeSymbol
            )}/chart?from=${rangeFrom}&to=${today}&mode=${detailMode}`,
            { cache: "no-store" }
          )
        ]);

        if (!snapshotRes.ok) {
          throw new Error(`Snapshot HTTP ${snapshotRes.status}`);
        }

        if (!chartRes.ok) {
          throw new Error(`Chart HTTP ${chartRes.status}`);
        }

        const snapshotPayload =
          (await snapshotRes.json()) as CompanySnapshotResponse;
        const chartPayload = (await chartRes.json()) as CompanyDetailChartResponse;

        if (!canceled) {
          setSnapshot(snapshotPayload);
          setDetailChart(chartPayload);
        }
      } catch (loadError) {
        if (!canceled) {
          setError(
            loadError instanceof Error
              ? `회사 상세 로딩 실패: ${loadError.message}`
              : "회사 상세 로딩 실패"
          );
        }
      } finally {
        if (!canceled) {
          setIsDetailLoading(false);
        }
      }
    }

    loadDetail();

    return () => {
      canceled = true;
    };
  }, [activeSymbol, detailRange, detailMode]);

  function toggleSymbol(symbol: string) {
    const exists = selectedSymbols.includes(symbol);

    if (exists && selectedSymbols.length === 1) {
      return;
    }

    const next = exists
      ? selectedSymbols.filter((item) => item !== symbol)
      : [...selectedSymbols, symbol];

    setSelectedSymbols(next);
  }

  const selectionSummary = selectedSymbols
    .map((symbol) => optionBySymbol.get(symbol)?.name ?? symbol)
    .join(", ");

  return (
    <article className="compare-shell">
      <aside className="company-list-panel">
        <div className="panel-head">
          <h2>Companies</h2>
          <p>{universe.length} tracked</p>
        </div>

        <input
          className="search-input"
          type="text"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search by name or ticker"
        />

        <div className="company-list">
          {filteredUniverse.map((item) => {
            const checked = selectedSymbols.includes(item.symbol);
            const active = activeSymbol === item.symbol;

            return (
              <button
                key={item.symbol}
                type="button"
                className={`company-row ${active ? "active" : ""}`}
                onClick={() => setActiveSymbol(item.symbol)}
              >
                <label className="check-wrap" onClick={(event) => event.stopPropagation()}>
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={() => toggleSymbol(item.symbol)}
                  />
                </label>
                <div className="company-texts">
                  <span className="company-name">{item.name}</span>
                  <span className="company-sub">{item.symbol} · {item.market}</span>
                </div>
              </button>
            );
          })}
        </div>
      </aside>

      <section className="compare-main">
        <div className="detail-panel">
          <div className="panel-head">
            <h2>Company Detail</h2>
            <div className="inline-controls">
              <button
                type="button"
                className={detailRange === "3M" ? "toggle active" : "toggle"}
                onClick={() => setDetailRange("3M")}
              >
                3M
              </button>
              <button
                type="button"
                className={detailRange === "1Y" ? "toggle active" : "toggle"}
                onClick={() => setDetailRange("1Y")}
              >
                1Y
              </button>
              <button
                type="button"
                className={detailMode === "PRICE" ? "toggle active" : "toggle"}
                onClick={() => setDetailMode("PRICE")}
              >
                PRICE
              </button>
              <button
                type="button"
                className={detailMode === "BASE100" ? "toggle active" : "toggle"}
                onClick={() => setDetailMode("BASE100")}
              >
                BASE100
              </button>
            </div>
          </div>

          <div className="metric-grid">
            <div className="metric-item">
              <span className="metric-label">Company</span>
              <span className="metric-value">{snapshot?.name ?? "N/A"}</span>
              <span className="metric-sub">{snapshot?.symbol ?? activeSymbol}</span>
            </div>
            <div className="metric-item">
              <span className="metric-label">Last Price</span>
              <span className="metric-value">
                {snapshot?.close !== null && snapshot?.close !== undefined
                  ? snapshot.close.toLocaleString()
                  : "N/A"}
              </span>
              <span className="metric-sub">{snapshot?.currency ?? "N/A"}</span>
            </div>
            <div className="metric-item">
              <span className="metric-label">Day Change</span>
              <span className="metric-value">{pctText(snapshot?.day_change_pct ?? null)}</span>
              <span className="metric-sub">vs prev close</span>
            </div>
            <div className="metric-item">
              <span className="metric-label">Market Cap</span>
              <span className="metric-value">{compactNumber(snapshot?.market_cap ?? null)}</span>
              <span className="metric-sub">{snapshot?.currency ?? "N/A"}</span>
            </div>
          </div>

          {isDetailLoading ? <p className="loading-text">Loading company detail...</p> : null}
          <PriceChart series={detailChart.series} height={260} monochrome />
        </div>

        <div className="compare-panel">
          <div className="panel-head">
            <h2>Compare Trend</h2>
            <p>{chartData.from} ~ {chartData.to}</p>
          </div>

          <div className="date-row">
            <input
              type="date"
              value={from}
              min={BASE_START_DATE}
              max={to}
              onChange={(event) => setFrom(event.target.value)}
            />
            <input
              type="date"
              value={to}
              min={from}
              onChange={(event) => setTo(event.target.value)}
            />
          </div>

          <div className="compare-meta-row">
            <span className="badge">Selected: {selectedSymbols.length}</span>
            <span className="badge">Symbols: {selectionSummary}</span>
            {isCompareLoading ? <span className="badge">Loading...</span> : null}
          </div>

          {error ? <p className="compare-error">{error}</p> : null}

          {chartData.series.length > 0 ? (
            <PriceChart series={chartData.series} height={320} monochrome />
          ) : (
            <p className="compare-error">
              선택한 조건에서 표시할 데이터가 없습니다. 종목 또는 날짜 범위를 조정해 주세요.
            </p>
          )}
        </div>
      </section>
    </article>
  );
}
