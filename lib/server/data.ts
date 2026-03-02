import {
  DEFAULT_CHART_FROM,
  DEFAULT_COMPARE_SYMBOLS,
  SECTION_ORDER
} from "@/lib/constants";
import { createServerSupabaseClient } from "@/lib/supabase/server";
import {
  CompareChartResponse,
  ChartPoint,
  ChartResponse,
  ChartSeries,
  CompanyDetailChartResponse,
  CompanySnapshotResponse,
  DataLagResponse,
  EvidenceItem,
  ReportEvidenceResponse,
  ReportLatestResponse,
  ReportLatestV2Response,
  ReportSection,
  ReportSectionV2,
  SectionKey,
  UniverseItem
} from "@/lib/types";
import {
  getRegistryCompareInstruments,
  getRegistryDefaultCompareSymbols
} from "@/lib/universe-registry";

interface LatestReportRow {
  report_date: string;
  cutoff_kst: string;
  status: "complete" | "partial";
  generated_at: string | null;
  section: SectionKey;
  title_ko: string;
  title_en: string;
  analysis_ko: string;
  analysis_en: string;
  chart_key: SectionKey;
  as_of_date: string;
}

interface LatestReportV2Row {
  report_date: string;
  cutoff_kst: string;
  status: "complete" | "partial";
  generated_at: string | null;
  section: SectionKey;
  title_ko: string;
  title_en: string;
  analysis_ko: string;
  analysis_en: string;
  analysis_steps_ko: unknown;
  analysis_steps_en: unknown;
  chart_key: SectionKey;
  as_of_date: string;
  evidence_count: number;
  confidence_score: number;
  evidences: unknown;
}

interface ChartRow {
  section_key: SectionKey;
  symbol: string;
  label: string;
  currency: string;
  trade_date: string;
  value: number;
  price_date_latest: string;
}

interface CompareChartRow {
  symbol: string;
  label: string;
  currency: string;
  trade_date: string;
  value: number;
  price_date_latest: string;
}

interface UniverseRow {
  symbol: string;
  name: string;
  name_ko: string | null;
  category: string | null;
  asset_type: string;
  market: string;
  currency: string;
  provider: string;
  provider_symbol: string;
  display_order: number;
  is_compare_default: boolean;
  is_active: boolean;
}

interface EvidenceRow {
  report_date: string;
  section_key: SectionKey;
  rank: number;
  evidence_type: "flow" | "macro" | "news" | "disclosure";
  weight: number;
  reason: string;
  issue_id: string | null;
  source_name: string | null;
  source_tier: number | null;
  symbol: string | null;
  title: string | null;
  summary: string | null;
  url: string | null;
  published_at_kst: string | null;
  language: string | null;
  topic_tags: string[] | null;
  sentiment: number | null;
  relevance_score: number | null;
}

interface CompanySnapshotViewRow {
  symbol: string;
  name: string;
  name_ko: string | null;
  market: string;
  currency: string;
  trade_date: string | null;
  price_date_actual: string | null;
  close: number | null;
  prev_close: number | null;
  day_change_pct: number | null;
  market_cap: number | null;
  shares_outstanding: number | null;
  metric_trade_date: string | null;
}

const ALLOWED_COMPARE_SYMBOLS = new Set(
  getRegistryCompareInstruments().map((item) => item.symbol)
);

function clampIsoDate(value: string | null, fallback: string): string {
  if (!value) {
    return fallback;
  }

  const isIsoDate = /^\d{4}-\d{2}-\d{2}$/.test(value);
  return isIsoDate ? value : fallback;
}

function getTodayIso(): string {
  const now = new Date();
  const yyyy = now.getUTCFullYear();
  const mm = String(now.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(now.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => (typeof item === "string" ? item : String(item ?? "")))
    .filter(Boolean);
}

function asEvidenceItems(value: unknown): EvidenceItem[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value.map((item) => {
    const row = (item ?? {}) as Partial<EvidenceItem>;
    return {
      rank: Number(row.rank ?? 0),
      evidence_type: (row.evidence_type ?? "news") as EvidenceItem["evidence_type"],
      weight: Number(row.weight ?? 0),
      reason: String(row.reason ?? ""),
      issue_id: row.issue_id ?? null,
      source_name: row.source_name ?? null,
      source_tier:
        row.source_tier !== undefined && row.source_tier !== null
          ? Number(row.source_tier)
          : null,
      symbol: row.symbol ?? null,
      title: row.title ?? null,
      summary: row.summary ?? null,
      url: row.url ?? null,
      published_at_kst: row.published_at_kst ?? null,
      language: row.language ?? null,
      topic_tags: Array.isArray(row.topic_tags)
        ? row.topic_tags.map((tag) => String(tag))
        : null,
      sentiment:
        row.sentiment !== undefined && row.sentiment !== null
          ? Number(row.sentiment)
          : null,
      relevance_score:
        row.relevance_score !== undefined && row.relevance_score !== null
          ? Number(row.relevance_score)
          : null
    };
  });
}

export async function getLatestReport(): Promise<ReportLatestResponse | null> {
  const supabase = createServerSupabaseClient();

  const { data, error } = await supabase
    .from("v_latest_report")
    .select("*")
    .order("report_date", { ascending: false })
    .order("section", { ascending: true });

  if (error) {
    throw new Error(`Failed to load latest report: ${error.message}`);
  }

  if (!data || data.length === 0) {
    return null;
  }

  const rows = data as LatestReportRow[];
  const reportDate = rows[0].report_date;
  const scopedRows = rows.filter((row) => row.report_date === reportDate);

  const sectionsByKey = new Map<SectionKey, ReportSection>();
  for (const row of scopedRows) {
    sectionsByKey.set(row.section, {
      section: row.section,
      title_ko: row.title_ko,
      title_en: row.title_en,
      analysis_ko: row.analysis_ko,
      analysis_en: row.analysis_en,
      chart_key: row.chart_key,
      as_of_date: row.as_of_date
    });
  }

  const sections = SECTION_ORDER.map((key) => sectionsByKey.get(key)).filter(
    (item): item is ReportSection => Boolean(item)
  );

  const asOfDates = sections.map((section) => section.as_of_date).sort();
  const asOfDate = asOfDates.length > 0 ? asOfDates[asOfDates.length - 1] : reportDate;

  return {
    report_date: reportDate,
    cutoff_kst: String(rows[0].cutoff_kst).slice(0, 5),
    status: rows[0].status,
    as_of_date: asOfDate,
    generated_at: rows[0].generated_at,
    sections
  };
}

export async function getLatestReportV2(): Promise<ReportLatestV2Response | null> {
  const supabase = createServerSupabaseClient();

  const { data, error } = await supabase
    .from("v_latest_report_v2")
    .select("*")
    .order("report_date", { ascending: false })
    .order("section", { ascending: true });

  if (error) {
    const fallback = await getLatestReport();
    if (!fallback) {
      return null;
    }

    return {
      ...fallback,
      sections: fallback.sections.map((section) => ({
        ...section,
        analysis_steps_ko: [],
        analysis_steps_en: [],
        evidence_count: 0,
        confidence_score: 0,
        evidences: []
      }))
    };
  }

  if (!data || data.length === 0) {
    return null;
  }

  const rows = data as LatestReportV2Row[];
  const reportDate = rows[0].report_date;
  const scopedRows = rows.filter((row) => row.report_date === reportDate);

  const sectionsByKey = new Map<SectionKey, ReportSectionV2>();
  for (const row of scopedRows) {
    sectionsByKey.set(row.section, {
      section: row.section,
      title_ko: row.title_ko,
      title_en: row.title_en,
      analysis_ko: row.analysis_ko,
      analysis_en: row.analysis_en,
      analysis_steps_ko: asStringArray(row.analysis_steps_ko),
      analysis_steps_en: asStringArray(row.analysis_steps_en),
      chart_key: row.chart_key,
      as_of_date: row.as_of_date,
      evidence_count: Number(row.evidence_count ?? 0),
      confidence_score: Number(row.confidence_score ?? 0),
      evidences: asEvidenceItems(row.evidences)
    });
  }

  const sections = SECTION_ORDER.map((key) => sectionsByKey.get(key)).filter(
    (item): item is ReportSectionV2 => Boolean(item)
  );

  const asOfDates = sections.map((section) => section.as_of_date).sort();
  const asOfDate = asOfDates.length > 0 ? asOfDates[asOfDates.length - 1] : reportDate;

  return {
    report_date: reportDate,
    cutoff_kst: String(rows[0].cutoff_kst).slice(0, 5),
    status: rows[0].status,
    as_of_date: asOfDate,
    generated_at: rows[0].generated_at,
    sections
  };
}

export async function getReportEvidence(
  reportDate: string,
  section: SectionKey
): Promise<ReportEvidenceResponse> {
  const supabase = createServerSupabaseClient();
  const date = clampIsoDate(reportDate, getTodayIso());

  const { data, error } = await supabase
    .from("v_report_evidence_v2")
    .select("*")
    .eq("report_date", date)
    .eq("section_key", section)
    .order("rank", { ascending: true });

  if (error) {
    throw new Error(`Failed to load evidences: ${error.message}`);
  }

  const rows = (data ?? []) as EvidenceRow[];
  const evidences: EvidenceItem[] = rows.map((row) => ({
    rank: Number(row.rank ?? 0),
    evidence_type: row.evidence_type,
    weight: Number(row.weight ?? 0),
    reason: String(row.reason ?? ""),
    issue_id: row.issue_id,
    source_name: row.source_name,
    source_tier: row.source_tier,
    symbol: row.symbol,
    title: row.title,
    summary: row.summary,
    url: row.url,
    published_at_kst: row.published_at_kst,
    language: row.language,
    topic_tags: row.topic_tags,
    sentiment: row.sentiment,
    relevance_score: row.relevance_score
  }));

  return {
    report_date: date,
    section,
    evidences
  };
}

export async function getChartBySection(
  section: SectionKey,
  from?: string,
  to?: string
): Promise<ChartResponse> {
  const supabase = createServerSupabaseClient();

  const fromDate = clampIsoDate(from ?? null, DEFAULT_CHART_FROM);
  const toDate = clampIsoDate(to ?? null, getTodayIso());

  const { data, error } = await supabase
    .from("v_chart_series_base100")
    .select("section_key,symbol,label,currency,trade_date,value,price_date_latest")
    .eq("section_key", section)
    .gte("trade_date", fromDate)
    .lte("trade_date", toDate)
    .order("trade_date", { ascending: true });

  if (error) {
    throw new Error(`Failed to load chart data for ${section}: ${error.message}`);
  }

  const rows = (data ?? []) as ChartRow[];
  const grouped = new Map<string, ChartSeries>();

  for (const row of rows) {
    const existing = grouped.get(row.symbol);
    const point: ChartPoint = {
      date: row.trade_date,
      value: Number(row.value)
    };

    if (!existing) {
      grouped.set(row.symbol, {
        symbol: row.symbol,
        label: row.label,
        currency: row.currency,
        price_date_latest: row.price_date_latest,
        points: [point]
      });
      continue;
    }

    existing.points.push(point);
    existing.price_date_latest = row.price_date_latest;
  }

  return {
    section,
    from: fromDate,
    to: toDate,
    mode: "BASE100",
    series: Array.from(grouped.values())
  };
}

export async function getCompareUniverse(): Promise<UniverseItem[]> {
  const supabase = createServerSupabaseClient();
  const { data, error } = await supabase
    .from("v_compare_universe")
    .select(
      "symbol,name,name_ko,category,asset_type,market,currency,provider,provider_symbol,display_order,is_compare_default,is_active"
    )
    .order("display_order", { ascending: true });

  if (error) {
    return getRegistryCompareInstruments().map((item) => ({
      symbol: item.symbol,
      name: item.name,
      name_ko: item.name_ko ?? null,
      category: item.category ?? null,
      asset_type: item.asset_type,
      market: item.market,
      currency: item.currency,
      provider: item.provider,
      provider_symbol: item.provider_symbol,
      display_order: item.display_order,
      is_compare_default: item.is_compare_default,
      is_active: item.is_active
    }));
  }

  return (data ?? []) as UniverseItem[];
}

async function getDbDefaultCompareSymbols(): Promise<string[]> {
  const supabase = createServerSupabaseClient();
  const { data, error } = await supabase
    .from("instruments")
    .select("symbol")
    .eq("is_active", true)
    .eq("is_compare_default", true)
    .order("display_order", { ascending: true });

  if (error || !data || data.length === 0) {
    return getRegistryDefaultCompareSymbols().length > 0
      ? getRegistryDefaultCompareSymbols()
      : DEFAULT_COMPARE_SYMBOLS;
  }

  return data.map((row) => String(row.symbol));
}

function normalizeRequestedSymbols(symbols: string[] | undefined): string[] {
  const unique = Array.from(new Set((symbols ?? []).filter(Boolean)));
  const filtered = unique.filter((symbol) => ALLOWED_COMPARE_SYMBOLS.has(symbol));
  return filtered;
}

export async function getCompareChart(
  symbols?: string[],
  from?: string,
  to?: string
): Promise<CompareChartResponse> {
  const supabase = createServerSupabaseClient();

  const fromDate = clampIsoDate(from ?? null, DEFAULT_CHART_FROM);
  const toDate = clampIsoDate(to ?? null, getTodayIso());

  const requested = normalizeRequestedSymbols(symbols);
  const selectedSymbols =
    requested.length > 0 ? requested : await getDbDefaultCompareSymbols();

  const universe = await getCompareUniverse();
  const metaBySymbol = new Map(universe.map((item) => [item.symbol, item]));

  const { data, error } = await supabase
    .from("v_compare_series_base100")
    .select("symbol,label,currency,trade_date,value,price_date_latest")
    .in("symbol", selectedSymbols)
    .gte("trade_date", fromDate)
    .lte("trade_date", toDate)
    .order("trade_date", { ascending: true });

  if (error) {
    throw new Error(`Failed to load compare chart data: ${error.message}`);
  }

  const rows = (data ?? []) as CompareChartRow[];
  const seriesBySymbol = new Map<string, ChartSeries>();

  for (const symbol of selectedSymbols) {
    const meta = metaBySymbol.get(symbol);
    seriesBySymbol.set(symbol, {
      symbol,
      label: meta?.name ?? symbol,
      currency: meta?.currency ?? "N/A",
      price_date_latest: "",
      points: []
    });
  }

  for (const row of rows) {
    const series = seriesBySymbol.get(row.symbol);
    if (!series) {
      continue;
    }

    series.points.push({
      date: row.trade_date,
      value: Number(row.value)
    });
    series.price_date_latest = row.price_date_latest;
  }

  return {
    from: fromDate,
    to: toDate,
    mode: "BASE100",
    selected_symbols: selectedSymbols,
    series: selectedSymbols
      .map((symbol) => seriesBySymbol.get(symbol))
      .filter((item): item is ChartSeries => Boolean(item && item.points.length > 0))
  };
}

export async function getCompanySnapshot(
  symbol: string,
  date?: string
): Promise<CompanySnapshotResponse | null> {
  const supabase = createServerSupabaseClient();

  const { data: instrumentData, error: instrumentError } = await supabase
    .from("instruments")
    .select("instrument_id,symbol,name,name_ko,market,currency")
    .eq("symbol", symbol)
    .limit(1)
    .single();

  if (instrumentError || !instrumentData) {
    return null;
  }

  const instrumentId = instrumentData.instrument_id as string;
  const asOfDate = clampIsoDate(date ?? null, getTodayIso());

  const { data: priceRows, error: priceError } = await supabase
    .from("daily_prices")
    .select("trade_date,price_date_actual,close")
    .eq("instrument_id", instrumentId)
    .lte("trade_date", asOfDate)
    .order("trade_date", { ascending: false })
    .limit(2);

  if (priceError) {
    throw new Error(`Failed to load price snapshot: ${priceError.message}`);
  }

  const latest = priceRows?.[0] ?? null;
  const prev = priceRows?.[1] ?? null;

  const { data: metricRows, error: metricError } = await supabase
    .from("daily_company_metrics")
    .select("trade_date,market_cap,shares_outstanding")
    .eq("instrument_id", instrumentId)
    .lte("trade_date", asOfDate)
    .order("trade_date", { ascending: false })
    .limit(1);

  if (metricError) {
    throw new Error(`Failed to load metric snapshot: ${metricError.message}`);
  }

  const metric = metricRows?.[0] ?? null;

  const close = latest?.close !== undefined && latest?.close !== null ? Number(latest.close) : null;
  const prevClose = prev?.close !== undefined && prev?.close !== null ? Number(prev.close) : null;

  const dayChangePct =
    close !== null && prevClose !== null && prevClose !== 0
      ? ((close - prevClose) / prevClose) * 100
      : null;

  return {
    symbol: instrumentData.symbol,
    name: instrumentData.name,
    name_ko: instrumentData.name_ko,
    market: instrumentData.market,
    currency: instrumentData.currency,
    trade_date: latest?.trade_date ?? null,
    price_date_actual: latest?.price_date_actual ?? null,
    close,
    prev_close: prevClose,
    day_change_pct: dayChangePct,
    market_cap:
      metric?.market_cap !== undefined && metric?.market_cap !== null
        ? Number(metric.market_cap)
        : null,
    shares_outstanding:
      metric?.shares_outstanding !== undefined && metric?.shares_outstanding !== null
        ? Number(metric.shares_outstanding)
        : null,
    metric_trade_date: metric?.trade_date ?? null
  };
}

export async function getCompanyDetailChart(
  symbol: string,
  from?: string,
  to?: string,
  mode: "PRICE" | "BASE100" = "PRICE"
): Promise<CompanyDetailChartResponse> {
  const supabase = createServerSupabaseClient();
  const fromDate = clampIsoDate(from ?? null, DEFAULT_CHART_FROM);
  const toDate = clampIsoDate(to ?? null, getTodayIso());

  const { data: instrumentData, error: instrumentError } = await supabase
    .from("instruments")
    .select("instrument_id,name,currency")
    .eq("symbol", symbol)
    .limit(1)
    .single();

  if (instrumentError || !instrumentData) {
    throw new Error(`Unknown symbol: ${symbol}`);
  }

  const { data, error } = await supabase
    .from("daily_prices")
    .select("trade_date,close")
    .eq("instrument_id", instrumentData.instrument_id)
    .gte("trade_date", fromDate)
    .lte("trade_date", toDate)
    .order("trade_date", { ascending: true });

  if (error) {
    throw new Error(`Failed to load company chart: ${error.message}`);
  }

  const rows = data ?? [];
  const firstClose = rows.length > 0 && rows[0].close ? Number(rows[0].close) : null;

  const points: ChartPoint[] = rows.map((row) => {
    const close = Number(row.close);
    const value =
      mode === "BASE100" && firstClose && firstClose !== 0
        ? (close / firstClose) * 100
        : close;

    return {
      date: String(row.trade_date),
      value
    };
  });

  return {
    symbol,
    from: fromDate,
    to: toDate,
    mode,
    series: [
      {
        symbol,
        label: instrumentData.name,
        currency: instrumentData.currency,
        price_date_latest: rows.length > 0 ? String(rows[rows.length - 1].trade_date) : "",
        points
      }
    ]
  };
}

export async function getDataLag(): Promise<DataLagResponse> {
  const supabase = createServerSupabaseClient();

  const { data, error } = await supabase
    .from("daily_prices")
    .select("trade_date")
    .order("trade_date", { ascending: false })
    .limit(1);

  if (error) {
    throw new Error(`Failed to load data lag: ${error.message}`);
  }

  const latestTradeDate = data?.[0]?.trade_date as string | undefined;
  if (!latestTradeDate) {
    return {
      latest_trade_date: null,
      lag_days: null,
      status: "unknown"
    };
  }

  const latest = new Date(`${latestTradeDate}T00:00:00Z`);
  const nowKst = new Date(
    new Date().toLocaleString("en-US", {
      timeZone: "Asia/Seoul"
    })
  );
  const lagDays = Math.floor(
    (nowKst.getTime() - latest.getTime()) / (24 * 60 * 60 * 1000)
  );

  return {
    latest_trade_date: latestTradeDate,
    lag_days: lagDays,
    status: lagDays > 1 ? "warn" : "ok"
  };
}
