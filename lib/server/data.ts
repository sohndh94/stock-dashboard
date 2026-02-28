import { DEFAULT_CHART_FROM, SECTION_ORDER } from "@/lib/constants";
import { createServerSupabaseClient } from "@/lib/supabase/server";
import {
  ChartPoint,
  ChartResponse,
  ChartSeries,
  DataLagResponse,
  ReportLatestResponse,
  ReportSection,
  SectionKey
} from "@/lib/types";

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

interface ChartRow {
  section_key: SectionKey;
  symbol: string;
  label: string;
  currency: string;
  trade_date: string;
  value: number;
  price_date_latest: string;
}

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
