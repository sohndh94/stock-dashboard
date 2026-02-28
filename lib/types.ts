export type SectionKey = "kospi" | "bio" | "samsung_bio";

export interface ReportSection {
  section: SectionKey;
  title_ko: string;
  title_en: string;
  analysis_ko: string;
  analysis_en: string;
  chart_key: SectionKey;
  as_of_date: string;
}

export interface ReportLatestResponse {
  report_date: string;
  cutoff_kst: string;
  status: "complete" | "partial";
  as_of_date: string;
  generated_at: string | null;
  sections: ReportSection[];
}

export interface ChartPoint {
  date: string;
  value: number;
}

export interface ChartSeries {
  symbol: string;
  label: string;
  currency: string;
  price_date_latest: string;
  points: ChartPoint[];
}

export interface ChartResponse {
  section: SectionKey;
  from: string;
  to: string;
  mode: "BASE100";
  series: ChartSeries[];
}

export interface DataLagResponse {
  latest_trade_date: string | null;
  lag_days: number | null;
  status: "ok" | "warn" | "unknown";
}
