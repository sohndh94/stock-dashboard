export type SectionKey = "kospi" | "bio" | "samsung_bio";

export interface EvidenceItem {
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

export interface ReportSection {
  section: SectionKey;
  title_ko: string;
  title_en: string;
  analysis_ko: string;
  analysis_en: string;
  chart_key: SectionKey;
  as_of_date: string;
  analysis_steps_ko?: string[];
  analysis_steps_en?: string[];
  evidence_count?: number;
  confidence_score?: number;
  evidences?: EvidenceItem[];
}

export interface ReportSectionV2 extends ReportSection {
  analysis_steps_ko: string[];
  analysis_steps_en: string[];
  evidence_count: number;
  confidence_score: number;
  evidences: EvidenceItem[];
}

export interface ReportLatestResponse {
  report_date: string;
  cutoff_kst: string;
  status: "complete" | "partial";
  as_of_date: string;
  generated_at: string | null;
  sections: ReportSection[];
}

export interface ReportLatestV2Response {
  report_date: string;
  cutoff_kst: string;
  status: "complete" | "partial";
  as_of_date: string;
  generated_at: string | null;
  sections: ReportSectionV2[];
}

export interface ReportEvidenceResponse {
  report_date: string;
  section: SectionKey;
  evidences: EvidenceItem[];
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

export interface UniverseItem {
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

export type CompareSymbolOption = UniverseItem;

export interface CompareChartResponse {
  from: string;
  to: string;
  mode: "BASE100";
  selected_symbols: string[];
  series: ChartSeries[];
}

export interface CompanySnapshotResponse {
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

export interface CompanyDetailChartResponse {
  symbol: string;
  from: string;
  to: string;
  mode: "PRICE" | "BASE100";
  series: ChartSeries[];
}

export interface DataLagResponse {
  latest_trade_date: string | null;
  lag_days: number | null;
  status: "ok" | "warn" | "unknown";
}
