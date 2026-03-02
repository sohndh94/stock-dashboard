import CompareDashboard from "@/app/components/CompareDashboard";
import SectionCard from "@/app/components/SectionCard";
import { DEFAULT_CHART_FROM, DISCLAIMER_LINES, SECTION_ORDER } from "@/lib/constants";
import {
  getChartBySection,
  getCompanyDetailChart,
  getCompanySnapshot,
  getCompareChart,
  getCompareUniverse,
  getLatestReportV2
} from "@/lib/server/data";
import { SectionKey } from "@/lib/types";

export const revalidate = 300;
export const dynamic = "force-dynamic";

function formatStatusLabel(status: "complete" | "partial") {
  return status === "complete" ? "Complete" : "Partial";
}

function hasSupabaseRuntimeEnv() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL ?? process.env.SUPABASE_URL;
  const key =
    process.env.SUPABASE_SERVICE_ROLE_KEY ??
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

  return Boolean(url && key);
}

export default async function HomePage() {
  if (!hasSupabaseRuntimeEnv()) {
    return (
      <main className="page">
        <section className="hero">
          <h1>Daily Bio Market Dashboard</h1>
          <p>Supabase 환경변수가 없어 데이터를 불러오지 못했습니다.</p>
        </section>
      </main>
    );
  }

  const report = await getLatestReportV2();

  if (!report) {
    return (
      <main className="page">
        <section className="hero">
          <h1>Daily Bio Market Dashboard</h1>
          <p>리포트 데이터가 아직 없습니다. 백필/일배치 실행 후 다시 확인해 주세요.</p>
        </section>
      </main>
    );
  }

  const universe = await getCompareUniverse();
  const defaultSymbols = universe.filter((item) => item.is_compare_default).map((item) => item.symbol);
  const selectedSymbols = defaultSymbols.length > 0 ? defaultSymbols : universe.slice(0, 6).map((item) => item.symbol);
  const activeSymbol = selectedSymbols[0] ?? universe[0]?.symbol ?? "207940.KS";

  const [compareChart, charts, initialSnapshot, initialDetailChart] = await Promise.all([
    getCompareChart(selectedSymbols, DEFAULT_CHART_FROM, report.as_of_date),
    Promise.all(SECTION_ORDER.map((section) => getChartBySection(section as SectionKey))),
    getCompanySnapshot(activeSymbol, report.as_of_date),
    getCompanyDetailChart(activeSymbol, DEFAULT_CHART_FROM, report.as_of_date, "PRICE")
  ]);

  const chartBySection = new Map(charts.map((chart) => [chart.section, chart]));

  return (
    <main className="page">
      <section className="hero">
        <h1>Daily Bio Market Dashboard</h1>
        <div className="meta-row">
          <span className="badge">Report date: {report.report_date}</span>
          <span className="badge">As-of date: {report.as_of_date}</span>
          <span className="badge">Cutoff (KST): {report.cutoff_kst}</span>
          <span className="badge">
            Last updated: {report.generated_at ? new Date(report.generated_at).toLocaleString("ko-KR") : "N/A"}
          </span>
          <span className={`badge ${report.status}`}>
            Status: {formatStatusLabel(report.status)}
          </span>
        </div>
      </section>

      <section style={{ marginTop: 16 }}>
        <CompareDashboard
          initialData={compareChart}
          universe={universe}
          initialActiveSymbol={activeSymbol}
          initialSnapshot={initialSnapshot}
          initialDetailChart={initialDetailChart}
        />
      </section>

      <section className="section-grid" style={{ marginTop: 18 }}>
        {report.sections.map((section) => {
          const chart = chartBySection.get(section.section);
          if (!chart) {
            return null;
          }

          return <SectionCard key={section.section} section={section} chart={chart} />;
        })}
      </section>

      <section className="disclaimer">
        {DISCLAIMER_LINES.map((line) => (
          <p key={line} style={{ margin: "0 0 6px 0" }}>
            {line}
          </p>
        ))}
      </section>
    </main>
  );
}
