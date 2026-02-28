import SectionCard from "@/app/components/SectionCard";
import { DISCLAIMER_LINES, SECTION_ORDER } from "@/lib/constants";
import { getChartBySection, getLatestReport } from "@/lib/server/data";
import { SectionKey } from "@/lib/types";

export const revalidate = 300;

function formatStatusLabel(status: "complete" | "partial") {
  return status === "complete" ? "Complete" : "Partial";
}

export default async function HomePage() {
  const report = await getLatestReport();

  if (!report) {
    return (
      <main className="page">
        <section className="hero">
          <h1 style={{ fontFamily: "var(--font-heading)" }}>Daily Bio Market Dashboard</h1>
          <p>리포트 데이터가 아직 없습니다. 백필/일배치 실행 후 다시 확인해 주세요.</p>
        </section>
      </main>
    );
  }

  const chartPromises = SECTION_ORDER.map((section) =>
    getChartBySection(section as SectionKey)
  );
  const charts = await Promise.all(chartPromises);
  const chartBySection = new Map(charts.map((chart) => [chart.section, chart]));

  return (
    <main className="page">
      <section className="hero">
        <h1 style={{ fontFamily: "var(--font-heading)" }}>Daily Bio Market Dashboard</h1>
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

      <section className="section-grid" style={{ marginTop: 16 }}>
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
