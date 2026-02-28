import PriceChart from "@/app/components/PriceChart";
import { ChartResponse, ReportSection } from "@/lib/types";

interface SectionCardProps {
  section: ReportSection;
  chart: ChartResponse;
}

export default function SectionCard({ section, chart }: SectionCardProps) {
  return (
    <article className="section-card">
      <h2>
        {section.title_ko} / {section.title_en}
      </h2>
      <div className="section-body">
        <p>{section.analysis_ko}</p>
        <p>{section.analysis_en}</p>
      </div>
      <PriceChart data={chart} />
      <div className="meta-row">
        <span className="badge">Source: pykrx</span>
        <span className="badge">Source: yfinance</span>
      </div>
      <p className="source-note">
        as-of: {section.as_of_date} | scale: 2025-07-01 = 100 (BASE100)
      </p>
    </article>
  );
}
