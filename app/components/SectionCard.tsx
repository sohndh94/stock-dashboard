import PriceChart from "@/app/components/PriceChart";
import { ChartResponse, ReportSection } from "@/lib/types";

interface SectionCardProps {
  section: ReportSection;
  chart: ChartResponse;
}

export default function SectionCard({ section, chart }: SectionCardProps) {
  const hasEvidence = (section.evidence_count ?? 0) >= 2;
  const stepsKo = section.analysis_steps_ko ?? [];
  const stepsEn = section.analysis_steps_en ?? [];
  const evidences = (section.evidences ?? []).slice(0, 2);

  return (
    <article className="section-card">
      <h3>
        {section.title_ko} / {section.title_en}
      </h3>

      <div className="section-body">
        {hasEvidence ? (
          <>
            <p>{section.analysis_ko}</p>
            <p>{section.analysis_en}</p>
          </>
        ) : (
          <p className="evidence-warning">
            근거 링크가 충분하지 않아 해석 문장을 보류했습니다.
          </p>
        )}
      </div>

      {(stepsKo.length > 0 || stepsEn.length > 0) && (
        <div className="steps-grid">
          {stepsKo.length > 0 && (
            <div>
              <p className="step-title">KO Steps</p>
              {stepsKo.map((step) => (
                <p key={step} className="step-line">
                  {step}
                </p>
              ))}
            </div>
          )}
          {stepsEn.length > 0 && (
            <div>
              <p className="step-title">EN Steps</p>
              {stepsEn.map((step) => (
                <p key={step} className="step-line">
                  {step}
                </p>
              ))}
            </div>
          )}
        </div>
      )}

      {evidences.length > 0 && (
        <div className="evidence-panel">
          {evidences.map((evidence) => (
            <p
              key={`${section.section}-${evidence.rank}-${evidence.url ?? evidence.title ?? "na"}`}
              className="step-line"
            >
              [{evidence.rank}] {evidence.evidence_type.toUpperCase()} ·{" "}
              {evidence.url ? (
                <a href={evidence.url} target="_blank" rel="noreferrer">
                  {evidence.title ?? evidence.source_name ?? "source"}
                </a>
              ) : (
                evidence.title ?? evidence.source_name ?? "source"
              )}
            </p>
          ))}
        </div>
      )}

      <PriceChart series={chart.series} height={300} monochrome />

      <p className="source-note">
        as-of {section.as_of_date} | base 100 from 2025-07-01
      </p>
    </article>
  );
}
