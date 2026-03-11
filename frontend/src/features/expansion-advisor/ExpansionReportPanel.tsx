import { useTranslation } from "react-i18next";
import type { RecommendationReportResponse } from "../../lib/api/expansionAdvisor";

function assumptionRows(assumptions: Record<string, unknown> | undefined) {
  return Object.entries(assumptions || {});
}

export default function ExpansionReportPanel({ report, loading, onSelectCandidateId }: { report: RecommendationReportResponse | null; loading: boolean; onSelectCandidateId?: (candidateId: string) => void }) {
  const { t } = useTranslation();
  if (loading) return <div>{t("expansionAdvisor.loadingReport")}</div>;
  if (!report) return null;
  const rec = report.recommendation || {};
  const top = report.top_candidates || [];
  const assumptions = assumptionRows(report.assumptions);
  return (
    <div>
      <h4>{t("expansionAdvisor.recommendationReport")}</h4>
      <div>version: {report.meta?.version || "-"}</div>
      <div>{t("expansionAdvisor.bestCandidate")}: {String(rec.best_candidate_id || "-")}</div>
      <div>{t("expansionAdvisor.runnerUp")}: {String(rec.runner_up_candidate_id || "-")}</div>
      <div>{t("expansionAdvisor.bestPassCandidate")}: {String(rec.best_pass_candidate_id || "-")}</div>
      <div>{t("expansionAdvisor.bestConfidenceCandidate")}: {String(rec.best_confidence_candidate_id || "-")}</div>
      <div>{t("expansionAdvisor.whyBest")}: {String(rec.why_best || "-")}</div>
      <div>{t("expansionAdvisor.mainRisk")}: {String(rec.main_risk || "-")}</div>
      <div>{t("expansionAdvisor.bestFormat")}: {String(rec.best_format || "-")}</div>
      <div>{t("expansionAdvisor.summary")}: {String(rec.summary || rec.report_summary || "-")}</div>
      <div style={{ display: "grid", gap: 6 }}>
        {top.map((item) => (
          <button
            key={item.id}
            type="button"
            disabled={!item.id}
            onClick={() => {
              if (item.id) onSelectCandidateId?.(item.id);
            }}
            style={{ border: "1px solid #d8e1dd", borderRadius: 6, padding: 8, textAlign: "left", background: "#fff" }}
          >
            <strong>{item.id}</strong> #{item.rank_position ?? "-"} — {item.final_score ?? "-"}
            <div>{t("expansionAdvisor.confidenceGrade")}: {item.confidence_grade || "-"}</div>
            <div>{t("expansionAdvisor.gateVerdict")}: {item.gate_verdict || t("expansionAdvisor.fail")}</div>
            <div>{t("expansionAdvisor.topPositives")}: {(item.top_positives_json || []).slice(0, 2).join(" • ") || "-"}</div>
            <div>{t("expansionAdvisor.topRisks")}: {(item.top_risks_json || []).slice(0, 2).join(" • ") || "-"}</div>
            <div>snapshot: {String(item.feature_snapshot_json?.data_completeness_score ?? "-")}</div>
            <div>breakdown: {String(item.score_breakdown_json?.final_score ?? "-")}</div>
          </button>
        ))}
      </div>
      <div>
        {t("expansionAdvisor.assumptions")}: {assumptions.length ? null : "-"}
        {assumptions.length ? (
          <ul>
            {assumptions.map(([key, value]) => (
              <li key={key}>
                {key}: {String(value)}
              </li>
            ))}
          </ul>
        ) : null}
      </div>
    </div>
  );
}
