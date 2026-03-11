import { useTranslation } from "react-i18next";
import type { RecommendationReportResponse } from "../../lib/api/expansionAdvisor";

export default function ExpansionReportPanel({ report, loading }: { report: RecommendationReportResponse | null; loading: boolean }) {
  const { t } = useTranslation();
  if (loading) return <div>{t("expansionAdvisor.loadingReport")}</div>;
  if (!report) return null;
  const rec = report.recommendation || {};
  const top = report.top_candidates || [];
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
          <div key={item.id} style={{ border: "1px solid #d8e1dd", borderRadius: 6, padding: 8 }}>
            <strong>{item.id}</strong> #{item.rank_position ?? "-"} — {item.final_score ?? "-"}
            <div>{t("expansionAdvisor.confidenceGrade")}: {item.confidence_grade || "-"}</div>
            <div>{t("expansionAdvisor.gateVerdict")}: {item.gate_verdict || (item.gate_status_json?.overall_pass ? t("expansionAdvisor.pass") : t("expansionAdvisor.fail"))}</div>
            <div>{t("expansionAdvisor.topPositives")}: {(item.top_positives_json || []).slice(0, 2).join(" • ") || "-"}</div>
            <div>{t("expansionAdvisor.topRisks")}: {(item.top_risks_json || []).slice(0, 2).join(" • ") || "-"}</div>
            <div>snapshot: {String(item.feature_snapshot_json?.data_completeness_score ?? "-")}</div>
            <div>breakdown: {String(item.score_breakdown_json?.final_score ?? "-")}</div>
          </div>
        ))}
      </div>
      <div>{t("expansionAdvisor.assumptions")}: {(report.assumptions || []).join("; ") || "-"}</div>
    </div>
  );
}
