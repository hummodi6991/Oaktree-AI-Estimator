import { useTranslation } from "react-i18next";

export default function ExpansionReportPanel({ report }: { report: Record<string, any> | null }) {
  const { t } = useTranslation();
  if (!report) return null;
  const rec = report.recommendation || {};
  const top = report.top_candidates || [];
  return (
    <div>
      <h4>{t("expansionAdvisor.recommendationReport")}</h4>
      <div>{t("expansionAdvisor.bestCandidate")}: {rec.best_candidate_id || "-"}</div>
      <div>{t("expansionAdvisor.runnerUp")}: {rec.runner_up_candidate_id || "-"}</div>
      <div>{t("expansionAdvisor.bestPassCandidate")}: {rec.best_pass_candidate_id || "-"}</div>
      <div>{t("expansionAdvisor.bestConfidenceCandidate")}: {rec.best_confidence_candidate_id || "-"}</div>
      <div>{t("expansionAdvisor.whyBest")}: {rec.why_best || "-"}</div>
      <div>{t("expansionAdvisor.mainRisk")}: {rec.main_risk || "-"}</div>
      <div>{t("expansionAdvisor.bestFormat")}: {rec.best_format || "-"}</div>
      <div>{t("expansionAdvisor.summary")}: {rec.report_summary || "-"}</div>
      <ul>{top.slice(0, 3).map((item: any) => <li key={item.id}>{item.id} — {item.final_score} — {t("expansionAdvisor.confidenceGrade")} {item.confidence_grade || "-"} — {t("expansionAdvisor.gateVerdict")} {item.gate_status_json?.overall_pass ? t("expansionAdvisor.pass") : t("expansionAdvisor.fail")}</li>)}</ul>
    </div>
  );
}
