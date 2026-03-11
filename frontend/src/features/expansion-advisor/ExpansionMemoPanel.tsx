import { useTranslation } from "react-i18next";
import type { CandidateMemoResponse } from "../../lib/api/expansionAdvisor";

function toList(input: unknown): string[] {
  return Array.isArray(input) ? input.map(String) : [];
}

export default function ExpansionMemoPanel({ memo, loading }: { memo: CandidateMemoResponse | null; loading: boolean }) {
  const { t } = useTranslation();
  if (loading) return <div>{t("expansionAdvisor.loadingMemo")}</div>;
  if (!memo) return null;

  const recommendation = (memo.recommendation || {}) as Record<string, unknown>;
  const marketResearch = (memo.market_research || {}) as Record<string, unknown>;
  const candidate = (memo.candidate || {}) as Record<string, unknown>;
  const gates = (candidate.gate_status || {}) as Record<string, boolean>;
  const gateReasons = (candidate.gate_reasons || {}) as Record<string, unknown>;
  const featureSnapshot = (candidate.feature_snapshot || {}) as Record<string, unknown>;
  const scoreBreakdown = (candidate.score_breakdown_json || {}) as Record<string, unknown>;
  const comps = (candidate.comparable_competitors || []) as Array<Record<string, unknown>>;
  const positives = toList(candidate.top_positives_json);
  const risks = toList(candidate.top_risks_json);

  return (
    <div>
      <h4>{t("expansionAdvisor.decisionMemo")}</h4>
      <div><strong>{String(recommendation.headline || "-")}</strong> — {String(recommendation.verdict || "-")}</div>
      <div>{t("expansionAdvisor.gateVerdict")}: {String(recommendation.gate_verdict || "-")}</div>
      <div>{t("expansionAdvisor.bestUseCase")}: {String(recommendation.best_use_case || "-")}</div>
      <div>{t("expansionAdvisor.mainWatchout")}: {String(recommendation.main_watchout || "-")}</div>

      <h5>{t("expansionAdvisor.candidateSummary")}</h5>
      <div>{t("expansionAdvisor.finalScore")}: {String(candidate.final_score ?? "-")}</div>
      <div>rank_position: {String(candidate.rank_position ?? "-")}</div>
      <div>{t("expansionAdvisor.confidenceGrade")}: {String(candidate.confidence_grade ?? "-")}</div>
      <div>{t("expansionAdvisor.economicsScore")}: {String(candidate.economics_score ?? "-")}</div>
      <div>{t("expansionAdvisor.brandFit")} : {String(candidate.brand_fit_score ?? "-")}</div>
      <div>payback_band: {String(candidate.payback_band ?? "-")}</div>
      <div>estimated_payback_months: {String(candidate.estimated_payback_months ?? "-")}</div>

      <h5>{t("expansionAdvisor.gateChecklist")}</h5>
      <ul>{Object.entries(gates).map(([k, v]) => <li key={k}>{k}: {v ? t("expansionAdvisor.pass") : t("expansionAdvisor.fail")}</li>)}</ul>

      <h5>{t("expansionAdvisor.gateReasons")}</h5>
      <div>{t("expansionAdvisor.passed")}: {toList(gateReasons.passed).join(", ") || "-"}</div>
      <div>{t("expansionAdvisor.failed")}: {toList(gateReasons.failed).join(", ") || "-"}</div>
      <div>{t("expansionAdvisor.unknown")}: {toList(gateReasons.unknown).join(", ") || "-"}</div>

      <div><strong>{t("expansionAdvisor.topPositives")}</strong><ul>{positives.length ? positives.map((s) => <li key={s}>{s}</li>) : <li>-</li>}</ul></div>
      <div><strong>{t("expansionAdvisor.topRisks")}</strong><ul>{risks.length ? risks.map((s) => <li key={s}>{s}</li>) : <li>-</li>}</ul></div>
      <div>{t("expansionAdvisor.demandThesis")}: {String(candidate.demand_thesis ?? "-")}</div>
      <div>{t("expansionAdvisor.costThesis")}: {String(candidate.cost_thesis ?? "-")}</div>

      <h5>{t("expansionAdvisor.comparableCompetitors")}</h5>
      <ul>{comps.length ? comps.map((c, i) => <li key={`${String(c.id || i)}`}>{String(c.name || "-")} - {String(c.distance_m || "-")}m</li>) : <li>-</li>}</ul>

      <h5>{t("expansionAdvisor.featureSnapshot")}</h5>
      <div>data_completeness_score: {String(featureSnapshot.data_completeness_score ?? "-")}</div>
      <div>missing_context: {toList(featureSnapshot.missing_context).join(", ") || "-"}</div>

      <h5>{t("expansionAdvisor.scoreBreakdown")}</h5>
      <div>final_score: {String(scoreBreakdown.final_score ?? "-")}</div>

      <h5>{t("expansionAdvisor.marketResearch")}</h5>
      <div>{String(marketResearch.delivery_market_summary || "")}</div>
      <div>{String(marketResearch.competitive_context || "")}</div>
      <div>{String(marketResearch.district_fit_summary || "")}</div>
    </div>
  );
}
