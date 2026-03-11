import { useTranslation } from "react-i18next";

export default function ExpansionMemoPanel({ memo, loading }: { memo: Record<string, unknown> | null; loading: boolean }) {
  const { t } = useTranslation();
  if (loading) return <div>{t("expansionAdvisor.loadingMemo")}</div>;
  if (!memo) return null;
  const recommendation = (memo.recommendation || {}) as Record<string, unknown>;
  const marketResearch = (memo.market_research || {}) as Record<string, unknown>;
  const candidate = (memo.candidate || {}) as Record<string, unknown>;
  const gates = (candidate.gate_status || {}) as Record<string, boolean>;
  const gateReasons = (candidate.gate_reasons || {}) as Record<string, unknown>;
  const featureSnapshot = (candidate.feature_snapshot || {}) as Record<string, unknown>;
  const comps = (candidate.comparable_competitors || []) as Array<Record<string, unknown>>;
  const strengths = (candidate.key_strengths || []) as string[];
  const risks = (candidate.key_risks || []) as string[];
  return (
    <div>
      <h4>{t("expansionAdvisor.decisionMemo")}</h4>
      <div><strong>{String(recommendation.verdict || "")}</strong> - {String(recommendation.headline || "")}</div>
      <div>{t("expansionAdvisor.brandFit")}: {String(candidate.brand_fit_score ?? "-")}</div>
      <div>{t("expansionAdvisor.confidenceGrade")}: {String(candidate.confidence_grade ?? "-")}</div>
      <div><strong>{t("expansionAdvisor.gateChecklist")}</strong><ul>{Object.entries(gates).map(([k, v]) => <li key={k}>{k}: {v ? t("expansionAdvisor.pass") : t("expansionAdvisor.fail")}</li>)}</ul></div>
      <div>
        <strong>{t("expansionAdvisor.gateReasons")}</strong>
        <div>{t("expansionAdvisor.passed")}: {JSON.stringify(gateReasons.passed || [])}</div>
        <div>{t("expansionAdvisor.failed")}: {JSON.stringify(gateReasons.failed || [])}</div>
      </div>
      <div>
        <strong>{t("expansionAdvisor.featureSnapshot")}</strong>
        <ul>{Object.entries(featureSnapshot).slice(0, 8).map(([k, v]) => <li key={k}>{k}: {String(v)}</li>)}</ul>
      </div>
      <div><strong>{t("expansionAdvisor.strengths")}</strong><ul>{strengths.map((s) => <li key={s}>{s}</li>)}</ul></div>
      <div><strong>{t("expansionAdvisor.risks")}</strong><ul>{risks.map((s) => <li key={s}>{s}</li>)}</ul></div>
      <div>{t("expansionAdvisor.demandThesis")}: {String(candidate.demand_thesis ?? "-")}</div>
      <div>{t("expansionAdvisor.costThesis")}: {String(candidate.cost_thesis ?? "-")}</div>
      <h5>{t("expansionAdvisor.marketResearch")}</h5>
      <div>{String(marketResearch.delivery_market_summary || "")}</div>
      <div>{String(marketResearch.competitive_context || "")}</div>
      <div>{String(marketResearch.district_fit_summary || "")}</div>
      <h5>{t("expansionAdvisor.comparableCompetitors")}</h5>
      <ul>{comps.map((c, i) => <li key={`${String(c.id || i)}`}>{String(c.name || "-")} - {String(c.distance_m || "-")}m</li>)}</ul>
    </div>
  );
}
