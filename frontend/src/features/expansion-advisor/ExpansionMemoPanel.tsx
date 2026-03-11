import { useTranslation } from "react-i18next";

export default function ExpansionMemoPanel({ memo, loading }: { memo: Record<string, unknown> | null; loading: boolean }) {
  const { t } = useTranslation();
  if (loading) return <div>{t("expansionAdvisor.loadingMemo")}</div>;
  if (!memo) return null;
  const recommendation = (memo.recommendation || {}) as Record<string, unknown>;
  const marketResearch = (memo.market_research || {}) as Record<string, unknown>;
  const candidate = (memo.candidate || {}) as Record<string, unknown>;
  return (
    <div>
      <h4>{t("expansionAdvisor.decisionMemo")}</h4>
      <div><strong>{String(recommendation.verdict || "")}</strong> - {String(recommendation.headline || "")}</div>
      <div>Brand fit: {String(candidate.brand_fit_score ?? "-")}</div>
      <div>Strengths: {JSON.stringify(candidate.key_strengths || [])}</div>
      <div>Risks: {JSON.stringify(candidate.key_risks || [])}</div>
      <h5>Market research</h5>
      <div>{String(marketResearch.delivery_market_summary || "")}</div>
      <div>{String(marketResearch.competitive_context || "")}</div>
      <div>{String(marketResearch.district_fit_summary || "")}</div>
    </div>
  );
}
