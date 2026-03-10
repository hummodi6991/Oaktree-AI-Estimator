import { useTranslation } from "react-i18next";

export default function ExpansionMemoPanel({ memo, loading }: { memo: Record<string, unknown> | null; loading: boolean }) {
  const { t } = useTranslation();
  if (loading) return <div>{t("expansionAdvisor.loadingMemo")}</div>;
  if (!memo) return null;
  const recommendation = (memo.recommendation || {}) as Record<string, unknown>;
  return <div><h4>{t("expansionAdvisor.decisionMemo")}</h4><div>{String(recommendation.headline || "")}</div></div>;
}
