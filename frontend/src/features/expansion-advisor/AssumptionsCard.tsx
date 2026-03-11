import { useTranslation } from "react-i18next";
import type { ExpansionCandidate, RecommendationReportResponse } from "../../lib/api/expansionAdvisor";
import { deriveAssumptions, type AssumptionConfidence, type AssumptionItem } from "./studyAdapters";

const CONFIDENCE_ORDER: AssumptionConfidence[] = ["strong", "estimated", "missing"];

const CONFIDENCE_ICON: Record<AssumptionConfidence, string> = {
  strong: "\u2713",
  estimated: "\u2248",
  missing: "?",
};

const CONFIDENCE_CLASS: Record<AssumptionConfidence, string> = {
  strong: "ea-assumption--strong",
  estimated: "ea-assumption--estimated",
  missing: "ea-assumption--missing",
};

type Props = {
  candidate: ExpansionCandidate;
  report?: RecommendationReportResponse | null;
  compact?: boolean;
};

export default function AssumptionsCard({ candidate, report, compact }: Props) {
  const { t } = useTranslation();
  const items = deriveAssumptions(candidate, report);

  if (items.length === 0) return null;

  const grouped = new Map<AssumptionConfidence, AssumptionItem[]>();
  for (const item of items) {
    const list = grouped.get(item.confidence) || [];
    list.push(item);
    grouped.set(item.confidence, list);
  }

  const confidenceLabel: Record<AssumptionConfidence, string> = {
    strong: t("expansionAdvisor.acStrong"),
    estimated: t("expansionAdvisor.acEstimated"),
    missing: t("expansionAdvisor.acMissing"),
  };

  if (compact) {
    const strongCount = (grouped.get("strong") || []).length;
    const estimatedCount = (grouped.get("estimated") || []).length;
    const missingCount = (grouped.get("missing") || []).length;

    return (
      <div className="ea-assumptions-strip">
        <span className="ea-assumptions-strip__title">{t("expansionAdvisor.assumptionsConfidence")}</span>
        <div className="ea-assumptions-strip__badges">
          {strongCount > 0 && (
            <span className="ea-badge ea-badge--green">{strongCount} {t("expansionAdvisor.acStrongShort")}</span>
          )}
          {estimatedCount > 0 && (
            <span className="ea-badge ea-badge--amber">{estimatedCount} {t("expansionAdvisor.acEstimatedShort")}</span>
          )}
          {missingCount > 0 && (
            <span className="ea-badge ea-badge--red">{missingCount} {t("expansionAdvisor.acMissingShort")}</span>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="ea-assumptions-card">
      <h4 className="ea-assumptions-card__title">{t("expansionAdvisor.assumptionsConfidence")}</h4>
      {CONFIDENCE_ORDER.map((confidence) => {
        const group = grouped.get(confidence);
        if (!group || group.length === 0) return null;
        return (
          <div key={confidence} className="ea-assumptions-card__group">
            <h5 className="ea-assumptions-card__group-title">
              <span className={`ea-assumption-icon ${CONFIDENCE_CLASS[confidence]}`}>{CONFIDENCE_ICON[confidence]}</span>
              {confidenceLabel[confidence]}
            </h5>
            <div className="ea-assumptions-card__items">
              {group.map((item, i) => (
                <div key={i} className={`ea-assumption-row ${CONFIDENCE_CLASS[confidence]}`}>
                  <span className="ea-assumption-row__label">{item.label}</span>
                  <span className="ea-assumption-row__detail">{item.detail}</span>
                </div>
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}
