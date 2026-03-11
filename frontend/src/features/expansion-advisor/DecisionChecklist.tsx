import { useTranslation } from "react-i18next";
import type { ExpansionCandidate, CandidateMemoResponse } from "../../lib/api/expansionAdvisor";
import { deriveDecisionChecklist, type ChecklistCategory, type ChecklistItem } from "./studyAdapters";

const CATEGORY_ORDER: ChecklistCategory[] = [
  "market_demand",
  "site_fit",
  "cannibalization",
  "delivery_market",
  "economics",
  "unknowns",
];

const STATUS_ICON: Record<ChecklistItem["status"], string> = {
  strong: "\u2713",
  caution: "\u25B3",
  risk: "\u2717",
  verify: "?",
};

const STATUS_CLASS: Record<ChecklistItem["status"], string> = {
  strong: "ea-checklist-item--strong",
  caution: "ea-checklist-item--caution",
  risk: "ea-checklist-item--risk",
  verify: "ea-checklist-item--verify",
};

type Props = {
  candidate: ExpansionCandidate;
  memo?: CandidateMemoResponse | null;
};

export default function DecisionChecklist({ candidate, memo }: Props) {
  const { t } = useTranslation();
  const items = deriveDecisionChecklist(candidate, memo);

  if (items.length === 0) return null;

  const grouped = new Map<ChecklistCategory, ChecklistItem[]>();
  for (const item of items) {
    const list = grouped.get(item.category) || [];
    list.push(item);
    grouped.set(item.category, list);
  }

  const categoryLabel: Record<ChecklistCategory, string> = {
    market_demand: t("expansionAdvisor.checklistMarketDemand"),
    site_fit: t("expansionAdvisor.checklistSiteFit"),
    cannibalization: t("expansionAdvisor.checklistCannibalization"),
    delivery_market: t("expansionAdvisor.checklistDeliveryMarket"),
    economics: t("expansionAdvisor.checklistEconomics"),
    unknowns: t("expansionAdvisor.checklistUnknowns"),
  };

  return (
    <div className="ea-decision-checklist">
      <h4 className="ea-decision-checklist__title">{t("expansionAdvisor.validationChecklist")}</h4>
      {CATEGORY_ORDER.map((cat) => {
        const catItems = grouped.get(cat);
        if (!catItems || catItems.length === 0) return null;
        return (
          <div key={cat} className="ea-decision-checklist__group">
            <h5 className="ea-decision-checklist__group-title">{categoryLabel[cat]}</h5>
            <div className="ea-decision-checklist__items">
              {catItems.map((item, i) => (
                <div key={i} className={`ea-checklist-item ${STATUS_CLASS[item.status]}`}>
                  <span className="ea-checklist-item__icon">{STATUS_ICON[item.status]}</span>
                  <span className="ea-checklist-item__label">{item.label}</span>
                </div>
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}
