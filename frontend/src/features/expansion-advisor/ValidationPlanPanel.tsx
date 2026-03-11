import { useTranslation } from "react-i18next";
import type { ExpansionCandidate, CandidateMemoResponse, RecommendationReportResponse } from "../../lib/api/expansionAdvisor";
import { deriveValidationPlan, type ValidationPriority, type ValidationPlanItem } from "./studyAdapters";

const PRIORITY_ORDER: ValidationPriority[] = ["must_verify", "nice_to_confirm", "already_strong"];

const PRIORITY_ICON: Record<ValidationPriority, string> = {
  must_verify: "\u25CF",
  nice_to_confirm: "\u25CB",
  already_strong: "\u2713",
};

const PRIORITY_CLASS: Record<ValidationPriority, string> = {
  must_verify: "ea-vplan-item--must",
  nice_to_confirm: "ea-vplan-item--nice",
  already_strong: "ea-vplan-item--strong",
};

type Props = {
  candidate: ExpansionCandidate;
  memo?: CandidateMemoResponse | null;
  report?: RecommendationReportResponse | null;
};

export default function ValidationPlanPanel({ candidate, memo, report }: Props) {
  const { t } = useTranslation();
  const items = deriveValidationPlan(candidate, memo, report);

  if (items.length === 0) return null;

  const grouped = new Map<ValidationPriority, ValidationPlanItem[]>();
  for (const item of items) {
    const list = grouped.get(item.priority) || [];
    list.push(item);
    grouped.set(item.priority, list);
  }

  const priorityLabel: Record<ValidationPriority, string> = {
    must_verify: t("expansionAdvisor.vpMustVerify"),
    nice_to_confirm: t("expansionAdvisor.vpNiceToConfirm"),
    already_strong: t("expansionAdvisor.vpAlreadyStrong"),
  };

  return (
    <div className="ea-validation-plan">
      <h4 className="ea-validation-plan__title">{t("expansionAdvisor.validationPlan")}</h4>
      {PRIORITY_ORDER.map((priority) => {
        const group = grouped.get(priority);
        if (!group || group.length === 0) return null;
        return (
          <div key={priority} className="ea-validation-plan__group">
            <h5 className="ea-validation-plan__group-title">{priorityLabel[priority]}</h5>
            <div className="ea-validation-plan__items">
              {group.map((item, i) => (
                <div key={i} className={`ea-vplan-item ${PRIORITY_CLASS[priority]}`}>
                  <span className="ea-vplan-item__icon">{PRIORITY_ICON[priority]}</span>
                  <div className="ea-vplan-item__content">
                    <span className="ea-vplan-item__label">{item.label}</span>
                    <span className="ea-vplan-item__detail">{item.detail}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}
