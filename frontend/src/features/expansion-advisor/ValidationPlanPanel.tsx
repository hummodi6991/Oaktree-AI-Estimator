import { useTranslation } from "react-i18next";
import type { ExpansionCandidate, CandidateMemoResponse, RecommendationReportResponse } from "../../lib/api/expansionAdvisor";
import { deriveValidationPlan, type ValidationPriority, type ValidationPlanItem } from "./studyAdapters";

type Props = {
  candidate: ExpansionCandidate;
  memo?: CandidateMemoResponse | null;
  report?: RecommendationReportResponse | null;
};

export default function ValidationPlanPanel({ candidate, memo, report }: Props) {
  const { t } = useTranslation();
  const items = deriveValidationPlan(candidate, memo, report);

  if (items.length === 0) return null;

  const mustVerify = items.filter((i) => i.priority === "must_verify" || i.priority === "nice_to_confirm");
  const confirmed = items.filter((i) => i.priority === "already_strong");

  return (
    <div className="ea-validation-plan">
      <h4 className="ea-validation-plan__title">{t("expansionAdvisor.validationPlan")}</h4>
      <div className="ea-memo-validation-grid">
        {/* Left: Must verify */}
        <div className="ea-memo-validation-col">
          <h5 className="ea-memo-validation-col__title ea-memo-validation-col__title--must">
            {t("expansionAdvisor.vpMustVerify")}
          </h5>
          <div className="ea-validation-plan__items">
            {mustVerify.map((item, i) => (
              <div key={i} className="ea-memo-validation-item ea-memo-validation-item--must">
                <span className={`ea-memo-validation-dot ea-memo-validation-dot--${item.priority === "must_verify" ? "must" : "nice"}`} />
                <div className="ea-vplan-item__content">
                  <span className="ea-vplan-item__label">{item.label}</span>
                  <span className="ea-vplan-item__detail">{item.detail}</span>
                </div>
              </div>
            ))}
            {mustVerify.length === 0 && <span className="ea-detail__text">—</span>}
          </div>
        </div>

        {/* Right: Already confirmed */}
        <div className="ea-memo-validation-col">
          <h5 className="ea-memo-validation-col__title ea-memo-validation-col__title--confirmed">
            {t("expansionAdvisor.vpAlreadyStrong")}
          </h5>
          <div className="ea-validation-plan__items">
            {confirmed.map((item, i) => (
              <div key={i} className="ea-memo-validation-item ea-memo-validation-item--confirmed">
                <span className="ea-memo-validation-check">&#10003;</span>
                <div className="ea-vplan-item__content">
                  <span className="ea-vplan-item__label">{item.label}</span>
                  <span className="ea-vplan-item__detail">{item.detail}</span>
                </div>
              </div>
            ))}
            {confirmed.length === 0 && <span className="ea-detail__text">—</span>}
          </div>
        </div>
      </div>
    </div>
  );
}
