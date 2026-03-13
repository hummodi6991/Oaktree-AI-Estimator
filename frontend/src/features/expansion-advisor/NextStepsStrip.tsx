import { useTranslation } from "react-i18next";
import type { ExpansionCandidate, RecommendationReportResponse } from "../../lib/api/expansionAdvisor";
import { findRunnerUp } from "./studyAdapters";
import { candidateDistrictLabel } from "./formatHelpers";

type Props = {
  candidates: ExpansionCandidate[];
  shortlistIds: string[];
  leadCandidateId: string | null;
  report: RecommendationReportResponse | null;
  onOpenMemo: (candidateId: string) => void;
  onOpenReport: () => void;
  onCompare: () => void;
};

export default function NextStepsStrip({
  candidates,
  shortlistIds,
  leadCandidateId,
  report,
  onOpenMemo,
  onOpenReport,
  onCompare,
}: Props) {
  const { t } = useTranslation();

  if (!leadCandidateId) return null;

  const lead = candidates.find((c) => c.id === leadCandidateId);
  if (!lead) return null;

  const leadPasses = lead.gate_status_json?.overall_pass === true;
  const runnerUp = findRunnerUp(candidates, shortlistIds, leadCandidateId);
  const unknowns = lead.gate_reasons_json?.unknown || [];
  const missing = lead.feature_snapshot_json?.missing_context || [];
  const nextValidation = unknowns[0]?.replace(/_/g, " ") || missing[0]?.replace(/_/g, " ") || t("expansionAdvisor.nextStepSiteVisit");

  return (
    <div className="ea-next-steps">
      <div className="ea-next-steps__items">
        <div className="ea-next-steps__item ea-next-steps__item--lead">
          <span className="ea-next-steps__label">{leadPasses ? t("expansionAdvisor.leadSiteSelected") : t("expansionAdvisor.exploratoryCandidateSelected")}</span>
          <span className="ea-next-steps__value">
            #{lead.rank_position} {candidateDistrictLabel(lead, lead.parcel_id || "—")}
          </span>
        </div>
        {runnerUp && (
          <div className="ea-next-steps__item">
            <span className="ea-next-steps__label">{t("expansionAdvisor.runnerUpCandidate")}</span>
            <span className="ea-next-steps__value">
              #{runnerUp.rank_position} {candidateDistrictLabel(runnerUp, runnerUp.parcel_id || "—")}
            </span>
          </div>
        )}
        <div className="ea-next-steps__item">
          <span className="ea-next-steps__label">{t("expansionAdvisor.nextValidation")}</span>
          <span className="ea-next-steps__value">{nextValidation}</span>
        </div>
      </div>
      <div className="ea-next-steps__actions">
        <button type="button" className="oak-btn oak-btn--sm oak-btn--primary" onClick={() => onOpenMemo(leadCandidateId)}>
          {leadPasses ? t("expansionAdvisor.openLeadMemo") : t("expansionAdvisor.openCandidateMemo")}
        </button>
        <button type="button" className="oak-btn oak-btn--sm oak-btn--tertiary" onClick={onOpenReport}>
          {t("expansionAdvisor.openExecutiveReport")}
        </button>
        <button type="button" className="oak-btn oak-btn--sm oak-btn--tertiary" onClick={onCompare}>
          {t("expansionAdvisor.compareAgain")}
        </button>
      </div>
    </div>
  );
}
