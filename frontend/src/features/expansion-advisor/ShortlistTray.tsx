import { useTranslation } from "react-i18next";
import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";

type Props = {
  candidates: ExpansionCandidate[];
  shortlistIds: string[];
  compareIds: string[];
  selectedCandidateId: string | null;
  onSelectCandidate: (candidateId: string) => void;
  onRemoveShortlist: (candidateId: string) => void;
  onOpenMemo: (candidateId: string) => void;
  onCompare: () => void;
  compareEnabled: boolean;
};

export default function ShortlistTray({
  candidates,
  shortlistIds,
  compareIds,
  selectedCandidateId,
  onSelectCandidate,
  onRemoveShortlist,
  onOpenMemo,
  onCompare,
  compareEnabled,
}: Props) {
  const { t } = useTranslation();

  // Build shortlisted candidates in shortlist order, preserving rank
  const shortlisted = shortlistIds
    .map((id) => candidates.find((c) => c.id === id))
    .filter(Boolean) as ExpansionCandidate[];

  if (shortlisted.length === 0) {
    return (
      <div className="ea-shortlist-tray ea-shortlist-tray--empty">
        <span className="ea-shortlist-tray__empty-text">
          {t("expansionAdvisor.shortlistEmpty")}
        </span>
      </div>
    );
  }

  return (
    <div className="ea-shortlist-tray">
      <div className="ea-shortlist-tray__header">
        <h4 className="ea-shortlist-tray__title">
          {t("expansionAdvisor.shortlistLabel")} ({shortlisted.length})
        </h4>
        {compareEnabled && (
          <button
            type="button"
            className="oak-btn oak-btn--sm oak-btn--primary"
            onClick={onCompare}
          >
            {t("expansionAdvisor.compareShortlist")}
          </button>
        )}
      </div>
      <div className="ea-shortlist-tray__items">
        {shortlisted.map((candidate) => {
          const isSelected = candidate.id === selectedCandidateId;
          const isCompared = compareIds.includes(candidate.id);
          return (
            <div
              key={candidate.id}
              className={`ea-shortlist-tray__item${isSelected ? " ea-shortlist-tray__item--selected" : ""}${isCompared ? " ea-shortlist-tray__item--compared" : ""}`}
              onClick={() => onSelectCandidate(candidate.id)}
              role="button"
              tabIndex={0}
              onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") onSelectCandidate(candidate.id); }}
            >
              <div className="ea-shortlist-tray__item-info">
                <span className="ea-candidate__rank">#{candidate.rank_position}</span>
                <span className="ea-shortlist-tray__item-district">
                  {candidate.district || candidate.parcel_id}
                </span>
                <ScorePill value={candidate.final_score} />
              </div>
              <div className="ea-shortlist-tray__item-actions" onClick={(e) => e.stopPropagation()}>
                <button
                  type="button"
                  className="oak-btn oak-btn--xs oak-btn--tertiary"
                  onClick={() => onOpenMemo(candidate.id)}
                  title={t("expansionAdvisor.viewDecisionMemo")}
                >
                  {t("expansionAdvisor.memo")}
                </button>
                <button
                  type="button"
                  className="oak-btn oak-btn--xs oak-btn--tertiary"
                  onClick={() => onRemoveShortlist(candidate.id)}
                  title={t("expansionAdvisor.removeShortlist")}
                >
                  &times;
                </button>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
