import { useTranslation } from "react-i18next";
import type { ExpansionCandidate, RecommendationReportResponse } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";

type Props = {
  title: string;
  candidateCount: number;
  shortlistCount: number;
  bestCandidate: ExpansionCandidate | null;
  report: RecommendationReportResponse | null;
  activeSavedId: string | null;
  searchId: string;
  onSaveStudy: () => void;
  onOpenReport: () => void;
  onCompareShortlist: () => void;
  compareEnabled: boolean;
};

export default function StudyHeader({
  title,
  candidateCount,
  shortlistCount,
  bestCandidate,
  report,
  activeSavedId,
  onSaveStudy,
  onOpenReport,
  onCompareShortlist,
  compareEnabled,
}: Props) {
  const { t } = useTranslation();

  return (
    <div className="ea-study-header">
      <div className="ea-study-header__top">
        <div className="ea-study-header__info">
          <h2 className="ea-study-header__title">{title}</h2>
          <div className="ea-study-header__meta">
            <span className="ea-study-header__meta-item">
              {t("expansionAdvisor.riyadhScope")}
            </span>
            <span className="ea-study-header__meta-sep">&middot;</span>
            <span className="ea-study-header__meta-item">
              {t("expansionAdvisor.candidateCount", { count: candidateCount })}
            </span>
            {shortlistCount > 0 && (
              <>
                <span className="ea-study-header__meta-sep">&middot;</span>
                <span className="ea-study-header__meta-item">
                  {t("expansionAdvisor.shortlistedCount", { count: shortlistCount })}
                </span>
              </>
            )}
          </div>
        </div>
        <div className="ea-study-header__ctas">
          <button
            type="button"
            className="oak-btn oak-btn--sm oak-btn--primary"
            onClick={onSaveStudy}
          >
            {activeSavedId ? t("expansionAdvisor.updateStudy") : t("expansionAdvisor.saveStudy")}
          </button>
          <button
            type="button"
            className="oak-btn oak-btn--sm oak-btn--tertiary"
            onClick={onOpenReport}
          >
            {t("expansionAdvisor.openExecutiveReport")}
          </button>
          {compareEnabled && (
            <button
              type="button"
              className="oak-btn oak-btn--sm oak-btn--tertiary"
              onClick={onCompareShortlist}
            >
              {t("expansionAdvisor.compareShortlist")}
            </button>
          )}
        </div>
      </div>
      {bestCandidate && (
        <div className="ea-study-header__best">
          <span className="ea-study-header__best-label">{t("expansionAdvisor.topRanked")}:</span>
          <span className="ea-study-header__best-name">
            #{bestCandidate.rank_position} {bestCandidate.district || bestCandidate.parcel_id}
          </span>
          <ScorePill value={bestCandidate.final_score} />
          {report?.recommendation?.why_best && (
            <span className="ea-study-header__best-reason">
              — {report.recommendation.why_best.slice(0, 80)}
              {(report.recommendation.why_best.length || 0) > 80 ? "…" : ""}
            </span>
          )}
        </div>
      )}
    </div>
  );
}
