import { useTranslation } from "react-i18next";
import type { ExpansionCandidate, RecommendationReportResponse } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";

type Props = {
  title: string;
  candidateCount: number;
  shortlistCount: number;
  bestCandidate: ExpansionCandidate | null;
  leadCandidate?: ExpansionCandidate | null;
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
  leadCandidate,
  report,
  activeSavedId,
  onSaveStudy,
  onOpenReport,
  onCompareShortlist,
  compareEnabled,
}: Props) {
  const { t } = useTranslation();

  const heroCandidate = leadCandidate || bestCandidate;
  const heroLabel = leadCandidate ? t("expansionAdvisor.leadSite") : t("expansionAdvisor.topRanked");

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
      {heroCandidate && (
        <div className={`ea-study-header__best${leadCandidate ? " ea-study-header__best--lead" : ""}`}>
          <span className="ea-study-header__best-label">{heroLabel}:</span>
          <span className="ea-study-header__best-name">
            #{heroCandidate.rank_position} {heroCandidate.district || heroCandidate.parcel_id}
          </span>
          <ScorePill value={heroCandidate.final_score} />
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
