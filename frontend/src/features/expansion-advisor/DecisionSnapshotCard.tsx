import { useTranslation } from "react-i18next";
import type { ExpansionCandidate, CandidateMemoResponse, RecommendationReportResponse } from "../../lib/api/expansionAdvisor";
import { buildDecisionSnapshot } from "./studyAdapters";
import ScorePill from "./ScorePill";
import ConfidenceBadge from "./ConfidenceBadge";

type Props = {
  candidate: ExpansionCandidate;
  report?: RecommendationReportResponse | null;
  memo?: CandidateMemoResponse | null;
  prominent?: boolean;
};

export default function DecisionSnapshotCard({ candidate, report, memo, prominent }: Props) {
  const { t } = useTranslation();
  const snap = buildDecisionSnapshot(candidate, report, memo);

  return (
    <div className={`ea-decision-snapshot${prominent ? " ea-decision-snapshot--prominent" : ""}`}>
      <div className="ea-decision-snapshot__header">
        <h4 className="ea-decision-snapshot__title">{t("expansionAdvisor.decisionSnapshot")}</h4>
        <div className="ea-decision-snapshot__badges">
          <ScorePill value={snap.finalScore} />
          {/* Verdict badge */}
          <span className={`ea-badge ea-badge--${snap.gateVerdict === "pass" ? "green" : snap.gateVerdict === "fail" ? "red" : "neutral"}`}>
            {snap.gateVerdict === "pass" ? t("expansionAdvisor.gatePass") : snap.gateVerdict === "fail" ? t("expansionAdvisor.gateFail") : t("expansionAdvisor.gateNeedsValidation")}
          </span>
          {/* Confidence badge — separate from verdict */}
          <ConfidenceBadge grade={snap.confidenceGrade} />
        </div>
      </div>
      <div className="ea-decision-snapshot__body">
        {/* No-pass notice when no gates pass */}
        {!snap.allGatesPass && (
          <div className="ea-decision-snapshot__notice">
            {t("expansionAdvisor.noPassNotice")}
          </div>
        )}
        <div className="ea-decision-snapshot__row">
          <span className="ea-decision-snapshot__label">{snap.siteLabel}</span>
          <span className="ea-decision-snapshot__value ea-decision-snapshot__value--lead">{snap.leadSite}</span>
        </div>
        <div className="ea-decision-snapshot__row">
          <span className="ea-decision-snapshot__label">{snap.whyItWinsLabel}</span>
          <span className="ea-decision-snapshot__value">{snap.whyItWins}</span>
        </div>
        <div className="ea-decision-snapshot__row">
          <span className="ea-decision-snapshot__label">{t("expansionAdvisor.mainRisk")}</span>
          <span className="ea-decision-snapshot__value">{snap.mainRisk}</span>
        </div>
        <div className="ea-decision-snapshot__row">
          <span className="ea-decision-snapshot__label">{t("expansionAdvisor.reportBestFormat")}</span>
          <span className="ea-decision-snapshot__value">{snap.bestFormat}</span>
        </div>
        <div className="ea-decision-snapshot__row">
          <span className="ea-decision-snapshot__label">{t("expansionAdvisor.nextValidation")}</span>
          <span className="ea-decision-snapshot__value">{snap.nextValidation}</span>
        </div>
      </div>
    </div>
  );
}
