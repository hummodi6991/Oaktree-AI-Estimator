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
  /** Search-level pass count — prevents contradicting the search header */
  searchPassCount?: number;
  onOpenMemo?: () => void;
};

export default function DecisionSnapshotCard({ candidate, report, memo, prominent, searchPassCount: passCount, onOpenMemo }: Props) {
  const { t } = useTranslation();
  const snap = buildDecisionSnapshot(candidate, report, memo, passCount);

  return (
    <div className={`ea-decision-snapshot ea-decision-snapshot--compact${prominent ? " ea-decision-snapshot--prominent" : ""}`}>
      {/* Compact header row: Score | Verdict | Confidence | Format | Open Memo */}
      <div className="ea-decision-snapshot__row-compact">
        <ScorePill value={snap.finalScore} />
        <span className={`ea-badge ea-badge--${snap.gateVerdict === "pass" ? "green" : snap.gateVerdict === "fail" ? "red" : "neutral"}`}>
          {snap.gateVerdict === "pass" ? t("expansionAdvisor.gatePass") : snap.gateVerdict === "fail" ? t("expansionAdvisor.gateFail") : t("expansionAdvisor.gateNeedsValidation")}
        </span>
        <ConfidenceBadge grade={snap.confidenceGrade} />
        {snap.bestFormat && (
          <span className="ea-decision-snapshot__format">{snap.bestFormat}</span>
        )}
        {onOpenMemo && (
          <button
            type="button"
            className="oak-btn oak-btn--sm oak-btn--primary"
            onClick={onOpenMemo}
          >
            {t("expansionAdvisor.viewDecisionMemo")}
          </button>
        )}
      </div>

      {/* Lead site — subtle secondary line */}
      <div className="ea-decision-snapshot__runner-up">
        <span className="ea-decision-snapshot__label">{snap.siteLabel}</span>
        <span className="ea-decision-snapshot__value">{snap.leadSite}</span>
      </div>
    </div>
  );
}
