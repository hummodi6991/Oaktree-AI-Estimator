import { useTranslation } from "react-i18next";
import type { ExpansionCandidate, CompareCandidatesResponse } from "../../lib/api/expansionAdvisor";
import { deriveCompareOutcome } from "./studyAdapters";

type Props = {
  result: CompareCandidatesResponse | null;
  candidates: ExpansionCandidate[];
  leadCandidateId: string | null;
  onSelectCandidateId?: (candidateId: string) => void;
};

export default function CompareOutcomeBanner({ result, candidates, leadCandidateId, onSelectCandidateId }: Props) {
  const { t } = useTranslation();

  if (!result || !result.items.length) return null;

  const outcome = deriveCompareOutcome(result, candidates, leadCandidateId);

  return (
    <div className={`ea-compare-outcome${!outcome.leadsAligned ? " ea-compare-outcome--misaligned" : ""}`}>
      <div className="ea-compare-outcome__main">
        <span className="ea-compare-outcome__label">{t("expansionAdvisor.coWinsOverall")}</span>
        <span
          className="ea-compare-outcome__winner"
          style={{ cursor: outcome.winnerId ? "pointer" : "default" }}
          onClick={() => outcome.winnerId && onSelectCandidateId?.(outcome.winnerId)}
        >
          {outcome.winnerLabel}
        </span>
        {!outcome.leadsAligned && (
          <span className="ea-badge ea-badge--amber">{t("expansionAdvisor.coLeadMismatch")}</span>
        )}
      </div>
      {outcome.runnerUpStrengths.length > 0 && (
        <div className="ea-compare-outcome__detail">
          <span className="ea-compare-outcome__label">{t("expansionAdvisor.coRunnerUpStronger")}</span>
          <span className="ea-compare-outcome__strengths">
            {outcome.runnerUpStrengths.join(", ")}
          </span>
        </div>
      )}
      {outcome.whatWouldChange !== "—" && (
        <div className="ea-compare-outcome__detail">
          <span className="ea-compare-outcome__label">{t("expansionAdvisor.coWhatWouldChange")}</span>
          <span className="ea-compare-outcome__change">{outcome.whatWouldChange}</span>
        </div>
      )}
    </div>
  );
}
