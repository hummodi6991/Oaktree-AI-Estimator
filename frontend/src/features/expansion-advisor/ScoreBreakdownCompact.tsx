import { useTranslation } from "react-i18next";
import type { CandidateScoreBreakdown } from "../../lib/api/expansionAdvisor";
import { parseScoreBreakdown } from "./studyAdapters";
import { fmtScore, scoreColor } from "./formatHelpers";

type Props = {
  breakdown: CandidateScoreBreakdown | undefined;
};

export default function ScoreBreakdownCompact({ breakdown }: Props) {
  const { t } = useTranslation();
  const components = parseScoreBreakdown(breakdown);

  if (components.length === 0) return null;

  const maxWeighted = Math.max(...components.map((c) => c.weighted), 0.01);

  return (
    <div className="ea-score-breakdown-compact">
      <h5 className="ea-score-breakdown-compact__title">
        {t("expansionAdvisor.scoreBreakdown")}
      </h5>
      {components.map((comp) => (
        <div key={comp.label} className="ea-score-breakdown-compact__row">
          <span className="ea-score-breakdown-compact__label">{comp.label}</span>
          <div className="ea-score-breakdown-compact__bar-wrap">
            <div
              className={`ea-score-breakdown-compact__bar ea-score-breakdown-compact__bar--${scoreColor(comp.input)}`}
              style={{ width: `${Math.max((comp.weighted / maxWeighted) * 100, 2)}%` }}
            />
          </div>
          <span className="ea-score-breakdown-compact__value">
            {fmtScore(comp.weighted)}
          </span>
          <span className="ea-score-breakdown-compact__weight">
            ({(comp.weight * 100).toFixed(0)}%)
          </span>
        </div>
      ))}
    </div>
  );
}
