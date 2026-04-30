import { useTranslation } from "react-i18next";
import type { CandidateScoreBreakdown } from "../../lib/api/expansionAdvisor";
import { parseScoreBreakdown } from "./studyAdapters";
import { fmtScore, scoreColor } from "./formatHelpers";
import { normalizeWeightPercent } from "./scoreInvariants";

type Props = {
  breakdown: CandidateScoreBreakdown | undefined;
};

type EconomicsDetail = {
  value_score?: number | null;
  rent_burden?: {
    percentile?: number | null;
    source_label?: string | null;
  };
};

export default function ScoreBreakdownCompact({ breakdown }: Props) {
  const { t } = useTranslation();
  const components = parseScoreBreakdown(breakdown);

  if (components.length === 0) return null;

  const maxWeighted = Math.max(...components.map((c) => c.weighted), 0.01);

  // value_score / percentile / comp scope context — replaces the
  // memo-drawer-only surfacing of these signals.
  const ed = (breakdown as unknown as { economics_detail?: EconomicsDetail } | undefined)?.economics_detail || {};
  const valueScore = typeof ed.value_score === "number" ? ed.value_score : null;
  const percentile = typeof ed.rent_burden?.percentile === "number" ? ed.rent_burden.percentile : null;
  const sourceLabel = typeof ed.rent_burden?.source_label === "string" ? ed.rent_burden.source_label : null;
  const showValueRow = valueScore !== null;

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
            {fmtScore(comp.weighted, 1)} pts
          </span>
          <span className="ea-score-breakdown-compact__weight">
            {normalizeWeightPercent(comp.weight)}% weight
          </span>
        </div>
      ))}
      {showValueRow && (
        <div className="ea-score-breakdown-compact__kv-row">
          <span className="ea-score-breakdown-compact__kv-label">
            {t("expansionAdvisor.valueScoreLabel")}
          </span>
          <span className="ea-score-breakdown-compact__kv-value">
            {valueScore!.toFixed(1)}/100
            {percentile !== null && (
              <span className="ea-score-breakdown-compact__sub">
                {" · p"}{Math.round(percentile * 100)}
                {sourceLabel && ` · ${t(`expansionAdvisor.compScope.${sourceLabel}`, { defaultValue: sourceLabel })}`}
              </span>
            )}
          </span>
        </div>
      )}
    </div>
  );
}
