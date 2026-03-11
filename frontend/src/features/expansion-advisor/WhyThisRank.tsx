import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";
import { parseScoreBreakdown, parseGateEntries, parseFeatureSnapshot } from "./studyAdapters";
import { fmtPct } from "./formatHelpers";

type Props = {
  candidate: ExpansionCandidate;
};

export default function WhyThisRank({ candidate }: Props) {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState(false);

  const components = parseScoreBreakdown(candidate.score_breakdown_json);
  const gates = parseGateEntries(candidate.gate_status_json, candidate.gate_reasons_json);
  const snapshot = parseFeatureSnapshot(candidate.feature_snapshot_json);

  const passedGates = gates.filter((g) => g.status === "pass");
  const failedGates = gates.filter((g) => g.status === "fail");
  const unknownGates = gates.filter((g) => g.status === "unknown");

  if (components.length === 0 && gates.length === 0) return null;

  return (
    <div className="ea-why-rank" onClick={(e) => e.stopPropagation()}>
      <button
        type="button"
        className="ea-why-rank__toggle"
        onClick={() => setExpanded(!expanded)}
        aria-expanded={expanded}
      >
        <span className="ea-why-rank__toggle-icon">{expanded ? "▾" : "▸"}</span>
        {t("expansionAdvisor.whyThisRank")}
      </button>

      {expanded && (
        <div className="ea-why-rank__body">
          {/* Score components */}
          {components.length > 0 && (
            <div className="ea-why-rank__section">
              <h6 className="ea-why-rank__section-title">{t("expansionAdvisor.scoreComponents")}</h6>
              <div className="ea-why-rank__score-grid">
                {components.map((comp) => (
                  <div key={comp.label} className="ea-why-rank__score-row">
                    <span className="ea-why-rank__score-label">{comp.label}</span>
                    <div className="ea-why-rank__score-bar-container">
                      <div
                        className="ea-why-rank__score-bar"
                        style={{ width: `${Math.min(100, Math.max(0, comp.input))}%` }}
                      />
                    </div>
                    <span className="ea-why-rank__score-value">{Math.round(comp.input)}</span>
                    <span className="ea-why-rank__score-weight">×{comp.weight.toFixed(2)}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Gate status */}
          {gates.length > 0 && (
            <div className="ea-why-rank__section">
              <h6 className="ea-why-rank__section-title">{t("expansionAdvisor.gateChecklist")}</h6>
              <div className="ea-why-rank__gates">
                {failedGates.length > 0 && (
                  <div className="ea-why-rank__gate-group">
                    <span className="ea-badge ea-badge--red">{t("expansionAdvisor.gatesFailed")} ({failedGates.length})</span>
                    <div className="ea-why-rank__gate-items">
                      {failedGates.map((g) => (
                        <span key={g.name} className="ea-gate-item ea-gate-item--fail" title={g.explanation}>
                          ✗ {g.name}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
                {passedGates.length > 0 && (
                  <div className="ea-why-rank__gate-group">
                    <span className="ea-badge ea-badge--green">{t("expansionAdvisor.gatesPassed")} ({passedGates.length})</span>
                    <div className="ea-why-rank__gate-items">
                      {passedGates.map((g) => (
                        <span key={g.name} className="ea-gate-item ea-gate-item--pass" title={g.explanation}>
                          ✓ {g.name}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
                {unknownGates.length > 0 && (
                  <div className="ea-why-rank__gate-group">
                    <span className="ea-badge ea-badge--neutral">{t("expansionAdvisor.gatesUnknown")} ({unknownGates.length})</span>
                    <div className="ea-why-rank__gate-items">
                      {unknownGates.map((g) => (
                        <span key={g.name} className="ea-gate-item ea-gate-item--unknown" title={g.explanation}>
                          ? {g.name}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Data completeness */}
          {snapshot.completeness > 0 && (
            <div className="ea-why-rank__section">
              <h6 className="ea-why-rank__section-title">{t("expansionAdvisor.dataQuality")}</h6>
              <div className="ea-why-rank__data-quality">
                <span>{t("expansionAdvisor.dataCompleteness")}: {fmtPct(snapshot.completeness)}</span>
                {snapshot.missingSources.length > 0 && (
                  <span className="ea-why-rank__missing">
                    {t("expansionAdvisor.missingData")}: {snapshot.missingSources.join(", ")}
                  </span>
                )}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
