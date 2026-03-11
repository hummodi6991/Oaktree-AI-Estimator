import { useTranslation } from "react-i18next";
import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";
import ConfidenceBadge from "./ConfidenceBadge";
import PaybackBadge from "./PaybackBadge";
import { fmtSAR, fmtMeters, fmtScore, gateColor } from "./formatHelpers";

type Props = {
  candidate: ExpansionCandidate;
  selected: boolean;
  shortlisted: boolean;
  compared: boolean;
  onSelect: () => void;
  onToggleShortlist: () => void;
  onCompareToggle: () => void;
};

export default function ExpansionCandidateCard({
  candidate,
  selected,
  shortlisted,
  compared,
  onSelect,
  onToggleShortlist,
  onCompareToggle,
}: Props) {
  const { t } = useTranslation();
  const pass = Boolean(candidate.gate_status_json?.overall_pass);
  const gc = gateColor(candidate.gate_status_json?.overall_pass ?? null);
  const positives = (candidate.top_positives_json || []).slice(0, 2);
  const risks = (candidate.top_risks_json || []).slice(0, 2);

  const cls = [
    "ea-candidate",
    selected && "ea-candidate--selected",
    shortlisted && "ea-candidate--shortlisted",
    compared && "ea-candidate--compared",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <div className={cls} onClick={onSelect} role="button" tabIndex={0} onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") onSelect(); }}>
      {/* Top row: rank + district + badges */}
      <div className="ea-candidate__top">
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {candidate.rank_position ? (
            <span className="ea-candidate__rank">#{candidate.rank_position}</span>
          ) : null}
          <span className="ea-candidate__district">{candidate.district || t("common.notAvailable")}</span>
        </div>
        <div className="ea-candidate__badges">
          <ScorePill value={candidate.final_score} />
          <ConfidenceBadge grade={candidate.confidence_grade} />
          <span className={`ea-badge ea-badge--${gc}`}>
            {pass ? t("expansionAdvisor.gatePass") : t("expansionAdvisor.gateFail")}
          </span>
        </div>
      </div>

      {/* Metrics row */}
      <div className="ea-candidate__metrics">
        <div className="ea-candidate__metric">
          <span className="ea-candidate__metric-label">{t("expansionAdvisor.payback")}:</span>
          <PaybackBadge band={candidate.payback_band} months={candidate.estimated_payback_months} />
        </div>
        <div className="ea-candidate__metric">
          <span className="ea-candidate__metric-label">{t("expansionAdvisor.rent")}:</span>
          <span>{fmtSAR(candidate.estimated_annual_rent_sar)}</span>
        </div>
        <div className="ea-candidate__metric">
          <span className="ea-candidate__metric-label">{t("expansionAdvisor.economicsLabel")}:</span>
          <span>{fmtScore(candidate.economics_score)}</span>
        </div>
        <div className="ea-candidate__metric">
          <span className="ea-candidate__metric-label">{t("expansionAdvisor.nearestBranch")}:</span>
          <span>{fmtMeters(candidate.distance_to_nearest_branch_m)}</span>
        </div>
        {candidate.cannibalization_score != null && (
          <div className="ea-candidate__metric">
            <span className="ea-candidate__metric-label">{t("expansionAdvisor.cannibalization")}:</span>
            <ScorePill value={candidate.cannibalization_score} />
          </div>
        )}
      </div>

      {/* Insights */}
      <div className="ea-candidate__insights">
        {positives.map((text, i) => (
          <div key={`p-${i}`} className="ea-candidate__insight">
            <span className="ea-candidate__insight-icon ea-candidate__insight-icon--positive">+</span>
            <span>{text}</span>
          </div>
        ))}
        {risks.map((text, i) => (
          <div key={`r-${i}`} className="ea-candidate__insight">
            <span className="ea-candidate__insight-icon ea-candidate__insight-icon--risk">!</span>
            <span>{text}</span>
          </div>
        ))}
      </div>

      {/* Actions */}
      <div className="ea-candidate__actions" onClick={(e) => e.stopPropagation()}>
        <button type="button" className="oak-btn oak-btn--sm oak-btn--primary" onClick={onSelect}>
          {t("expansionAdvisor.viewMemo")}
        </button>
        <button
          type="button"
          className={`oak-btn oak-btn--sm ${shortlisted ? "oak-btn--secondary" : "oak-btn--tertiary"}`}
          onClick={onToggleShortlist}
        >
          {shortlisted ? t("expansionAdvisor.removeShortlist") : t("expansionAdvisor.shortlist")}
        </button>
        <button
          type="button"
          className={`oak-btn oak-btn--sm ${compared ? "oak-btn--secondary" : "oak-btn--tertiary"}`}
          onClick={onCompareToggle}
        >
          {compared ? t("expansionAdvisor.removeCompare") : t("expansionAdvisor.compareCandidates")}
        </button>
      </div>
    </div>
  );
}
