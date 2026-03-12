import { useTranslation } from "react-i18next";
import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";
import ConfidenceBadge from "./ConfidenceBadge";
import PaybackBadge from "./PaybackBadge";
import WhyThisRank from "./WhyThisRank";
import { fmtSAR, fmtMeters, fmtScore } from "./formatHelpers";

type Props = {
  candidate: ExpansionCandidate;
  selected: boolean;
  shortlisted: boolean;
  compared: boolean;
  isLead?: boolean;
  localSortActive?: boolean;
  onSelect: () => void;
  onToggleShortlist: () => void;
  onCompareToggle: () => void;
  onOpenMemo?: () => void;
};

export default function ExpansionCandidateCard({
  candidate,
  selected,
  shortlisted,
  compared,
  isLead,
  localSortActive,
  onSelect,
  onToggleShortlist,
  onCompareToggle,
  onOpenMemo,
}: Props) {
  const { t } = useTranslation();
  const pass = Boolean(candidate.gate_status_json?.overall_pass);
  const positives = (candidate.top_positives_json || []).slice(0, 2);
  const risks = (candidate.top_risks_json || []).slice(0, 2);

  const isTop3 = (candidate.rank_position ?? 999) <= 3;

  const cls = [
    "ea-candidate",
    selected && "ea-candidate--selected",
    shortlisted && "ea-candidate--shortlisted",
    compared && "ea-candidate--compared",
    isLead && "ea-candidate--lead",
    isTop3 && "ea-candidate--top3",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <div className={cls} onClick={onSelect} role="button" tabIndex={0} onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") onSelect(); }} data-candidate-id={candidate.id}>
      {/* Top row: rank + district + badges */}
      <div className="ea-candidate__top">
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {isLead && <span className="ea-lead-tag">{t("expansionAdvisor.leadSite")}</span>}
          {candidate.rank_position ? (
            <span className="ea-candidate__rank" title={localSortActive ? t("expansionAdvisor.backendRank") : undefined}>
              #{candidate.rank_position}
            </span>
          ) : null}
          <span className="ea-candidate__district">{candidate.district || t("common.notAvailable")}</span>
          {localSortActive && (
            <span className="ea-badge ea-badge--neutral ea-candidate__rank-note">
              {t("expansionAdvisor.backendRankLabel", { rank: candidate.rank_position })}
            </span>
          )}
        </div>
        <div className="ea-candidate__badges">
          <ScorePill value={candidate.final_score} />
          <ConfidenceBadge grade={candidate.confidence_grade} />
          <span className={`ea-badge ea-badge--${pass ? "green" : "red"}`}>
            {pass ? t("expansionAdvisor.gatePass") : t("expansionAdvisor.gateFail")}
          </span>
          {candidate.payback_band && (
            <PaybackBadge band={candidate.payback_band} months={candidate.estimated_payback_months} />
          )}
        </div>
      </div>

      {/* Decision summary */}
      {candidate.decision_summary && (
        <p className="ea-candidate__summary">{candidate.decision_summary}</p>
      )}

      {/* Metrics grid */}
      <div className="ea-candidate__metrics">
        <div className="ea-candidate__metric">
          <span className="ea-candidate__metric-label">{t("expansionAdvisor.annualRent")}:</span>
          <span>{fmtSAR(candidate.estimated_annual_rent_sar)}</span>
        </div>
        <div className="ea-candidate__metric">
          <span className="ea-candidate__metric-label">{t("expansionAdvisor.fitoutCost")}:</span>
          <span>{fmtSAR(candidate.estimated_fitout_cost_sar)}</span>
        </div>
        <div className="ea-candidate__metric">
          <span className="ea-candidate__metric-label">{t("expansionAdvisor.revenueIndex")}:</span>
          <span>{fmtScore(candidate.estimated_revenue_index, 1)}</span>
        </div>
        <div className="ea-candidate__metric">
          <span className="ea-candidate__metric-label">{t("expansionAdvisor.economicsLabel")}:</span>
          <span>{fmtScore(candidate.economics_score)}</span>
        </div>
        <div className="ea-candidate__metric">
          <span className="ea-candidate__metric-label">{t("expansionAdvisor.brandFitLabel")}:</span>
          <span>{fmtScore(candidate.brand_fit_score)}</span>
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
        {candidate.provider_whitespace_score != null && (
          <div className="ea-candidate__metric">
            <span className="ea-candidate__metric-label">{t("expansionAdvisor.providerWhitespace")}:</span>
            <span>{fmtScore(candidate.provider_whitespace_score)}</span>
          </div>
        )}
      </div>

      {/* Insights */}
      {(positives.length > 0 || risks.length > 0) && (
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
      )}

      {/* Why this rank? - expandable drill-down */}
      <WhyThisRank candidate={candidate} />

      {/* Actions */}
      <div className="ea-candidate__actions" onClick={(e) => e.stopPropagation()}>
        <button type="button" className="oak-btn oak-btn--sm oak-btn--primary" onClick={onOpenMemo || onSelect}>
          {t("expansionAdvisor.viewDecisionMemo")}
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
          {compared ? t("expansionAdvisor.removeCompare") : t("expansionAdvisor.addToCompare")}
        </button>
      </div>
    </div>
  );
}
