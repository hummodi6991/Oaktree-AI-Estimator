import { useTranslation } from "react-i18next";
import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";
import TierBadge from "./TierBadge";
import { fmtSARCompact, fmtM2, fmtMeters, candidateDistrictLabel, getDisplayScore } from "./formatHelpers";

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
  onShowOnMap?: () => void;
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
  onShowOnMap,
}: Props) {
  const { t } = useTranslation();
  const gateOverall = candidate.gate_status_json?.overall_pass;
  const gateVerdict: "pass" | "fail" | "unknown" =
    gateOverall === true ? "pass" : gateOverall === false ? "fail" : "unknown";
  const allGatesPass = gateVerdict === "pass";
  const showLeadTag = isLead && allGatesPass;
  const showExploratoryTag = isLead && !allGatesPass;
  const positives = (candidate.top_positives_json || []).slice(0, 1);
  const risks = (candidate.top_risks_json || []).slice(0, 1);

  // Check if positives/risks are identical boilerplate — skip if so
  const hasUniquePositive = positives.length > 0 && positives[0] && positives[0] !== "—";
  const hasUniqueRisk = risks.length > 0 && risks[0] && risks[0] !== "—";

  const isTop3 = (candidate.rank_position ?? 999) <= 3;
  const isCommercialUnit = candidate.source_type === "commercial_unit" || candidate.source_type === "aqar";

  // Derive tier from explicit field, feature snapshot, or source_type fallback
  const cl = candidate.feature_snapshot_json?.candidate_location as
    | Record<string, unknown>
    | undefined;
  const clRentConfidence =
    candidate.rent_confidence ?? (cl?.rent_confidence as string | undefined) ?? null;

  // Nearest branch — only show as pill when < 5km
  const nearestBranchM = candidate.distance_to_nearest_branch_m;
  const showNearestBranch = nearestBranchM != null && nearestBranchM < 5000;

  const cls = [
    "ea-candidate",
    selected && "ea-candidate--selected",
    shortlisted && "ea-candidate--shortlisted",
    compared && "ea-candidate--compared",
    isLead && "ea-candidate--lead",
    isTop3 && "ea-candidate--top3",
    isCommercialUnit && "ea-candidate--commercial-unit",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <div className={cls} onClick={onSelect} role="button" tabIndex={0} onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") onSelect(); }} data-candidate-id={candidate.id}>
      {/* Top row: rank + district + badges */}
      <div className="ea-candidate__top">
        <div className="ea-candidate__identity">
          {showLeadTag && <span className="ea-lead-tag ea-lead-tag--sm">{t("expansionAdvisor.leadSite")}</span>}
          {showExploratoryTag && <span className="ea-lead-tag ea-lead-tag--sm ea-lead-tag--exploratory">{t("expansionAdvisor.topExploratoryCandidate")}</span>}
          {candidate.rank_position ? (
            <span className="ea-candidate__rank" title={localSortActive ? t("expansionAdvisor.backendRank") : undefined}>
              #{candidate.rank_position}
            </span>
          ) : null}
          <span className="ea-candidate__district">{candidateDistrictLabel(candidate, t("common.notAvailable"))}</span>
        </div>
        <div className="ea-candidate__badges">
          <ScorePill value={getDisplayScore(candidate)} />
          {showNearestBranch && (
            <span className="ea-badge ea-badge--neutral ea-candidate__nearest-pill">
              {fmtMeters(nearestBranchM)}
            </span>
          )}
        </div>
      </div>

      {/* Tier badge + listing link */}
      <TierBadge
        sourceTier={candidate.source_tier}
        sourceType={candidate.source_type}
        listingUrl={candidate.listing_url}
        rentConfidence={clRentConfidence}
      />

      {/* Commercial unit hero image */}
      {isCommercialUnit && candidate.image_url && (
        <div className="ea-candidate__unit-image ea-candidate__unit-image--hero">
          <img
            src={candidate.image_url}
            alt={candidate.unit_neighborhood || t("expansionAdvisor.commercialUnit")}
            loading="lazy"
            onError={(e) => { (e.target as HTMLImageElement).style.display = "none"; }}
          />
        </div>
      )}

      {/* Commercial unit key facts */}
      {isCommercialUnit && (
        <div className="ea-candidate__unit-details ea-candidate__unit-details--prominent">
          {candidate.unit_price_sar_annual != null && (
            <div className="ea-candidate__metric ea-candidate__metric--featured">
              <span className="ea-candidate__metric-label">{t("expansionAdvisor.annualRentActual")}:</span>
              <span className="ea-candidate__metric-value--actual">{fmtSARCompact(candidate.unit_price_sar_annual)}</span>
            </div>
          )}
          {candidate.unit_area_sqm != null && (
            <div className="ea-candidate__metric ea-candidate__metric--featured">
              <span className="ea-candidate__metric-label">{t("expansionAdvisor.areaSqm")}:</span>
              <span>{fmtM2(candidate.unit_area_sqm)}</span>
            </div>
          )}
          {candidate.unit_street_width_m != null && (
            <div className="ea-candidate__metric ea-candidate__metric--featured">
              <span className="ea-candidate__metric-label">{t("expansionAdvisor.streetWidth")}:</span>
              <span>{fmtMeters(candidate.unit_street_width_m)}</span>
            </div>
          )}
        </div>
      )}

      {/* Key metrics — clean horizontal row, no "Est. ~" prefix */}
      <div className="ea-candidate__metrics">
        {candidate.area_m2 != null && (
          <span className="ea-candidate__metric-compact">{fmtM2(candidate.area_m2)}</span>
        )}
        <span className="ea-candidate__metric-compact">{fmtSARCompact(candidate.display_annual_rent_sar ?? candidate.estimated_annual_rent_sar)}/yr</span>
        {candidate.estimated_fitout_cost_sar != null && (
          <span className="ea-candidate__metric-compact">{fmtSARCompact(candidate.estimated_fitout_cost_sar)}</span>
        )}
      </div>

      {/* Insights — top 1 positive + top 1 risk only */}
      {(hasUniquePositive || hasUniqueRisk) && (
        <div className="ea-candidate__insights">
          {hasUniquePositive && (
            <div className="ea-candidate__insight">
              <span className="ea-candidate__insight-icon ea-candidate__insight-icon--positive">+</span>
              <span className="ea-candidate__insight-text">{positives[0]}</span>
            </div>
          )}
          {hasUniqueRisk && (
            <div className="ea-candidate__insight">
              <span className="ea-candidate__insight-icon ea-candidate__insight-icon--risk">!</span>
              <span className="ea-candidate__insight-text">{risks[0]}</span>
            </div>
          )}
        </div>
      )}

      {/* Actions — icon buttons on hover, memo stays as text */}
      <div className="ea-candidate__actions" onClick={(e) => e.stopPropagation()}>
        <button type="button" className="oak-btn oak-btn--sm oak-btn--primary" onClick={onOpenMemo || onSelect}>
          {t("expansionAdvisor.viewDecisionMemo")}
        </button>
        <div className="ea-candidate__icon-actions">
          {onShowOnMap && (
            <button
              type="button"
              className={`ea-candidate__icon-btn${selected ? " ea-candidate__icon-btn--active" : ""}`}
              onClick={onShowOnMap}
              title={t("expansionAdvisor.showOnMap")}
              aria-label={t("expansionAdvisor.showOnMap")}
            >
              <svg width="16" height="16" viewBox="0 0 20 20" fill="currentColor"><path fillRule="evenodd" d="M5.05 4.05a7 7 0 119.9 9.9L10 18.9l-4.95-4.95a7 7 0 010-9.9zM10 11a2 2 0 100-4 2 2 0 000 4z" clipRule="evenodd"/></svg>
            </button>
          )}
          <button
            type="button"
            className={`ea-candidate__icon-btn${compared ? " ea-candidate__icon-btn--active" : ""}`}
            onClick={onCompareToggle}
            title={compared ? t("expansionAdvisor.removeCompare") : t("expansionAdvisor.addToCompare")}
            aria-label={compared ? t("expansionAdvisor.removeCompare") : t("expansionAdvisor.addToCompare")}
          >
            <svg width="16" height="16" viewBox="0 0 20 20" fill="currentColor"><path d="M5 4a1 1 0 00-2 0v7.268a2 2 0 000 3.464V16a1 1 0 102 0v-1.268a2 2 0 000-3.464V4zM11 4a1 1 0 10-2 0v1.268a2 2 0 000 3.464V16a1 1 0 102 0V8.732a2 2 0 000-3.464V4zM17 4a1 1 0 10-2 0v7.268a2 2 0 000 3.464V16a1 1 0 102 0v-1.268a2 2 0 000-3.464V4z"/></svg>
          </button>
          <button
            type="button"
            className={`ea-candidate__icon-btn${shortlisted ? " ea-candidate__icon-btn--active" : ""}`}
            onClick={onToggleShortlist}
            title={shortlisted ? t("expansionAdvisor.removeShortlist") : t("expansionAdvisor.shortlist")}
            aria-label={shortlisted ? t("expansionAdvisor.removeShortlist") : t("expansionAdvisor.shortlist")}
          >
            <svg width="16" height="16" viewBox="0 0 20 20" fill={shortlisted ? "currentColor" : "none"} stroke="currentColor" strokeWidth="1.5"><path d="M5 4a2 2 0 012-2h6a2 2 0 012 2v14l-5-2.5L5 18V4z"/></svg>
          </button>
        </div>
      </div>
    </div>
  );
}
