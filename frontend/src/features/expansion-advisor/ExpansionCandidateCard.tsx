import { useTranslation } from "react-i18next";
import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";
// ConfidenceBadge removed from card — technical detail, available in memo
import PaybackBadge from "./PaybackBadge";
import WhyThisRank from "./WhyThisRank";
import TierBadge from "./TierBadge";
import { fmtSAR, fmtMeters, fmtM2, fmtMonths, candidateDistrictLabel, getDisplayScore, fmtEstimated } from "./formatHelpers";

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
  // Distinguish true pass, explicit fail, and unknown (null/undefined = some
  // gates lacked data so verdict is indeterminate, not a hard fail).
  const gateVerdict: "pass" | "fail" | "unknown" =
    gateOverall === true ? "pass" : gateOverall === false ? "fail" : "unknown";
  const allGatesPass = gateVerdict === "pass";
  // Only show "Lead Site" tag when the candidate actually passes all gates.
  // Otherwise use exploratory framing.
  const showLeadTag = isLead && allGatesPass;
  const showExploratoryTag = isLead && !allGatesPass;
  const positives = (candidate.top_positives_json || []).slice(0, 2);
  const risks = (candidate.top_risks_json || []).slice(0, 2);

  const isTop3 = (candidate.rank_position ?? 999) <= 3;
  const isCommercialUnit = candidate.source_type === "commercial_unit" || candidate.source_type === "aqar";

  // Derive tier from explicit field, feature snapshot, or source_type fallback
  const cl = candidate.feature_snapshot_json?.candidate_location as
    | Record<string, unknown>
    | undefined;
  const sourceTier =
    candidate.source_tier ??
    (cl?.source_tier as number | undefined) ??
    (isCommercialUnit ? 1 : null);
  const clSourceType =
    candidate.source_type ?? (cl?.source_type as string | undefined) ?? null;
  const clCurrentCategory =
    candidate.current_category ?? (cl?.current_category as string | undefined) ?? null;
  const clAvgRating = (cl?.cl_avg_rating as number | undefined) ?? null;
  const clRentConfidence =
    candidate.rent_confidence ?? (cl?.rent_confidence as string | undefined) ?? null;

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
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {showLeadTag && <span className="ea-lead-tag">{t("expansionAdvisor.leadSite")}</span>}
          {showExploratoryTag && <span className="ea-lead-tag ea-lead-tag--exploratory">{t("expansionAdvisor.topExploratoryCandidate")}</span>}
          {candidate.rank_position ? (
            <span className="ea-candidate__rank" title={localSortActive ? t("expansionAdvisor.backendRank") : undefined}>
              #{candidate.rank_position}
            </span>
          ) : null}
          <span className="ea-candidate__district">{candidateDistrictLabel(candidate, t("common.notAvailable"))}</span>
          {localSortActive && (
            <span className="ea-badge ea-badge--neutral ea-candidate__rank-note">
              {t("expansionAdvisor.backendRankLabel", { rank: candidate.rank_position })}
            </span>
          )}
        </div>
        <div className="ea-candidate__badges">
          <ScorePill value={getDisplayScore(candidate)} />
          {/* Verdict badge — separate from confidence */}
          <span className={`ea-badge ea-badge--${gateVerdict === "pass" ? "green" : gateVerdict === "fail" ? "red" : "amber"}`}>
            {gateVerdict === "pass" ? t("expansionAdvisor.gatePass") : gateVerdict === "fail" ? t("expansionAdvisor.gateFail") : t("expansionAdvisor.gateNeedsValidation")}
          </span>
          {candidate.payback_band && (
            <PaybackBadge band={candidate.payback_band} months={candidate.estimated_payback_months} />
          )}
        </div>
      </div>

      {/* Tier badge — unified source tier indicator */}
      <TierBadge
        sourceTier={sourceTier}
        sourceType={clSourceType}
        isVacant={candidate.is_vacant ?? (cl?.is_vacant as boolean | undefined) ?? null}
        currentCategory={clCurrentCategory}
        clAvgRating={clAvgRating}
        listingUrl={candidate.listing_url}
        rentConfidence={clRentConfidence}
      />

      {/* Commercial unit hero image — larger and prominent */}
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

      {/* Commercial unit key facts — prominent display */}
      {isCommercialUnit && (
        <div className="ea-candidate__unit-details ea-candidate__unit-details--prominent">
          {candidate.unit_price_sar_annual != null && (
            <div className="ea-candidate__metric ea-candidate__metric--featured">
              <span className="ea-candidate__metric-label">{t("expansionAdvisor.annualRentActual")}:</span>
              <span className="ea-candidate__metric-value--actual">{fmtSAR(candidate.unit_price_sar_annual)}</span>
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

      {/* Key metrics — clean, estimated values clearly labeled */}
      <div className="ea-candidate__metrics">
        {candidate.area_m2 != null && (
          <div className="ea-candidate__metric">
            <span className="ea-candidate__metric-label">{t("expansionAdvisor.areaLabel")}:</span>
            <span>{fmtEstimated(fmtM2(candidate.area_m2), clRentConfidence !== "actual")}</span>
          </div>
        )}
        <div className="ea-candidate__metric">
          <span className="ea-candidate__metric-label">{t("expansionAdvisor.annualRent")}:</span>
          <span>{fmtEstimated(fmtSAR(candidate.display_annual_rent_sar ?? candidate.estimated_annual_rent_sar), clRentConfidence !== "actual")}</span>
        </div>
        <div className="ea-candidate__metric">
          <span className="ea-candidate__metric-label">{t("expansionAdvisor.payback")}:</span>
          <span>{fmtMonths(candidate.estimated_payback_months)}</span>
        </div>
        {candidate.estimated_fitout_cost_sar != null && (
          <div className="ea-candidate__metric">
            <span className="ea-candidate__metric-label">{t("expansionAdvisor.fitoutCostLabel")}:</span>
            <span>{fmtEstimated(fmtSAR(candidate.estimated_fitout_cost_sar), true)}</span>
          </div>
        )}
        {candidate.distance_to_nearest_branch_m != null && (
          <div className="ea-candidate__metric">
            <span className="ea-candidate__metric-label">{t("expansionAdvisor.nearestBranch")}:</span>
            <span>{fmtMeters(candidate.distance_to_nearest_branch_m)}</span>
          </div>
        )}
      </div>

      {/* Insights */}
      {(positives.length > 0 || risks.length > 0) && (
        <div className="ea-candidate__insights">
          {positives.filter((text) => text && text !== "—").map((text, i) => (
            <div key={`p-${i}`} className="ea-candidate__insight">
              <span className="ea-candidate__insight-icon ea-candidate__insight-icon--positive">+</span>
              <span className="ea-candidate__insight-text">{text}</span>
            </div>
          ))}
          {risks.filter((text) => text && text !== "—").map((text, i) => (
            <div key={`r-${i}`} className="ea-candidate__insight">
              <span className="ea-candidate__insight-icon ea-candidate__insight-icon--risk">!</span>
              <span className="ea-candidate__insight-text">{text}</span>
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
        {onShowOnMap && (
          <button
            type="button"
            className="oak-btn oak-btn--sm oak-btn--tertiary"
            onClick={onShowOnMap}
          >
            {t("expansionAdvisor.showOnMap")}
          </button>
        )}
      </div>
    </div>
  );
}
