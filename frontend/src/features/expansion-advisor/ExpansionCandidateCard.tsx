import { useTranslation } from "react-i18next";
import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";
import ConfidenceBadge from "./ConfidenceBadge";
import PaybackBadge from "./PaybackBadge";
import WhyThisRank from "./WhyThisRank";
import { fmtSAR, fmtMeters, fmtScore, fmtM2, fmtSarPerM2, fmtMonths, candidateDistrictLabel, getDisplayScore, marketGapLabel, demandLevelLabel, locationMatchLabel, economicsStrengthLabel, dataCoverageLabel, branchOverlapLabel } from "./formatHelpers";

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
  const isCommercialUnit = candidate.source_type === "commercial_unit";

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
          {/* Confidence badge — data quality, not site approval */}
          <ConfidenceBadge grade={candidate.confidence_grade} />
          {candidate.payback_band && (
            <PaybackBadge band={candidate.payback_band} months={candidate.estimated_payback_months} />
          )}
        </div>
      </div>

      {/* Commercial unit hero image — larger and prominent */}
      {isCommercialUnit && candidate.image_url && (
        <div className="ea-candidate__unit-image ea-candidate__unit-image--hero">
          <img
            src={candidate.image_url}
            alt={candidate.unit_neighborhood || t("expansionAdvisor.commercialUnit")}
            loading="lazy"
            onError={(e) => { (e.target as HTMLImageElement).style.display = "none"; }}
          />
          <span className="ea-candidate__aqar-trust">{t("expansionAdvisor.realListingFromAqar")}</span>
        </div>
      )}

      {/* Commercial unit badge */}
      {isCommercialUnit && (
        <div className="ea-candidate__unit-badge">
          <span className="ea-badge ea-badge--blue">{t("expansionAdvisor.commercialUnit")}</span>
          <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
            candidate.unit_listing_type === 'showroom'
              ? 'bg-amber-100 text-amber-800'
              : 'bg-teal-100 text-teal-800'
          }`}>
            {candidate.unit_listing_type === 'showroom'
              ? t('expansionAdvisor.unitTypeShowroom')
              : t('expansionAdvisor.unitTypeStore')}
          </span>
        </div>
      )}

      {/* Decision summary */}
      {candidate.decision_summary && (
        <p className="ea-candidate__summary">{candidate.decision_summary}</p>
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
          {candidate.listing_url && (
            <a
              href={candidate.listing_url}
              target="_blank"
              rel="noopener noreferrer"
              className="oak-btn oak-btn--sm oak-btn--primary ea-candidate__aqar-btn"
              onClick={(e) => e.stopPropagation()}
            >
              {t("expansionAdvisor.viewOnAqar")} &#8599;
            </a>
          )}
        </div>
      )}

      {/* Metrics grid — business-friendly labels with raw values as tooltips */}
      <div className="ea-candidate__metrics">
        {candidate.area_m2 != null && (
          <div className="ea-candidate__metric">
            <span className="ea-candidate__metric-label">{t("expansionAdvisor.areaLabel")}:</span>
            <span>{fmtM2(candidate.area_m2)}</span>
          </div>
        )}
        <div className="ea-candidate__metric">
          <span className="ea-candidate__metric-label">{t("expansionAdvisor.annualRent")}:</span>
          <span>{fmtSAR(candidate.display_annual_rent_sar ?? candidate.estimated_annual_rent_sar)}</span>
        </div>
        <div className="ea-candidate__metric">
          <span className="ea-candidate__metric-label">{t("expansionAdvisor.payback")}:</span>
          <span>{fmtMonths(candidate.estimated_payback_months)}</span>
        </div>
        {candidate.provider_whitespace_score != null && (
          <div className="ea-candidate__metric" title={`${fmtScore(candidate.provider_whitespace_score)}/100`}>
            <span className="ea-candidate__metric-label">{t("expansionAdvisor.marketGap")}:</span>
            <span>{t(`expansionAdvisor.marketGap_${marketGapLabel(candidate.provider_whitespace_score)}`)}</span>
            <span className="ea-candidate__metric-raw">{fmtScore(candidate.provider_whitespace_score)}</span>
          </div>
        )}
        <div className="ea-candidate__metric" title={`${fmtScore(candidate.estimated_revenue_index, 1)}/100`}>
          <span className="ea-candidate__metric-label">{t("expansionAdvisor.demandLevel")}:</span>
          <span>{t(`expansionAdvisor.demandLevel_${demandLevelLabel(candidate.estimated_revenue_index)}`)}</span>
          <span className="ea-candidate__metric-raw">{fmtScore(candidate.estimated_revenue_index, 1)}</span>
        </div>
        <div className="ea-candidate__metric" title={`${fmtScore(candidate.brand_fit_score)}/100`}>
          <span className="ea-candidate__metric-label">{t("expansionAdvisor.locationMatch")}:</span>
          <span>{t(`expansionAdvisor.locationMatch_${locationMatchLabel(candidate.brand_fit_score)}`)}</span>
          <span className="ea-candidate__metric-raw">{fmtScore(candidate.brand_fit_score)}</span>
        </div>
        <div className="ea-candidate__metric" title={`${fmtScore(candidate.economics_score)}/100`}>
          <span className="ea-candidate__metric-label">{t("expansionAdvisor.economicsLabel")}:</span>
          <span>{t(`expansionAdvisor.economics_${economicsStrengthLabel(candidate.economics_score)}`)}</span>
          <span className="ea-candidate__metric-raw">{fmtScore(candidate.economics_score)}</span>
        </div>
        <div className="ea-candidate__metric" title={candidate.confidence_grade || undefined}>
          <span className="ea-candidate__metric-label">{t("expansionAdvisor.dataCoverage")}:</span>
          <span>{t(`expansionAdvisor.dataCoverage_${dataCoverageLabel(candidate.confidence_grade)}`)}</span>
        </div>
        {candidate.cannibalization_score != null && (
          <div className="ea-candidate__metric" title={`${fmtScore(candidate.cannibalization_score)}/100`}>
            <span className="ea-candidate__metric-label">{t("expansionAdvisor.branchOverlap")}:</span>
            <span>{t(`expansionAdvisor.branchOverlap_${branchOverlapLabel(candidate.cannibalization_score)}`)}</span>
            <span className="ea-candidate__metric-raw">{fmtScore(candidate.cannibalization_score)}</span>
          </div>
        )}
        <div className="ea-candidate__metric">
          <span className="ea-candidate__metric-label">{t("expansionAdvisor.nearestBranch")}:</span>
          <span>{fmtMeters(candidate.distance_to_nearest_branch_m)}</span>
        </div>
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
