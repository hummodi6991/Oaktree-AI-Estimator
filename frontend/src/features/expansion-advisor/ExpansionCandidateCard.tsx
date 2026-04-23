import { useTranslation } from "react-i18next";
import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";
import TierBadge from "./TierBadge";
import { fmtSARCompact, fmtM2, fmtMeters, candidateDistrictLabel, getDisplayScore } from "./formatHelpers";
import type { MemoDrawerSection } from "./ExpansionMemoPanel";
import { classifyCandidateTier, type CandidateTier } from "./tiers";

type Props = {
  candidate: ExpansionCandidate;
  selected: boolean;
  shortlisted: boolean;
  compared: boolean;
  isLead?: boolean;
  localSortActive?: boolean;
  /** Visual tier assignment. Derived at render time from candidate fields
   *  (see tiers.ts). Premier gets an accent treatment + "Premier" pill,
   *  Exploratory renders muted, Standard is the baseline unchanged. */
  tier?: CandidateTier;
  onSelect: () => void;
  onCompareToggle: () => void;
  onOpenMemo?: (options?: { section?: MemoDrawerSection }) => void;
  onShowOnMap?: () => void;
  /** Retained for backward compatibility with tests and saved-study restoration;
   *  Patch 16 removed the in-card shortlist button so this is never invoked. */
  onToggleShortlist?: () => void;
};

export default function ExpansionCandidateCard({
  candidate,
  selected,
  shortlisted,
  compared,
  isLead,
  localSortActive,
  tier,
  onSelect,
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

  // Phase 4 — listing recency + district momentum pills.
  // Thresholds mirror the backend call site in _top_positives_and_risks;
  // see _LISTING_FRESHNESS_DAYS and _MOMENTUM_DISPLAY_THRESHOLD in
  // app/services/expansion_advisor.py. The two must match numerically
  // by convention, not by shared config.
  const LISTING_FRESHNESS_DAYS = 7;
  const MOMENTUM_DISPLAY_THRESHOLD = 70;
  type ListingAge = {
    effective_age_days?: number | null;
    source?: string | null;
    created_days?: number | null;
    updated_days?: number | null;
  };
  type DistrictMomentum = { momentum_score?: number | null; sample_floor_applied?: boolean | null };
  const listingAge = candidate.feature_snapshot_json?.listing_age as ListingAge | undefined;
  const momentum = candidate.feature_snapshot_json?.district_momentum as DistrictMomentum | undefined;
  // Read created_days and updated_days independently. The GREATEST-based
  // `source` field is retained for memo/rerank back-compat but must NOT
  // drive the pill: the scraper's daily cadence makes aqar_updated win
  // the tie-break on ~93% of rows, which would hide the "New" pill on
  // genuinely new listings.
  const createdDays = typeof listingAge?.created_days === "number" ? listingAge.created_days : null;
  const updatedDays = typeof listingAge?.updated_days === "number" ? listingAge.updated_days : null;
  const freshness: "new" | "updated" | null =
    createdDays !== null && createdDays <= LISTING_FRESHNESS_DAYS ? "new"
    : updatedDays !== null && updatedDays <= LISTING_FRESHNESS_DAYS ? "updated"
    : null;
  const showTopTierMarket =
    typeof momentum?.momentum_score === "number"
    && momentum.momentum_score >= MOMENTUM_DISPLAY_THRESHOLD
    && momentum.sample_floor_applied === false;

  // Tier is derived from candidate fields when the parent doesn't pass it
  // explicitly. The parent (ExpansionResultsPanel) always passes it today,
  // but keeping the fallback keeps the card usable in isolation (e.g. in
  // tests and any future caller that renders a single card).
  const resolvedTier: CandidateTier = tier ?? classifyCandidateTier(candidate);
  const isPremier = resolvedTier === "premier";
  const isExploratory = resolvedTier === "exploratory";

  const cls = [
    "ea-candidate",
    selected && "ea-candidate--selected",
    shortlisted && "ea-candidate--shortlisted",
    compared && "ea-candidate--compared",
    isLead && "ea-candidate--lead",
    isTop3 && "ea-candidate--top3",
    isCommercialUnit && "ea-candidate--commercial-unit",
    isPremier && "ea-candidate--premier",
    isExploratory && "ea-candidate--exploratory",
  ]
    .filter(Boolean)
    .join(" ");

  // "Why #N" chip — jumps to the Decision Memo's Ranking-logic card.
  // Rendered only when final_rank is a usable number; a fallback "Why #?"
  // is worse than no chip.
  const finalRank = candidate.final_rank;
  const hasFinalRank = typeof finalRank === "number" && Number.isFinite(finalRank);
  const rerankDelta = typeof candidate.rerank_delta === "number" ? candidate.rerank_delta : 0;
  const showDelta =
    candidate.rerank_applied === true &&
    candidate.rerank_status === "applied" &&
    rerankDelta !== 0;
  const whyChipArrow = showDelta ? (rerankDelta < 0 ? "↑" : "↓") : "";
  const whyChipMagnitude = showDelta ? Math.abs(rerankDelta) : 0;
  const whyChipLabel = hasFinalRank
    ? showDelta
      ? t("expansionAdvisor.whyRankMoved", {
          rank: finalRank,
          arrow: whyChipArrow,
          delta: whyChipMagnitude,
        })
      : t("expansionAdvisor.whyRank", { rank: finalRank })
    : "";
  const whyChipTitle = showDelta ? candidate.rerank_reason?.summary || undefined : undefined;
  const whyChipDisabled = !onOpenMemo;
  const whyChipClass = [
    "oak-btn",
    "oak-btn--xs",
    "oak-btn--tertiary",
    "ea-candidate__why-chip",
    whyChipDisabled && "ea-candidate__why-chip--disabled",
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
          {isPremier && (
            <span
              className="ea-badge ea-badge--premier ea-candidate__premier-pill"
              title={t("expansionAdvisor.premierBadgeTooltip")}
            >
              {t("expansionAdvisor.premierBadge")}
            </span>
          )}
          <ScorePill value={getDisplayScore(candidate)} />
          {showNearestBranch && (
            <span className="ea-badge ea-badge--neutral ea-candidate__nearest-pill">
              {fmtMeters(nearestBranchM)}
            </span>
          )}
          {freshness === "new" && (
            <span
              className="ea-badge ea-badge--green ea-candidate__freshness-pill"
              title={t("expansionAdvisor.newBadgeTooltip")}
            >
              {t("expansionAdvisor.newBadge")}
            </span>
          )}
          {freshness === "updated" && (
            <span
              className="ea-badge ea-badge--green ea-candidate__freshness-pill"
              title={t("expansionAdvisor.updatedBadgeTooltip")}
            >
              {t("expansionAdvisor.updatedBadge")}
            </span>
          )}
          {showTopTierMarket && (
            <span
              className="ea-badge ea-badge--amber ea-candidate__momentum-pill"
              title={t("expansionAdvisor.topTierMarketTooltip")}
            >
              {t("expansionAdvisor.topTierMarketTag")}
            </span>
          )}
        </div>
      </div>

      {/* "Why #N" chip — opens Decision Memo scrolled to Ranking logic. */}
      {hasFinalRank && (
        <div className="ea-candidate__why-row" onClick={(e) => e.stopPropagation()}>
          <button
            type="button"
            className={whyChipClass}
            title={whyChipTitle}
            aria-disabled={whyChipDisabled ? "true" : undefined}
            disabled={whyChipDisabled}
            onClick={whyChipDisabled
              ? undefined
              : (e) => {
                  e.stopPropagation();
                  onOpenMemo?.({ section: "decision-logic" });
                }}
          >
            {whyChipLabel}
          </button>
        </div>
      )}

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
        <button type="button" className="oak-btn oak-btn--sm oak-btn--primary" onClick={() => (onOpenMemo ? onOpenMemo() : onSelect())}>
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
        </div>
      </div>
    </div>
  );
}
