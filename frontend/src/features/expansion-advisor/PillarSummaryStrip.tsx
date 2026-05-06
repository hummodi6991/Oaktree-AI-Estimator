import { useTranslation } from "react-i18next";
import type {
  ExpansionAdvisorMeta,
  ExpansionAdvisorDemoteLegDrops,
  ExpansionAdvisorDemoteLegThresholds,
  ExpansionAdvisorDemoteLegEnabled,
  ExpansionAdvisorHardFloorDrops,
  ExpansionAdvisorHardFloorThresholds,
} from "../../lib/api/expansionAdvisor";

type Props = {
  meta?: ExpansionAdvisorMeta | null;
};

/* ─── Pillar Summary Strip ──────────────────────────────────────────────── */

// Renders three tiles summarizing the three-pillar directive. Hidden when no
// demote-leg diagnostics are present; otherwise each tile renders even if its
// leg fired zero demotes — the directive being visible is the feature.
export function PillarSummaryStrip({ meta }: Props) {
  const { t } = useTranslation();

  const demoteDrops: ExpansionAdvisorDemoteLegDrops | null | undefined =
    meta?.demote_leg_drops;
  const demoteThresholds: ExpansionAdvisorDemoteLegThresholds | null | undefined =
    meta?.demote_leg_thresholds;
  const demoteEnabled: ExpansionAdvisorDemoteLegEnabled | null | undefined =
    meta?.demote_leg_enabled;
  const hardFloorDrops: ExpansionAdvisorHardFloorDrops | null | undefined =
    meta?.hard_floor_drops;
  const hardFloorThresholds: ExpansionAdvisorHardFloorThresholds | null | undefined =
    meta?.hard_floor_thresholds;

  if (!demoteDrops) {
    return null;
  }

  const radianceEnabled = demoteEnabled?.radiance_growth !== false;

  const popHardFloorDropped = hardFloorDrops?.dropped_population ?? 0;
  const popSoftDemoted = demoteDrops.dropped_population ?? 0;
  const popHardThreshold = hardFloorThresholds?.hard_floor_pop_threshold;
  const popPercentile = demoteThresholds?.pop_percentile;

  const brandHardFloorDropped = hardFloorDrops?.dropped_commercial ?? 0;
  const demandSoftDemoted = demoteDrops.dropped_demand ?? 0;
  const brandHardThreshold = hardFloorThresholds?.hard_floor_brand_threshold;
  const demandPercentile = demoteThresholds?.demand_percentile;

  const radianceSoftDemoted = demoteDrops.dropped_radiance_growth ?? 0;
  const radianceThreshold = demoteThresholds?.radiance_yoy_demote_threshold;

  return (
    <div
      className="ea-pillar-summary"
      role="group"
      aria-label={t("expansionAdvisor.pillarStripAriaLabel")}
    >
      {/* Pillar 1 — Well-populated areas */}
      <div className="ea-pillar-summary__tile">
        <h4 className="ea-pillar-summary__title">
          {t("expansionAdvisor.pillarPopulationTitle")}
        </h4>
        <div className="ea-pillar-summary__row">
          <span className="ea-pillar-summary__metric">
            {t("expansionAdvisor.pillarPopulationHardFloor", {
              count: popHardFloorDropped,
              threshold: popHardThreshold ?? "—",
            })}
          </span>
        </div>
        <div className="ea-pillar-summary__row">
          <span className="ea-pillar-summary__metric">
            {t("expansionAdvisor.pillarPopulationSoftDemote", {
              count: popSoftDemoted,
              percentile:
                popPercentile != null ? Math.round(popPercentile * 100) : "—",
            })}
          </span>
        </div>
      </div>

      {/* Pillar 2 — Strong sales potential */}
      <div className="ea-pillar-summary__tile">
        <h4 className="ea-pillar-summary__title">
          {t("expansionAdvisor.pillarSalesTitle")}
        </h4>
        <div className="ea-pillar-summary__row">
          <span className="ea-pillar-summary__metric">
            {t("expansionAdvisor.pillarSalesHardFloor", {
              count: brandHardFloorDropped,
              threshold: brandHardThreshold ?? "—",
            })}
          </span>
        </div>
        <div className="ea-pillar-summary__row">
          <span className="ea-pillar-summary__metric">
            {t("expansionAdvisor.pillarSalesSoftDemote", {
              count: demandSoftDemoted,
              percentile:
                demandPercentile != null
                  ? Math.round(demandPercentile * 100)
                  : "—",
            })}
          </span>
        </div>
      </div>

      {/* Pillar 3 — Business growth */}
      <div className="ea-pillar-summary__tile">
        <h4 className="ea-pillar-summary__title">
          {t("expansionAdvisor.pillarGrowthTitle")}
          {!radianceEnabled && (
            <span className="ea-pillar-summary__disabled-badge">
              {t("expansionAdvisor.pillarDisabledBadge")}
            </span>
          )}
        </h4>
        <div className="ea-pillar-summary__row">
          <span className="ea-pillar-summary__metric">
            {t("expansionAdvisor.pillarGrowthSoftDemote", {
              count: radianceSoftDemoted,
              threshold: radianceThreshold ?? "—",
            })}
          </span>
        </div>
      </div>
    </div>
  );
}

export default PillarSummaryStrip;
