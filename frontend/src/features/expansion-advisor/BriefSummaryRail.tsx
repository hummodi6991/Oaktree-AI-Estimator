import { useTranslation } from "react-i18next";
import type { ExpansionBrief } from "../../lib/api/expansionAdvisor";
import { safeDistrictLabel } from "./formatHelpers";

type Props = {
  brief: ExpansionBrief;
  onEditBrief: () => void;
  onRunAgain: () => void;
  loading: boolean;
};

export default function BriefSummaryRail({ brief, onEditBrief, onRunAgain, loading }: Props) {
  const { t } = useTranslation();
  const profile = brief.brand_profile || {};
  const branchCount = (brief.existing_branches || []).filter(
    (b) => Number.isFinite(b.lat) && Number.isFinite(b.lon) && (b.lat !== 0 || b.lon !== 0),
  ).length;

  return (
    <div className="ea-brief-rail">
      <div className="ea-brief-rail__header">
        <h4 className="ea-brief-rail__title">{t("expansionAdvisor.brandBriefSummary")}</h4>
        <div className="ea-brief-rail__actions">
          <button
            type="button"
            className="oak-btn oak-btn--sm oak-btn--tertiary"
            onClick={onEditBrief}
          >
            {t("expansionAdvisor.editBrief")}
          </button>
          <button
            type="button"
            className="oak-btn oak-btn--sm oak-btn--primary"
            onClick={onRunAgain}
            disabled={loading}
          >
            {loading ? t("expansionAdvisor.searchingCta") : t("expansionAdvisor.runAgain")}
          </button>
        </div>
      </div>
      <div className="ea-brief-rail__items">
        <div className="ea-brief-rail__item">
          <span className="ea-brief-rail__label">{t("expansionAdvisor.brandName")}</span>
          <span className="ea-brief-rail__value">{brief.brand_name || "—"}</span>
        </div>
        <div className="ea-brief-rail__item">
          <span className="ea-brief-rail__label">{t("expansionAdvisor.category")}</span>
          <span className="ea-brief-rail__value">{brief.category || "—"}</span>
        </div>
        <div className="ea-brief-rail__item">
          <span className="ea-brief-rail__label">{t("expansionAdvisor.serviceModel")}</span>
          <span className="ea-brief-rail__value">{t(`expansionAdvisor.${brief.service_model === "delivery_first" ? "deliveryFirst" : brief.service_model === "dine_in" ? "dineIn" : brief.service_model}`)}</span>
        </div>
        <div className="ea-brief-rail__item">
          <span className="ea-brief-rail__label">{t("expansionAdvisor.areaRange")}</span>
          <span className="ea-brief-rail__value">
            {brief.min_area_m2}–{brief.max_area_m2} m²
            {brief.target_area_m2 ? ` (${t("expansionAdvisor.target")}: ${brief.target_area_m2} m²)` : ""}
          </span>
        </div>
        {brief.target_districts.length > 0 && (
          <div className="ea-brief-rail__item">
            <span className="ea-brief-rail__label">{t("expansionAdvisor.targetDistrictsLabel")}</span>
            <span className="ea-brief-rail__value">{brief.target_districts.map((d) => safeDistrictLabel(d, null, d)).join(", ")}</span>
          </div>
        )}
        <div className="ea-brief-rail__item">
          <span className="ea-brief-rail__label">{t("expansionAdvisor.existingBranchCount")}</span>
          <span className="ea-brief-rail__value">{branchCount}</span>
        </div>
        {profile.cannibalization_tolerance_m != null && (
          <div className="ea-brief-rail__item">
            <span className="ea-brief-rail__label">{t("expansionAdvisor.cannibalizationTolerance")}</span>
            <span className="ea-brief-rail__value">{profile.cannibalization_tolerance_m} m</span>
          </div>
        )}
        {profile.price_tier && (
          <div className="ea-brief-rail__item">
            <span className="ea-brief-rail__label">{t("expansionAdvisor.priceTier")}</span>
            <span className="ea-brief-rail__value">{t(`expansionAdvisor.${profile.price_tier}`)}</span>
          </div>
        )}
        {profile.primary_channel && (
          <div className="ea-brief-rail__item">
            <span className="ea-brief-rail__label">{t("expansionAdvisor.primaryChannel")}</span>
            <span className="ea-brief-rail__value">{t(`expansionAdvisor.${profile.primary_channel === "dine_in" ? "dineIn" : profile.primary_channel}`)}</span>
          </div>
        )}
      </div>
      {branchCount > 0 && (
        <div className="ea-brief-rail__branches">
          <span className="ea-brief-rail__label">{t("expansionAdvisor.existingBranchesLabel")}</span>
          <div className="ea-brief-rail__branch-chips">
            {brief.existing_branches
              .filter((b) => Number.isFinite(b.lat) && Number.isFinite(b.lon) && (b.lat !== 0 || b.lon !== 0))
              .map((b, i) => (
                <span key={i} className="ea-brief-rail__chip">
                  {b.name || b.district || `${b.lat.toFixed(3)}, ${b.lon.toFixed(3)}`}
                </span>
              ))}
          </div>
        </div>
      )}
    </div>
  );
}
