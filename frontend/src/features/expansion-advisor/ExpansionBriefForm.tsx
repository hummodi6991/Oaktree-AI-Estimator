import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import type { ExpansionBrief } from "../../lib/api/expansionAdvisor";

export const defaultBrief: ExpansionBrief = {
  brand_name: "",
  category: "qsr",
  service_model: "qsr",
  min_area_m2: 100,
  max_area_m2: 500,
  target_area_m2: 200,
  target_districts: [],
  existing_branches: [],
  limit: 25,
  brand_profile: {
    primary_channel: "balanced",
    parking_sensitivity: "medium",
    frontage_sensitivity: "medium",
    visibility_sensitivity: "medium",
    expansion_goal: "balanced",
  },
};

export default function ExpansionBriefForm({ initialValue, onSubmit, loading }: { initialValue: ExpansionBrief; onSubmit: (brief: ExpansionBrief) => void; loading: boolean }) {
  const { t } = useTranslation();
  const [brief, setBrief] = useState<ExpansionBrief>(initialValue);
  const [branchesText, setBranchesText] = useState("");

  useEffect(() => setBrief(initialValue), [initialValue]);

  return (
    <form onSubmit={(e) => { e.preventDefault(); onSubmit(brief); }} style={{ display: "grid", gap: 8 }}>
      <h3>{t("expansionAdvisor.briefTitle")}</h3>
      <strong>{t("expansionAdvisor.brandBasics")}</strong>
      <input placeholder={t("expansionAdvisor.brandName")} value={brief.brand_name} onChange={(e) => setBrief({ ...brief, brand_name: e.target.value })} />
      <input placeholder={t("expansionAdvisor.category")} value={brief.category} onChange={(e) => setBrief({ ...brief, category: e.target.value })} />
      <select value={brief.brand_profile?.price_tier || ""} onChange={(e) => setBrief({ ...brief, brand_profile: { ...(brief.brand_profile || {}), price_tier: (e.target.value || null) as any } })}><option value="">{t("expansionAdvisor.priceTier")}</option><option value="value">{t("expansionAdvisor.value")}</option><option value="mid">{t("expansionAdvisor.mid")}</option><option value="premium">{t("expansionAdvisor.premium")}</option></select>
      <input type="number" placeholder={t("expansionAdvisor.averageCheck")} value={brief.brand_profile?.average_check_sar ?? ""} onChange={(e) => setBrief({ ...brief, brand_profile: { ...(brief.brand_profile || {}), average_check_sar: Number(e.target.value) || null } })} />
      <input placeholder={t("expansionAdvisor.targetCustomer")} value={brief.brand_profile?.target_customer ?? ""} onChange={(e) => setBrief({ ...brief, brand_profile: { ...(brief.brand_profile || {}), target_customer: e.target.value || null } })} />

      <strong>{t("expansionAdvisor.operatingStrategy")}</strong>
      <select value={brief.service_model} onChange={(e) => setBrief({ ...brief, service_model: e.target.value as ExpansionBrief["service_model"] })}><option value="qsr">QSR</option><option value="dine_in">Dine In</option><option value="delivery_first">Delivery First</option><option value="cafe">Cafe</option></select>
      <select value={brief.brand_profile?.primary_channel || "balanced"} onChange={(e) => setBrief({ ...brief, brand_profile: { ...(brief.brand_profile || {}), primary_channel: e.target.value as any } })}><option value="balanced">{t("expansionAdvisor.balanced")}</option><option value="dine_in">{t("expansionAdvisor.dineIn")}</option><option value="delivery">{t("expansionAdvisor.delivery")}</option></select>
      <select value={brief.brand_profile?.expansion_goal || "balanced"} onChange={(e) => setBrief({ ...brief, brand_profile: { ...(brief.brand_profile || {}), expansion_goal: e.target.value as any } })}><option value="balanced">{t("expansionAdvisor.balanced")}</option><option value="flagship">{t("expansionAdvisor.flagship")}</option><option value="neighborhood">{t("expansionAdvisor.neighborhood")}</option><option value="delivery_led">{t("expansionAdvisor.deliveryLed")}</option></select>
      <input type="number" placeholder={t("expansionAdvisor.cannibalizationTolerance")} value={brief.brand_profile?.cannibalization_tolerance_m ?? ""} onChange={(e) => setBrief({ ...brief, brand_profile: { ...(brief.brand_profile || {}), cannibalization_tolerance_m: Number(e.target.value) || null } })} />

      <strong>{t("expansionAdvisor.marketPreferences")}</strong>
      <select value={brief.brand_profile?.parking_sensitivity || "medium"} onChange={(e) => setBrief({ ...brief, brand_profile: { ...(brief.brand_profile || {}), parking_sensitivity: e.target.value as any } })}><option value="low">{t("expansionAdvisor.parkingLow")}</option><option value="medium">{t("expansionAdvisor.parkingMedium")}</option><option value="high">{t("expansionAdvisor.parkingHigh")}</option></select>
      <select value={brief.brand_profile?.frontage_sensitivity || "medium"} onChange={(e) => setBrief({ ...brief, brand_profile: { ...(brief.brand_profile || {}), frontage_sensitivity: e.target.value as any } })}><option value="low">{t("expansionAdvisor.frontageLow")}</option><option value="medium">{t("expansionAdvisor.frontageMedium")}</option><option value="high">{t("expansionAdvisor.frontageHigh")}</option></select>
      <select value={brief.brand_profile?.visibility_sensitivity || "medium"} onChange={(e) => setBrief({ ...brief, brand_profile: { ...(brief.brand_profile || {}), visibility_sensitivity: e.target.value as any } })}><option value="low">{t("expansionAdvisor.visibilityLow")}</option><option value="medium">{t("expansionAdvisor.visibilityMedium")}</option><option value="high">{t("expansionAdvisor.visibilityHigh")}</option></select>
      <input placeholder={t("expansionAdvisor.targetDistricts")} value={brief.target_districts.join(",")} onChange={(e) => setBrief({ ...brief, target_districts: e.target.value.split(",").map((d) => d.trim()).filter(Boolean) })} />
      <input placeholder={t("expansionAdvisor.preferredDistricts")} value={(brief.brand_profile?.preferred_districts || []).join(",")} onChange={(e) => setBrief({ ...brief, brand_profile: { ...(brief.brand_profile || {}), preferred_districts: e.target.value.split(",").map((d) => d.trim()).filter(Boolean) } })} />
      <input placeholder={t("expansionAdvisor.excludedDistricts")} value={(brief.brand_profile?.excluded_districts || []).join(",")} onChange={(e) => setBrief({ ...brief, brand_profile: { ...(brief.brand_profile || {}), excluded_districts: e.target.value.split(",").map((d) => d.trim()).filter(Boolean) } })} />

      <input type="number" placeholder={t("expansionAdvisor.minArea")} value={brief.min_area_m2} onChange={(e) => setBrief({ ...brief, min_area_m2: Number(e.target.value) })} />
      <input type="number" placeholder={t("expansionAdvisor.maxArea")} value={brief.max_area_m2} onChange={(e) => setBrief({ ...brief, max_area_m2: Number(e.target.value) })} />
      <input type="number" placeholder={t("expansionAdvisor.targetArea")} value={brief.target_area_m2 ?? ""} onChange={(e) => setBrief({ ...brief, target_area_m2: Number(e.target.value) })} />
      <textarea placeholder={t("expansionAdvisor.existingBranches")} value={branchesText} onChange={(e) => { const value = e.target.value; setBranchesText(value); const branches = value.split("\n").map((row) => row.trim()).filter(Boolean).map((row) => { const [name, lat, lon, district] = row.split(",").map((x) => x.trim()); return { name, lat: Number(lat), lon: Number(lon), district }; }).filter((b) => Number.isFinite(b.lat) && Number.isFinite(b.lon)); setBrief({ ...brief, existing_branches: branches }); }} />
      <input type="number" placeholder={t("expansionAdvisor.searchLimit")} value={brief.limit} onChange={(e) => setBrief({ ...brief, limit: Number(e.target.value) })} />
      <button type="submit" className="oak-btn oak-btn--primary" disabled={loading || !brief.brand_name}>{loading ? t("expansionAdvisor.loadingSearch") : t("expansionAdvisor.runSearch")}</button>
    </form>
  );
}
