import { useEffect, useMemo, useState } from "react";
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

export type BriefValidationErrors = {
  brand_name?: string;
  area_range?: string;
  branches?: string[];
};

export function validateBrief(brief: ExpansionBrief): BriefValidationErrors {
  const errors: BriefValidationErrors = {};
  if (!brief.brand_name.trim()) errors.brand_name = "validationRequired";
  if (brief.min_area_m2 > 0 && brief.max_area_m2 > 0 && brief.min_area_m2 > brief.max_area_m2) {
    errors.area_range = "validationAreaRange";
  }
  const branchErrors: string[] = [];
  for (let i = 0; i < (brief.existing_branches || []).length; i++) {
    const b = brief.existing_branches[i];
    if (b.lat !== 0 || b.lon !== 0) {
      if (b.lat < -90 || b.lat > 90) branchErrors[i] = "validationLatRange";
      else if (b.lon < -180 || b.lon > 180) branchErrors[i] = "validationLonRange";
    }
  }
  if (branchErrors.length > 0) errors.branches = branchErrors;
  return errors;
}

type Props = {
  initialValue: ExpansionBrief;
  onSubmit: (brief: ExpansionBrief) => void;
  loading: boolean;
};

export default function ExpansionBriefForm({ initialValue, onSubmit, loading }: Props) {
  const { t } = useTranslation();
  const [brief, setBrief] = useState<ExpansionBrief>(initialValue);
  const [touched, setTouched] = useState(false);

  useEffect(() => setBrief(initialValue), [initialValue]);

  const set = <K extends keyof ExpansionBrief>(key: K, value: ExpansionBrief[K]) =>
    setBrief((prev) => ({ ...prev, [key]: value }));

  const setProfile = (key: string, value: unknown) =>
    setBrief((prev) => ({ ...prev, brand_profile: { ...(prev.brand_profile || {}), [key]: value } }));

  const branches = brief.existing_branches || [];
  const errors = useMemo(() => validateBrief(brief), [brief]);
  const hasErrors = Boolean(errors.brand_name || errors.area_range || (errors.branches && errors.branches.length > 0));
  const showErrors = touched && hasErrors;

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setTouched(true);
    if (hasErrors) return;
    onSubmit(brief);
  };

  return (
    <form className="ea-form" onSubmit={handleSubmit}>
      {/* Brand basics */}
      <div className="ea-form__section">
        <h4 className="ea-form__section-title">{t("expansionAdvisor.brandBasics")}</h4>
        <div className="ea-form__row">
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.brandName")}</label>
            <input
              className={`ea-form__input${touched && errors.brand_name ? " ea-form__input--error" : ""}`}
              value={brief.brand_name}
              onChange={(e) => set("brand_name", e.target.value)}
              disabled={loading}
              placeholder="e.g. Al Baik, Kudu"
            />
            {touched && errors.brand_name && <span className="ea-form__error">{t(`expansionAdvisor.${errors.brand_name}`)}</span>}
          </div>
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.category")}</label>
            <input className="ea-form__input" value={brief.category} onChange={(e) => set("category", e.target.value)} disabled={loading} placeholder="e.g. Burgers, Coffee" />
          </div>
        </div>
        <div className="ea-form__row">
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.serviceModel")}</label>
            <select className="ea-form__select" value={brief.service_model} onChange={(e) => set("service_model", e.target.value as ExpansionBrief["service_model"])} disabled={loading}>
              <option value="qsr">{t("expansionAdvisor.qsr")}</option>
              <option value="dine_in">{t("expansionAdvisor.dineIn")}</option>
              <option value="delivery_first">{t("expansionAdvisor.deliveryFirst")}</option>
              <option value="cafe">{t("expansionAdvisor.cafe")}</option>
            </select>
          </div>
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.priceTier")}</label>
            <select className="ea-form__select" value={brief.brand_profile?.price_tier || ""} onChange={(e) => setProfile("price_tier", e.target.value || null)} disabled={loading}>
              <option value="">{t("common.notAvailable")}</option>
              <option value="value">{t("expansionAdvisor.value")}</option>
              <option value="mid">{t("expansionAdvisor.mid")}</option>
              <option value="premium">{t("expansionAdvisor.premium")}</option>
            </select>
          </div>
        </div>
        <div className="ea-form__row">
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.averageCheck")}</label>
            <input className="ea-form__input" type="number" value={brief.brand_profile?.average_check_sar ?? ""} onChange={(e) => setProfile("average_check_sar", Number(e.target.value) || null)} disabled={loading} />
          </div>
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.targetCustomer")}</label>
            <input className="ea-form__input" value={brief.brand_profile?.target_customer ?? ""} onChange={(e) => setProfile("target_customer", e.target.value || null)} disabled={loading} placeholder="e.g. Families, Young professionals" />
          </div>
        </div>
      </div>

      {/* Operating strategy */}
      <div className="ea-form__section">
        <h4 className="ea-form__section-title">{t("expansionAdvisor.operatingStrategy")}</h4>
        <div className="ea-form__row">
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.primaryChannel")}</label>
            <select className="ea-form__select" value={brief.brand_profile?.primary_channel || "balanced"} onChange={(e) => setProfile("primary_channel", e.target.value)} disabled={loading}>
              <option value="balanced">{t("expansionAdvisor.balanced")}</option>
              <option value="dine_in">{t("expansionAdvisor.dineIn")}</option>
              <option value="delivery">{t("expansionAdvisor.delivery")}</option>
            </select>
          </div>
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.expansionGoal")}</label>
            <select className="ea-form__select" value={brief.brand_profile?.expansion_goal || "balanced"} onChange={(e) => setProfile("expansion_goal", e.target.value)} disabled={loading}>
              <option value="balanced">{t("expansionAdvisor.balanced")}</option>
              <option value="flagship">{t("expansionAdvisor.flagship")}</option>
              <option value="neighborhood">{t("expansionAdvisor.neighborhood")}</option>
              <option value="delivery_led">{t("expansionAdvisor.deliveryLed")}</option>
            </select>
          </div>
        </div>
        <div className="ea-form__row">
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.cannibalizationTolerance")}</label>
            <input className="ea-form__input" type="number" value={brief.brand_profile?.cannibalization_tolerance_m ?? ""} onChange={(e) => setProfile("cannibalization_tolerance_m", Number(e.target.value) || null)} disabled={loading} />
          </div>
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.searchLimit")}</label>
            <input className="ea-form__input" type="number" value={brief.limit} onChange={(e) => set("limit", Number(e.target.value) || 25)} disabled={loading} min={1} max={100} />
          </div>
        </div>
      </div>

      {/* Market preferences */}
      <div className="ea-form__section">
        <h4 className="ea-form__section-title">{t("expansionAdvisor.marketPreferences")}</h4>
        <div className="ea-form__row">
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.parkingSensitivity")}</label>
            <select className="ea-form__select" value={brief.brand_profile?.parking_sensitivity || "medium"} onChange={(e) => setProfile("parking_sensitivity", e.target.value)} disabled={loading}>
              <option value="low">{t("expansionAdvisor.low")}</option>
              <option value="medium">{t("expansionAdvisor.medium")}</option>
              <option value="high">{t("expansionAdvisor.high")}</option>
            </select>
          </div>
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.frontageSensitivity")}</label>
            <select className="ea-form__select" value={brief.brand_profile?.frontage_sensitivity || "medium"} onChange={(e) => setProfile("frontage_sensitivity", e.target.value)} disabled={loading}>
              <option value="low">{t("expansionAdvisor.low")}</option>
              <option value="medium">{t("expansionAdvisor.medium")}</option>
              <option value="high">{t("expansionAdvisor.high")}</option>
            </select>
          </div>
        </div>
        <div className="ea-form__field">
          <label className="ea-form__label">{t("expansionAdvisor.visibilitySensitivity")}</label>
          <select className="ea-form__select" value={brief.brand_profile?.visibility_sensitivity || "medium"} onChange={(e) => setProfile("visibility_sensitivity", e.target.value)} disabled={loading}>
            <option value="low">{t("expansionAdvisor.low")}</option>
            <option value="medium">{t("expansionAdvisor.medium")}</option>
            <option value="high">{t("expansionAdvisor.high")}</option>
          </select>
        </div>
      </div>

      {/* Unit sizing */}
      <div className="ea-form__section">
        <h4 className="ea-form__section-title">{t("expansionAdvisor.unitSizing")}</h4>
        <div className="ea-form__row">
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.minArea")}</label>
            <input className={`ea-form__input${touched && errors.area_range ? " ea-form__input--error" : ""}`} type="number" value={brief.min_area_m2} onChange={(e) => set("min_area_m2", Number(e.target.value))} disabled={loading} min={0} />
          </div>
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.maxArea")}</label>
            <input className={`ea-form__input${touched && errors.area_range ? " ea-form__input--error" : ""}`} type="number" value={brief.max_area_m2} onChange={(e) => set("max_area_m2", Number(e.target.value))} disabled={loading} min={0} />
          </div>
        </div>
        {touched && errors.area_range && <span className="ea-form__error">{t(`expansionAdvisor.${errors.area_range}`)}</span>}
        <div className="ea-form__field">
          <label className="ea-form__label">{t("expansionAdvisor.targetArea")}</label>
          <input className="ea-form__input" type="number" value={brief.target_area_m2 ?? ""} onChange={(e) => set("target_area_m2", Number(e.target.value) || null)} disabled={loading} min={0} />
        </div>
      </div>

      {/* Geography */}
      <div className="ea-form__section">
        <h4 className="ea-form__section-title">{t("expansionAdvisor.geography")}</h4>
        <div className="ea-form__field">
          <label className="ea-form__label">{t("expansionAdvisor.targetDistricts")}</label>
          <input className="ea-form__input" value={brief.target_districts.join(", ")} onChange={(e) => set("target_districts", e.target.value.split(",").map((d) => d.trim()).filter(Boolean))} disabled={loading} placeholder="e.g. Al Olaya, Al Malqa, Al Nakheel" />
        </div>
        <div className="ea-form__row">
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.preferredDistricts")}</label>
            <input className="ea-form__input" value={(brief.brand_profile?.preferred_districts || []).join(", ")} onChange={(e) => setProfile("preferred_districts", e.target.value.split(",").map((d) => d.trim()).filter(Boolean))} disabled={loading} />
          </div>
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.excludedDistricts")}</label>
            <input className="ea-form__input" value={(brief.brand_profile?.excluded_districts || []).join(", ")} onChange={(e) => setProfile("excluded_districts", e.target.value.split(",").map((d) => d.trim()).filter(Boolean))} disabled={loading} />
          </div>
        </div>
      </div>

      {/* Existing branches */}
      <div className="ea-form__section">
        <h4 className="ea-form__section-title">{t("expansionAdvisor.existingBranchesLabel")}</h4>
        <div className="ea-branch-list">
          {branches.map((branch, index) => {
            const branchError = errors.branches?.[index];
            return (
              <div key={index} className={`ea-branch-row${branchError ? " ea-branch-row--error" : ""}`}>
                <input placeholder={t("expansionAdvisor.branchName")} value={branch.name || ""} onChange={(e) => { const next = [...branches]; next[index] = { ...next[index], name: e.target.value }; set("existing_branches", next); }} disabled={loading} />
                <input type="number" placeholder={t("expansionAdvisor.branchLat")} value={branch.lat ?? ""} step="any" onChange={(e) => { const next = [...branches]; next[index] = { ...next[index], lat: Number(e.target.value) }; set("existing_branches", next); }} disabled={loading} />
                <input type="number" placeholder={t("expansionAdvisor.branchLon")} value={branch.lon ?? ""} step="any" onChange={(e) => { const next = [...branches]; next[index] = { ...next[index], lon: Number(e.target.value) }; set("existing_branches", next); }} disabled={loading} />
                <input placeholder={t("expansionAdvisor.branchDistrict")} value={branch.district || ""} onChange={(e) => { const next = [...branches]; next[index] = { ...next[index], district: e.target.value }; set("existing_branches", next); }} disabled={loading} />
                <button type="button" className="oak-btn oak-btn--sm oak-btn--tertiary" onClick={() => set("existing_branches", branches.filter((_, i) => i !== index))} disabled={loading}>{t("expansionAdvisor.removeBranch")}</button>
                {touched && branchError && <span className="ea-form__error">{t(`expansionAdvisor.${branchError}`)}</span>}
              </div>
            );
          })}
          {branches.length === 0 && <p style={{ margin: 0, fontSize: "var(--oak-fs-xs)", color: "var(--oak-text-light)" }}>{t("expansionAdvisor.noBranchesYet")}</p>}
          <button type="button" className="oak-btn oak-btn--sm oak-btn--tertiary" onClick={() => set("existing_branches", [...branches, { name: "", lat: 24.7136, lon: 46.6753, district: "" }])} disabled={loading}>+ {t("expansionAdvisor.addBranch")}</button>
        </div>
      </div>

      {showErrors && <div className="ea-form__validation-summary">{t("expansionAdvisor.validationRequired")}</div>}
      <button type="submit" className="oak-btn oak-btn--primary" disabled={loading || !brief.brand_name.trim()}>{loading ? t("expansionAdvisor.searchingCta") : t("expansionAdvisor.runSearchCta")}</button>
    </form>
  );
}
