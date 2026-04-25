/**
 * Coerce values that arrive from the backend as either numbers or stringified
 * Decimals (e.g. "45.00" from SQLAlchemy Numeric columns) into a JS number.
 *
 * Returns `null` for null/undefined, empty/whitespace strings, and anything
 * that fails to parse — never NaN.
 */
export const toNumeric = (value: unknown): number | null => {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed === "") return null;
    const n = parseFloat(trimmed);
    return Number.isFinite(n) ? n : null;
  }
  return null;
};

/**
 * Coerce a candidate-shaped object's numeric fields in place. Fields that
 * are absent on the input are left absent on the output (so existing
 * `field != null` and optional-chaining checks keep working).
 *
 * Used at the API boundary by `normalize*Response` to neutralize the
 * Numeric-as-string serialization the backend uses for precision.
 */
export function coerceCandidateNumerics<T extends Record<string, unknown>>(
  candidate: T,
  fields: readonly string[],
): T {
  const out: Record<string, unknown> = { ...candidate };
  for (const key of fields) {
    if (key in out) {
      const v = out[key];
      // Preserve explicit null/undefined as-is so callers can still
      // distinguish "missing" from "zero" downstream.
      if (v == null) continue;
      out[key] = toNumeric(v);
    }
  }
  return out as T;
}

/**
 * Numeric fields on `ExpansionCandidate` that the backend serializes from
 * SQLAlchemy Numeric columns and therefore arrive as strings (e.g. "45.00").
 *
 * Keep this list in sync with the candidate shape — adding a Numeric column
 * on the backend means adding the field name here.
 */
export const CANDIDATE_NUMERIC_FIELDS = [
  "area_m2",
  "final_score",
  "economics_score",
  "brand_fit_score",
  "provider_density_score",
  "provider_whitespace_score",
  "multi_platform_presence_score",
  "delivery_competition_score",
  "demand_score",
  "whitespace_score",
  "fit_score",
  "zoning_fit_score",
  "frontage_score",
  "access_score",
  "parking_score",
  "access_visibility_score",
  "confidence_score",
  "cannibalization_score",
  "distance_to_nearest_branch_m",
  "estimated_rent_sar_m2_year",
  "estimated_annual_rent_sar",
  "estimated_fitout_cost_sar",
  "estimated_revenue_index",
  "display_annual_rent_sar",
  "unit_price_sar_annual",
  "unit_area_sqm",
  "unit_street_width_m",
] as const;
