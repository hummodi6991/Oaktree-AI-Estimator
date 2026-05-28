import type {
  CandidateFeatureSnapshot,
  CandidateScoreBreakdown,
  ExpansionCandidate,
} from "../../lib/api/expansionAdvisor";

/* ─── Source tokens ────────────────────────────────────────────────────────
 *
 * Every input attribution renders against one of these tokens. The token
 * is mapped to a human label via i18n key `expansionAdvisor.scoreSources.<token>`
 * (see en.json / ar.json).
 *
 * Tokens are stable identifiers — the i18n label can change without
 * touching the score-input wiring.
 */
export type SourceToken =
  | "aqar"
  | "bayut"
  | "google_places"
  | "black_marble"
  | "osm"
  | "arcgis_riyadh_parcels"
  | "operator_brief"
  | "hungerstation"
  | "talabat"
  | "mrsool"
  | "population_grid"
  | "oaktree_llm"
  | "oaktree_internal"
  | "expansion_road_context"
  | "expansion_parking_asset"
  | "expansion_delivery_market";

export type ResolvedInputValue = string | number | boolean | null;

export type ResolvedInput = {
  value: ResolvedInputValue;
  source: SourceToken;
};

export type ResolveCtx = {
  candidate: Partial<ExpansionCandidate> | Record<string, unknown>;
  scoreBreakdown: CandidateScoreBreakdown | undefined;
  featureSnapshot: CandidateFeatureSnapshot | Record<string, unknown> | undefined;
  contextSources: Record<string, unknown>;
};

export type InputDescriptor = {
  /** i18n key suffix under expansionAdvisor.scoreComponents.<comp>.inputs */
  key: string;
  resolve: (ctx: ResolveCtx) => ResolvedInput;
};

/* ─── Defensive readers ──────────────────────────────────────────────────── */

function asNumber(v: unknown): number | null {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  return null;
}
function asString(v: unknown): string | null {
  if (typeof v === "string" && v.length > 0) return v;
  return null;
}
function asBool(v: unknown): boolean | null {
  if (typeof v === "boolean") return v;
  return null;
}
function readNested(obj: unknown, ...path: string[]): unknown {
  let cur: unknown = obj;
  for (const k of path) {
    if (cur && typeof cur === "object" && k in (cur as Record<string, unknown>)) {
      cur = (cur as Record<string, unknown>)[k];
    } else {
      return undefined;
    }
  }
  return cur;
}

/* ─── Per-source override resolution ─────────────────────────────────────── */

/**
 * Several inputs have conditional sources surfaced through
 * feature_snapshot_json.context_sources.<X>_source. When the override is
 * one of the known infrastructure tokens, use it; otherwise fall back to
 * the supplied default.
 */
function overrideSource(
  contextSources: Record<string, unknown>,
  field: string,
  fallback: SourceToken,
): SourceToken {
  const raw = contextSources[field];
  if (typeof raw !== "string" || raw.length === 0) return fallback;
  switch (raw) {
    case "aqar":
      return "aqar";
    case "bayut":
      return "bayut";
    case "google_places":
      return "google_places";
    case "expansion_road_context":
      return "expansion_road_context";
    case "expansion_parking_asset":
      return "expansion_parking_asset";
    case "expansion_delivery_market":
      return "expansion_delivery_market";
    case "hungerstation":
      return "hungerstation";
    case "talabat":
      return "talabat";
    case "mrsool":
      return "mrsool";
    case "osm":
      return "osm";
    case "arcgis_riyadh_parcels":
      return "arcgis_riyadh_parcels";
    case "operator_brief":
      return "operator_brief";
    case "black_marble":
      return "black_marble";
    case "population_grid":
      return "population_grid";
    case "oaktree_llm":
      return "oaktree_llm";
    case "oaktree_internal":
      return "oaktree_internal";
    default:
      return fallback;
  }
}

/**
 * Resolve the candidate's listing platform ("aqar" | "bayut") from the
 * candidate record. Returns null for non-listing candidates so callers can
 * fall back to the legacy "aqar" default.
 */
function candidatePlatform(
  candidate: Partial<ExpansionCandidate> | Record<string, unknown>,
): "aqar" | "bayut" | null {
  const p = (candidate as Record<string, unknown>).platform;
  return p === "aqar" || p === "bayut" ? p : null;
}

/**
 * Source resolution for rent-derived inputs. The raw rent_source value is
 * the listing-economics tag ("commercial_unit_actual" / "+micro"), which is
 * never in overrideSource()'s infrastructure allowlist — those inputs are
 * sourced from the listing itself, so they attribute to the candidate's
 * platform. Falls back to "aqar" when the platform is unknown.
 */
function resolveRentSource(
  contextSources: Record<string, unknown>,
  platform: "aqar" | "bayut" | null,
): SourceToken {
  const fallback: SourceToken = platform ?? "aqar";
  const raw = contextSources["rent_source"];
  if (
    typeof raw === "string" &&
    (raw === "commercial_unit_actual" || raw === "commercial_unit_actual+micro")
  ) {
    return fallback;
  }
  return overrideSource(contextSources, "rent_source", fallback);
}

/* ─── Component → inputs map ─────────────────────────────────────────────── */

export const PER_COMPONENT_INPUTS: Record<string, InputDescriptor[]> = {
  occupancy_economics: [
    {
      key: "estimated_annual_rent_sar",
      resolve: ({ candidate, contextSources }) => ({
        value: asNumber((candidate as Record<string, unknown>).estimated_annual_rent_sar),
        source: resolveRentSource(contextSources, candidatePlatform(candidate)),
      }),
    },
    {
      key: "estimated_fitout_cost_sar",
      resolve: ({ candidate }) => ({
        value: asNumber((candidate as Record<string, unknown>).estimated_fitout_cost_sar),
        source: "operator_brief",
      }),
    },
    {
      key: "area_m2",
      resolve: ({ candidate }) => {
        const c = candidate as Record<string, unknown>;
        const v = asNumber(c.area_m2) ?? asNumber(c.unit_area_sqm);
        return { value: v, source: "arcgis_riyadh_parcels" };
      },
    },
    {
      key: "cannibalization_score",
      resolve: ({ candidate }) => ({
        value: asNumber((candidate as Record<string, unknown>).cannibalization_score),
        source: "operator_brief",
      }),
    },
    {
      key: "rent_burden_percentile",
      resolve: ({ candidate, scoreBreakdown, contextSources }) => {
        const v = asNumber(readNested(scoreBreakdown, "economics_detail", "rent_burden", "percentile"));
        return {
          value: v != null ? Math.round(v * 100) / 100 : null,
          source: resolveRentSource(contextSources, candidatePlatform(candidate)),
        };
      },
    },
  ],

  listing_quality: [
    {
      key: "effective_age_days",
      resolve: ({ candidate, featureSnapshot }) => {
        const la = readNested(featureSnapshot, "listing_age") as
          | Record<string, unknown>
          | undefined;
        const v =
          asNumber(la?.effective_age_days) ??
          asNumber(la?.updated_days) ??
          asNumber(la?.created_days);
        return {
          value: v,
          source: candidatePlatform(candidate) ?? "aqar",
        };
      },
    },
    {
      key: "has_image",
      resolve: ({ candidate }) => ({
        value: Boolean((candidate as Record<string, unknown>).image_url),
        source: candidatePlatform(candidate) ?? "aqar",
      }),
    },
    {
      key: "llm_suitability_score",
      resolve: ({ featureSnapshot }) => ({
        value: asNumber(readNested(featureSnapshot, "listing_quality_signals", "llm_suitability_score")),
        source: "oaktree_llm",
      }),
    },
    {
      key: "llm_listing_quality_score",
      resolve: ({ featureSnapshot }) => ({
        value: asNumber(readNested(featureSnapshot, "listing_quality_signals", "llm_listing_quality_score")),
        source: "oaktree_llm",
      }),
    },
    {
      key: "is_furnished",
      resolve: ({ candidate, featureSnapshot }) => ({
        value: asBool(readNested(featureSnapshot, "listing_quality_signals", "is_furnished")),
        source: candidatePlatform(candidate) ?? "aqar",
      }),
    },
    {
      key: "has_drive_thru",
      resolve: ({ candidate, featureSnapshot }) => ({
        value: asBool(readNested(featureSnapshot, "listing_quality_signals", "has_drive_thru")),
        source: candidatePlatform(candidate) ?? "aqar",
      }),
    },
    {
      key: "district_momentum_score",
      resolve: ({ featureSnapshot }) => ({
        value: asNumber(readNested(featureSnapshot, "district_momentum", "momentum_score")),
        source: "oaktree_internal",
      }),
    },
  ],

  brand_fit: [
    {
      key: "district",
      resolve: ({ candidate }) => {
        const c = candidate as Record<string, unknown>;
        const v = asString(c.district_display) ?? asString(c.district);
        return { value: v, source: "operator_brief" };
      },
    },
    {
      key: "area_match",
      resolve: ({ candidate }) => {
        const c = candidate as Record<string, unknown>;
        const v = asNumber(c.area_m2) ?? asNumber(c.unit_area_sqm);
        return { value: v, source: "arcgis_riyadh_parcels" };
      },
    },
    {
      key: "service_model_fit",
      resolve: ({ candidate }) => ({
        value: asString((candidate as Record<string, unknown>).source_type),
        source: "operator_brief",
      }),
    },
    {
      key: "cannibalization_distance_m",
      resolve: ({ candidate }) => ({
        value: asNumber((candidate as Record<string, unknown>).distance_to_nearest_branch_m),
        source: "operator_brief",
      }),
    },
  ],

  access_visibility: [
    {
      key: "street_width_m",
      resolve: ({ candidate, contextSources }) => {
        const c = candidate as Record<string, unknown>;
        return {
          value: asNumber(c.unit_street_width_m),
          source: overrideSource(contextSources, "road_source", "expansion_road_context"),
        };
      },
    },
    {
      key: "nearest_major_road_distance_m",
      resolve: ({ featureSnapshot, contextSources }) => ({
        value: asNumber(readNested(featureSnapshot, "nearest_major_road_distance_m")),
        source: overrideSource(contextSources, "road_source", "osm"),
      }),
    },
    {
      key: "touches_road",
      resolve: ({ featureSnapshot, contextSources }) => ({
        value: asBool(readNested(featureSnapshot, "touches_road")),
        source: overrideSource(contextSources, "road_source", "osm"),
      }),
    },
    {
      key: "frontage_score",
      resolve: ({ candidate }) => ({
        value: asNumber((candidate as Record<string, unknown>).frontage_score),
        source: "oaktree_internal",
      }),
    },
    {
      key: "parking_score",
      resolve: ({ candidate, contextSources }) => ({
        value: asNumber((candidate as Record<string, unknown>).parking_score),
        source: overrideSource(contextSources, "parking_source", "expansion_parking_asset"),
      }),
    },
  ],

  // All four demand inputs read from feature_snapshot_json, which is always
  // populated. The score_breakdown_json.market_viability_flag block is only
  // written when a viability demotion leg fires, so non-demoted candidates
  // (the common case) would otherwise strand these rows as em-dashes.
  demand_potential: [
    {
      key: "population_reach",
      resolve: ({ featureSnapshot }) => ({
        value: asNumber(readNested(featureSnapshot, "population_reach")),
        source: "population_grid",
      }),
    },
    {
      key: "realized_demand_30d",
      resolve: ({ featureSnapshot, contextSources }) => ({
        value: asNumber(readNested(featureSnapshot, "realized_demand_30d")),
        source: overrideSource(contextSources, "delivery_source", "expansion_delivery_market"),
      }),
    },
    {
      key: "realized_demand_branches",
      resolve: ({ featureSnapshot, contextSources }) => ({
        value: asNumber(readNested(featureSnapshot, "realized_demand_branches")),
        source: overrideSource(contextSources, "delivery_source", "expansion_delivery_market"),
      }),
    },
    {
      key: "radiance_growth_pct",
      resolve: ({ featureSnapshot }) => ({
        value: asNumber(readNested(featureSnapshot, "radiance_growth", "value_yoy_pct")),
        source: "black_marble",
      }),
    },
  ],

  landlord_signal: [
    {
      key: "llm_landlord_signal_score",
      resolve: ({ scoreBreakdown }) => ({
        value: asNumber(readNested(scoreBreakdown, "inputs", "landlord_signal")),
        source: "oaktree_llm",
      }),
    },
  ],

  competition_whitespace: [
    {
      key: "competitor_count",
      resolve: ({ featureSnapshot, contextSources }) => ({
        value: asNumber(readNested(featureSnapshot, "competitor_count")),
        source: overrideSource(contextSources, "competitor_source", "google_places"),
      }),
    },
    {
      key: "nearest_branch_distance_m",
      resolve: ({ candidate }) => ({
        value: asNumber((candidate as Record<string, unknown>).distance_to_nearest_branch_m),
        source: "operator_brief",
      }),
    },
    {
      key: "whitespace_input",
      resolve: ({ scoreBreakdown }) => ({
        value: asNumber(readNested(scoreBreakdown, "inputs", "competition_whitespace")),
        source: "oaktree_internal",
      }),
    },
  ],

  chain_strength: [
    {
      key: "max_chain_strength",
      resolve: ({ scoreBreakdown }) => {
        const direct = asNumber(readNested(scoreBreakdown, "inputs", "chain_strength_max"));
        const fallback = asNumber(readNested(scoreBreakdown, "inputs", "chain_strength"));
        return { value: direct ?? fallback, source: "oaktree_internal" };
      },
    },
    {
      key: "top_chain_name",
      resolve: ({ featureSnapshot }) => ({
        value: asString(readNested(featureSnapshot, "brand_presence", "top_chain_strength_name")),
        source: "google_places",
      }),
    },
    {
      key: "unique_brands",
      resolve: ({ featureSnapshot }) => ({
        value: asNumber(readNested(featureSnapshot, "brand_presence", "unique_brands")),
        source: "google_places",
      }),
    },
  ],

  confidence: [
    {
      key: "data_completeness_score",
      resolve: ({ featureSnapshot }) => ({
        value: asNumber(readNested(featureSnapshot, "data_completeness_score")),
        source: "oaktree_internal",
      }),
    },
    {
      key: "area_confidence",
      resolve: ({ featureSnapshot }) => ({
        value: asString(readNested(featureSnapshot, "area_confidence")),
        source: "arcgis_riyadh_parcels",
      }),
    },
    {
      key: "missing_context_count",
      resolve: ({ featureSnapshot }) => {
        const v = readNested(featureSnapshot, "missing_context");
        const n = Array.isArray(v) ? v.length : null;
        return { value: n, source: "oaktree_internal" };
      },
    },
  ],

  delivery_demand: [
    {
      key: "provider_listing_count",
      resolve: ({ featureSnapshot, contextSources }) => ({
        value: asNumber(readNested(featureSnapshot, "provider_listing_count")),
        source: overrideSource(contextSources, "delivery_source", "expansion_delivery_market"),
      }),
    },
    {
      key: "provider_platform_count",
      resolve: ({ featureSnapshot, contextSources }) => ({
        value: asNumber(readNested(featureSnapshot, "provider_platform_count")),
        source: overrideSource(contextSources, "delivery_source", "expansion_delivery_market"),
      }),
    },
    {
      key: "multi_platform_presence_score",
      resolve: ({ candidate }) => ({
        value: asNumber((candidate as Record<string, unknown>).multi_platform_presence_score),
        source: "oaktree_internal",
      }),
    },
  ],
};

/* ─── Canonical viability legs (stable order, matches backend) ───────────── */

export const VIABILITY_LEG_ORDER: readonly string[] = [
  "rent_per_capita_high",
  "population_below_quartile",
  "rent_high",
  "economics_below_threshold",
  "demand_low",
  "radiance_growth_low",
] as const;
