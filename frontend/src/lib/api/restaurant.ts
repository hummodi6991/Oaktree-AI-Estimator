import { fetchWithAuth, buildApiUrl } from "../../api";

export type RestaurantCategory = {
  key: string;
  name_en: string;
  name_ar: string;
};

export type HeatmapCell = {
  opportunity_score: number;
  confidence_score: number;
  final_score: number;
};

export type TopCell = {
  h3_index: string;
  lat: number;
  lon: number;
  opportunity_score: number;
  confidence_score: number;
  final_score: number;
  area_label?: string;
};

export type ScoreResult = {
  opportunity_score: number;
  confidence_score: number;
  final_score: number;
  factors?: Record<string, number>;
  contributions?: Array<{
    factor: string;
    score: number;
    weight: number;
    weighted_contribution: number;
  }>;
  nearby_competitors?: Array<{
    id: string;
    name: string;
    category: string;
    rating: number | null;
    source: string;
    distance_m: number;
  }>;
};

// --- Normalization helpers for mixed backend response shapes ---

const debugRestaurant =
  typeof window !== "undefined" &&
  new URLSearchParams(window.location.search).has("debug_restaurant");

/** Scale a score that might be in 0..1 range up to 0..100. */
function normalizeScore01To100(x: unknown): number {
  if (typeof x !== "number" || !Number.isFinite(x)) return 0;
  return x <= 1.01 && x >= 0 ? x * 100 : x;
}

/** Coerce any value to a finite number, defaulting to 0. */
function safeNum(x: unknown): number {
  return typeof x === "number" && Number.isFinite(x) ? x : 0;
}

/**
 * Normalize a raw top-cell object (plain object or GeoJSON Feature) into
 * the canonical TopCell shape with scores in 0..100.
 */
function normalizeTopCell(raw: Record<string, any>): TopCell | null {
  // Unwrap GeoJSON Feature
  let props = raw;
  let lat: number | undefined;
  let lon: number | undefined;

  if (raw.type === "Feature" && raw.geometry) {
    props = { ...raw.properties };
    const coords = raw.geometry.coordinates; // [lng, lat]
    if (Array.isArray(coords) && coords.length >= 2) {
      lon = coords[0];
      lat = coords[1];
    }
  }

  lat = safeNum(lat ?? props.lat ?? props.latitude);
  lon = safeNum(lon ?? props.lon ?? props.lng ?? props.longitude);

  if (lat === 0 && lon === 0) return null; // skip invalid entries

  return {
    lat,
    lon,
    opportunity_score: normalizeScore01To100(props.opportunity_score ?? props.opportunity),
    confidence_score: normalizeScore01To100(props.confidence_score ?? props.confidence),
    final_score: normalizeScore01To100(props.final_score ?? props.score),
    area_label: props.area_label,
    h3_index: props.h3_index ?? "",
  };
}

/** Normalize a raw score-location response into canonical ScoreResult. */
function normalizeScoreResult(raw: Record<string, any>): ScoreResult {
  return {
    opportunity_score: normalizeScore01To100(raw.opportunity_score ?? raw.opportunity),
    confidence_score: normalizeScore01To100(raw.confidence_score ?? raw.confidence),
    final_score: normalizeScore01To100(raw.final_score ?? raw.score),
    factors: raw.factors,
    contributions: raw.contributions,
    nearby_competitors: raw.nearby_competitors,
  };
}

// In-memory cache for heatmap responses per category
const heatmapCache = new Map<string, GeoJSON.FeatureCollection>();

export async function fetchCategories(): Promise<RestaurantCategory[]> {
  const res = await fetchWithAuth(buildApiUrl("/v1/restaurant/categories"));
  const data = await res.json();
  return Array.isArray(data) ? data : [];
}

export async function fetchOpportunityHeatmap(
  category: string,
  options?: { force?: boolean },
): Promise<GeoJSON.FeatureCollection> {
  const cacheKey = category;
  if (!options?.force && heatmapCache.has(cacheKey)) {
    return heatmapCache.get(cacheKey)!;
  }

  const params = new URLSearchParams({ category });
  const res = await fetchWithAuth(
    buildApiUrl(`/v1/restaurant/opportunity-heatmap?${params.toString()}`),
  );
  const data = await res.json();
  if (debugRestaurant) console.log("[restaurant] raw heatmap:", data);

  // Ensure each feature has canonical score properties for tooltip rendering
  const fc: GeoJSON.FeatureCollection =
    data.type === "FeatureCollection"
      ? data
      : { type: "FeatureCollection", features: data.features ?? [] };

  for (const f of fc.features) {
    const p = f.properties ?? {};
    if (p.final_score == null && p.score != null) p.final_score = p.score;
    if (p.confidence_score == null && p.confidence != null)
      p.confidence_score = normalizeScore01To100(p.confidence);
    if (p.opportunity_score == null && p.opportunity != null)
      p.opportunity_score = p.opportunity;
    f.properties = p;
  }

  heatmapCache.set(cacheKey, fc);
  return fc;
}

export async function fetchTopCells(
  category: string,
): Promise<TopCell[]> {
  const params = new URLSearchParams({ category });
  const res = await fetchWithAuth(
    buildApiUrl(`/v1/restaurant/opportunity-top-cells?${params.toString()}`),
  );
  const data = await res.json();
  if (debugRestaurant) console.log("[restaurant] raw top-cells:", data);

  const candidates: any[] = Array.isArray(data)
    ? data
    : data?.cells ?? data?.parcels ?? data?.features ?? [];

  return candidates
    .map((raw: any) => normalizeTopCell(raw))
    .filter((c): c is TopCell => c !== null);
}

export async function scoreLocation(
  lat: number,
  lon: number,
  category: string,
): Promise<ScoreResult> {
  const res = await fetchWithAuth(buildApiUrl("/v1/restaurant/score"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ lat, lon, category }),
  });
  const data = await res.json();
  if (debugRestaurant) console.log("[restaurant] raw score:", data);
  return normalizeScoreResult(data);
}

export function clearHeatmapCache() {
  heatmapCache.clear();
}
