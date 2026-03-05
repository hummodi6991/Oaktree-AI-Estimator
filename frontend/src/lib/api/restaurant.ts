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
  heatmapCache.set(cacheKey, data);
  return data;
}

export async function fetchTopCells(
  category: string,
): Promise<TopCell[]> {
  const params = new URLSearchParams({ category });
  const res = await fetchWithAuth(
    buildApiUrl(`/v1/restaurant/opportunity-top-cells?${params.toString()}`),
  );
  const data = await res.json();
  return Array.isArray(data) ? data : data?.cells ?? data?.parcels ?? [];
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
  return res.json();
}

export function clearHeatmapCache() {
  heatmapCache.clear();
}
