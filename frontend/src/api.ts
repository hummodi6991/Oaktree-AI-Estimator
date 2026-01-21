import type { Geometry } from "geojson";

const RAW_BASE = (import.meta.env.VITE_API_BASE_URL || "") as string;
const API_BASE = typeof RAW_BASE === "string" ? RAW_BASE.replace(/\/+$/, "") : "";

function withBase(path: string): string {
  if (/^https?:\/\//i.test(path)) {
    return path;
  }
  const normalized = path.startsWith("/") ? path : `/${path}`;
  if (!API_BASE) {
    return normalized;
  }
  if (normalized === "/") {
    return API_BASE || "/";
  }
  return `${API_BASE}${normalized}`;
}

export const buildApiUrl = withBase;

export async function fetchWithAuth(input: RequestInfo | URL, init: RequestInit = {}) {
  const apiKey = typeof window !== "undefined" ? window.localStorage.getItem("oaktree_api_key") : null;
  const headers = new Headers(init.headers || (input instanceof Request ? input.headers : undefined));
  if (apiKey) {
    headers.set("X-API-Key", apiKey);
  }
  const res = await fetch(input, { ...init, headers });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    const suffix = text ? `: ${text}` : "";
    throw new Error(`${res.status} ${res.statusText}${suffix}`);
  }
  return res;
}

async function readJson<T = any>(res: Response): Promise<T> {
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    const suffix = text ? `: ${text}` : "";
    throw new Error(`${res.status} ${res.statusText}${suffix}`);
  }
  const text = await res.text();
  if (!text) {
    return {} as T;
  }
  try {
    return JSON.parse(text) as T;
  } catch (error) {
    throw new Error(`Invalid JSON response: ${text}`);
  }
}

export type ParcelSummary = {
  parcel_id?: string | null;
  geometry?: Geometry | null;
  area_m2?: number | null;
  parcel_area_m2?: number | null;
  perimeter_m?: number | null;
  site_area_m2?: number | null;
  footprint_area_m2?: number | null;
  building_count?: number | null;
  parcel_method?: string | null;
  landuse_raw?: string | null;
  classification_raw?: string | null;
  landuse_code?: string | null;
  landuse_method?: string | null;
  residential_share?: number | null;
  commercial_share?: number | null;
  residential_share_osm?: number | null;
  commercial_share_osm?: number | null;
  residential_share_ovt?: number | null;
  commercial_share_ovt?: number | null;
  ovt_attr_conf?: number | null;
  osm_conf?: number | null;
  ovt_conf?: number | null;
  component_count?: number | null;
  component_area_m2_sum?: number | null;
  source_url?: string | null;
};

export type IdentifyResponse = {
  found: boolean;
  tolerance_m?: number;
  source?: string;
  message?: string;
  parcel?: ParcelSummary | null;
};

export type CollateResponse = {
  found: boolean;
  source?: string;
  message?: string;
  parcel_ids?: string[];
  missing_ids?: string[];
  parcel?: ParcelSummary | null;
};

export type LanduseResponse = {
  landuse_code?: string | null;
  landuse_method?: string | null;
  landuse_raw?: string | null;
  residential_share?: number | null;
  commercial_share?: number | null;
  residential_share_osm?: number | null;
  commercial_share_osm?: number | null;
  residential_share_ovt?: number | null;
  commercial_share_ovt?: number | null;
  osm_conf?: number | null;
  ovt_conf?: number | null;
};

export type InferParcelResponse = {
  found: boolean;
  parcel_id?: string | null;
  method?: string | null;
  area_m2?: number | null;
  perimeter_m?: number | null;
  geom?: Geometry | null;
  debug?: Record<string, unknown> | null;
};

export async function identify(lng: number, lat: number) {
  const res = await fetchWithAuth(withBase("/v1/geo/identify"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ lng, lat }),
  });
  return readJson<IdentifyResponse>(res);
}

export async function landuse(lng: number, lat: number) {
  const params = new URLSearchParams({ lng: String(lng), lat: String(lat) });
  const res = await fetchWithAuth(withBase(`/v1/geo/landuse?${params.toString()}`));
  return readJson<LanduseResponse>(res);
}

export async function collateParcels(parcelIds: string[]) {
  const res = await fetchWithAuth(withBase("/v1/geo/collate"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ parcel_ids: parcelIds }),
  });
  return readJson<CollateResponse>(res);
}

export async function inferParcel(params: {
  lng: number;
  lat: number;
  buildingId: number;
  partIndex: number;
  radiusM?: number;
  roadBufM?: number;
  k?: number;
}) {
  const search = new URLSearchParams({
    lng: String(params.lng),
    lat: String(params.lat),
    building_id: String(params.buildingId),
    part_index: String(params.partIndex),
  });
  if (params.radiusM != null) search.set("radius_m", String(params.radiusM));
  if (params.roadBufM != null) search.set("road_buf_m", String(params.roadBufM));
  if (params.k != null) search.set("k", String(params.k));
  const res = await fetchWithAuth(withBase(`/v1/geo/infer-parcel?${search.toString()}`));
  return readJson<InferParcelResponse>(res);
}

export async function landPrice(
  city: string,
  district: string | undefined,
  provider: string,
  parcelId?: string,
  lng?: number,
  lat?: number,
) {
  const params = new URLSearchParams();
  if (city) params.set("city", city);
  if (district) params.set("district", district);
  if (provider) params.set("provider", provider);
  if (parcelId) params.set("parcel_id", parcelId);
  if (lng != null) params.set("lng", String(lng));
  if (lat != null) params.set("lat", String(lat));
  const res = await fetchWithAuth(withBase(`/v1/pricing/land?${params.toString()}`));
  return readJson(res);
}

export async function createEstimate(payload: FormData | Record<string, unknown>) {
  const init: RequestInit =
    payload instanceof FormData
      ? { method: "POST", body: payload }
      : {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload ?? {}),
        };
  const res = await fetchWithAuth(withBase("/v1/estimates"), init);
  return readJson(res);
}

type EstimateStrategy = "build_to_sell" | "build_to_rent";

export async function makeEstimate(params: {
  geometry: any;
  excelInputs: any;
  assetProgram?: string;
  strategy?: EstimateStrategy;
  city?: string;
  far?: number;
  efficiency?: number;
  landUseOverride?: string;
}) {
  const {
    geometry,
    excelInputs,
    assetProgram,
    strategy,
    city,
    far,
    efficiency,
    landUseOverride,
  } = params;

  return createEstimate({
    geometry,
    excel_inputs: excelInputs,
    asset_program: assetProgram,
    strategy,
    city,
    far,
    efficiency,
    land_use_override: landUseOverride,
  });
}

export async function runScenario(estimateId: string, patch: Record<string, unknown>) {
  const res = await fetchWithAuth(withBase(`/v1/estimates/${encodeURIComponent(estimateId)}/scenario`), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(patch ?? {}),
  });
  return readJson(res);
}

export async function getComps(params: {
  city?: string;
  type?: string;
  since?: string;
  lng?: number;
  lat?: number;
  radiusM?: number;
}) {
  const search = new URLSearchParams();
  if (params.city) search.set("city", params.city);
  if (params.type) search.set("type", params.type);
  if (params.since) search.set("since", params.since);
  if (params.lng != null) search.set("lng", String(params.lng));
  if (params.lat != null) search.set("lat", String(params.lat));
  if (params.radiusM != null) search.set("radius_m", String(params.radiusM));
  const query = search.toString();
  const url = query ? `/v1/comps?${query}` : "/v1/comps";
  const res = await fetchWithAuth(withBase(url));
  return readJson(res);
}

export async function getFreshness() {
  const candidates = ["/health", "/v1/health", "/healthz", "/"];
  let lastError: Error | null = null;
  for (const path of candidates) {
    try {
      const res = await fetchWithAuth(withBase(path));
      try {
        return await readJson(res);
      } catch {
        return {};
      }
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
    }
  }
  throw (lastError || new Error("Backend is not reachable"));
}

export function exportCsvUrl(estimateId: string, options?: Record<string, string | number | boolean>) {
  const encodedId = encodeURIComponent(estimateId);
  const params = new URLSearchParams({ format: "csv" });
  if (options) {
    for (const [key, value] of Object.entries(options)) {
      if (value === undefined || value === null) continue;
      params.set(key, String(value));
    }
  }
  const query = params.toString();
  const path = `/v1/estimates/${encodedId}/export${query ? `?${query}` : ""}`;
  return withBase(path);
}

export function memoPdfUrl(estimateId: string) {
  const encodedId = encodeURIComponent(estimateId);
  return withBase(`/v1/estimates/${encodedId}/memo.pdf`);
}
