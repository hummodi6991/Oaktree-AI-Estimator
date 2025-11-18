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
  perimeter_m?: number | null;
  landuse_raw?: string | null;
  classification_raw?: string | null;
  landuse_code?: string | null;
  source_url?: string | null;
};

export type IdentifyResponse = {
  found: boolean;
  tolerance_m?: number;
  source?: string;
  message?: string;
  parcel?: ParcelSummary | null;
};

export async function identify(lng: number, lat: number) {
  const res = await fetch(withBase("/v1/geo/identify"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ lng, lat }),
  });
  return readJson<IdentifyResponse>(res);
}

export async function landPrice(
  city: string,
  district?: string,
  provider: "srem" | "suhail" = "srem",
  parcelId?: string,
) {
  const params = new URLSearchParams({ city });
  if (district) params.set("district", district);
  params.set("provider", provider);
  if (parcelId) params.set("parcel_id", parcelId);
  const res = await fetch(withBase(`/v1/pricing/land?${params.toString()}`));
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
  const res = await fetch(withBase("/v1/estimates"), init);
  return readJson(res);
}

export async function makeEstimate(geometry: any, excelInputs: any, landUseOverride?: string) {
  return createEstimate({
    geometry,
    excel_inputs: excelInputs,
    land_use_override: landUseOverride,
  });
}

export async function runScenario(estimateId: string, patch: Record<string, unknown>) {
  const res = await fetch(withBase(`/v1/estimates/${encodeURIComponent(estimateId)}/scenario`), {
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
  const res = await fetch(withBase(url));
  return readJson(res);
}

export async function getFreshness() {
  const candidates = ["/health", "/v1/health", "/healthz", "/"];
  let lastError: Error | null = null;
  for (const path of candidates) {
    try {
      const res = await fetch(withBase(path));
      if (!res.ok) {
        lastError = new Error(`${res.status} ${res.statusText}`);
        continue;
      }
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
