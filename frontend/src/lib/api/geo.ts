import { buildApiUrl, fetchWithAuth } from "../../api";

export type Parcel = {
  parcel_id: string;
  geometry: any;
  area_m2?: number;
  parcel_area_m2?: number;
  perimeter_m?: number;
  parcel_method?: string;
  landuse_code?: string;
  landuse_raw?: string;
  landuse_method?: string;
  classification_raw?: string;
  component_count?: number;
  component_area_m2_sum?: number;
  source_url?: string;
};

export type IdentifyPointResponse = {
  found: boolean;
  source?: string;
  tolerance_m?: number;
  parcel?: Parcel;
};

export type CollateResponse = {
  found: boolean;
  source: string;
  parcel_ids: string[];
  missing_ids: string[];
  message?: string;
  parcel?: Parcel;
};

export async function identifyPoint(lng: number, lat: number, tolM?: number) {
  const body: Record<string, number> = { lng, lat };
  if (tolM != null) body.tol_m = tolM;
  const res = await fetchWithAuth(buildApiUrl("/v1/geo/identify"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return (await res.json()) as IdentifyPointResponse;
}

export async function collateParcels(parcelIds: string[]) {
  const res = await fetchWithAuth(buildApiUrl("/v1/geo/collate"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ parcel_ids: parcelIds }),
  });
  return (await res.json()) as CollateResponse;
}
