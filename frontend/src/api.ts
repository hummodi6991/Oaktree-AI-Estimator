// Use VITE_API_BASE_URL when provided; otherwise talk to the same origin.
const ENV_BASE = (import.meta.env as any).VITE_API_BASE_URL as string | undefined;
const BASE =
  (ENV_BASE && ENV_BASE.trim() !== "" ? ENV_BASE : window.location.origin).replace(/\/+$/, "");

export type EstimateRequest = {
  geometry: any;
  asset_program: string;
  unit_mix: { type: string; count: number; avg_m2?: number }[];
  finish_level: "low" | "mid" | "high";
  timeline: { start: string; months: number };
  financing_params: { margin_bps: number; ltv: number };
  strategy: "build_to_sell" | "build_to_lease" | "hotel";
  city?: string | null;
  far?: number;
  efficiency?: number;
};

export async function getFreshness() {
  const r = await fetch(`${BASE}/v1/metadata/freshness`);
  if (!r.ok) throw new Error("freshness failed");
  return r.json();
}

export async function createEstimate(body: EstimateRequest) {
  const r = await fetch(`${BASE}/v1/estimates`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function runScenario(id: string, patch: { price_uplift_pct?: number; soft_cost_pct?: number; margin_bps?: number }) {
  const r = await fetch(`${BASE}/v1/estimates/${id}/scenario`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(patch),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export function memoPdfUrl(id: string) {
  return `${BASE}/v1/estimates/${id}/memo.pdf`;
}

export function exportCsvUrl(id: string) {
  return `${BASE}/v1/estimates/${id}/export?format=csv`;
}

export async function getComps(params: { city?: string; type?: string; since?: string }) {
  const q = new URLSearchParams();
  if (params.city) q.set("city", params.city);
  if (params.type) q.set("type", params.type);
  if (params.since) q.set("since", params.since);
  const r = await fetch(`${BASE}/v1/comps?${q.toString()}`);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}
