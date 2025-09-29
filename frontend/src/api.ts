const BASE = import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000";

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
