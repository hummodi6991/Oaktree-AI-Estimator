import type { Geometry } from "geojson";

export const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000";

export async function apiSearch(q: string) {
  const params = new URLSearchParams({ q, limit: "12" });
  const res = await fetch(`${API_BASE}/v1/search?${params.toString()}`);
  if (!res.ok) throw new Error(`Search failed (${res.status})`);
  return await res.json();
}

export async function apiIdentify(lng: number, lat: number) {
  const res = await fetch(`${API_BASE}/v1/geo/identify`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ lng, lat })
  });
  if (!res.ok) throw new Error(`Identify failed (${res.status})`);
  return await res.json();
}

export async function apiCreateEstimate(siteGeom: Geometry) {
  // Minimal “Excel-mode” payload; your backend already enriches / infers.
  const payload = {
    geometry: siteGeom,
    asset_program: "residential_midrise",
    strategy: "build_to_sell",
    city: "Riyadh",
    efficiency: 0.82,
    far: 2.0,
    timeline: { start: new Date().toISOString().slice(0, 10).replace(/-\d\d$/, "-01"), months: 18 },
    financing_params: { margin_bps: 250, ltv: 0.6 },
    excel_inputs: {
      area_ratio: { residential: 1.6, retail: 0.6, office: 0.3, basement: 1.0 },
      unit_cost: { residential: 2200, retail: 2600, office: 2400, basement: 2200 },
      contingency_pct: 0.05,
      consultants_pct: 0.06,
      transaction_pct: 0.025,
      feasibility_fee_pct: 0.02,
      opex_pct: 0.05
    }
  };

  const res = await fetch(`${API_BASE}/v1/estimates`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  if (!res.ok) throw new Error(`Estimate failed (${res.status})`);
  return await res.json();
}

export function estimatePdfUrl(estimateId: string) {
  return `${API_BASE}/v1/estimates/${encodeURIComponent(estimateId)}/memo.pdf`;
}
