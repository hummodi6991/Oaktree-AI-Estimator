import { useState } from "react";
import type { Geometry } from "geojson";

import { landPrice, makeEstimate } from "../api";

type Centroid = [number, number];

function polygonCentroidAndArea(coords: number[][][]): { area: number; centroid: Centroid } | null {
  if (!coords?.length) return null;
  const ring = coords[0];
  if (!ring || ring.length < 3) return null;
  let crossSum = 0;
  let cxSum = 0;
  let cySum = 0;
  const len = ring.length;
  for (let i = 0; i < len; i += 1) {
    const [x0, y0] = ring[i];
    const [x1, y1] = ring[(i + 1) % len];
    const cross = x0 * y1 - x1 * y0;
    crossSum += cross;
    cxSum += (x0 + x1) * cross;
    cySum += (y0 + y1) * cross;
  }
  if (!crossSum) return null;
  const centroid: Centroid = [cxSum / (3 * crossSum), cySum / (3 * crossSum)];
  return { area: Math.abs(crossSum) / 2, centroid };
}

function centroidFromGeometry(geometry?: Geometry | null): Centroid | null {
  if (!geometry) return null;
  if (geometry.type === "Point") {
    return geometry.coordinates as Centroid;
  }
  if (geometry.type === "Polygon") {
    return polygonCentroidAndArea(geometry.coordinates as number[][][])?.centroid || null;
  }
  if (geometry.type === "MultiPolygon") {
    const coords = geometry.coordinates as number[][][][];
    let totalArea = 0;
    let cx = 0;
    let cy = 0;
    for (const poly of coords) {
      const details = polygonCentroidAndArea(poly);
      if (!details) continue;
      totalArea += details.area;
      cx += details.centroid[0] * details.area;
      cy += details.centroid[1] * details.area;
    }
    if (!totalArea) return null;
    return [cx / totalArea, cy / totalArea];
  }
  return null;
}

function extractExcelRoi(estimate: any): number | null {
  const candidates = [
    estimate?.totals?.excel_roi,
    estimate?.notes?.excel_breakdown?.roi,
    estimate?.notes?.excel_roi,
  ];
  for (const value of candidates) {
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
  }

  const totals = estimate?.totals;
  const pieces = [
    totals?.revenues,
    totals?.land_value,
    totals?.hard_costs,
    totals?.soft_costs,
    totals?.financing,
  ];
  if (pieces.every((value) => typeof value === "number")) {
    const numerator =
      totals.revenues -
      (totals.land_value + totals.hard_costs + totals.soft_costs + totals.financing);
    const denominator =
      totals.land_value + totals.hard_costs + totals.soft_costs + totals.financing;
    if (denominator) {
      return numerator / denominator;
    }
  }

  return null;
}

type ExcelFormProps = {
  parcel: any;
  landUseOverride?: string;
};

export default function ExcelForm({ parcel, landUseOverride }: ExcelFormProps) {
  const [provider, setProvider] = useState<"srem" | "suhail">("srem");
  const [price, setPrice] = useState<number | null>(null);
  const [inputs, setInputs] = useState<any>({
    area_ratio: { residential: 2.0 },
    unit_cost: { residential: 0 },
    cp_sqm_per_space: { residential: 0 },
    fitout_rate: 0,
    contingency_pct: 0.0,
    consultants_pct: 0.0,
    feasibility_fee: 0,
    land_price_sar_m2: 0,
    transaction_pct: 0.0,
    efficiency: { residential: 0.82 },
    rent_sar_m2_yr: { residential: 0 },
    escalation_pct: 0.0,
    escalation_interval_years: 2,
  });

  async function fetchPrice() {
    const centroid = centroidFromGeometry(parcel?.geometry as Geometry | null);
    const res = await landPrice(
      "Riyadh",
      parcel?.district || undefined,
      provider,
      parcel?.parcel_id || undefined,
      centroid?.[0],
      centroid?.[1],
    );
    setPrice(res.sar_per_m2);
    setInputs((current: any) => ({ ...current, land_price_sar_m2: res.sar_per_m2 }));
  }

  async function runEstimate() {
    if (!parcel) return;
    const estimate = await makeEstimate(parcel.geometry, inputs, landUseOverride);
    console.log(estimate);
    const roi = extractExcelRoi(estimate);
    const display = typeof roi === "number" && Number.isFinite(roi) ? roi : -1;
    alert(`ROI (Excel mode): ${display.toFixed(3)}`);
  }

  return (
    <div>
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <span>Provider:</span>
        <select value={provider} onChange={(event) => setProvider(event.target.value as any)}>
          <option value="srem">البورصة العقارية</option>
          <option value="suhail">سُهيل</option>
        </select>
        <button onClick={fetchPrice}>Fetch land price</button>
        {price != null && <strong>SAR/m²: {price}</strong>}
      </div>

      <button onClick={runEstimate} style={{ marginTop: 12 }}>
        Calculate (Excel method)
      </button>
    </div>
  );
}
