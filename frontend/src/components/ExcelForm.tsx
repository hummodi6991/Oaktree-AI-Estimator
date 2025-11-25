import { useMemo, useState } from "react";
import type { Geometry } from "geojson";

import { landPrice, makeEstimate } from "../api";

const PROVIDERS = [
  {
    value: "aqar" as const,
    label: "Saudi Arabia Real Estate dataset – Kaggle (aqar.fm scrape)",
  },
];

const DEFAULT_EXCEL_INPUTS = {
  area_ratio: { residential: 1.6, basement: 0.5 },
  unit_cost: { residential: 2200, basement: 1200 },
  efficiency: { residential: 0.82 },
  cp_sqm_per_space: { basement: 30 },
  rent_sar_m2_yr: { residential: 2400 },
  fitout_rate: 400,
  contingency_pct: 0.1,
  consultants_pct: 0.06,
  feasibility_fee: 1500000,
  transaction_pct: 0.03,
  land_price_sar_m2: 0,
};

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
  const [provider, setProvider] = useState<"aqar">("aqar");
  const [price, setPrice] = useState<number | null>(null);
  const [inputs, setInputs] = useState<any>(DEFAULT_EXCEL_INPUTS);
  const [estimate, setEstimate] = useState<any | null>(null);
  const [error, setError] = useState<string | null>(null);

  const selectedLandUse = landUseOverride || parcel?.landuse_code || "";
  const assetProgram =
    selectedLandUse === "m" ? "mixed_use_midrise" : "residential_midrise";

  async function fetchPrice() {
    setError(null);
    const centroid = centroidFromGeometry(parcel?.geometry as Geometry | null);
    try {
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
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    }
  }

  async function runEstimate() {
    if (!parcel) return;
    setError(null);
    try {
      const result = await makeEstimate({
        geometry: parcel.geometry,
        excelInputs: inputs,
        assetProgram,
        strategy: "build_to_sell",
        city: "Riyadh",
        far: 2.0,
        efficiency: 0.82,
        landUseOverride,
      });
      setEstimate(result);
      const roi = extractExcelRoi(result);
      const display = typeof roi === "number" && Number.isFinite(roi) ? roi : -1;
      alert(`ROI (Excel mode): ${display.toFixed(3)}`);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    }
  }

  const summary = estimate?.notes?.notes?.summary;
  const breakdown = estimate?.notes?.notes?.cost_breakdown;
  const breakdownEntries = useMemo(() => {
    if (!breakdown || typeof breakdown !== "object") return [];
    return Object.entries(breakdown);
  }, [breakdown]);

  return (
    <div>
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <span>Provider:</span>
        <select value={provider} onChange={(event) => setProvider(event.target.value as any)}>
          {PROVIDERS.map((item) => (
            <option key={item.value} value={item.value}>
              {item.label}
            </option>
          ))}
        </select>
        <button onClick={fetchPrice}>Fetch land price</button>
        {price != null && <strong>SAR/m²: {price}</strong>}
      </div>

      <button onClick={runEstimate} style={{ marginTop: 12 }}>
        Calculate (Excel method)
      </button>

      {error && (
        <div style={{ marginTop: 12, color: "#fca5a5" }}>
          Error: {error}
        </div>
      )}

      {summary && (
        <p className="mt-4 text-sm text-gray-200" style={{ marginTop: 16 }}>
          {summary}
        </p>
      )}

      {breakdownEntries.length > 0 && (
        <table className="mt-4 text-sm" style={{ marginTop: 12 }}>
          <tbody>
            {breakdownEntries.map(([key, value]) => (
              <tr key={key}>
                <td style={{ paddingRight: 12 }}>{key.replace(/_/g, " ")}</td>
                <td>
                  {Number(value).toLocaleString("en-US", { maximumFractionDigits: 0 })} SAR
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
