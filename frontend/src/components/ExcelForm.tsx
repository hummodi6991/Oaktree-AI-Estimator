import { useState } from "react";
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

type ExcelResult = {
  roi: number;
  costs: {
    land_cost: number;
    construction_direct_cost: number;
    fitout_cost: number;
    contingency_cost: number;
    consultants_cost: number;
    feasibility_fee: number;
    transaction_cost: number;
    grand_total_capex: number;
    y1_income: number;
  };
  summary: string;
};

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

type ExcelFormProps = {
  parcel: any;
  landUseOverride?: string;
};

export default function ExcelForm({ parcel, landUseOverride }: ExcelFormProps) {
  const [provider, setProvider] = useState<"aqar">("aqar");
  const [price, setPrice] = useState<number | null>(null);
  const [inputs, setInputs] = useState<any>(DEFAULT_EXCEL_INPUTS);
  const [error, setError] = useState<string | null>(null);
  const [excelResult, setExcelResult] = useState<ExcelResult | null>(null);

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
    setExcelResult(null);
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
      const notes = result?.notes || {};
      const costs = notes.cost_breakdown || {};

      setExcelResult({
        roi: notes.excel_roi ?? result?.totals?.excel_roi ?? 0,
        costs: {
          land_cost: costs.land_cost ?? 0,
          construction_direct_cost: costs.construction_direct_cost ?? 0,
          fitout_cost: costs.fitout_cost ?? 0,
          contingency_cost: costs.contingency_cost ?? 0,
          consultants_cost: costs.consultants_cost ?? 0,
          feasibility_fee: costs.feasibility_fee ?? 0,
          transaction_cost: costs.transaction_cost ?? 0,
          grand_total_capex: costs.grand_total_capex ?? 0,
          y1_income: costs.y1_income ?? 0,
        },
        summary: notes.summary ?? "",
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    }
  }

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

      {excelResult && (
        <div
          style={{
            marginTop: "1rem",
            padding: "1rem",
            borderRadius: "0.5rem",
            background: "rgba(0,0,0,0.3)",
            color: "white",
            maxWidth: "480px",
            fontSize: "0.9rem",
          }}
        >
          <h3 style={{ marginTop: 0, marginBottom: "0.5rem" }}>
            Excel method – cost breakdown
          </h3>

          <p style={{ marginTop: 0, marginBottom: "0.75rem" }}>
            {excelResult.summary ||
              `Unlevered ROI: ${(excelResult.roi * 100).toFixed(1)}%`}
          </p>

          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <tbody>
              <tr>
                <td>Land cost</td>
                <td style={{ textAlign: "right" }}>
                  {excelResult.costs.land_cost.toLocaleString()} SAR
                </td>
              </tr>
              <tr>
                <td>Construction (direct)</td>
                <td style={{ textAlign: "right" }}>
                  {excelResult.costs.construction_direct_cost.toLocaleString()} SAR
                </td>
              </tr>
              <tr>
                <td>Fit-out</td>
                <td style={{ textAlign: "right" }}>
                  {excelResult.costs.fitout_cost.toLocaleString()} SAR
                </td>
              </tr>
              <tr>
                <td>Contingency</td>
                <td style={{ textAlign: "right" }}>
                  {excelResult.costs.contingency_cost.toLocaleString()} SAR
                </td>
              </tr>
              <tr>
                <td>Consultants</td>
                <td style={{ textAlign: "right" }}>
                  {excelResult.costs.consultants_cost.toLocaleString()} SAR
                </td>
              </tr>
              <tr>
                <td>Feasibility fee</td>
                <td style={{ textAlign: "right" }}>
                  {excelResult.costs.feasibility_fee.toLocaleString()} SAR
                </td>
              </tr>
              <tr>
                <td>Transaction costs</td>
                <td style={{ textAlign: "right" }}>
                  {excelResult.costs.transaction_cost.toLocaleString()} SAR
                </td>
              </tr>
              <tr>
                <td><strong>Total capex</strong></td>
                <td style={{ textAlign: "right" }}>
                  <strong>{excelResult.costs.grand_total_capex.toLocaleString()} SAR</strong>
                </td>
              </tr>
              <tr>
                <td>Year 1 net income</td>
                <td style={{ textAlign: "right" }}>
                  {excelResult.costs.y1_income.toLocaleString()} SAR
                </td>
              </tr>
              <tr>
                <td><strong>Unlevered ROI</strong></td>
                <td style={{ textAlign: "right" }}>
                  <strong>{(excelResult.roi * 100).toFixed(1)}%</strong>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
