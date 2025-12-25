import { useState } from "react";
import type { Geometry } from "geojson";

import { landPrice, makeEstimate } from "../api";

const PROVIDERS = [
  {
    value: "aqar" as const,
    label: "Hedonic model (trained partly on Kaggle data)",
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

const formatPercent = (value?: number | null) =>
  value != null ? `${(value * 100).toFixed(1)}%` : "n/a";

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
  breakdown: Record<string, any>;
  inputs: any;
  siteArea?: number;
  landPrice?: { ppm2?: number; source_type?: string };
  summary: string;
  excelRent?: {
    rent_sar_m2_yr?: Record<string, number>;
    rent_source_metadata?: Record<string, any>;
  };
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
  const [suggestedPrice, setSuggestedPrice] = useState<number | null>(null);
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
      setSuggestedPrice(res.sar_per_m2);
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
      const currentInputs = inputs;
      const result = await makeEstimate({
        geometry: parcel.geometry,
        excelInputs: currentInputs,
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
        breakdown: notes.excel_breakdown || {},
        inputs: currentInputs,
        siteArea: notes.site_area_m2,
        landPrice: notes.excel_land_price,
        summary: notes.summary ?? "",
        excelRent: notes.excel_rent,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    }
  }

  const breakdown = excelResult?.breakdown || {};
  const builtArea = breakdown.built_area || {};
  const nla = breakdown.nla || {};
  const directCost = breakdown.direct_cost || {};
  const incomeComponents = breakdown.y1_income_components || {};
  const explanations = breakdown.explanations || {};
  const usedInputs = excelResult?.inputs || {};
  const unitCost = usedInputs.unit_cost || {};
  const rentRates = usedInputs.rent_sar_m2_yr || {};
  const efficiency = usedInputs.efficiency || {};
  const areaRatio = usedInputs.area_ratio || {};
  const excelRent = excelResult?.excelRent;
  const rentMeta = excelRent?.rent_source_metadata as any;
  const rentRatesFromNotes = excelRent?.rent_sar_m2_yr as Record<string, number> | undefined;

  let residentialRentYr: number | null = null;
  if (rentRatesFromNotes && typeof rentRatesFromNotes === "object") {
    residentialRentYr =
      (rentRatesFromNotes as any).residential ??
      ((Object.values(rentRatesFromNotes)[0] as number | undefined) ?? null);
  }

  const residentialRentMo =
    residentialRentYr != null ? residentialRentYr / 12 : null;
  const contingencyPct = usedInputs.contingency_pct ?? null;
  const consultantsPct = usedInputs.consultants_pct ?? null;
  const transactionPct = usedInputs.transaction_pct ?? null;
  const fitoutRate = usedInputs.fitout_rate ?? null;
  const siteArea = excelResult?.siteArea ?? null;
  const landCost = excelResult?.costs?.land_cost ?? null;
  const landCostValue = excelResult?.costs?.land_cost ?? 0;
  const landPricePpm2 =
    excelResult?.landPrice?.ppm2 ??
    (siteArea && siteArea > 0 && landCost != null && landCost > 0
      ? landCost / siteArea
      : null);
  const fitoutArea = Object.entries(builtArea).reduce(
    (acc, [key, value]) => {
      const numericValue = typeof value === "number" ? value : Number(value) || 0;
      return key.toLowerCase().startsWith("basement") ? acc : acc + numericValue;
    },
    0,
  );
  const constructionSubtotal = typeof breakdown.sub_total === "number" ? breakdown.sub_total : 0;
  const contingencyAmount = typeof breakdown.contingency_cost === "number" ? breakdown.contingency_cost : 0;
  const consultantsBase = constructionSubtotal + contingencyAmount;

  const formatArea = (value: number | null | undefined) => {
    if (value == null || Number.isNaN(Number(value))) return "";
    return `${Number(value).toLocaleString()} m²`;
  };

  const buaNote = (key: string) => {
    const noteKey = `${key}_bua`;
    if (explanations[noteKey]) return explanations[noteKey];
    const ratio = areaRatio?.[key];
    if (siteArea != null && ratio != null) {
      return `Site area ${siteArea.toLocaleString()} m² × area ratio ${Number(ratio).toFixed(2)}`;
    }
    return "Built-up area based on provided ratios";
  };

  const landNote =
    explanations.land_cost ||
    (siteArea && landPricePpm2
      ? `Site area ${siteArea.toLocaleString()} m² × ${landPricePpm2.toLocaleString()} SAR/m² (${excelResult?.landPrice?.source_type || "input"})`
      : "Site area × land price per m²");

  const fitoutNote =
    explanations.fitout ||
    (fitoutRate != null
      ? `Non-basement area ${fitoutArea.toLocaleString()} m² × ${fitoutRate.toLocaleString()} SAR/m²`
      : "Fit-out applied to above-ground areas");

  const contingencyNote =
    explanations.contingency ||
    `Subtotal ${constructionSubtotal.toLocaleString()} SAR × contingency ${formatPercent(contingencyPct)}`;

  const consultantsNote =
    explanations.consultants ||
    `(Subtotal + contingency) ${consultantsBase.toLocaleString()} SAR × consultants ${formatPercent(consultantsPct)}`;

  const transactionNote =
    explanations.transaction_cost ||
    `Land cost ${landCostValue.toLocaleString()} SAR × transaction ${formatPercent(transactionPct)}`;

  const directNote =
    explanations.construction_direct ||
    Object.keys(directCost)
      .map((key) => {
        const area = builtArea[key] ?? 0;
        const costPerUnit = unitCost[key] ?? 0;
        return `${key}: ${area.toLocaleString()} m² × ${costPerUnit.toLocaleString()} SAR/m²`;
      })
      .filter(Boolean)
      .join("; ");

  const incomeNote =
    explanations.y1_income ||
    Object.keys(incomeComponents)
      .map((key) => {
        const nlaVal = nla[key] ?? 0;
        const efficiencyVal = efficiency[key] ?? null;
        const baseArea = builtArea[key] ?? null;
        const efficiencyText =
          efficiencyVal != null && baseArea != null
            ? `NLA ${nlaVal.toLocaleString()} m² (built area ${baseArea.toLocaleString()} m² × efficiency ${(efficiencyVal * 100).toFixed(0)}%)`
            : `NLA ${nlaVal.toLocaleString()} m²`;
        const rent = rentRates[key] ?? 0;
        return `${key}: ${efficiencyText} × ${rent.toLocaleString()} SAR/m²/yr`;
      })
      .filter(Boolean)
      .join("; ");

  const noteStyle = { fontSize: "0.8rem", color: "#cbd5f5" } as const;
  const baseCellStyle = { padding: "0.65rem 0.75rem", verticalAlign: "top" } as const;
  const itemColumnStyle = { ...baseCellStyle, paddingLeft: 0 } as const;
  const amountColumnStyle = { ...baseCellStyle, textAlign: "right", paddingRight: "1.5rem" } as const;
  const calcColumnStyle = {
    ...baseCellStyle,
    ...noteStyle,
    paddingLeft: "1rem",
    lineHeight: 1.5,
    wordBreak: "break-word",
  } as const;
  const itemHeaderStyle = { ...itemColumnStyle, fontWeight: 600 } as const;
  const amountHeaderStyle = { ...baseCellStyle, textAlign: "right", paddingRight: "1.5rem", fontWeight: 600 } as const;
  const calcHeaderStyle = { ...baseCellStyle, textAlign: "left", fontWeight: 600, paddingLeft: "1rem" } as const;
  const revenueItems = Object.keys(incomeComponents || {}).map((key) => {
    const nlaVal = nla[key] ?? 0;
    const efficiencyVal = efficiency[key] ?? null;
    const baseArea = builtArea[key] ?? null;
    const efficiencyText =
      efficiencyVal != null && baseArea != null
        ? `NLA ${nlaVal.toLocaleString()} m² (built area ${baseArea.toLocaleString()} m² × efficiency ${(efficiencyVal * 100).toFixed(0)}%)`
        : `NLA ${nlaVal.toLocaleString()} m²`;
    const rent = rentRates[key] ?? 0;
    return {
      key,
      amount: incomeComponents[key] ?? 0,
      note: `${efficiencyText} × ${rent.toLocaleString()} SAR/m²/yr`,
    };
  });

  return (
    <div>
      <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
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
          {price != null && <strong>Suggested SAR/m²: {price}</strong>}
        </div>
        <label style={{ display: "flex", flexDirection: "column", gap: 4, color: "white" }}>
          <span>Override land price (SAR/m², optional)</span>
          <input
            type="number"
            value={inputs.land_price_sar_m2 ?? ""}
            onChange={(event) =>
              setInputs((current: any) => ({
                ...current,
                land_price_sar_m2: event.target.value === "" ? 0 : Number(event.target.value),
              }))
            }
            style={{ padding: "4px 6px", borderRadius: 4, border: "1px solid rgba(255,255,255,0.2)" }}
          />
          <span style={{ fontSize: "0.8rem", color: "#cbd5f5" }}>
            Suggested from fetch: {suggestedPrice != null ? `${suggestedPrice.toLocaleString()} SAR/m²` : "Not fetched yet"}
          </span>
        </label>
      </div>

      <button onClick={runEstimate} style={{ marginTop: 12 }}>
        Calculate estimate
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
            maxWidth: "100%",
            fontSize: "0.9rem",
          }}
        >
          <h3 style={{ marginTop: 0, marginBottom: "0.5rem" }}>
            Financial breakdown
          </h3>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))",
              gap: "1rem",
              alignItems: "start",
            }}
          >
            <div style={{ background: "rgba(255,255,255,0.03)", borderRadius: 8, padding: "0.75rem" }}>
              <h4 style={{ marginTop: 0, marginBottom: "0.5rem" }}>Cost breakdown</h4>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={itemHeaderStyle}>Item</th>
                    <th style={amountHeaderStyle}>Amount</th>
                    <th style={calcHeaderStyle}>How we calculated it</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td style={itemColumnStyle}>Residential BUA</td>
                    <td style={amountColumnStyle}>{formatArea(builtArea.residential)}</td>
                    <td style={calcColumnStyle}>{buaNote("residential")}</td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>Basement BUA</td>
                    <td style={amountColumnStyle}>{formatArea(builtArea.basement)}</td>
                    <td style={calcColumnStyle}>{buaNote("basement")}</td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>Land cost</td>
                    <td style={amountColumnStyle}>
                      {excelResult.costs.land_cost.toLocaleString()} SAR
                    </td>
                    <td style={calcColumnStyle}>
                      {landNote}
                    </td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>Construction (direct)</td>
                    <td style={amountColumnStyle}>
                      {excelResult.costs.construction_direct_cost.toLocaleString()} SAR
                    </td>
                    <td style={calcColumnStyle}>
                      {explanations.construction_direct
                        ? directNote
                        : directNote
                        ? `${directNote}; sums to construction subtotal of ${constructionSubtotal.toLocaleString()} SAR before fit-out`
                        : "Sum of built area × unit cost for each use"}
                    </td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>Fit-out</td>
                    <td style={amountColumnStyle}>
                      {excelResult.costs.fitout_cost.toLocaleString()} SAR
                    </td>
                    <td style={calcColumnStyle}>
                      {fitoutNote}
                    </td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>Contingency</td>
                    <td style={amountColumnStyle}>
                      {excelResult.costs.contingency_cost.toLocaleString()} SAR
                    </td>
                    <td style={calcColumnStyle}>
                      {contingencyNote}
                    </td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>Consultants</td>
                    <td style={amountColumnStyle}>
                      {excelResult.costs.consultants_cost.toLocaleString()} SAR
                    </td>
                    <td style={calcColumnStyle}>
                      {consultantsNote}
                    </td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>Feasibility fee</td>
                    <td style={amountColumnStyle}>
                      {excelResult.costs.feasibility_fee.toLocaleString()} SAR
                    </td>
                    <td style={calcColumnStyle}>
                      Fixed allowance from inputs: {(usedInputs.feasibility_fee ?? 0).toLocaleString()} SAR
                    </td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>Transaction costs</td>
                    <td style={amountColumnStyle}>
                      {excelResult.costs.transaction_cost.toLocaleString()} SAR
                    </td>
                    <td style={calcColumnStyle}>
                      {transactionNote}
                    </td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>
                      <strong>Total capex</strong>
                    </td>
                    <td style={amountColumnStyle}>
                      <strong>{excelResult.costs.grand_total_capex.toLocaleString()} SAR</strong>
                    </td>
                    <td style={calcColumnStyle}>
                      Land + construction + fit-out + contingency + consultants + feasibility + transaction costs
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>

            <div style={{ background: "rgba(255,255,255,0.03)", borderRadius: 8, padding: "0.75rem" }}>
              <h4 style={{ marginTop: 0, marginBottom: "0.5rem" }}>Revenue breakdown</h4>
              {rentMeta?.provider === "REGA" && residentialRentMo != null && (
                <p style={{ marginTop: 0, marginBottom: "0.75rem", fontSize: "0.8rem", color: "#cbd5f5" }}>
                  Base rent uses the <strong>REGA residential rent benchmark</strong> for {rentMeta.district || rentMeta.city || "the selected city"}: {" "}
                  {residentialRentMo.toLocaleString("en-US", { maximumFractionDigits: 0 })} SAR/m²/month ({residentialRentYr!.toLocaleString("en-US", { maximumFractionDigits: 0 })} SAR/m²/year). This rent fully overrides any manual rent inputs.
                </p>
              )}
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={itemHeaderStyle}>Item</th>
                    <th style={amountHeaderStyle}>Amount</th>
                    <th style={calcHeaderStyle}>How we calculated it</th>
                  </tr>
                </thead>
                <tbody>
                  {revenueItems.map((item) => (
                    <tr key={item.key}>
                      <td style={itemColumnStyle}>{item.key.replace(/_/g, " ")}</td>
                      <td style={amountColumnStyle}>{Number(item.amount || 0).toLocaleString()} SAR</td>
                      <td style={calcColumnStyle}>{item.note}</td>
                    </tr>
                  ))}
                  <tr>
                    <td style={itemColumnStyle}>Year 1 net income</td>
                    <td style={amountColumnStyle}>
                      {excelResult.costs.y1_income.toLocaleString()} SAR
                    </td>
                    <td style={calcColumnStyle}>
                      {incomeNote ||
                        "Net leasable area per use × rent rate (SAR/m²/yr), using efficiency to convert built area to NLA"}
                    </td>
                  </tr>
                  <tr>
                    <td style={itemColumnStyle}>
                      <strong>Unlevered ROI</strong>
                    </td>
                    <td style={amountColumnStyle}>
                      <strong>{(excelResult.roi * 100).toFixed(1)}%</strong>
                    </td>
                    <td style={calcColumnStyle}>Year 1 net income ÷ total capex</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          <p style={{ marginTop: "0.75rem", marginBottom: 0 }}>
            {excelResult.summary || `Unlevered ROI: ${(excelResult.roi * 100).toFixed(1)}%`}
          </p>
        </div>
      )}
    </div>
  );
}
