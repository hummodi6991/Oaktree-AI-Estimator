type AnyObj = Record<string, unknown>;

function fmtMoney(x: unknown): string {
  const n = Number(x);
  if (!Number.isFinite(n)) return "—";
  return `${n.toLocaleString(undefined, { maximumFractionDigits: 0 })} SAR`;
}

function fmtNumber(x: unknown): string {
  const n = Number(x);
  if (!Number.isFinite(n)) return "—";
  return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
}

function fmtPct01(x: unknown): string {
  const n = Number(x);
  if (!Number.isFinite(n)) return "—";
  return `${(n * 100).toFixed(1)}%`;
}

function get(obj: AnyObj | null | undefined, path: string[]): unknown {
  let cur: unknown = obj;
  for (const k of path) {
    if (!cur || typeof cur !== "object") return undefined;
    cur = (cur as AnyObj)[k];
  }
  return cur;
}

type Props = {
  estimate: AnyObj | null;
};

export default function EstimateCalculationsPanel({ estimate }: Props) {
  if (!estimate) return null;

  const notes = (estimate.notes && typeof estimate.notes === "object" ? (estimate.notes as AnyObj) : {}) as AnyObj;
  const excel = (get(notes, ["excel_breakdown"]) as AnyObj) || ({} as AnyObj);
  const cost = (get(notes, ["cost_breakdown"]) as AnyObj) || ({} as AnyObj);

  const builtArea = ((excel.built_area as AnyObj) || (cost.built_area as AnyObj) || {}) as AnyObj;
  const unitCost =
    ((excel.unit_cost_sar_m2 as AnyObj) ||
      (excel.unit_cost as AnyObj) ||
      (cost.unit_cost_sar_m2 as AnyObj) ||
      {}) as AnyObj;

  const farAbove = excel.far_above_ground ?? cost.far_above_ground;
  const roi = excel.roi ?? cost.roi;

  const rows: Array<{ label: string; value: string }> = [
    { label: "Effective FAR (above-ground)", value: fmtNumber(farAbove) },
  ];

  const buas: Array<{ key: string; label: string }> = [
    { key: "residential", label: "Residential BUA" },
    { key: "retail", label: "Retail BUA" },
    { key: "office", label: "Office BUA" },
    { key: "basement", label: "Basement BUA" },
    { key: "upper_annex_non_far", label: "Upper annex (non-FAR, +0.5 floor)" },
  ];
  for (const row of buas) {
    const value = builtArea[row.key];
    if (row.key === "upper_annex_non_far") {
      const n = Number(value);
      if (!Number.isFinite(n) || n <= 0) continue;
    }
    if (value !== undefined) rows.push({ label: row.label, value: `${fmtNumber(value)} m²` });
  }

  const landCost = excel.land_cost ?? cost.land_cost;
  const constructionDirect =
    excel.construction_direct_cost ??
    excel.construction_direct ??
    cost.construction_direct_cost ??
    cost.construction_direct;
  const fitout = excel.fitout_cost ?? cost.fitout_cost;
  const contingency = excel.contingency_cost ?? cost.contingency_cost;
  const consultants = excel.consultants_cost ?? cost.consultants_cost;
  const feasibility = excel.feasibility_fee ?? cost.feasibility_fee;
  const transaction = excel.transaction_cost ?? cost.transaction_cost;
  const totalCapex = excel.grand_total_capex ?? cost.grand_total_capex;

  rows.push({ label: "Land cost", value: fmtMoney(landCost) });
  rows.push({ label: "Construction (direct)", value: fmtMoney(constructionDirect) });

  const upperAnnexArea = builtArea.upper_annex_non_far;
  const upperAnnexUnit = unitCost.upper_annex_non_far;
  const upperAnnexDirect =
    (excel.direct_cost as AnyObj)?.upper_annex_non_far ?? (cost.direct_cost as AnyObj)?.upper_annex_non_far;
  const upperAreaN = Number(upperAnnexArea);
  const upperUnitN = Number(upperAnnexUnit);
  const upperDirectN = Number(upperAnnexDirect);
  let upperComputed: number | undefined;
  if (Number.isFinite(upperDirectN)) upperComputed = upperDirectN;
  else if (Number.isFinite(upperAreaN) && upperAreaN > 0 && Number.isFinite(upperUnitN)) {
    upperComputed = upperAreaN * upperUnitN;
  }

  if (Number.isFinite(upperAreaN) && upperAreaN > 0 && upperComputed !== undefined) {
    rows.push({ label: "Upper annex construction cost (non-FAR)", value: fmtMoney(upperComputed) });
  }

  rows.push({ label: "Fit-out", value: fmtMoney(fitout) });
  rows.push({ label: "Contingency", value: fmtMoney(contingency) });
  rows.push({ label: "Consultants", value: fmtMoney(consultants) });
  rows.push({ label: "Feasibility fee", value: fmtMoney(feasibility) });
  rows.push({ label: "Transaction costs", value: fmtMoney(transaction) });
  rows.push({ label: "Total capex", value: fmtMoney(totalCapex) });

  const y1Income = excel.y1_income ?? cost.y1_income;
  const y1IncomeEff = excel.y1_income_effective ?? cost.y1_income_effective;
  const opexPct = excel.opex_pct ?? cost.opex_pct;
  const opex = excel.opex_cost ?? cost.opex_cost;
  const y1Noi = excel.y1_noi ?? cost.y1_noi;

  if (y1Income !== undefined) rows.push({ label: "Year-1 income", value: fmtMoney(y1Income) });
  if (y1IncomeEff !== undefined) rows.push({ label: "Year-1 effective income", value: fmtMoney(y1IncomeEff) });
  if (opexPct !== undefined) rows.push({ label: "OPEX %", value: fmtPct01(opexPct) });
  if (opex !== undefined) rows.push({ label: "OPEX", value: fmtMoney(opex) });
  if (y1Noi !== undefined) rows.push({ label: "Year-1 NOI", value: fmtMoney(y1Noi) });
  if (roi !== undefined) rows.push({ label: "ROI", value: fmtPct01(roi) });

  return (
    <div className="ot-card" style={{ width: "100%" }}>
      <h3 className="unit-cost-panel__title">Calculations</h3>
      <div style={{ fontSize: 12, color: "rgba(15, 23, 42, 0.6)", marginTop: 4 }}>
        Auto-populated from the estimate output
      </div>
      <div style={{ marginTop: 12 }}>
        {rows.map((row, idx) => (
          <div
            key={row.label}
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              padding: "8px 0",
              borderTop: idx === 0 ? "none" : "1px solid rgba(148,163,184,0.25)",
            }}
          >
            <div style={{ fontSize: 12, color: "rgba(15, 23, 42, 0.7)" }}>{row.label}</div>
            <div style={{ fontSize: 12, fontWeight: 600, color: "rgba(15, 23, 42, 0.95)" }}>{row.value}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
