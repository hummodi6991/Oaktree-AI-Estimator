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

export function EstimateCalculationsPanel({ estimate }: { estimate: unknown }) {
  const root = (estimate ?? {}) as AnyObj;
  const notes = (root.notes ?? {}) as AnyObj;
  const excel = ((notes.excel_breakdown as AnyObj) ?? (notes.excel as AnyObj) ?? {}) as AnyObj;

  const rows: Array<{ k: string; v: string }> = [
    { k: "Site area (m²)", v: fmtNumber(notes.site_area_m2) },
    { k: "Land price (SAR/m²)", v: fmtNumber(excel.land_price_sar_m2) },
    { k: "Land cost", v: fmtMoney(excel.land_cost) },
    { k: "Construction subtotal", v: fmtMoney(excel.sub_total ?? excel.construction_subtotal) },
    { k: "Contingency", v: fmtMoney(excel.contingency_cost) },
    { k: "Consultants", v: fmtMoney(excel.consultants_cost) },
    { k: "Feasibility fee", v: fmtMoney(excel.feasibility_fee_sar ?? excel.feasibility_fee) },
    { k: "Transaction costs", v: fmtMoney(excel.transaction_cost) },
    { k: "Total CapEx", v: fmtMoney(excel.grand_total_capex ?? excel.total_capex) },
    { k: "Year-1 income", v: fmtMoney(excel.y1_income ?? excel.year1_income) },
    { k: "ROI (Year-1 / CapEx)", v: fmtPct01(excel.roi ?? excel.ROI) },
  ];

  const hasAnything = Object.keys(excel).length > 0 || Object.keys(notes).length > 0;
  if (!hasAnything) return null;

  return (
    <div className="oak-card" style={{ width: "100%" }}>
      <div className="oak-card-header">Calculations</div>
      <div className="oak-card-body">
        {rows.map((row) => (
          <div className="oak-row" key={row.k}>
            <div className="oak-k">{row.k}</div>
            <div className="oak-v">{row.v}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
