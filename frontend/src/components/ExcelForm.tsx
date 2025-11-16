import { useState } from "react";

import { landPrice, makeEstimate } from "../api";

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
    const res = await landPrice("Riyadh", parcel?.district, provider, parcel?.parcel_id);
    setPrice(res.sar_per_m2);
    setInputs((current: any) => ({ ...current, land_price_sar_m2: res.sar_per_m2 }));
  }

  async function runEstimate() {
    if (!parcel) return;
    const estimate = await makeEstimate(parcel.geometry, inputs, landUseOverride);
    console.log(estimate);
    const totals = estimate.totals;
    const numerator =
      totals.revenues -
      (totals.land_value + totals.hard_costs + totals.soft_costs + totals.financing);
    const denominator =
      totals.land_value + totals.hard_costs + totals.soft_costs + totals.financing;
    const roi = denominator > 0 ? numerator / denominator : 0;
    alert(`ROI (Excel mode): ${roi.toFixed(3)}`);
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
