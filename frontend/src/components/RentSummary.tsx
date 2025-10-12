import React from "react";
import { RentOutputs } from "../lib/types";

export default function RentSummary({ rent }: { rent: RentOutputs }) {
  const rows: [string, any][] = [
    ["Rent (SAR/m²/mo)", rent.rent_ppm2],
    ["Avg Unit Rent (SAR/unit/mo)", rent.rent_avg_unit],
    [
      "Occupancy",
      rent.occupancy != null
        ? `${(rent.occupancy * (rent.occupancy <= 1 ? 100 : 1)).toFixed(1)}%`
        : null,
    ],
    ["Annual Rent Revenue (SAR/yr)", rent.annual_rent_revenue],
    ["NOI (SAR/yr)", rent.noi],
    [
      "Cap rate",
      rent.cap_rate != null
        ? `${(rent.cap_rate * (rent.cap_rate <= 1 ? 100 : 1)).toFixed(2)}%`
        : null,
    ],
    ["Stabilized Value (NOI/Cap)", rent.stabilized_value],
  ];

  const fmt = (v: any) => {
    if (v == null) return "—";
    if (typeof v === "string") return v;
    const num = Number(v);
    return Number.isFinite(num) ? Intl.NumberFormat().format(num) : String(v);
  };

  return (
    <div className="card">
      <h3 className="card-title">Rent (Build-to-Rent) Summary</h3>
      <div className="grid grid-cols-2 gap-2">
        {rows.map(([k, v]) => (
          <div key={k} className="flex justify-between">
            <span className="text-slate-500">{k}</span>
            <span className="font-medium">{fmt(v)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
