export type LandUseCode = "s" | "m";

export type ExcelInputs = {
  // IMPORTANT: backend prefers this explicit field for landuse inference【turn10file7†file 89.txt†L41-L55】
  land_use_code?: LandUseCode;
  area_ratio: Record<string, number | string>;
  unit_cost: Record<string, number>;
  efficiency: Record<string, number>;
  cp_sqm_per_space: Record<string, number>;
  rent_sar_m2_yr: Record<string, number>;
  fitout_rate: number;
  contingency_pct: number;
  consultants_pct: number;
  transaction_pct: number;
  feasibility_fee_pct: number;
  opex_pct: number;
  y1_income_effective_pct?: number;
  land_price_sar_m2?: number;
};

// Baseline template: Residential ("s")
export const TEMPLATE_S: ExcelInputs = {
  land_use_code: "s",
  area_ratio: {
    residential: 1.6,
    basement: 1.0,
  },
  unit_cost: {
    residential: 2200,
    basement: 1200,
  },
  efficiency: {
    residential: 0.82,
  },
  cp_sqm_per_space: {
    basement: 30,
  },
  rent_sar_m2_yr: {
    residential: 2400,
  },
  fitout_rate: 400,
  y1_income_effective_pct: 90,
  contingency_pct: 0.05,
  consultants_pct: 0.06,
  transaction_pct: 0.05,
  feasibility_fee_pct: 0.02,
  opex_pct: 0.05,
};

// Mixed-use ("m") template: must include commercial keys so backend sees "m" even if land_use_code missing.
// Backend heuristic checks area_ratio keys for residential+commercial to infer "m"【turn10file7†file 89.txt†L57-L81】
export const TEMPLATE_M: ExcelInputs = {
  land_use_code: "m",
  area_ratio: {
    residential: 1.2,
    retail: 0.6,
    office: 0.4,
    basement: 1.0,
  },
  unit_cost: {
    residential: 2200,
    retail: 2600,
    office: 2400,
    basement: 1200,
  },
  efficiency: {
    residential: 0.82,
    retail: 0.88,
    office: 0.72,
  },
  cp_sqm_per_space: {
    residential: 35,
    retail: 30,
    office: 28,
    basement: 30,
  },
  rent_sar_m2_yr: {
    residential: 2400,
    retail: 3500,
    office: 3000,
  },
  fitout_rate: 400,
  y1_income_effective_pct: 90,
  contingency_pct: 0.05,
  consultants_pct: 0.06,
  transaction_pct: 0.05,
  feasibility_fee_pct: 0.02,
  opex_pct: 0.05,
};

export function templateForLandUse(code: LandUseCode): ExcelInputs {
  return code === "m" ? TEMPLATE_M : TEMPLATE_S;
}

export function cloneTemplate(t: ExcelInputs): ExcelInputs {
  // Avoid accidental shared mutation in React state
  return {
    ...t,
    area_ratio: { ...t.area_ratio },
    unit_cost: { ...t.unit_cost },
    efficiency: { ...t.efficiency },
    cp_sqm_per_space: { ...t.cp_sqm_per_space },
    rent_sar_m2_yr: { ...t.rent_sar_m2_yr },
  };
}
