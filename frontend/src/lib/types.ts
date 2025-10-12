export type RentOutputs = {
  rent_ppm2?: number; // SAR/m²/month
  rent_avg_unit?: number; // SAR/unit/month
  occupancy?: number; // 0–1 or %
  cap_rate?: number; // 0–1
  noi?: number; // SAR/yr
  annual_rent_revenue?: number; // SAR/yr (backend-provided if available)
  stabilized_value?: number; // SAR (NOI/cap_rate if both present)
};

export type RentExplainabilityRow = {
  id: string;
  date: string;
  district?: string;
  rent_ppm2?: number;
  source?: string;
};

export type EstimateResponse = Record<string, any> & {
  // existing fields…
  rent?: RentOutputs; // if backend groups under `rent`
  // fallback keys (some backends flatten fields). Leave optional:
  rent_ppm2?: number;
  rent_avg_unit?: number;
  occupancy?: number;
  cap_rate?: number;
  noi?: number;
  annual_rent_revenue?: number;
  stabilized_value?: number;

  explainability?: {
    // keep existing sale comparables
    rent_comparables?: RentExplainabilityRow[];
    top_rent_comparables?: RentExplainabilityRow[]; // support either key
  };
  key_assumptions?: Record<string, any>; // we’ll read rent_* if present
};
