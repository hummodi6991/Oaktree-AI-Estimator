export type RentOutputs = {
  rent_ppm2?: number; // SAR/m²/month
  rent_avg_unit?: number; // SAR/unit/month
  occupancy?: number; // 0–1 or %
  cap_rate?: number; // 0–1
  noi?: number; // SAR/yr
  annual_rent_revenue?: number; // SAR/yr (backend-provided if available)
  stabilized_value?: number; // SAR (NOI/cap_rate if both present)
};

export type RentDriver = {
  name?: string;
  direction?: string;
  magnitude?: number;
  unit?: string;
};

export type RentTopComparable = {
  identifier?: string;
  id?: string;
  Identifier?: string;
  date?: string;
  Date?: string;
  city?: string;
  City?: string;
  district?: string;
  District?: string;
  rent_ppm2?: number;
  rent_per_m2?: number;
  sar_per_m2?: number;
  price_per_m2?: number;
  Rent_SAR_m2_mo?: number;
  source?: string;
  Source?: string;
  source_url?: string;
};

export type RentExplainability = {
  drivers?: RentDriver[];
  top_comps?: RentTopComparable[];
  rent_comparables?: RentTopComparable[];
  top_rent_comparables?: RentTopComparable[];
};

export type RentBlock = RentOutputs & {
  explainability?: RentExplainability;
  rent_explainability?: RentExplainability;
  drivers?: RentDriver[];
  top_comps?: RentTopComparable[];
  rent_comparables?: RentTopComparable[];
  top_rent_comparables?: RentTopComparable[];
};

export type EstimateResponse = Record<string, any> & {
  // existing fields…
  rent?: RentBlock; // if backend groups under `rent`
  rent_explainability?: RentExplainability;
  // fallback keys (some backends flatten fields). Leave optional:
  rent_ppm2?: number;
  rent_avg_unit?: number;
  occupancy?: number;
  cap_rate?: number;
  noi?: number;
  annual_rent_revenue?: number;
  stabilized_value?: number;

  explainability?: RentExplainability & {
    // keep existing sale comparables
    top_comps?: any[];
  };
  key_assumptions?: Record<string, any>; // we’ll read rent_* if present
};
