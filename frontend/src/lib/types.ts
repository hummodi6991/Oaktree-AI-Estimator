export type ExplainabilityRow = {
  name: string;
  direction: string;
  magnitude: number;
  unit?: string;
};

export type RentComparable = {
  id: string;
  date?: string | null;
  city?: string | null;
  district?: string | null;
  sar_per_m2?: number | null;
  source?: string | null;
  source_url?: string | null;
};

export type RentBlock = {
  drivers: ExplainabilityRow[];
  top_comps: RentComparable[];
  rent_comparables: RentComparable[];
  top_rent_comparables: RentComparable[];
  rent_price_per_m2?: number | null;
  rent_unit_rate?: number | null;
  rent_vacancy_pct?: number | null;
  rent_growth_pct?: number | null;
};

export type EstimateResponse = Record<string, any> & {
  id: string;
  strategy: "build_to_sell" | "build_to_rent";
  totals: Record<string, number>;
  assumptions: Array<Record<string, any>>;
  notes: Record<string, any>;
  rent: RentBlock;
  metrics?: { irr_annual?: number };
  confidence_bands?: { p5?: number; p50?: number; p95?: number };
  land_value_breakdown?: Record<string, any>;
  explainability?: {
    top_comps?: any[];
    drivers?: ExplainabilityRow[];
  };
};
