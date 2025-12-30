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

export type EstimateStrategy = "build_to_sell" | "build_to_rent";

export interface EstimateTotals {
  land_value: number;
  hard_costs: number;
  soft_costs: number;
  financing: number;
  revenues: number;
  p50_profit: number;
  excel_roi?: number;

  parking_required_spaces?: number;
  parking_provided_spaces?: number;
  parking_deficit_spaces?: number;
  parking_compliant?: boolean;

  [key: string]: number | boolean | undefined;
}

export interface EstimateNotes {
  [key: string]: any;
  parking?: any;
  notes?: any;
}

export type EstimateResponse = Record<string, any> & {
  id: string;
  strategy: EstimateStrategy;
  totals: EstimateTotals;
  assumptions: Array<Record<string, any>>;
  notes: EstimateNotes;
  rent: RentBlock;
  metrics?: { irr_annual?: number };
  confidence_bands?: { p5?: number; p50?: number; p95?: number };
  land_value_breakdown?: Record<string, any>;
  explainability?: {
    top_comps?: any[];
    drivers?: ExplainabilityRow[];
  };
};
