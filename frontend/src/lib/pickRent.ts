import { EstimateResponse, RentOutputs } from "./types";

export function pickRent(e: EstimateResponse): { rent: RentOutputs | null; comps: any[] } {
  const src = (e.rent ?? {}) as any;
  const val = (k: string) => src[k] ?? (e as any)[k] ?? e.key_assumptions?.[k] ?? undefined;

  const rent: RentOutputs = {
    rent_ppm2: val("rent_ppm2"),
    rent_avg_unit: val("rent_avg_unit"),
    occupancy: val("occupancy") ?? val("occ"),
    cap_rate: val("cap_rate") ?? val("exit_cap_rate"),
    noi: val("noi"),
    annual_rent_revenue: val("annual_rent_revenue"),
    stabilized_value: val("stabilized_value"),
  };

  if (!rent.stabilized_value && rent.noi && rent.cap_rate) {
    rent.stabilized_value = rent.noi / rent.cap_rate;
  }

  const comps = (e.explainability?.rent_comparables ?? e.explainability?.top_rent_comparables ?? []) as any[];

  const hasAny = Object.values(rent).some((v) => v != null);
  return { rent: hasAny ? rent : null, comps };
}
