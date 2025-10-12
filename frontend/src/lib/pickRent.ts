import { EstimateResponse, RentBlock, RentDriver, RentExplainability, RentOutputs, RentTopComparable } from "./types";

type RentPick = { rent: RentOutputs | null; comps: RentTopComparable[]; drivers: RentDriver[] };

export function pickRent(e: EstimateResponse): RentPick {
  const src = (e.rent ?? {}) as RentBlock;
  const val = (k: string) => src[k as keyof RentBlock] ?? (e as any)[k] ?? e.key_assumptions?.[k] ?? undefined;

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

  const explainability: RentExplainability =
    src.explainability ??
    src.rent_explainability ??
    e.rent_explainability ??
    {};

  const driversList = explainability.drivers ?? src.drivers ?? [];

  const comparablesSources: (RentTopComparable[] | undefined)[] = [
    explainability.top_comps,
    explainability.top_rent_comparables,
    explainability.rent_comparables,
    src.top_comps,
    src.top_rent_comparables,
    src.rent_comparables,
    e.explainability?.top_rent_comparables,
    e.explainability?.rent_comparables,
  ];

  const comps = comparablesSources.find((list) => Array.isArray(list) && list.length > 0) ?? [];

  const hasAny = Object.values(rent).some((v) => v != null);
  return {
    rent: hasAny ? rent : null,
    comps: Array.isArray(comps) ? comps : [],
    drivers: Array.isArray(driversList) ? driversList : [],
  };
}
