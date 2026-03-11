import { useTranslation } from "react-i18next";
import type { CompareCandidatesResponse } from "../../lib/api/expansionAdvisor";

const SUMMARY_KEY_ORDER = [
  "best_overall_candidate_id",
  "best_gate_pass_candidate_id",
  "best_economics_candidate_id",
  "fastest_payback_candidate_id",
  "lowest_rent_burden_candidate_id",
  "best_brand_fit_candidate_id",
  "highest_demand_candidate_id",
  "strongest_delivery_market_candidate_id",
  "strongest_whitespace_candidate_id",
  "lowest_cannibalization_candidate_id",
  "most_confident_candidate_id",
] as const;

function value(input?: string | number) {
  return input ?? "-";
}

export function getOrderedCompareSummaryEntries(summary: Record<string, string | null> = {}) {
  const sortedKnown = SUMMARY_KEY_ORDER.map((key) => [key, summary[key]] as const).filter((entry) => entry[1]);
  const knownKeys = new Set(SUMMARY_KEY_ORDER);
  const extras = Object.entries(summary).filter(([key, val]) => !knownKeys.has(key as (typeof SUMMARY_KEY_ORDER)[number]) && val);
  return [...sortedKnown, ...extras];
}

export default function ExpansionComparePanel({ compareIds, result, loading, error, onCompare }: { compareIds: string[]; result: CompareCandidatesResponse | null; loading: boolean; error: string | null; onCompare: () => void }) {
  const { t } = useTranslation();
  const enabled = compareIds.length >= 2 && compareIds.length <= 6;
  const summaryEntries = getOrderedCompareSummaryEntries(result?.summary || {});
  return (
    <div style={{ display: "grid", gap: 8 }}>
      <button disabled={!enabled || loading} onClick={onCompare}>{enabled ? t("expansionAdvisor.compareCandidates") : t("expansionAdvisor.compareNeedTwo")}</button>
      {error ? <small>{error}</small> : null}
      {result?.items?.length ? (
        <div>
          <table>
            <thead>
              <tr>
                <th>candidate_id</th><th>rank_position</th><th>final_score</th><th>confidence_grade</th><th>gate</th><th>zoning_fit_score</th><th>frontage_score</th><th>access_score</th><th>parking_score</th><th>access_visibility_score</th><th>economics_score</th><th>brand_fit_score</th><th>provider_density_score</th><th>provider_whitespace_score</th><th>estimated_payback_months</th><th>payback_band</th>
              </tr>
            </thead>
            <tbody>
              {result.items.map((item) => (
                <tr key={item.candidate_id}>
                  <td>{item.candidate_id}</td><td>{value(item.rank_position)}</td><td>{value(item.final_score)}</td><td>{value(item.confidence_grade)}</td><td>{item.gate_status_json?.overall_pass ? t("expansionAdvisor.pass") : t("expansionAdvisor.fail")}</td><td>{value(item.zoning_fit_score)}</td><td>{value(item.frontage_score)}</td><td>{value(item.access_score)}</td><td>{value(item.parking_score)}</td><td>{value(item.access_visibility_score)}</td><td>{value(item.economics_score)}</td><td>{value(item.brand_fit_score)}</td><td>{value(item.provider_density_score)}</td><td>{value(item.provider_whitespace_score)}</td><td>{value(item.estimated_payback_months)}</td><td>{value(item.payback_band)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {summaryEntries.length ? <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginTop: 8 }}>{summaryEntries.map(([k, v]) => <span key={k} style={{ border: "1px solid #d8e1dd", borderRadius: 12, padding: "2px 8px" }}>{k.replace(/_/g, " ")}: {v}</span>)}</div> : null}
        </div>
      ) : null}
    </div>
  );
}
