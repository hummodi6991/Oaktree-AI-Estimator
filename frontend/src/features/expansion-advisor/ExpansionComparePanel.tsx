import { useTranslation } from "react-i18next";
import type { CompareCandidatesResponse } from "../../lib/api/expansionAdvisor";

function value(input?: string | number) {
  return input ?? "-";
}

export default function ExpansionComparePanel({ compareIds, result, loading, error, onCompare }: { compareIds: string[]; result: CompareCandidatesResponse | null; loading: boolean; error: string | null; onCompare: () => void }) {
  const { t } = useTranslation();
  const enabled = compareIds.length >= 2 && compareIds.length <= 6;
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
                <tr key={item.id}>
                  <td>{item.id}</td><td>{value(item.rank_position)}</td><td>{value(item.final_score)}</td><td>{value(item.confidence_grade)}</td><td>{item.gate_status_json?.overall_pass ? t("expansionAdvisor.pass") : t("expansionAdvisor.fail")}</td><td>{value(item.zoning_fit_score)}</td><td>{value(item.frontage_score)}</td><td>{value(item.access_score)}</td><td>{value(item.parking_score)}</td><td>{value(item.access_visibility_score)}</td><td>{value(item.economics_score)}</td><td>{value(item.brand_fit_score)}</td><td>{value(item.provider_density_score)}</td><td>{value(item.provider_whitespace_score)}</td><td>{value(item.estimated_payback_months)}</td><td>{value(item.payback_band)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {result.summary ? <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginTop: 8 }}>{Object.entries(result.summary).map(([k, v]) => <span key={k} style={{ border: "1px solid #d8e1dd", borderRadius: 12, padding: "2px 8px" }}>{k.replace(/_/g, " ")}: {v}</span>)}</div> : null}
        </div>
      ) : null}
    </div>
  );
}
