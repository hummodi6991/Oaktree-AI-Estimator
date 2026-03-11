import { useTranslation } from "react-i18next";
import type { CompareCandidatesResponse } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";
import ConfidenceBadge from "./ConfidenceBadge";
import PaybackBadge from "./PaybackBadge";
import { fmtScore, gateColor } from "./formatHelpers";

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

function summaryLabel(key: string) {
  return key.replace(/_candidate_id$/, "").replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase()).trim();
}

export function getOrderedCompareSummaryEntries(summary: Record<string, string | null> = {}) {
  const sortedKnown = SUMMARY_KEY_ORDER.map((key) => [key, summary[key]] as const).filter((entry) => entry[1]);
  const knownKeys = new Set(SUMMARY_KEY_ORDER);
  const extras = Object.entries(summary).filter(([key, val]) => !knownKeys.has(key as (typeof SUMMARY_KEY_ORDER)[number]) && val);
  return [...sortedKnown, ...extras];
}

const METRIC_ROWS = [
  { label: "Rank", key: "rank_position" },
  { label: "Overall Score", key: "final_score" },
  { label: "Confidence", key: "confidence_grade" },
  { label: "Screening Gate", key: "gate" },
  { label: "Economics", key: "economics_score" },
  { label: "Brand Fit", key: "brand_fit_score" },
  { label: "Zoning", key: "zoning_fit_score" },
  { label: "Frontage", key: "frontage_score" },
  { label: "Access", key: "access_score" },
  { label: "Parking", key: "parking_score" },
  { label: "Visibility", key: "access_visibility_score" },
  { label: "Provider Density", key: "provider_density_score" },
  { label: "Whitespace", key: "provider_whitespace_score" },
  { label: "Payback Band", key: "payback_band" },
  { label: "Payback (months)", key: "estimated_payback_months" },
];

function findBestIdx(items: Record<string, unknown>[], key: string): number {
  let bestIdx = -1;
  let bestVal = -Infinity;
  for (let i = 0; i < items.length; i++) {
    const v = items[i][key];
    if (typeof v === "number" && v > bestVal) { bestVal = v; bestIdx = i; }
  }
  return bestIdx;
}

export default function ExpansionComparePanel({
  compareIds,
  result,
  loading,
  error,
  onCompare,
  onSelectCandidateId,
  onClose,
}: {
  compareIds: string[];
  result: CompareCandidatesResponse | null;
  loading: boolean;
  error: string | null;
  onCompare: () => void;
  onSelectCandidateId?: (candidateId: string) => void;
  onClose?: () => void;
}) {
  const { t } = useTranslation();
  const enabled = compareIds.length >= 2 && compareIds.length <= 6;
  const summaryEntries = getOrderedCompareSummaryEntries(result?.summary || {});
  const items = result?.items || [];

  // Inline compact view when no result yet
  if (!result && !loading && !error) {
    return (
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <button className="oak-btn oak-btn--sm oak-btn--primary" disabled={!enabled || loading} onClick={onCompare}>
          {enabled ? t("expansionAdvisor.compareSelected", { count: compareIds.length }) : t("expansionAdvisor.compareNeedTwo")}
        </button>
      </div>
    );
  }

  // Drawer when we have a result
  return (
    <div className="ea-drawer-backdrop" onClick={() => onClose?.()}>
      <div className="ea-drawer" onClick={(e) => e.stopPropagation()}>
        <div className="ea-drawer__header">
          <h3 className="ea-drawer__title">{t("expansionAdvisor.compareCandidates")}</h3>
          <button className="ea-drawer__close" onClick={() => onClose?.()}>{t("expansionAdvisor.close")}</button>
        </div>
        <div className="ea-drawer__body">
          {loading && <div className="ea-state ea-state--loading">{t("expansionAdvisor.loading")}</div>}
          {error && <div className="ea-state ea-state--error">{error}</div>}

          {/* Winner highlights */}
          {summaryEntries.length > 0 && (
            <div className="ea-detail__section">
              <h5 className="ea-detail__section-title">{t("expansionAdvisor.compareSummary")}</h5>
              <div className="ea-compare-winners">
                {summaryEntries.map(([k, v]) => (
                  <div key={k} className="ea-compare-winner-item" style={{ cursor: v ? "pointer" : "default" }} onClick={() => v && onSelectCandidateId?.(v)}>
                    <span className="ea-compare-winner-item__label">{summaryLabel(k)}</span>
                    <span className="ea-badge ea-badge--green">{v ? (items.find((i) => i.candidate_id === v)?.district || v.slice(0, 8)) : "—"}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {items.length > 0 && (
            <div style={{ overflowX: "auto" }}>
              <table className="ea-compare-table">
                <thead>
                  <tr>
                    <th>{t("expansionAdvisor.score")}</th>
                    {items.map((item) => (
                      <th key={item.candidate_id} style={{ cursor: "pointer" }} onClick={() => item.candidate_id && onSelectCandidateId?.(item.candidate_id)}>
                        {item.district || item.candidate_id?.slice(0, 8) || "—"}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {METRIC_ROWS.map((row) => {
                    const bestIdx = row.key !== "gate" && row.key !== "confidence_grade" && row.key !== "payback_band" ? findBestIdx(items as unknown as Record<string, unknown>[], row.key) : -1;
                    return (
                      <tr key={row.key}>
                        <td style={{ fontWeight: 500, color: "var(--oak-text-light)" }}>{row.label}</td>
                        {items.map((item, idx) => {
                          const raw = (item as Record<string, unknown>)[row.key];
                          const isBest = idx === bestIdx;
                          const cellClass = isBest ? "ea-compare-winner" : "";
                          if (row.key === "final_score") return <td key={item.candidate_id} className={cellClass}><ScorePill value={item.final_score} /></td>;
                          if (row.key === "confidence_grade") return <td key={item.candidate_id}><ConfidenceBadge grade={item.confidence_grade} /></td>;
                          if (row.key === "gate") return <td key={item.candidate_id}><span className={`ea-badge ea-badge--${gateColor(item.gate_status_json?.overall_pass ?? null)}`}>{item.gate_status_json?.overall_pass ? t("expansionAdvisor.gatePass") : t("expansionAdvisor.gateFail")}</span></td>;
                          if (row.key === "payback_band") return <td key={item.candidate_id}><PaybackBadge band={item.payback_band} months={item.estimated_payback_months} /></td>;
                          if (typeof raw === "number") return <td key={item.candidate_id} className={cellClass}>{fmtScore(raw)}</td>;
                          return <td key={item.candidate_id}>{raw != null ? String(raw) : "—"}</td>;
                        })}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}

          {enabled && !loading && (
            <button className="oak-btn oak-btn--primary" onClick={onCompare}>
              {t("expansionAdvisor.compareSelected", { count: compareIds.length })}
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
