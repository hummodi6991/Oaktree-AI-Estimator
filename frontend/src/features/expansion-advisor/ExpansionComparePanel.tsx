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
  return key.replace(/_candidate_id$/, "").replace(/_/g, " ").trim();
}

export function getOrderedCompareSummaryEntries(summary: Record<string, string | null> = {}) {
  const sortedKnown = SUMMARY_KEY_ORDER.map((key) => [key, summary[key]] as const).filter((entry) => entry[1]);
  const knownKeys = new Set(SUMMARY_KEY_ORDER);
  const extras = Object.entries(summary).filter(([key, val]) => !knownKeys.has(key as (typeof SUMMARY_KEY_ORDER)[number]) && val);
  return [...sortedKnown, ...extras];
}

const DIMENSION_GROUPS = [
  {
    headingKey: "overallDimension",
    rows: [
      { label: "Rank", key: "rank_position" },
      { label: "Final score", key: "final_score" },
      { label: "Confidence", key: "confidence_grade" },
      { label: "Gate", key: "gate" },
    ],
  },
  {
    headingKey: "demandDimension",
    rows: [
      { label: "Brand fit", key: "brand_fit_score" },
      { label: "Provider density", key: "provider_density_score" },
      { label: "Whitespace", key: "provider_whitespace_score" },
    ],
  },
  {
    headingKey: "economicsDimension",
    rows: [
      { label: "Economics", key: "economics_score" },
      { label: "Payback", key: "payback_band" },
      { label: "Payback months", key: "estimated_payback_months" },
    ],
  },
  {
    headingKey: "siteDimension",
    rows: [
      { label: "Zoning", key: "zoning_fit_score" },
      { label: "Frontage", key: "frontage_score" },
      { label: "Access", key: "access_score" },
      { label: "Parking", key: "parking_score" },
      { label: "Visibility", key: "access_visibility_score" },
    ],
  },
] as const;

function bestCandidateForKey(items: Record<string, unknown>[], key: string): string | null {
  let bestId: string | null = null;
  let bestVal = -Infinity;
  for (const item of items) {
    const val = item[key];
    if (typeof val === "number" && val > bestVal) {
      bestVal = val;
      bestId = item.candidate_id as string;
    }
  }
  return bestId;
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

  function renderCell(item: Record<string, unknown>, key: string, isBest: boolean) {
    const raw = item[key];
    const cls = isBest ? "ea-compare-winner" : "";
    if (key === "final_score") return <td key={item.candidate_id as string} className={cls}><ScorePill value={item.final_score as number | undefined} /></td>;
    if (key === "confidence_grade") return <td key={item.candidate_id as string} className={cls}><ConfidenceBadge grade={item.confidence_grade as string | undefined} /></td>;
    if (key === "gate") return <td key={item.candidate_id as string}><span className={`ea-badge ea-badge--${gateColor((item.gate_status_json as Record<string, boolean> | undefined)?.overall_pass ?? null)}`}>{(item.gate_status_json as Record<string, boolean> | undefined)?.overall_pass ? t("expansionAdvisor.gatePass") : t("expansionAdvisor.gateFail")}</span></td>;
    if (key === "payback_band") return <td key={item.candidate_id as string} className={cls}><PaybackBadge band={item.payback_band as string | undefined} months={item.estimated_payback_months as number | undefined} /></td>;
    if (typeof raw === "number") return <td key={item.candidate_id as string} className={cls}>{fmtScore(raw)}</td>;
    return <td key={item.candidate_id as string} className={cls}>{raw != null ? String(raw) : "\u2014"}</td>;
  }

  // Drawer when we have a result
  return (
    <div className="ea-drawer-backdrop" onClick={() => onClose?.()}>
      <div className="ea-drawer ea-drawer--wide" onClick={(e) => e.stopPropagation()}>
        <div className="ea-drawer__header">
          <h3 className="ea-drawer__title">{t("expansionAdvisor.compareCandidates")}</h3>
          <button className="ea-drawer__close" onClick={() => onClose?.()}>{t("expansionAdvisor.close")}</button>
        </div>
        <div className="ea-drawer__body">
          {loading && <div className="ea-state ea-state--loading">{t("expansionAdvisor.loading")}</div>}
          {error && <div className="ea-state ea-state--error">{error}</div>}

          {/* Best-on-X highlights */}
          {summaryEntries.length > 0 && (
            <div className="ea-compare-highlights">
              <h5 className="ea-detail__section-title">{t("expansionAdvisor.bestOnHighlights")}</h5>
              <div className="ea-compare-highlights__grid">
                {summaryEntries.map(([k, v]) => (
                  <div key={k} className="ea-compare-highlight" style={{ cursor: v ? "pointer" : "default" }} onClick={() => v && onSelectCandidateId?.(v)}>
                    <span className="ea-compare-highlight__label">{summaryLabel(k)}</span>
                    <span className="ea-badge ea-badge--green">{v ? (items.find((i) => i.candidate_id === v)?.district || v.slice(0, 8)) : "\u2014"}</span>
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
                    <th>{t("expansionAdvisor.dimension")}</th>
                    {items.map((item) => (
                      <th key={item.candidate_id} style={{ cursor: "pointer" }} onClick={() => item.candidate_id && onSelectCandidateId?.(item.candidate_id)}>
                        {item.district || item.candidate_id?.slice(0, 8) || "\u2014"}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {DIMENSION_GROUPS.map((group) => (
                    <>{/* dimension group */}
                      <tr key={`heading-${group.headingKey}`} className="ea-compare-table__group-row">
                        <td colSpan={items.length + 1} className="ea-compare-table__group-heading">{t(`expansionAdvisor.${group.headingKey}`)}</td>
                      </tr>
                      {group.rows.map((row) => {
                        const bestId = bestCandidateForKey(items as unknown as Record<string, unknown>[], row.key);
                        return (
                          <tr key={row.key}>
                            <td style={{ fontWeight: 500, color: "var(--oak-text-light)" }}>{row.label}</td>
                            {items.map((item) => renderCell(item as unknown as Record<string, unknown>, row.key, item.candidate_id === bestId))}
                          </tr>
                        );
                      })}
                    </>
                  ))}
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
