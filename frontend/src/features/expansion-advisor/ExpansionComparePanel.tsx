import React from "react";
import { useTranslation } from "react-i18next";
import type { CompareCandidatesResponse } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";
import ConfidenceBadge from "./ConfidenceBadge";
import PaybackBadge from "./PaybackBadge";
import { fmtScore, fmtSAR, fmtMonths, fmtSarPerM2Year, gateColor, candidateDistrictLabel } from "./formatHelpers";

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

const SUMMARY_LABELS: Record<string, string> = {
  best_overall_candidate_id: "Best Overall",
  best_gate_pass_candidate_id: "Best Gate Pass",
  best_economics_candidate_id: "Best Economics",
  fastest_payback_candidate_id: "Fastest Payback",
  lowest_rent_burden_candidate_id: "Lowest Rent Burden",
  best_brand_fit_candidate_id: "Best Brand Fit",
  highest_demand_candidate_id: "Highest Demand",
  strongest_delivery_market_candidate_id: "Strongest Delivery",
  strongest_whitespace_candidate_id: "Strongest Whitespace",
  lowest_cannibalization_candidate_id: "Lowest Cannibalization",
  most_confident_candidate_id: "Most Confident",
};

function summaryLabel(key: string) {
  return SUMMARY_LABELS[key] || key.replace(/_candidate_id$/, "").replace(/_/g, " ").trim();
}

export function getOrderedCompareSummaryEntries(summary: Record<string, string | null> = {}) {
  const sortedKnown = SUMMARY_KEY_ORDER.map((key) => [key, summary[key]] as const).filter((entry) => entry[1]);
  const knownKeys = new Set(SUMMARY_KEY_ORDER);
  const extras = Object.entries(summary).filter(([key, val]) => !knownKeys.has(key as (typeof SUMMARY_KEY_ORDER)[number]) && val);
  return [...sortedKnown, ...extras];
}

type DimensionGroup = {
  label: string;
  rows: Array<{ label: string; key: string; fmt?: "sar" | "months" | "sar_m2_year" }>;
};

const DIMENSION_GROUPS: DimensionGroup[] = [
  {
    label: "Overall Rank & Score",
    rows: [
      { label: "Rank", key: "rank_position" },
      { label: "Final score", key: "final_score" },
      { label: "Confidence", key: "confidence_grade" },
      { label: "Gate status", key: "gate" },
    ],
  },
  {
    label: "Demand & Whitespace",
    rows: [
      { label: "Demand", key: "demand_score" },
      { label: "Fit", key: "fit_score" },
      { label: "Brand fit", key: "brand_fit_score" },
      { label: "Provider density", key: "provider_density_score" },
      { label: "Whitespace", key: "provider_whitespace_score" },
      { label: "Delivery competition", key: "delivery_competition_score" },
      { label: "Multi-platform", key: "multi_platform_presence_score" },
    ],
  },
  {
    label: "Economics & Rent",
    rows: [
      { label: "Economics", key: "economics_score" },
      { label: "Payback", key: "payback_band" },
      { label: "Payback months", key: "estimated_payback_months", fmt: "months" },
      { label: "Rent/m²/yr", key: "estimated_rent_sar_m2_year", fmt: "sar_m2_year" },
      { label: "Annual rent", key: "display_annual_rent_sar", fmt: "sar" },
      { label: "Cannibalization", key: "cannibalization_score" },
    ],
  },
  {
    label: "Site Quality",
    rows: [
      { label: "Zoning", key: "zoning_fit_score" },
      { label: "Frontage", key: "frontage_score" },
      { label: "Access", key: "access_score" },
      { label: "Parking", key: "parking_score" },
      { label: "Visibility", key: "access_visibility_score" },
    ],
  },
];

function findBestOnKey(items: Array<Record<string, unknown>>, key: string): string | null {
  if (!items.length) return null;
  if (key === "gate" || key === "confidence_grade" || key === "payback_band") return null;
  // For payback/cannibalization/rent, lower is better
  const lowerIsBetter = key === "estimated_payback_months" || key === "cannibalization_score" || key === "estimated_rent_sar_m2_year" || key === "estimated_annual_rent_sar" || key === "display_annual_rent_sar";
  let best: { id: string | null; val: number } = { id: null, val: lowerIsBetter ? Infinity : -Infinity };
  for (const item of items) {
    const raw = item[key];
    if (typeof raw === "number") {
      if (lowerIsBetter ? raw < best.val : raw > best.val) {
        best = { id: item.candidate_id as string, val: raw };
      }
    }
  }
  return best.id;
}

export default function ExpansionComparePanel({
  compareIds,
  result,
  loading,
  error,
  leadCandidateId,
  onCompare,
  onSelectCandidateId,
  onClose,
}: {
  compareIds: string[];
  result: CompareCandidatesResponse | null;
  loading: boolean;
  error: string | null;
  leadCandidateId?: string | null;
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
      <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
        <button className="oak-btn oak-btn--sm oak-btn--primary" disabled={!enabled || loading} onClick={onCompare}>
          {enabled ? t("expansionAdvisor.compareSelected", { count: compareIds.length }) : t("expansionAdvisor.compareNeedTwo")}
        </button>
        {compareIds.length > 6 && <span className="ea-form__error">{t("expansionAdvisor.compareLimitWarning")}</span>}
        {compareIds.length === 1 && <span style={{ fontSize: "var(--oak-fs-xs)", color: "var(--oak-text-light)" }}>{t("expansionAdvisor.compareMinWarning")}</span>}
      </div>
    );
  }

  // Drawer when we have a result
  return (
    <div className="ea-drawer-backdrop" onClick={() => onClose?.()}>
      <div className="ea-drawer ea-drawer--wide" onClick={(e) => e.stopPropagation()}>
        <div className="ea-drawer__header">
          <h3 className="ea-drawer__title">{t("expansionAdvisor.compareDimensions")}</h3>
          <button className="ea-drawer__close" onClick={() => onClose?.()}>{t("expansionAdvisor.close")}</button>
        </div>
        <div className="ea-drawer__body">
          {loading && <div className="ea-state ea-state--loading">{t("expansionAdvisor.loading")}</div>}
          {error && <div className="ea-state ea-state--error">{error}</div>}

          {/* Best-on highlights — surface winners */}
          {summaryEntries.length > 0 && (
            <div className="ea-compare-highlights">
              {summaryEntries.map(([k, v]) => (
                <span
                  key={k}
                  className="ea-compare-highlight"
                  style={{ cursor: v ? "pointer" : "default" }}
                  onClick={() => v && onSelectCandidateId?.(v)}
                >
                  <span className="ea-compare-highlight__dim">{summaryLabel(k)}</span>
                  <span className="ea-badge ea-badge--green">{t("expansionAdvisor.compareBestHighlight")}</span>
                </span>
              ))}
            </div>
          )}

          {items.length > 0 && (
            <div style={{ overflowX: "auto" }}>
              <table className="ea-compare-table">
                <thead>
                  <tr>
                    <th>{t("expansionAdvisor.compareDimensions")}</th>
                    {items.map((item) => (
                      <th key={item.candidate_id} style={{ cursor: "pointer" }} onClick={() => item.candidate_id && onSelectCandidateId?.(item.candidate_id)} className={item.candidate_id === leadCandidateId ? "ea-compare-table__lead" : ""}>
                        {item.candidate_id === leadCandidateId && (item as Record<string, unknown>).gate_verdict === "pass" && <span className="ea-lead-tag ea-lead-tag--sm">{t("expansionAdvisor.leadSite")}</span>}{" "}
                        {item.candidate_id === leadCandidateId && (item as Record<string, unknown>).gate_verdict !== "pass" && <span className="ea-lead-tag ea-lead-tag--sm ea-lead-tag--exploratory">{t("expansionAdvisor.topExploratoryCandidate")}</span>}{" "}
                        {candidateDistrictLabel(item, item.candidate_id?.slice(0, 8) || "—")}
                        {item.rank_position ? <span className="ea-compare-table__rank"> #{item.rank_position}</span> : null}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {DIMENSION_GROUPS.map((group) => (
                    <React.Fragment key={`group-${group.label}`}>
                      <tr className="ea-compare-table__group-header">
                        <td colSpan={items.length + 1}>{group.label}</td>
                      </tr>
                      {group.rows.map((row) => {
                        const bestId = findBestOnKey(items as unknown as Array<Record<string, unknown>>, row.key);
                        return (
                          <tr key={row.key}>
                            <td style={{ fontWeight: 500, color: "var(--oak-text-light)" }}>{row.label}</td>
                            {items.map((item) => {
                              const raw = (item as Record<string, unknown>)[row.key];
                              const isBest = bestId != null && item.candidate_id === bestId;
                              const cellCls = isBest ? "ea-compare-winner" : "";
                              if (row.key === "final_score") return <td key={item.candidate_id} className={cellCls}><ScorePill value={item.final_score} /></td>;
                              if (row.key === "confidence_grade") return <td key={item.candidate_id}><ConfidenceBadge grade={item.confidence_grade} /></td>;
                              if (row.key === "gate") { const gv = item.gate_status_json?.overall_pass; return <td key={item.candidate_id}><span className={`ea-badge ea-badge--${gateColor(gv ?? null)}`}>{gv === true ? t("expansionAdvisor.gatePass") : gv === false ? t("expansionAdvisor.gateFail") : t("expansionAdvisor.gateUnknown")}</span></td>; }
                              if (row.key === "payback_band") return <td key={item.candidate_id}><PaybackBadge band={item.payback_band} months={item.estimated_payback_months} /></td>;
                              if (row.fmt === "sar" && typeof raw === "number") return <td key={item.candidate_id} className={cellCls}>{fmtSAR(raw)}</td>;
                              if (row.fmt === "sar_m2_year" && typeof raw === "number") return <td key={item.candidate_id} className={cellCls}>{fmtSarPerM2Year(raw)}</td>;
                              if (row.fmt === "months" && typeof raw === "number") return <td key={item.candidate_id} className={cellCls}>{fmtMonths(raw)}</td>;
                              if (typeof raw === "number") return <td key={item.candidate_id} className={cellCls}>{fmtScore(raw)}</td>;
                              return <td key={item.candidate_id}>{raw != null ? String(raw) : "—"}</td>;
                            })}
                          </tr>
                        );
                      })}
                    </React.Fragment>
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
