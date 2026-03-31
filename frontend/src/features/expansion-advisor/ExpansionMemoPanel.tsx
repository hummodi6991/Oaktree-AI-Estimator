import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { CandidateMemoResponse, RecommendationReportResponse } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";
import ConfidenceBadge from "./ConfidenceBadge";
import PaybackBadge from "./PaybackBadge";
import GateSummary from "./GateSummary";
import CopySummaryBlock from "./CopySummaryBlock";
import { fmtScore, fmtMeters, fmtSAR, businessGateLabel, safeDistrictLabel, getDisplayScore } from "./formatHelpers";

function toList(input: unknown): string[] {
  return Array.isArray(input) ? input.map(String) : [];
}

export default function ExpansionMemoPanel({
  memo,
  loading,
  isLeadCandidate,
  report,
  onClose,
  onBackToDetail,
  onBackToCompare,
  onOpenCompare,
  hasShortlist,
  hasCompare,
}: {
  memo: CandidateMemoResponse | null;
  loading: boolean;
  isLeadCandidate?: boolean;
  report?: RecommendationReportResponse | null;
  onClose?: () => void;
  onBackToDetail?: () => void;
  onBackToCompare?: () => void;
  onOpenCompare?: () => void;
  hasShortlist?: boolean;
  hasCompare?: boolean;
}) {
  const { t } = useTranslation();
  const [presentationMode, setPresentationMode] = useState(false);

  if (!memo && !loading) return null;

  return (
    <div className="ea-drawer-backdrop" onClick={() => onClose?.()}>
      <div className={`ea-drawer ea-drawer--wide${presentationMode ? " ea-drawer--presentation" : ""}`} onClick={(e) => e.stopPropagation()}>
        <div className="ea-drawer__header">
          {isLeadCandidate && (() => {
            const cand = memo?.candidate || {};
            const gatePass = (cand.gate_status as Record<string, unknown> | undefined)?.overall_pass === true;
            return <span className={`ea-lead-tag${gatePass ? "" : " ea-lead-tag--exploratory"}`}>{gatePass ? t("expansionAdvisor.leadCandidate") : t("expansionAdvisor.topExploratoryCandidate")}</span>;
          })()}
          <h3 className="ea-drawer__title">{t("expansionAdvisor.decisionMemo")}</h3>
          <div style={{ display: "flex", gap: 6, alignItems: "center", marginInlineStart: "auto", flexWrap: "wrap" }}>
            {onBackToDetail && (
              <button className="oak-btn oak-btn--xs oak-btn--tertiary" onClick={onBackToDetail}>
                {t("expansionAdvisor.memoBackToDetail")}
              </button>
            )}
            {hasCompare && onBackToCompare && (
              <button className="oak-btn oak-btn--xs oak-btn--tertiary" onClick={onBackToCompare}>
                {t("expansionAdvisor.memoBackToCompare")}
              </button>
            )}
            {hasShortlist && !hasCompare && onOpenCompare && (
              <button className="oak-btn oak-btn--xs oak-btn--tertiary" onClick={onOpenCompare}>
                {t("expansionAdvisor.memoOpenCompare")}
              </button>
            )}
            <button className="oak-btn oak-btn--xs oak-btn--tertiary" onClick={() => setPresentationMode((m) => !m)}>
              {presentationMode ? t("expansionAdvisor.exitPresentation") : t("expansionAdvisor.presentationMode")}
            </button>
            <button className="ea-drawer__close" onClick={() => onClose?.()}>{t("expansionAdvisor.close")}</button>
          </div>
        </div>
        <div className="ea-drawer__body">
          {loading && <div className="ea-state ea-state--loading">{t("expansionAdvisor.loadingMemo")}</div>}

          {memo && (() => {
            const rec = memo.recommendation || {};
            const mr = memo.market_research || {};
            const cand = memo.candidate || {};
            const gates = (cand.gate_status || {}) as Record<string, boolean | null | undefined>;
            const gateReasons = cand.gate_reasons;
            const snapshot = cand.feature_snapshot;
            const breakdown = cand.score_breakdown_json;
            const comps = (cand.comparable_competitors || []) as Array<Record<string, unknown>>;
            const positives = toList(cand.top_positives_json);
            const risks = toList(cand.top_risks_json);

            return (
              <>
                {/* Verdict hero */}
                <div className="ea-memo-verdict">
                  {rec.verdict && (
                    <span className={`ea-memo-verdict__badge ea-badge ea-badge--${rec.verdict?.toLowerCase() === "go" ? "green" : rec.verdict?.toLowerCase() === "caution" ? "amber" : "red"}`}>
                      {rec.verdict}
                    </span>
                  )}
                  {rec.headline && <h4 className="ea-memo-verdict__headline">{rec.headline}</h4>}
                </div>

                {/* Best use case + watchout */}
                {(rec.best_use_case || rec.main_watchout) && (
                  <div className="ea-report-section">
                    {rec.best_use_case && (
                      <div className="ea-memo-callout ea-memo-callout--positive">
                        <span className="ea-memo-callout__label">{t("expansionAdvisor.bestUseCase")}</span>
                        <p className="ea-detail__text">{rec.best_use_case}</p>
                      </div>
                    )}
                    {rec.main_watchout && (
                      <div className="ea-memo-callout ea-memo-callout--risk">
                        <span className="ea-memo-callout__label">{t("expansionAdvisor.mainWatchout")}</span>
                        <p className="ea-detail__text">{rec.main_watchout}</p>
                      </div>
                    )}
                  </div>
                )}

                {/* Scorecard */}
                <div className="ea-report-section">
                  <h5 className="ea-detail__section-title">{t("expansionAdvisor.memoScorecard")}</h5>
                  <div className="ea-detail__grid">
                    <div className="ea-detail__kv">
                      <span className="ea-detail__kv-label">{t("expansionAdvisor.finalScore")}</span>
                      <ScorePill value={(breakdown?.display_score as number | undefined) ?? (cand.final_score as number | undefined)} large />
                    </div>
                    <div className="ea-detail__kv">
                      <span className="ea-detail__kv-label">{t("expansionAdvisor.rank")}</span>
                      <span className="ea-detail__kv-value">#{String(cand.rank_position ?? "—")}</span>
                    </div>
                    <div className="ea-detail__kv">
                      <span className="ea-detail__kv-label">{t("expansionAdvisor.confidence")}</span>
                      <ConfidenceBadge grade={cand.confidence_grade as string | undefined} />
                    </div>
                    <div className="ea-detail__kv">
                      <span className="ea-detail__kv-label">{t("expansionAdvisor.economicsLabel")}</span>
                      <span className="ea-detail__kv-value">{fmtScore(cand.economics_score as number | undefined)}</span>
                    </div>
                    <div className="ea-detail__kv">
                      <span className="ea-detail__kv-label">{t("expansionAdvisor.brandFitLabel")}</span>
                      <span className="ea-detail__kv-value">{fmtScore(cand.brand_fit_score as number | undefined)}</span>
                    </div>
                    <div className="ea-detail__kv">
                      <span className="ea-detail__kv-label">{t("expansionAdvisor.payback")}</span>
                      <PaybackBadge band={cand.payback_band as string | undefined} months={cand.estimated_payback_months as number | undefined} />
                    </div>
                  </div>
                </div>

                {/* Theses - Cost & Revenue View */}
                {(cand.demand_thesis || cand.cost_thesis) && (
                  <div className="ea-report-section">
                    <h5 className="ea-detail__section-title">{t("expansionAdvisor.memoCostView")}</h5>
                    {cand.demand_thesis && (
                      <div className="ea-memo-callout ea-memo-callout--neutral">
                        <span className="ea-memo-callout__label">{t("expansionAdvisor.demandThesis")}</span>
                        <p className="ea-detail__text">{String(cand.demand_thesis)}</p>
                      </div>
                    )}
                    {cand.cost_thesis && (
                      <div className="ea-memo-callout ea-memo-callout--neutral">
                        <span className="ea-memo-callout__label">{t("expansionAdvisor.costThesis")}</span>
                        <p className="ea-detail__text">{String(cand.cost_thesis)}</p>
                      </div>
                    )}
                  </div>
                )}

                {/* Site requirements */}
                <div className="ea-report-section">
                  <h5 className="ea-detail__section-title">{t("expansionAdvisor.siteRequirements")}</h5>
                  {rec.gate_verdict && <p className="ea-detail__text" style={{ fontStyle: "italic" }}>{rec.gate_verdict}</p>}
                  <GateSummary gates={gates} unknownGates={toList(gateReasons?.unknown)} />
                  {gateReasons && (
                    <div style={{ fontSize: "var(--oak-fs-xs)", marginTop: 6, display: "grid", gap: 4 }}>
                      {toList(gateReasons.passed).length > 0 && <div><span className="ea-badge ea-badge--green">{t("expansionAdvisor.gatesPassed")}</span> {toList(gateReasons.passed).map(businessGateLabel).join(", ")}</div>}
                      {toList(gateReasons.failed).length > 0 && <div><span className="ea-badge ea-badge--red">{t("expansionAdvisor.gatesFailed")}</span> {toList(gateReasons.failed).map(businessGateLabel).join(", ")}</div>}
                      {toList(gateReasons.unknown).length > 0 && <div><span className="ea-badge ea-badge--amber">{t("expansionAdvisor.gatesNeedVerification")}</span> {toList(gateReasons.unknown).map(businessGateLabel).join(", ")}</div>}
                    </div>
                  )}
                </div>

                {/* Strengths / risks */}
                <div className="ea-report-section">
                  <h5 className="ea-detail__section-title">{t("expansionAdvisor.risksStrengths")}</h5>
                  <div className="ea-memo-two-col">
                    <div>
                      <span className="ea-memo-callout__label" style={{ color: "var(--oak-success, #16a34a)" }}>{t("expansionAdvisor.topPositives")}</span>
                      {positives.length > 0 ? <ul className="ea-memo-list">{positives.map((s, i) => <li key={i}>{s}</li>)}</ul> : <p className="ea-detail__text">—</p>}
                    </div>
                    <div>
                      <span className="ea-memo-callout__label" style={{ color: "var(--oak-error, #d4183d)" }}>{t("expansionAdvisor.topRisks")}</span>
                      {risks.length > 0 ? <ul className="ea-memo-list">{risks.map((s, i) => <li key={i}>{s}</li>)}</ul> : <p className="ea-detail__text">—</p>}
                    </div>
                  </div>
                </div>

                {/* Market intelligence */}
                {(mr.delivery_market_summary || mr.competitive_context || mr.district_fit_summary) && (
                  <div className="ea-report-section">
                    <h5 className="ea-detail__section-title">{t("expansionAdvisor.memoMarketIntel")}</h5>
                    {mr.delivery_market_summary && <div className="ea-memo-callout ea-memo-callout--neutral"><span className="ea-memo-callout__label">{t("expansionAdvisor.deliveryMarket")}</span><p className="ea-detail__text">{mr.delivery_market_summary}</p></div>}
                    {mr.competitive_context && <div className="ea-memo-callout ea-memo-callout--neutral"><span className="ea-memo-callout__label">{t("expansionAdvisor.competitiveContext")}</span><p className="ea-detail__text">{mr.competitive_context}</p></div>}
                    {mr.district_fit_summary && <div className="ea-memo-callout ea-memo-callout--neutral"><span className="ea-memo-callout__label">{t("expansionAdvisor.districtFit")}</span><p className="ea-detail__text">{mr.district_fit_summary}</p></div>}
                  </div>
                )}

                {/* Competitors */}
                {comps.length > 0 && (
                  <div className="ea-report-section">
                    <h5 className="ea-detail__section-title">{t("expansionAdvisor.comparableCompetitors")}</h5>
                    <table className="ea-comp-table">
                      <thead><tr><th>{t("expansionAdvisor.branchName")}</th><th>{t("expansionAdvisor.district")}</th><th>{t("expansionAdvisor.nearestBranch")}</th></tr></thead>
                      <tbody>
                        {comps.map((c, i) => (
                          <tr key={String(c.id || i)}>
                            <td>{String(c.name || "—")}</td>
                            <td>{String(c.district_display || c.district || "—")}</td>
                            <td>{fmtMeters(c.distance_m as number | undefined)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}

                {/* Score breakdown — structured view */}
                {breakdown && breakdown.weighted_components && Object.keys(breakdown.weighted_components).length > 0 && (
                  <div className="ea-report-section">
                    <h5 className="ea-detail__section-title">{t("expansionAdvisor.memoScoreBreakdown")}</h5>
                    <div className="ea-detail__grid">
                      {Object.entries(breakdown.weighted_components).map(([key, val]) => (
                        <div key={key} className="ea-detail__kv">
                          <span className="ea-detail__kv-label">{key.replace(/_/g, " ").replace(/\bscore\b/gi, "").trim()}</span>
                          <span className="ea-detail__kv-value">{typeof val === "number" ? fmtScore(val) : String(val ?? "—")}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Technical details — collapsed by default */}
                {snapshot && (
                  <details className="ea-debug">
                    <summary>{t("expansionAdvisor.technicalDetails")}</summary>
                    <div className="ea-report-section">
                      <h5 className="ea-detail__section-title">{t("expansionAdvisor.memoFeatureSnapshot")}</h5>
                      <div className="ea-detail__grid">
                        <div className="ea-detail__kv">
                          <span className="ea-detail__kv-label">{t("expansionAdvisor.dataCompleteness")}</span>
                          <span className="ea-detail__kv-value">{snapshot.data_completeness_score != null ? `${Math.round(Number(snapshot.data_completeness_score))}%` : "—"}</span>
                        </div>
                      </div>
                      {Object.keys(snapshot.context_sources || {}).length > 0 && (
                        <div style={{ fontSize: "var(--oak-fs-xs)", marginTop: 4, color: "var(--oak-text-light)" }}>
                          {t("expansionAdvisor.contextSources")}: {Object.keys(snapshot.context_sources).join(", ")}
                        </div>
                      )}
                      {(snapshot.missing_context || []).length > 0 && (
                        <div style={{ fontSize: "var(--oak-fs-xs)", marginTop: 4, color: "var(--oak-error, #d4183d)" }}>
                          {t("expansionAdvisor.missingData")}: {snapshot.missing_context.join(", ")}
                        </div>
                      )}
                    </div>
                  </details>
                )}

                {/* Copy-ready summary block */}
                {isLeadCandidate && (
                  <CopySummaryBlock
                    candidate={null}
                    report={report || null}
                    memo={memo}
                  />
                )}
              </>
            );
          })()}
        </div>
      </div>
    </div>
  );
}
