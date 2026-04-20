import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { CandidateMemoResponse, RecommendationReportResponse } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";
import ConfidenceBadge from "./ConfidenceBadge";
import GateSummary from "./GateSummary";
import CopySummaryBlock from "./CopySummaryBlock";
import DecisionLogicCard from "./DecisionLogicCard";
import DecisionMemoNarrative from "./DecisionMemoNarrative";
import { fmtScore, fmtMeters, fmtSAR, fmtSARCompact, fmtM2, businessGateLabel, safeDistrictLabel, getDisplayScore } from "./formatHelpers";

function toList(input: unknown): string[] {
  return Array.isArray(input) ? input.map(String) : [];
}

/** Humanize score component keys for the breakdown */
const SCORE_LABEL_MAP: Record<string, string> = {
  competition_whitespace: "Competitor Openness",
  demand_potential: "Demand Strength",
  occupancy_economics: "Economics",
  delivery_demand: "Delivery Market",
  access_visibility: "Access & Visibility",
  brand_fit: "Brand Fit",
  confidence: "Data Quality",
  provider_density: "Provider Density",
  provider_whitespace: "Market Gap",
  multi_platform_presence: "Multi-platform",
  delivery_competition: "Delivery Competition",
  zoning_fit: "Zoning Fit",
  frontage: "Frontage",
  parking: "Parking",
  economics: "Economics",
  cannibalization: "Cannibalization",
};

function humanizeScoreLabel(key: string): string {
  const cleaned = key.replace(/_score$/, "").replace(/_/g, " ");
  return SCORE_LABEL_MAP[key.replace(/_score$/, "")] || cleaned.replace(/^\w/, (c) => c.toUpperCase());
}

type MemoTab = "economics" | "market" | "site" | "risks";

export default function ExpansionMemoPanel({
  memo,
  loading,
  isLeadCandidate,
  report,
  candidateRaw,
  briefRaw,
  lang,
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
  candidateRaw?: Record<string, unknown> | null;
  briefRaw?: Record<string, unknown> | null;
  lang?: string;
  onClose?: () => void;
  onBackToDetail?: () => void;
  onBackToCompare?: () => void;
  onOpenCompare?: () => void;
  hasShortlist?: boolean;
  hasCompare?: boolean;
}) {
  const { t } = useTranslation();
  const [presentationMode, setPresentationMode] = useState(false);
  const [activeTab, setActiveTab] = useState<MemoTab>("economics");

  if (!memo && !loading) return null;

  const effectiveLang = lang || "en";

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
            const positives = toList(cand.top_positives_json).slice(0, 3);
            const risks = toList(cand.top_risks_json).slice(0, 3);

            // Verdict color
            const verdictColor = rec.verdict?.toLowerCase() === "go" ? "green" : rec.verdict?.toLowerCase() === "caution" ? "amber" : "red";

            return (
              <>
                {/* ══ Section 1: LLM Decision Narrative (top, always visible) ══ */}
                {candidateRaw && briefRaw && (
                  <DecisionMemoNarrative
                    candidate={candidateRaw}
                    brief={briefRaw}
                    lang={effectiveLang}
                  />
                )}

                {/* ══ Section 1b: Verdict + confidence (always visible, compact) ══ */}
                {(rec.verdict || cand.confidence_grade) && (
                  <div className="ea-memo-verdict-row">
                    {rec.verdict && (
                      <span className={`ea-memo-verdict-badge ea-badge ea-badge--${verdictColor}`}>
                        {rec.verdict}
                      </span>
                    )}
                    <ConfidenceBadge grade={cand.confidence_grade as string | undefined} />
                  </div>
                )}

                {/* ══ Section 2: 4 Key Numbers (always visible) ══ */}
                <div className="ea-memo-key-numbers">
                  <div className="ea-memo-key-numbers__item">
                    <span className="ea-memo-key-numbers__value">
                      <ScorePill value={(breakdown?.display_score as number | undefined) ?? (cand.final_score as number | undefined)} large />
                    </span>
                    <span className="ea-memo-key-numbers__label">{t("decisionMemo.finalScore")}</span>
                  </div>
                  <div className="ea-memo-key-numbers__item">
                    <span className="ea-memo-key-numbers__value ea-memo-key-numbers__value--big">
                      {fmtM2((cand.area_m2 as number | undefined) ?? (cand.unit_area_sqm as number | undefined))}
                    </span>
                    <span className="ea-memo-key-numbers__label">{t("decisionMemo.area")}</span>
                  </div>
                  <div className="ea-memo-key-numbers__item">
                    <span className="ea-memo-key-numbers__value ea-memo-key-numbers__value--big">
                      {fmtSAR(cand.estimated_annual_rent_sar as number | undefined)}
                    </span>
                    <span className="ea-memo-key-numbers__label">{t("decisionMemo.annualRent")}</span>
                  </div>
                  <div className="ea-memo-key-numbers__item">
                    <span className="ea-memo-key-numbers__value ea-memo-key-numbers__value--big">
                      {cand.unit_street_width_m != null ? `${cand.unit_street_width_m} m` : "—"}
                    </span>
                    <span className="ea-memo-key-numbers__label">{t("decisionMemo.streetWidth")}</span>
                  </div>
                </div>

                {/* ══ Section 2b: Decision Logic (chunk 3c) — gates, score
                         contributions, ranking decision. Above the collapsed
                         score-breakdown disclosure so the card and the details
                         don't visually compete. ══ */}
                <DecisionLogicCard
                  gateReasons={gateReasons}
                  scoreBreakdown={breakdown}
                  deterministicRank={cand.deterministic_rank ?? null}
                  finalRank={cand.final_rank ?? null}
                  rerankStatus={cand.rerank_status ?? null}
                  rerankReason={cand.rerank_reason ?? null}
                  rerankDelta={typeof cand.rerank_delta === "number" ? cand.rerank_delta : 0}
                />

                {/* ══ Section 3: Full Score Breakdown (collapsed by default) ══ */}
                <details className="ea-memo-full-breakdown">
                  <summary className="ea-memo-full-breakdown__toggle">
                    {t("expansionAdvisor.showScoreBreakdown")}
                  </summary>

                  <div className="ea-memo-full-breakdown__content">
                    {/* ── Summary card ── */}
                    <div className="ea-memo-summary-card">
                      <div className="ea-memo-summary-card__top">
                        <div className="ea-memo-summary-card__score-donut">
                          <ScorePill value={(breakdown?.display_score as number | undefined) ?? (cand.final_score as number | undefined)} large />
                        </div>
                      </div>
                      {rec.headline && <p className="ea-memo-summary-card__headline">{rec.headline}</p>}
                    </div>

                    {/* ── Tabbed sections ── */}
                    <div className="ea-memo-tabs">
                      <div className="ea-memo-tabs__nav">
                        {(["economics", "market", "site", "risks"] as MemoTab[]).map((tab) => (
                          <button
                            key={tab}
                            type="button"
                            className={`ea-memo-tabs__tab${activeTab === tab ? " ea-memo-tabs__tab--active" : ""}`}
                            onClick={() => setActiveTab(tab)}
                          >
                            {t(`expansionAdvisor.memoTab_${tab}`)}
                          </button>
                        ))}
                      </div>

                      <div className="ea-memo-tabs__content">
                        {/* Economics tab */}
                        {activeTab === "economics" && (
                          <div className="ea-memo-tab-panel">
                            <div className="ea-detail__grid">
                              <div className="ea-detail__kv">
                                <span className="ea-detail__kv-label">{t("expansionAdvisor.annualRent")}</span>
                                <span className="ea-detail__kv-value">{fmtSAR(cand.estimated_annual_rent_sar as number | undefined)}</span>
                              </div>
                              <div className="ea-detail__kv">
                                <span className="ea-detail__kv-label">{t("expansionAdvisor.fitoutCost")}</span>
                                <span className="ea-detail__kv-value">{fmtSAR(cand.estimated_fitout_cost_sar as number | undefined)}</span>
                              </div>
                              <div className="ea-detail__kv">
                                <span className="ea-detail__kv-label">{t("expansionAdvisor.revenueIndex")}</span>
                                <span className="ea-detail__kv-value">
                                  {fmtScore(cand.estimated_revenue_index as number | undefined, 1)}
                                  <span className="ea-memo-disclaimer-icon" title={t("expansionAdvisor.revenueDisclaimer")}>&#9432;</span>
                                </span>
                              </div>
                              <div className="ea-detail__kv">
                                <span className="ea-detail__kv-label">{t("expansionAdvisor.economicsLabel")}</span>
                                <span className="ea-detail__kv-value">{fmtScore(cand.economics_score as number | undefined)}</span>
                              </div>
                              <div className="ea-detail__kv">
                                <span className="ea-detail__kv-label">{t("expansionAdvisor.brandFitLabel")}</span>
                                <span className="ea-detail__kv-value">{fmtScore(cand.brand_fit_score as number | undefined)}</span>
                              </div>
                            </div>
                            {cand.cost_thesis && (
                              <div className="ea-memo-callout ea-memo-callout--neutral" style={{ marginTop: 12 }}>
                                <span className="ea-memo-callout__label">{t("expansionAdvisor.costThesis")}</span>
                                <p className="ea-detail__text">{String(cand.cost_thesis)}</p>
                              </div>
                            )}
                          </div>
                        )}

                        {/* Market tab */}
                        {activeTab === "market" && (
                          <div className="ea-memo-tab-panel">
                            {cand.demand_thesis && (
                              <div className="ea-memo-callout ea-memo-callout--neutral">
                                <span className="ea-memo-callout__label">{t("expansionAdvisor.demandThesis")}</span>
                                <p className="ea-detail__text">{String(cand.demand_thesis)}</p>
                              </div>
                            )}
                            {mr.delivery_market_summary && (
                              <div className="ea-memo-callout ea-memo-callout--neutral">
                                <span className="ea-memo-callout__label">{t("expansionAdvisor.deliveryMarket")}</span>
                                <p className="ea-detail__text">{mr.delivery_market_summary}</p>
                              </div>
                            )}
                            {mr.competitive_context && (
                              <div className="ea-memo-callout ea-memo-callout--neutral">
                                <span className="ea-memo-callout__label">{t("expansionAdvisor.competitiveContext")}</span>
                                <p className="ea-detail__text">{mr.competitive_context}</p>
                              </div>
                            )}
                            {mr.district_fit_summary && (
                              <div className="ea-memo-callout ea-memo-callout--neutral">
                                <span className="ea-memo-callout__label">{t("expansionAdvisor.districtFit")}</span>
                                <p className="ea-detail__text">{mr.district_fit_summary}</p>
                              </div>
                            )}
                            {comps.length > 0 && (
                              <>
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
                              </>
                            )}
                          </div>
                        )}

                        {/* Site tab */}
                        {activeTab === "site" && (
                          <div className="ea-memo-tab-panel">
                            <div className="ea-detail__grid">
                              <div className="ea-detail__kv">
                                <span className="ea-detail__kv-label">{t("expansionAdvisor.areaLabel")}</span>
                                <span className="ea-detail__kv-value">{fmtM2(cand.area_m2 as number | undefined)}</span>
                              </div>
                              <div className="ea-detail__kv">
                                <span className="ea-detail__kv-label">{t("expansionAdvisor.rank")}</span>
                                <span className="ea-detail__kv-value">#{String(cand.rank_position ?? "—")}</span>
                              </div>
                              <div className="ea-detail__kv">
                                <span className="ea-detail__kv-label">{t("expansionAdvisor.confidence")}</span>
                                <ConfidenceBadge grade={cand.confidence_grade as string | undefined} />
                              </div>
                            </div>
                            {rec.gate_verdict && <p className="ea-detail__text" style={{ fontStyle: "italic", marginTop: 8 }}>{rec.gate_verdict}</p>}
                            <GateSummary gates={gates} unknownGates={toList(gateReasons?.unknown)} />
                            {gateReasons && (
                              <div style={{ fontSize: "var(--oak-fs-xs)", marginTop: 6, display: "grid", gap: 4 }}>
                                {toList(gateReasons.passed).length > 0 && <div><span className="ea-badge ea-badge--green">{t("expansionAdvisor.gatesPassed")}</span> {toList(gateReasons.passed).map(businessGateLabel).join(", ")}</div>}
                                {toList(gateReasons.failed).length > 0 && <div><span className="ea-badge ea-badge--red">{t("expansionAdvisor.gatesFailed")}</span> {toList(gateReasons.failed).map(businessGateLabel).join(", ")}</div>}
                                {toList(gateReasons.unknown).length > 0 && <div><span className="ea-badge ea-badge--amber">{t("expansionAdvisor.gatesNeedVerification")}</span> {toList(gateReasons.unknown).map(businessGateLabel).join(", ")}</div>}
                              </div>
                            )}
                          </div>
                        )}

                        {/* Risks & Validation tab */}
                        {activeTab === "risks" && (
                          <div className="ea-memo-tab-panel">
                            {/* Best use case + watchout */}
                            {(rec.best_use_case || rec.main_watchout) && (
                              <div style={{ display: "grid", gap: 8 }}>
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

                            {/* Strengths / risks — two-column */}
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

                            {/* Validation plan — 2-column layout */}
                            {(toList(gateReasons?.unknown).length > 0 || toList(gateReasons?.passed).length > 0) && (
                              <div className="ea-memo-validation-grid">
                                <div className="ea-memo-validation-col">
                                  <h6 className="ea-memo-validation-col__title ea-memo-validation-col__title--must">{t("expansionAdvisor.vpMustVerify")}</h6>
                                  {toList(gateReasons?.unknown).map((item, i) => (
                                    <div key={i} className="ea-memo-validation-item ea-memo-validation-item--must">
                                      <span className="ea-memo-validation-dot ea-memo-validation-dot--must" />
                                      <span>{businessGateLabel(item)}</span>
                                    </div>
                                  ))}
                                  {toList(gateReasons?.failed).map((item, i) => (
                                    <div key={`f-${i}`} className="ea-memo-validation-item ea-memo-validation-item--must">
                                      <span className="ea-memo-validation-dot ea-memo-validation-dot--fail" />
                                      <span>{businessGateLabel(item)}</span>
                                    </div>
                                  ))}
                                </div>
                                <div className="ea-memo-validation-col">
                                  <h6 className="ea-memo-validation-col__title ea-memo-validation-col__title--confirmed">{t("expansionAdvisor.vpAlreadyStrong")}</h6>
                                  {toList(gateReasons?.passed).map((item, i) => (
                                    <div key={i} className="ea-memo-validation-item ea-memo-validation-item--confirmed">
                                      <span className="ea-memo-validation-check">&#10003;</span>
                                      <span>{businessGateLabel(item)}</span>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    </div>

                    {/* Score breakdown — collapsed, with humanized labels */}
                    {breakdown && breakdown.weighted_components && Object.keys(breakdown.weighted_components).length > 0 && (
                      <details className="ea-report-section">
                        <summary className="ea-detail__section-title" style={{ cursor: "pointer" }}>{t("expansionAdvisor.memoScoreBreakdown")}</summary>
                        <div className="ea-detail__grid">
                          {Object.entries(breakdown.weighted_components).map(([key, val]) => (
                            <div key={key} className="ea-detail__kv">
                              <span className="ea-detail__kv-label">{humanizeScoreLabel(key)}</span>
                              <span className="ea-detail__kv-value">{typeof val === "number" ? fmtScore(val) : String(val ?? "—")}</span>
                            </div>
                          ))}
                        </div>
                      </details>
                    )}

                    {/* Technical details — collapsed */}
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
                  </div>
                </details>

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
