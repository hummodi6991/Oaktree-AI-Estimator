import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import type { CandidateMemoResponse, RecommendationReportResponse } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";
import ConfidenceBadge from "./ConfidenceBadge";
import GateSummary from "./GateSummary";
import CopySummaryBlock from "./CopySummaryBlock";
import DecisionLogicCard from "./DecisionLogicCard";
import DecisionMemoNarrative from "./DecisionMemoNarrative";
import ScoreBar from "./ScoreBar";
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

type MemoTab = "economics" | "market" | "site" | "risks" | "breakdown";

/**
 * Narrow enum of scrollable anchors inside the memo drawer. DOM order.
 * Keep in sync with the refs wired below. "score-breakdown" is deliberately
 * NOT included — it lives inside a closed <details> by default.
 */
export type MemoDrawerSection =
  | "narrative"
  | "verdict"
  | "quick-facts"
  | "decision-logic";

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
  initialSection,
  initialTab,
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
  initialSection?: MemoDrawerSection;
  /** Test affordance: pre-select a tab so SSR snapshots can assert tab content.
   *  In production no caller passes this; the default ("economics") is unchanged. */
  initialTab?: MemoTab;
}) {
  const { t } = useTranslation();
  const [presentationMode, setPresentationMode] = useState(false);
  const [activeTab, setActiveTab] = useState<MemoTab>(initialTab ?? "economics");

  // Scroll anchors for initialSection-driven scrollIntoView. Only attached
  // when initialSection is set — see conditional className below so the
  // default open path doesn't render the scroll-margin wrapper class.
  const narrativeRef = useRef<HTMLDivElement | null>(null);
  const verdictRowRef = useRef<HTMLDivElement | null>(null);
  const quickFactsRef = useRef<HTMLDivElement | null>(null);
  const decisionLogicRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!initialSection) return;
    if (loading) return;
    const refMap: Record<MemoDrawerSection, React.RefObject<HTMLDivElement | null>> = {
      "narrative": narrativeRef,
      "verdict": verdictRowRef,
      "quick-facts": quickFactsRef,
      "decision-logic": decisionLogicRef,
    };
    const el = refMap[initialSection]?.current;
    if (el) el.scrollIntoView({ behavior: "smooth", block: "start" });
  }, [initialSection, loading]);

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

            // Scroll-anchor class is only rendered when initialSection is set,
            // so the default-open drawer path emits identical markup to pre-3d.
            const anchorCls = initialSection ? " ea-memo-scroll-anchor" : "";

            return (
              <>
                {/* ══ Section 1: LLM Decision Narrative (top, always visible) ══ */}
                {candidateRaw && briefRaw && (
                  <div
                    ref={initialSection ? narrativeRef : undefined}
                    className={`ea-memo-section-narrative${anchorCls}`}
                  >
                    <DecisionMemoNarrative
                      candidate={candidateRaw}
                      brief={briefRaw}
                      lang={effectiveLang}
                    />
                  </div>
                )}

                {/* ══ Section 1b: Verdict + confidence (always visible, compact) ══ */}
                {(rec.verdict || cand.confidence_grade) && (
                  <div
                    ref={initialSection ? verdictRowRef : undefined}
                    className={`ea-memo-verdict-row${anchorCls}`}
                  >
                    {rec.verdict && (
                      <span className={`ea-memo-verdict-badge ea-badge ea-badge--${verdictColor}`}>
                        {rec.verdict}
                      </span>
                    )}
                    <ConfidenceBadge grade={cand.confidence_grade as string | undefined} />
                  </div>
                )}

                {/* ══ Section 2: 4 Key Numbers (always visible) ══ */}
                <div
                  ref={initialSection ? quickFactsRef : undefined}
                  className={`ea-memo-key-numbers${anchorCls}`}
                >
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
                <div
                  ref={initialSection ? decisionLogicRef : undefined}
                  className={`ea-memo-section-decision-logic${anchorCls}`}
                >
                  <DecisionLogicCard
                    gateReasons={gateReasons}
                    scoreBreakdown={breakdown}
                    deterministicRank={cand.deterministic_rank ?? null}
                    finalRank={cand.final_rank ?? null}
                    rerankStatus={cand.rerank_status ?? null}
                    rerankReason={cand.rerank_reason ?? null}
                    rerankDelta={typeof cand.rerank_delta === "number" ? cand.rerank_delta : 0}
                  />
                </div>

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
                        {(["economics", "market", "site", "risks", "breakdown"] as MemoTab[]).map((tab) => (
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
                                    {comps.map((c, i) => {
                                      const isArabic = effectiveLang === "ar";
                                      const displayName = (
                                        (isArabic ? (c.display_name_ar as string | null | undefined) : (c.display_name_en as string | null | undefined))
                                        ?? c.name
                                        ?? "—"
                                      );
                                      return (
                                        <tr key={String(c.id || i)}>
                                          <td>{String(displayName)}</td>
                                          <td>{String(c.district_display || c.district || "—")}</td>
                                          <td>{fmtMeters(c.distance_m as number | undefined)}</td>
                                        </tr>
                                      );
                                    })}
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

                        {/* Breakdown tab — Phase 1A enrichments grouped into
                            four sub-sections. Each row hides when its value
                            is null/undefined/empty (no "—" fallback), so the
                            tab adapts to data availability per candidate. */}
                        {activeTab === "breakdown" && (() => {
                          // Backend serializes SQLAlchemy Numeric columns as strings
                          // (e.g. "45.00") for precision. Coerce here so the
                          // typeof-number guard below doesn't silently hide bars.
                          const toNumber = (v: unknown): number | null => {
                            if (typeof v === "number" && Number.isFinite(v)) return v;
                            if (typeof v === "string") {
                              const n = parseFloat(v);
                              return Number.isFinite(n) ? n : null;
                            }
                            return null;
                          };
                          const parkingScore = toNumber(cand.parking_score);
                          const frontageScore = toNumber(cand.frontage_score);
                          const accessScore = toNumber(cand.access_score);
                          const accessVisibilityScore = toNumber(cand.access_visibility_score);
                          const zoningFitScore = toNumber(cand.zoning_fit_score);
                          const providerDensityScore = toNumber(cand.provider_density_score);
                          const providerWhitespaceScore = toNumber(cand.provider_whitespace_score);
                          const multiPlatformPresenceScore = toNumber(cand.multi_platform_presence_score);
                          const deliveryCompetitionScore = toNumber(cand.delivery_competition_score);
                          const cannibalizationScore = toNumber(cand.cannibalization_score);
                          const ctxSources = (snapshot?.context_sources || {}) as Record<string, unknown>;
                          const roadBand = typeof ctxSources.road_evidence_band === "string" ? ctxSources.road_evidence_band : null;
                          const parkingBand = typeof ctxSources.parking_evidence_band === "string" ? ctxSources.parking_evidence_band : null;
                          const rentBase = typeof ctxSources.rent_base_sar_m2_year === "number" ? ctxSources.rent_base_sar_m2_year : null;
                          const rentMicroAdj = ctxSources.rent_micro_adjustment as Record<string, unknown> | null | undefined;
                          const microMultiplier = typeof rentMicroAdj?.multiplier === "number" ? rentMicroAdj.multiplier : null;

                          const snap = snapshot as Record<string, unknown> | undefined;
                          const districtMomentum = snap?.district_momentum as Record<string, unknown> | undefined;
                          const dmSampleFloor = districtMomentum?.sample_floor_applied === true;
                          const dmScore = typeof districtMomentum?.momentum_score === "number" ? (districtMomentum.momentum_score as number) : null;
                          const dmPercentile = typeof districtMomentum?.percentile_composite === "number" ? (districtMomentum.percentile_composite as number) : null;
                          const dmActivity = typeof districtMomentum?.activity_30d === "number" ? (districtMomentum.activity_30d as number) : null;
                          const dmDistrictLabel = typeof districtMomentum?.district_label === "string" && (districtMomentum.district_label as string).length > 0 ? (districtMomentum.district_label as string) : null;

                          const listingAge = snap?.listing_age as Record<string, unknown> | undefined;
                          const createdDays = typeof listingAge?.created_days === "number" ? (listingAge.created_days as number) : null;
                          const updatedDays = typeof listingAge?.updated_days === "number" ? (listingAge.updated_days as number) : null;

                          const realizedDemand = typeof snap?.realized_demand_30d === "number" ? (snap.realized_demand_30d as number) : null;
                          const rdBranches = typeof snap?.realized_demand_branches === "number" ? (snap.realized_demand_branches as number) : null;
                          const rdWindow = typeof snap?.realized_demand_window_days === "number" ? (snap.realized_demand_window_days as number) : null;

                          const candLoc = snap?.candidate_location as Record<string, unknown> | undefined;
                          const isVacant = typeof candLoc?.is_vacant === "boolean" ? (candLoc.is_vacant as boolean) : null;
                          const currentTenant = typeof candLoc?.current_tenant === "string" && (candLoc.current_tenant as string).length > 0 ? (candLoc.current_tenant as string) : null;
                          const currentCategory = typeof candLoc?.current_category === "string" && (candLoc.current_category as string).length > 0 ? (candLoc.current_category as string) : null;
                          const showPropertyStatus = isVacant !== null || currentTenant !== null || currentCategory !== null;

                          const ROAD_BAND_LABEL_KEY: Record<string, string> = {
                            unknown: "expansionAdvisor.roadEvidenceBand_unknown",
                            direct_frontage: "expansionAdvisor.roadEvidenceBand_directFrontage",
                            none_found: "expansionAdvisor.roadEvidenceBand_noneFound",
                            limited: "expansionAdvisor.roadEvidenceBand_limited",
                            moderate: "expansionAdvisor.roadEvidenceBand_moderate",
                            strong: "expansionAdvisor.roadEvidenceBand_strong",
                          };
                          const PARKING_BAND_LABEL_KEY: Record<string, string> = {
                            unknown: "expansionAdvisor.parkingEvidenceBand_unknown",
                            none_found: "expansionAdvisor.parkingEvidenceBand_noneFound",
                            limited: "expansionAdvisor.parkingEvidenceBand_limited",
                            moderate: "expansionAdvisor.parkingEvidenceBand_moderate",
                            strong: "expansionAdvisor.parkingEvidenceBand_strong",
                          };

                          const fmtSignedPct = (mult: number): string => {
                            const pct = (mult - 1) * 100;
                            const rounded = pct.toFixed(1);
                            if (pct > 0) return `+${rounded}%`;
                            if (pct < 0) return `−${Math.abs(pct).toFixed(1)}%`;
                            return `0.0%`;
                          };

                          const fmtPercentileLabel = (frac: number): string => {
                            const pct = Math.round(Math.max(0, Math.min(1, frac)) * 100);
                            return t("expansionAdvisor.percentileValue", { value: pct, defaultValue: `${pct}th percentile` });
                          };

                          const fmtDaysAgo = (n: number): string => t("expansionAdvisor.daysAgo", { count: n, defaultValue: `${n} days ago` });

                          return (
                            <div className="ea-memo-tab-panel ea-memo-breakdown">
                              {/* Site grade */}
                              <section className="ea-memo-breakdown__section">
                                <h5 className="ea-detail__section-title">{t("expansionAdvisor.breakdownSiteGrade")}</h5>
                                <p className="ea-memo-breakdown__explainer" style={{ fontSize: "var(--oak-fs-xs)", color: "var(--oak-text-light)", marginTop: 0, marginBottom: 8 }}>{t("expansionAdvisor.breakdownSiteGradeExplainer")}</p>
                                <div className="ea-memo-breakdown__bars" style={{ display: "grid", gap: 8 }}>
                                  {parkingScore !== null && <ScoreBar label={t("expansionAdvisor.parkingScore")} value={parkingScore} />}
                                  {frontageScore !== null && <ScoreBar label={t("expansionAdvisor.frontageScore")} value={frontageScore} />}
                                  {accessScore !== null && <ScoreBar label={t("expansionAdvisor.accessScore")} value={accessScore} />}
                                  {accessVisibilityScore !== null && <ScoreBar label={t("expansionAdvisor.accessVisibility")} value={accessVisibilityScore} />}
                                  {zoningFitScore !== null && <ScoreBar label={t("expansionAdvisor.zoningFitScore")} value={zoningFitScore} />}
                                </div>
                                {(roadBand || parkingBand) && (
                                  <div className="ea-detail__grid" style={{ marginTop: 10 }}>
                                    {roadBand && ROAD_BAND_LABEL_KEY[roadBand] && (
                                      <div className="ea-detail__kv">
                                        <span className="ea-detail__kv-label">{t("expansionAdvisor.roadEvidenceBandLabel")}</span>
                                        <span className="ea-detail__kv-value">{t(ROAD_BAND_LABEL_KEY[roadBand])}</span>
                                      </div>
                                    )}
                                    {parkingBand && PARKING_BAND_LABEL_KEY[parkingBand] && (
                                      <div className="ea-detail__kv">
                                        <span className="ea-detail__kv-label">{t("expansionAdvisor.parkingEvidenceBandLabel")}</span>
                                        <span className="ea-detail__kv-value">{t(PARKING_BAND_LABEL_KEY[parkingBand])}</span>
                                      </div>
                                    )}
                                  </div>
                                )}
                              </section>

                              {/* Market signals */}
                              <section className="ea-memo-breakdown__section" style={{ marginTop: 16 }}>
                                <h5 className="ea-detail__section-title">{t("expansionAdvisor.breakdownMarketSignals")}</h5>
                                <p className="ea-memo-breakdown__explainer" style={{ fontSize: "var(--oak-fs-xs)", color: "var(--oak-text-light)", marginTop: 0, marginBottom: 8 }}>{t("expansionAdvisor.breakdownMarketSignalsExplainer")}</p>
                                <div className="ea-memo-breakdown__bars" style={{ display: "grid", gap: 8 }}>
                                  {providerDensityScore !== null && <ScoreBar label={t("expansionAdvisor.providerDensity")} value={providerDensityScore} />}
                                  {providerWhitespaceScore !== null && <ScoreBar label={t("expansionAdvisor.providerWhitespace")} value={providerWhitespaceScore} />}
                                  {multiPlatformPresenceScore !== null && <ScoreBar label={t("expansionAdvisor.multiPlatform")} value={multiPlatformPresenceScore} />}
                                  {deliveryCompetitionScore !== null && <ScoreBar label={t("expansionAdvisor.deliveryCompetition")} value={deliveryCompetitionScore} />}
                                  {cannibalizationScore !== null && <ScoreBar label={t("expansionAdvisor.cannibalization")} value={cannibalizationScore} />}
                                  {!dmSampleFloor && dmScore !== null && <ScoreBar label={t("expansionAdvisor.districtMomentumScore")} value={dmScore} />}
                                </div>
                                {dmSampleFloor ? (
                                  <p className="ea-memo-breakdown__note" style={{ fontSize: "var(--oak-fs-xs)", color: "var(--oak-text-light)", fontStyle: "italic", marginTop: 8 }}>
                                    {t("expansionAdvisor.districtMomentumBelowFloor")}
                                  </p>
                                ) : (
                                  (dmPercentile !== null || dmActivity !== null || dmDistrictLabel) && (
                                    <div className="ea-detail__grid" style={{ marginTop: 8 }}>
                                      {dmPercentile !== null && (
                                        <div className="ea-detail__kv">
                                          <span className="ea-detail__kv-label">{t("expansionAdvisor.districtMomentumPercentile")}</span>
                                          <span className="ea-detail__kv-value">{fmtPercentileLabel(dmPercentile)}</span>
                                        </div>
                                      )}
                                      {dmActivity !== null && (
                                        <div className="ea-detail__kv">
                                          <span className="ea-detail__kv-label">{t("expansionAdvisor.districtActivity30d")}</span>
                                          <span className="ea-detail__kv-value">{dmActivity}</span>
                                        </div>
                                      )}
                                      {dmDistrictLabel && (
                                        <div className="ea-detail__kv">
                                          <span className="ea-detail__kv-label">{t("expansionAdvisor.districtLabel")}</span>
                                          <span className="ea-detail__kv-value">{dmDistrictLabel}</span>
                                        </div>
                                      )}
                                    </div>
                                  )
                                )}
                              </section>

                              {/* Brand presence — major chains operating in the candidate's 500m micro-market */}
                              {(() => {
                                const brandPresence = snap?.brand_presence as Record<string, unknown> | undefined;
                                const topChains = brandPresence?.top_chains as Array<Record<string, unknown>> | undefined;
                                if (!topChains || topChains.length === 0) return null;
                                const radiusM = typeof brandPresence?.radius_m === "number" ? (brandPresence.radius_m as number) : 500;
                                const uniqueBrands = typeof brandPresence?.unique_brands === "number" ? (brandPresence.unique_brands as number) : topChains.length;
                                const totalBranches = typeof brandPresence?.total_branches === "number" ? (brandPresence.total_branches as number) : 0;
                                const isArabic = effectiveLang === "ar";

                                return (
                                  <section className="ea-memo-breakdown__section" style={{ marginTop: 16 }}>
                                    <h5 className="ea-detail__section-title">{t("expansionAdvisor.breakdownBrandPresence")}</h5>
                                    <p className="ea-memo-breakdown__explainer" style={{ fontSize: "var(--oak-fs-xs)", color: "var(--oak-text-light)", marginTop: 0, marginBottom: 8 }}>
                                      {t("expansionAdvisor.breakdownBrandPresenceExplainer")}
                                    </p>
                                    <div style={{ marginTop: 8 }}>
                                      <p style={{ marginBottom: 4, fontWeight: 500 }}>
                                        {t("expansionAdvisor.brandPresenceWithinRadius", {
                                          count: uniqueBrands,
                                          meters: radiusM,
                                          defaultValue: `${uniqueBrands} chains within ${radiusM}m`,
                                        })}
                                      </p>
                                      <p style={{ fontSize: "var(--oak-fs-xs)", color: "var(--oak-text-light)", marginBottom: 8 }}>
                                        {t("expansionAdvisor.brandPresenceUniqueBrandsBranches", {
                                          brands: uniqueBrands,
                                          branches: totalBranches,
                                          defaultValue: `${uniqueBrands} unique brands · ${totalBranches} branches`,
                                        })}
                                      </p>
                                      <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexWrap: "wrap", gap: "6px" }}>
                                        {topChains.map((chain, idx) => {
                                          const nameEn = chain.display_name_en as string | null | undefined;
                                          const nameAr = chain.display_name_ar as string | null | undefined;
                                          const fallbackId = chain.canonical_brand_id as string | null | undefined;
                                          const displayName = (isArabic ? nameAr : nameEn) ?? nameEn ?? nameAr ?? fallbackId ?? "—";
                                          const branchCount = typeof chain.branch_count === "number" ? (chain.branch_count as number) : 0;
                                          return (
                                            <li
                                              key={String(fallbackId || idx)}
                                              style={{
                                                padding: "4px 10px",
                                                borderRadius: 12,
                                                background: "var(--oak-bg-subtle, var(--oak-bg-soft, #f1f5f9))",
                                                fontSize: "var(--oak-fs-xs)",
                                              }}
                                            >
                                              {t("expansionAdvisor.brandPresenceBrandWithCount", {
                                                name: displayName,
                                                count: branchCount,
                                                defaultValue: `${displayName} (${branchCount})`,
                                              })}
                                            </li>
                                          );
                                        })}
                                      </ul>
                                    </div>
                                  </section>
                                );
                              })()}

                              {/* Economics & timing */}
                              <section className="ea-memo-breakdown__section" style={{ marginTop: 16 }}>
                                <h5 className="ea-detail__section-title">{t("expansionAdvisor.breakdownEconomicsTiming")}</h5>
                                <p className="ea-memo-breakdown__explainer" style={{ fontSize: "var(--oak-fs-xs)", color: "var(--oak-text-light)", marginTop: 0, marginBottom: 8 }}>{t("expansionAdvisor.breakdownEconomicsTimingExplainer")}</p>
                                <div className="ea-detail__grid">
                                  {realizedDemand !== null && (
                                    <div className="ea-detail__kv">
                                      <span className="ea-detail__kv-label">{t("expansionAdvisor.realizedDemand30d")}</span>
                                      <span className="ea-detail__kv-value">
                                        {fmtScore(realizedDemand, 1)}
                                        {rdBranches !== null && rdWindow !== null && (
                                          <span style={{ display: "block", fontSize: "var(--oak-fs-xs)", color: "var(--oak-text-light)", marginTop: 2 }}>
                                            {t("expansionAdvisor.realizedDemandSubline", { branches: rdBranches, window: rdWindow, defaultValue: `${rdBranches} branches, ${rdWindow}d window` })}
                                          </span>
                                        )}
                                      </span>
                                    </div>
                                  )}
                                  {rentBase !== null && (
                                    <div className="ea-detail__kv">
                                      <span className="ea-detail__kv-label">{t("expansionAdvisor.rentBaseline")}</span>
                                      <span className="ea-detail__kv-value">{t("expansionAdvisor.sarPerM2Year", { value: Math.round(rentBase as number) })}</span>
                                    </div>
                                  )}
                                  {microMultiplier !== null && (
                                    <div className="ea-detail__kv">
                                      <span className="ea-detail__kv-label">{t("expansionAdvisor.rentMicroAdjustment")}</span>
                                      <span className="ea-detail__kv-value">{fmtSignedPct(microMultiplier)}</span>
                                    </div>
                                  )}
                                  {createdDays !== null && (
                                    <div className="ea-detail__kv">
                                      <span className="ea-detail__kv-label">{t("expansionAdvisor.listingCreated")}</span>
                                      <span className="ea-detail__kv-value">{fmtDaysAgo(createdDays)}</span>
                                    </div>
                                  )}
                                  {updatedDays !== null && (
                                    <div className="ea-detail__kv">
                                      <span className="ea-detail__kv-label">{t("expansionAdvisor.listingUpdated")}</span>
                                      <span className="ea-detail__kv-value">{fmtDaysAgo(updatedDays)}</span>
                                    </div>
                                  )}
                                </div>
                              </section>

                              {/* Property status — whole-block conditional */}
                              {showPropertyStatus && (
                                <section className="ea-memo-breakdown__section" style={{ marginTop: 16 }}>
                                  <h5 className="ea-detail__section-title">{t("expansionAdvisor.breakdownPropertyStatus")}</h5>
                                  <p className="ea-memo-breakdown__explainer" style={{ fontSize: "var(--oak-fs-xs)", color: "var(--oak-text-light)", marginTop: 0, marginBottom: 8 }}>{t("expansionAdvisor.breakdownPropertyStatusExplainer")}</p>
                                  <div className="ea-detail__grid">
                                    {isVacant !== null && (
                                      <div className="ea-detail__kv">
                                        <span className="ea-detail__kv-label">{t("expansionAdvisor.vacancy")}</span>
                                        <span className="ea-detail__kv-value">{isVacant ? t("expansionAdvisor.vacancyVacant") : t("expansionAdvisor.vacancyOccupied")}</span>
                                      </div>
                                    )}
                                    {currentTenant && (
                                      <div className="ea-detail__kv">
                                        <span className="ea-detail__kv-label">{t("expansionAdvisor.currentTenant")}</span>
                                        <span className="ea-detail__kv-value">{currentTenant}</span>
                                      </div>
                                    )}
                                    {currentCategory && (
                                      <div className="ea-detail__kv">
                                        <span className="ea-detail__kv-label">{t("expansionAdvisor.currentUse")}</span>
                                        <span className="ea-detail__kv-value">{currentCategory}</span>
                                      </div>
                                    )}
                                  </div>
                                </section>
                              )}
                            </div>
                          );
                        })()}
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
