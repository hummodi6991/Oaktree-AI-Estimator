import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { CandidateMemoResponse, RecommendationReportResponse } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";
import ConfidenceBadge from "./ConfidenceBadge";
import PaybackBadge from "./PaybackBadge";
import GateSummary from "./GateSummary";
import CopySummaryBlock from "./CopySummaryBlock";
import { fmtScore, fmtMeters, fmtSAR } from "./formatHelpers";

function toList(input: unknown): string[] {
  return Array.isArray(input) ? input.map(String) : [];
}

export default function ExpansionMemoPanel({
  memo,
  loading,
  isLeadCandidate,
  report,
  onClose,
}: {
  memo: CandidateMemoResponse | null;
  loading: boolean;
  isLeadCandidate?: boolean;
  report?: RecommendationReportResponse | null;
  onClose?: () => void;
}) {
  const { t } = useTranslation();
  const [presentationMode, setPresentationMode] = useState(false);

  if (!memo && !loading) return null;

  return (
    <div className="ea-drawer-backdrop" onClick={() => onClose?.()}>
      <div className={`ea-drawer ea-drawer--wide${presentationMode ? " ea-drawer--presentation" : ""}`} onClick={(e) => e.stopPropagation()}>
        <div className="ea-drawer__header">
          {isLeadCandidate && <span className="ea-lead-tag">{t("expansionAdvisor.leadSite")}</span>}
          <h3 className="ea-drawer__title">{t("expansionAdvisor.decisionMemo")}</h3>
          <div style={{ display: "flex", gap: 6, alignItems: "center", marginInlineStart: "auto" }}>
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
            const gates = (cand.gate_status || {}) as Record<string, boolean>;
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
                      <ScorePill value={cand.final_score as number | undefined} large />
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

                {/* Gate audit */}
                <div className="ea-report-section">
                  <h5 className="ea-detail__section-title">{t("expansionAdvisor.memoGateAudit")}</h5>
                  {rec.gate_verdict && <p className="ea-detail__text" style={{ fontStyle: "italic" }}>{rec.gate_verdict}</p>}
                  <GateSummary gates={gates} unknownGates={toList(gateReasons?.unknown)} />
                  {gateReasons && (
                    <div style={{ fontSize: "var(--oak-fs-xs)", marginTop: 6, display: "grid", gap: 4 }}>
                      {toList(gateReasons.passed).length > 0 && <div><span className="ea-badge ea-badge--green">{t("expansionAdvisor.gatesPassed")}</span> {toList(gateReasons.passed).join(", ")}</div>}
                      {toList(gateReasons.failed).length > 0 && <div><span className="ea-badge ea-badge--red">{t("expansionAdvisor.gatesFailed")}</span> {toList(gateReasons.failed).join(", ")}</div>}
                      {toList(gateReasons.unknown).length > 0 && <div><span className="ea-badge ea-badge--neutral">{t("expansionAdvisor.gatesUnknown")}</span> {toList(gateReasons.unknown).join(", ")}</div>}
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
                            <td>{String(c.district || "—")}</td>
                            <td>{fmtMeters(c.distance_m as number | undefined)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}

                {/* Copy-ready summary block */}
                {isLeadCandidate && (
                  <CopySummaryBlock
                    candidate={null}
                    report={report || null}
                    memo={memo}
                  />
                )}

                {!presentationMode && (snapshot || breakdown) && (
                  <details className="ea-debug">
                    <summary>{t("expansionAdvisor.debugDetails")}</summary>
                    <pre>{JSON.stringify({ feature_snapshot: snapshot, score_breakdown: breakdown }, null, 2)}</pre>
                  </details>
                )}
              </>
            );
          })()}
        </div>
      </div>
    </div>
  );
}
