import { useTranslation } from "react-i18next";
import type { CandidateMemoResponse } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";
import ConfidenceBadge from "./ConfidenceBadge";
import PaybackBadge from "./PaybackBadge";
import GateSummary from "./GateSummary";
import { fmtScore, fmtMeters } from "./formatHelpers";

function toList(input: unknown): string[] {
  return Array.isArray(input) ? input.map(String) : [];
}

export default function ExpansionMemoPanel({
  memo,
  loading,
  onClose,
}: {
  memo: CandidateMemoResponse | null;
  loading: boolean;
  onClose?: () => void;
}) {
  const { t } = useTranslation();

  if (!memo && !loading) return null;

  return (
    <div className="ea-drawer-backdrop" onClick={() => onClose?.()}>
      <div className="ea-drawer" onClick={(e) => e.stopPropagation()}>
        <div className="ea-drawer__header">
          <h3 className="ea-drawer__title">{t("expansionAdvisor.decisionMemo")}</h3>
          <button className="ea-drawer__close" onClick={() => onClose?.()}>{t("expansionAdvisor.close")}</button>
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
                {/* Recommendation */}
                <div className="ea-report-section">
                  <h5 className="ea-detail__section-title">{t("expansionAdvisor.recommendation")}</h5>
                  {rec.headline && <p className="ea-detail__text" style={{ fontWeight: 600, fontSize: "var(--oak-fs-base)" }}>{rec.headline}</p>}
                  {rec.verdict && (
                    <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                      <span className="ea-detail__kv-label">{t("expansionAdvisor.verdict")}:</span>
                      <span className={`ea-badge ea-badge--${rec.verdict?.toLowerCase() === "go" ? "green" : rec.verdict?.toLowerCase() === "caution" ? "amber" : "red"}`}>
                        {rec.verdict}
                      </span>
                    </div>
                  )}
                  {rec.best_use_case && <p className="ea-detail__text"><strong>{t("expansionAdvisor.bestUseCase")}:</strong> {rec.best_use_case}</p>}
                  {rec.main_watchout && <p className="ea-detail__text"><strong>{t("expansionAdvisor.mainWatchout")}:</strong> {rec.main_watchout}</p>}
                  {rec.gate_verdict && <p className="ea-detail__text"><strong>{t("expansionAdvisor.gateVerdict")}:</strong> {rec.gate_verdict}</p>}
                </div>

                {/* Candidate summary */}
                <div className="ea-report-section">
                  <h5 className="ea-detail__section-title">{t("expansionAdvisor.candidateSummary")}</h5>
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

                {/* Gate checklist */}
                <div className="ea-report-section">
                  <h5 className="ea-detail__section-title">{t("expansionAdvisor.gateChecklist")}</h5>
                  <GateSummary gates={gates} />
                  {gateReasons && (
                    <div style={{ fontSize: "var(--oak-fs-xs)", marginTop: 6, display: "grid", gap: 4 }}>
                      {toList(gateReasons.passed).length > 0 && <div><span className="ea-badge ea-badge--green">{t("expansionAdvisor.gatesPassed")}</span> {toList(gateReasons.passed).join(", ")}</div>}
                      {toList(gateReasons.failed).length > 0 && <div><span className="ea-badge ea-badge--red">{t("expansionAdvisor.gatesFailed")}</span> {toList(gateReasons.failed).join(", ")}</div>}
                      {toList(gateReasons.unknown).length > 0 && <div><span className="ea-badge ea-badge--neutral">{t("expansionAdvisor.gatesUnknown")}</span> {toList(gateReasons.unknown).join(", ")}</div>}
                    </div>
                  )}
                </div>

                {/* Positives / risks */}
                <div className="ea-report-section">
                  <h5 className="ea-detail__section-title">{t("expansionAdvisor.topPositives")}</h5>
                  {positives.length > 0 ? <ul style={{ margin: 0, paddingInlineStart: 16 }}>{positives.map((s, i) => <li key={i} className="ea-detail__text">{s}</li>)}</ul> : <p className="ea-detail__text">—</p>}
                </div>
                <div className="ea-report-section">
                  <h5 className="ea-detail__section-title">{t("expansionAdvisor.topRisks")}</h5>
                  {risks.length > 0 ? <ul style={{ margin: 0, paddingInlineStart: 16 }}>{risks.map((s, i) => <li key={i} className="ea-detail__text">{s}</li>)}</ul> : <p className="ea-detail__text">—</p>}
                </div>

                {/* Theses */}
                {(cand.demand_thesis || cand.cost_thesis) && (
                  <div className="ea-report-section">
                    {cand.demand_thesis && <><h5 className="ea-detail__section-title">{t("expansionAdvisor.demandThesis")}</h5><p className="ea-detail__text">{String(cand.demand_thesis)}</p></>}
                    {cand.cost_thesis && <><h5 className="ea-detail__section-title">{t("expansionAdvisor.costThesis")}</h5><p className="ea-detail__text">{String(cand.cost_thesis)}</p></>}
                  </div>
                )}

                {/* Market research */}
                {(mr.delivery_market_summary || mr.competitive_context || mr.district_fit_summary) && (
                  <div className="ea-report-section">
                    <h5 className="ea-detail__section-title">{t("expansionAdvisor.marketResearch")}</h5>
                    {mr.delivery_market_summary && <><span className="ea-detail__kv-label">{t("expansionAdvisor.deliveryMarket")}</span><p className="ea-detail__text">{mr.delivery_market_summary}</p></>}
                    {mr.competitive_context && <><span className="ea-detail__kv-label">{t("expansionAdvisor.competitiveContext")}</span><p className="ea-detail__text">{mr.competitive_context}</p></>}
                    {mr.district_fit_summary && <><span className="ea-detail__kv-label">{t("expansionAdvisor.districtFit")}</span><p className="ea-detail__text">{mr.district_fit_summary}</p></>}
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

                {(snapshot || breakdown) && (
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
