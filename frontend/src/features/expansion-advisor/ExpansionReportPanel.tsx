import { useTranslation } from "react-i18next";
import type { RecommendationReportResponse } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";
import ConfidenceBadge from "./ConfidenceBadge";

export function triggerReportCandidateSelect(candidateId: string | undefined, onSelectCandidateId?: (candidateId: string) => void) {
  if (!candidateId) return;
  onSelectCandidateId?.(candidateId);
}

export default function ExpansionReportPanel({
  report,
  loading,
  onSelectCandidateId,
  onClose,
}: {
  report: RecommendationReportResponse | null;
  loading: boolean;
  onSelectCandidateId?: (candidateId: string) => void;
  onClose?: () => void;
}) {
  const { t } = useTranslation();

  if (!report && !loading) return null;

  return (
    <div className="ea-drawer-backdrop" onClick={() => onClose?.()}>
      <div className="ea-drawer ea-drawer--wide" onClick={(e) => e.stopPropagation()}>
        <div className="ea-drawer__header">
          <h3 className="ea-drawer__title">{t("expansionAdvisor.reportExecutiveSummary")}</h3>
          {report?.meta?.version && (
            <span style={{ fontSize: "var(--oak-fs-xs)", color: "var(--oak-text-light)", marginInlineEnd: "auto", marginInlineStart: 12 }}>
              v{report.meta.version}
            </span>
          )}
          <button className="ea-drawer__close" onClick={() => onClose?.()}>{t("expansionAdvisor.close")}</button>
        </div>
        <div className="ea-drawer__body">
          {loading && <div className="ea-state ea-state--loading">{t("expansionAdvisor.loadingReport")}</div>}

          {report && (() => {
            const rec = report.recommendation || {};
            const top = report.top_candidates || [];
            const assumptions = Object.entries(report.assumptions || {});

            return (
              <>
                {/* Summary narrative */}
                {(rec.summary || rec.report_summary) && (
                  <div className="ea-report-hero">
                    <p className="ea-report-hero__text">
                      {String(rec.summary || rec.report_summary)}
                    </p>
                  </div>
                )}

                {/* Best + runner up cards */}
                <div className="ea-report-section">
                  <div className="ea-report-picks">
                    <div className="ea-report-pick ea-report-pick--best" onClick={() => rec.best_candidate_id && onSelectCandidateId?.(rec.best_candidate_id)} style={{ cursor: rec.best_candidate_id ? "pointer" : "default" }}>
                      <span className="ea-report-pick__label">{t("expansionAdvisor.bestCandidate")}</span>
                      <span className="ea-report-pick__id">{rec.best_candidate_id?.slice(0, 8) || "—"}</span>
                    </div>
                    <div className="ea-report-pick" onClick={() => rec.runner_up_candidate_id && onSelectCandidateId?.(rec.runner_up_candidate_id)} style={{ cursor: rec.runner_up_candidate_id ? "pointer" : "default" }}>
                      <span className="ea-report-pick__label">{t("expansionAdvisor.runnerUp")}</span>
                      <span className="ea-report-pick__id">{rec.runner_up_candidate_id?.slice(0, 8) || "—"}</span>
                    </div>
                  </div>
                </div>

                {/* Reasoning callouts */}
                <div className="ea-report-section">
                  {rec.why_best && (
                    <div className="ea-memo-callout ea-memo-callout--positive">
                      <span className="ea-memo-callout__label">{t("expansionAdvisor.reportWhyThisSite")}</span>
                      <p className="ea-detail__text">{rec.why_best}</p>
                    </div>
                  )}
                  {rec.main_risk && (
                    <div className="ea-memo-callout ea-memo-callout--risk">
                      <span className="ea-memo-callout__label">{t("expansionAdvisor.reportKeyRisk")}</span>
                      <p className="ea-detail__text">{rec.main_risk}</p>
                    </div>
                  )}
                  {rec.best_format && (
                    <div className="ea-memo-callout ea-memo-callout--neutral">
                      <span className="ea-memo-callout__label">{t("expansionAdvisor.reportBestFormat")}</span>
                      <p className="ea-detail__text">{rec.best_format}</p>
                    </div>
                  )}
                </div>

                {/* Top candidates */}
                {top.length > 0 && (
                  <div className="ea-report-section">
                    <h5 className="ea-detail__section-title">{t("expansionAdvisor.reportTopPicks")}</h5>
                    <div style={{ display: "grid", gap: 8 }}>
                      {top.map((item, index) => (
                        <div
                          key={item.id || index}
                          className={`ea-candidate ${index === 0 ? "ea-candidate--shortlisted" : ""}`}
                          style={{ cursor: item.id ? "pointer" : "default" }}
                          onClick={() => item.id && triggerReportCandidateSelect(item.id, onSelectCandidateId)}
                        >
                          <div className="ea-candidate__top">
                            <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                              {item.rank_position && <span className="ea-candidate__rank">#{item.rank_position}</span>}
                              <span className="ea-candidate__district">{item.id?.slice(0, 8) || "—"}</span>
                            </div>
                            <div className="ea-candidate__badges">
                              <ScorePill value={item.final_score} />
                              <ConfidenceBadge grade={item.confidence_grade} />
                              {item.gate_verdict && (
                                <span className={`ea-badge ea-badge--${item.gate_verdict === "pass" ? "green" : "red"}`}>
                                  {item.gate_verdict}
                                </span>
                              )}
                            </div>
                          </div>
                          <div className="ea-candidate__insights">
                            {(item.top_positives_json || []).slice(0, 2).map((text, i) => (
                              <div key={`p-${i}`} className="ea-candidate__insight">
                                <span className="ea-candidate__insight-icon ea-candidate__insight-icon--positive">+</span>
                                <span>{text}</span>
                              </div>
                            ))}
                            {(item.top_risks_json || []).slice(0, 2).map((text, i) => (
                              <div key={`r-${i}`} className="ea-candidate__insight">
                                <span className="ea-candidate__insight-icon ea-candidate__insight-icon--risk">!</span>
                                <span>{text}</span>
                              </div>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Assumptions */}
                {assumptions.length > 0 && (
                  <div className="ea-report-section">
                    <h5 className="ea-detail__section-title">{t("expansionAdvisor.assumptions")}</h5>
                    <div className="ea-detail__grid">
                      {assumptions.map(([key, value]) => (
                        <div key={key} className="ea-detail__kv">
                          <span className="ea-detail__kv-label">{key.replace(/_/g, " ")}</span>
                          <span className="ea-detail__kv-value">{String(value)}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </>
            );
          })()}
        </div>
      </div>
    </div>
  );
}
