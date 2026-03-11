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
      <div className="ea-drawer" onClick={(e) => e.stopPropagation()}>
        <div className="ea-drawer__header">
          <h3 className="ea-drawer__title">{t("expansionAdvisor.executiveReport")}</h3>
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
                {/* Version meta */}
                {report.meta?.version && (
                  <div style={{ fontSize: "var(--oak-fs-xs)", color: "var(--oak-text-light)" }}>
                    v{report.meta.version}
                  </div>
                )}

                {/* Executive recommendation */}
                <div className="ea-report-section">
                  <h5 className="ea-detail__section-title">{t("expansionAdvisor.recommendation")}</h5>
                  {(rec.summary || rec.report_summary) && (
                    <p className="ea-detail__text" style={{ fontWeight: 500 }}>
                      {String(rec.summary || rec.report_summary)}
                    </p>
                  )}

                  <div className="ea-detail__grid">
                    <div className="ea-detail__kv">
                      <span className="ea-detail__kv-label">{t("expansionAdvisor.bestCandidate")}</span>
                      <span className="ea-detail__kv-value" style={{ cursor: rec.best_candidate_id ? "pointer" : "default" }} onClick={() => rec.best_candidate_id && onSelectCandidateId?.(rec.best_candidate_id)}>
                        {rec.best_candidate_id?.slice(0, 8) || "—"}
                      </span>
                    </div>
                    <div className="ea-detail__kv">
                      <span className="ea-detail__kv-label">{t("expansionAdvisor.runnerUp")}</span>
                      <span className="ea-detail__kv-value" style={{ cursor: rec.runner_up_candidate_id ? "pointer" : "default" }} onClick={() => rec.runner_up_candidate_id && onSelectCandidateId?.(rec.runner_up_candidate_id)}>
                        {rec.runner_up_candidate_id?.slice(0, 8) || "—"}
                      </span>
                    </div>
                    <div className="ea-detail__kv">
                      <span className="ea-detail__kv-label">{t("expansionAdvisor.bestPassCandidate")}</span>
                      <span className="ea-detail__kv-value">{rec.best_pass_candidate_id?.slice(0, 8) || "—"}</span>
                    </div>
                    <div className="ea-detail__kv">
                      <span className="ea-detail__kv-label">{t("expansionAdvisor.bestConfidenceCandidate")}</span>
                      <span className="ea-detail__kv-value">{rec.best_confidence_candidate_id?.slice(0, 8) || "—"}</span>
                    </div>
                  </div>

                  {rec.why_best && <p className="ea-detail__text"><strong>{t("expansionAdvisor.whyBest")}:</strong> {rec.why_best}</p>}
                  {rec.main_risk && <p className="ea-detail__text"><strong>{t("expansionAdvisor.mainRisk")}:</strong> {rec.main_risk}</p>}
                  {rec.best_format && <p className="ea-detail__text"><strong>{t("expansionAdvisor.bestFormat")}:</strong> {rec.best_format}</p>}
                </div>

                {/* Top candidates */}
                {top.length > 0 && (
                  <div className="ea-report-section">
                    <h5 className="ea-detail__section-title">{t("expansionAdvisor.candidateParcels")}</h5>
                    <div style={{ display: "grid", gap: 8 }}>
                      {top.map((item) => (
                        <div
                          key={item.id}
                          className="ea-candidate"
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
