import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { RecommendationReportResponse, ExpansionCandidate, CandidateMemoResponse } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";
import ConfidenceBadge from "./ConfidenceBadge";
import ScoreBreakdownCompact from "./ScoreBreakdownCompact";
import CopySummaryBlock from "./CopySummaryBlock";

export function triggerReportCandidateSelect(candidateId: string | undefined, onSelectCandidateId?: (candidateId: string) => void) {
  if (!candidateId) return;
  onSelectCandidateId?.(candidateId);
}

export default function ExpansionReportPanel({
  report,
  loading,
  leadCandidateId,
  leadCandidate,
  memo,
  onSelectCandidateId,
  onClose,
}: {
  report: RecommendationReportResponse | null;
  loading: boolean;
  leadCandidateId?: string | null;
  leadCandidate?: ExpansionCandidate | null;
  memo?: CandidateMemoResponse | null;
  onSelectCandidateId?: (candidateId: string) => void;
  onClose?: () => void;
}) {
  const { t } = useTranslation();
  const [presentationMode, setPresentationMode] = useState(false);

  if (!report && !loading) return null;

  return (
    <div className="ea-drawer-backdrop" onClick={() => onClose?.()}>
      <div className={`ea-drawer ea-drawer--wide${presentationMode ? " ea-drawer--presentation" : ""}`} onClick={(e) => e.stopPropagation()}>
        <div className="ea-drawer__header">
          <h3 className="ea-drawer__title">{t("expansionAdvisor.executiveReport")}</h3>
          {report?.meta?.version && (
            <span style={{ fontSize: "var(--oak-fs-xs)", color: "var(--oak-text-light)", marginInlineEnd: "auto", marginInlineStart: 12 }}>
              v{report.meta.version}
            </span>
          )}
          <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
            <button className="oak-btn oak-btn--xs oak-btn--tertiary" onClick={() => setPresentationMode((m) => !m)}>
              {presentationMode ? t("expansionAdvisor.exitPresentation") : t("expansionAdvisor.presentationMode")}
            </button>
            <button className="ea-drawer__close" onClick={() => onClose?.()}>{t("expansionAdvisor.close")}</button>
          </div>
        </div>
        <div className="ea-drawer__body">
          {loading && <div className="ea-state ea-state--loading">{t("expansionAdvisor.loadingReport")}</div>}

          {report && (() => {
            const rec = report.recommendation || {};
            const top = report.top_candidates || [];
            const assumptions = Object.entries(report.assumptions || {});

            return (
              <>
                {/* Executive recommendation hero */}
                <div className="ea-report-exec-hero">
                  {(rec.summary || rec.report_summary) && (
                    <div className="ea-report-hero">
                      <h4 className="ea-report-hero__label">{t("expansionAdvisor.reportRecommendation")}</h4>
                      <p className="ea-report-hero__text">
                        {String(rec.summary || rec.report_summary)}
                      </p>
                    </div>
                  )}

                  {/* Key decision callouts in executive grid */}
                  <div className="ea-report-exec-grid">
                    {rec.why_best && (
                      <div className="ea-memo-callout ea-memo-callout--positive">
                        <span className="ea-memo-callout__label">{t("expansionAdvisor.reportWhyBest")}</span>
                        <p className="ea-detail__text">{rec.why_best}</p>
                      </div>
                    )}
                    {rec.main_risk && (
                      <div className="ea-memo-callout ea-memo-callout--risk">
                        <span className="ea-memo-callout__label">{t("expansionAdvisor.reportMainRisk")}</span>
                        <p className="ea-detail__text">{rec.main_risk}</p>
                      </div>
                    )}
                    {rec.best_format && (
                      <div className="ea-memo-callout ea-memo-callout--neutral">
                        <span className="ea-memo-callout__label">{t("expansionAdvisor.reportBestFormatLabel")}</span>
                        <p className="ea-detail__text">{rec.best_format}</p>
                      </div>
                    )}
                  </div>
                </div>

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

                {/* Top candidates with rank/confidence/gate/score breakdown */}
                {top.length > 0 && (
                  <div className="ea-report-section">
                    <h5 className="ea-detail__section-title">{t("expansionAdvisor.reportTopCandidates")}</h5>
                    <div className="ea-report-top-grid">
                      {top.slice(0, 3).map((item, index) => (
                        <div
                          key={item.id || index}
                          className={`ea-report-top-card${index === 0 ? " ea-report-top-card--best" : ""}`}
                          style={{ cursor: item.id ? "pointer" : "default" }}
                          onClick={() => item.id && triggerReportCandidateSelect(item.id, onSelectCandidateId)}
                        >
                          <div className="ea-report-top-card__header">
                            <span className="ea-report-top-card__rank">
                              {item.rank_position ? t("expansionAdvisor.reportRankLabel", { rank: item.rank_position }) : `#${index + 1}`}
                            </span>
                            <div className="ea-report-top-card__badges">
                              <ScorePill value={item.final_score} />
                              <ConfidenceBadge grade={item.confidence_grade} />
                              {item.gate_verdict && (
                                <span className={`ea-badge ea-badge--${item.gate_verdict === "pass" ? "green" : "red"}`}>
                                  {item.gate_verdict}
                                </span>
                              )}
                            </div>
                          </div>
                          {/* Positives and risks */}
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
                          {item.score_breakdown_json && (
                            <ScoreBreakdownCompact breakdown={item.score_breakdown_json} />
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Lead candidate focus callouts */}
                {leadCandidateId && (
                  <div className="ea-report-section">
                    <h5 className="ea-detail__section-title">{t("expansionAdvisor.leadSiteAnalysis")}</h5>
                    {rec.why_best && (
                      <div className="ea-memo-callout ea-memo-callout--positive">
                        <span className="ea-memo-callout__label">{t("expansionAdvisor.whyLeadSite")}</span>
                        <p className="ea-detail__text">{rec.why_best}</p>
                      </div>
                    )}
                    {rec.main_risk && (
                      <div className="ea-memo-callout ea-memo-callout--risk">
                        <span className="ea-memo-callout__label">{t("expansionAdvisor.whyNotRunnerUp")}</span>
                        <p className="ea-detail__text">{rec.main_risk}</p>
                      </div>
                    )}
                    {rec.best_format && (
                      <div className="ea-memo-callout ea-memo-callout--neutral">
                        <span className="ea-memo-callout__label">{t("expansionAdvisor.beforeSigning")}</span>
                        <p className="ea-detail__text">{rec.best_format}</p>
                      </div>
                    )}
                  </div>
                )}

                {/* Copy-ready executive summary block */}
                <CopySummaryBlock
                  candidate={leadCandidate || null}
                  report={report}
                  memo={memo || null}
                />

                {/* Assumptions */}
                {!presentationMode && assumptions.length > 0 && (
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
