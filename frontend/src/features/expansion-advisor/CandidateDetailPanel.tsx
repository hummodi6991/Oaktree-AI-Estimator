import { useTranslation } from "react-i18next";
import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";
import ScorePill from "./ScorePill";
import ConfidenceBadge from "./ConfidenceBadge";
import PaybackBadge from "./PaybackBadge";
import GateSummary from "./GateSummary";
import ScoreBreakdownCompact from "./ScoreBreakdownCompact";
import { fmtSAR, fmtMeters, fmtScore, fmtPct, fmtSarPerM2, fmtM2, fmtMonths, humanGateLabel } from "./formatHelpers";

type Props = {
  candidate: ExpansionCandidate;
};

export default function CandidateDetailPanel({ candidate }: Props) {
  const { t } = useTranslation();
  const gates = candidate.gate_status_json || {};
  const gateReasons = candidate.gate_reasons_json;
  const snapshot = candidate.feature_snapshot_json;
  const breakdown = candidate.score_breakdown_json;
  const comps = (candidate.comparable_competitors_json || []).slice(0, 5);

  return (
    <div className="ea-detail">
      {/* Decision summary */}
      {candidate.decision_summary && (
        <div className="ea-detail__section">
          <h5 className="ea-detail__section-title">{t("expansionAdvisor.decisionSummary")}</h5>
          <p className="ea-detail__text">{candidate.decision_summary}</p>
        </div>
      )}

      {/* Theses */}
      {(candidate.demand_thesis || candidate.cost_thesis) && (
        <div className="ea-detail__section">
          {candidate.demand_thesis && (
            <>
              <h5 className="ea-detail__section-title">{t("expansionAdvisor.demandThesis")}</h5>
              <p className="ea-detail__text">{candidate.demand_thesis}</p>
            </>
          )}
          {candidate.cost_thesis && (
            <>
              <h5 className="ea-detail__section-title">{t("expansionAdvisor.costThesis")}</h5>
              <p className="ea-detail__text">{candidate.cost_thesis}</p>
            </>
          )}
        </div>
      )}

      {/* Score breakdown grid */}
      <div className="ea-detail__section">
        <h5 className="ea-detail__section-title">{t("expansionAdvisor.scoreBreakdown")}</h5>
        <div className="ea-detail__grid">
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.finalScore")}</span>
            <ScorePill value={candidate.final_score} large />
          </div>
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.confidence")}</span>
            <ConfidenceBadge grade={candidate.confidence_grade} />
          </div>
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.payback")}</span>
            <PaybackBadge band={candidate.payback_band} months={candidate.estimated_payback_months} />
          </div>
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.economicsLabel")}</span>
            <span className="ea-detail__kv-value">{fmtScore(candidate.economics_score)}</span>
          </div>
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.brandFitLabel")}</span>
            <span className="ea-detail__kv-value">{fmtScore(candidate.brand_fit_score)}</span>
          </div>
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.zoningFitScore")}</span>
            <span className="ea-detail__kv-value">{fmtScore(candidate.zoning_fit_score)}</span>
          </div>
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.frontageScore")}</span>
            <span className="ea-detail__kv-value">{fmtScore(candidate.frontage_score)}</span>
          </div>
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.accessScore")}</span>
            <span className="ea-detail__kv-value">{fmtScore(candidate.access_score)}</span>
          </div>
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.parkingScore")}</span>
            <span className="ea-detail__kv-value">{fmtScore(candidate.parking_score)}</span>
          </div>
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.accessVisibility")}</span>
            <span className="ea-detail__kv-value">{fmtScore(candidate.access_visibility_score)}</span>
          </div>
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.providerDensity")}</span>
            <span className="ea-detail__kv-value">{fmtScore(candidate.provider_density_score)}</span>
          </div>
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.providerWhitespace")}</span>
            <span className="ea-detail__kv-value">{fmtScore(candidate.provider_whitespace_score)}</span>
          </div>
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.multiPlatform")}</span>
            <span className="ea-detail__kv-value">{fmtScore(candidate.multi_platform_presence_score)}</span>
          </div>
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.deliveryCompetition")}</span>
            <span className="ea-detail__kv-value">{fmtScore(candidate.delivery_competition_score)}</span>
          </div>
        </div>
        {/* Economics & site row */}
        <div className="ea-detail__grid" style={{ marginTop: 8 }}>
          {candidate.area_m2 != null && (
            <div className="ea-detail__kv">
              <span className="ea-detail__kv-label">{t("expansionAdvisor.areaLabel")}</span>
              <span className="ea-detail__kv-value">{fmtM2(candidate.area_m2)}</span>
            </div>
          )}
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.rent")}</span>
            <span className="ea-detail__kv-value">{fmtSarPerM2(candidate.estimated_rent_sar_m2_year)}</span>
          </div>
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.annualRent")}</span>
            <span className="ea-detail__kv-value">{fmtSAR(candidate.estimated_annual_rent_sar)}</span>
          </div>
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.fitoutCost")}</span>
            <span className="ea-detail__kv-value">{fmtSAR(candidate.estimated_fitout_cost_sar)}</span>
          </div>
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.revenueIndex")}</span>
            <span className="ea-detail__kv-value">{fmtScore(candidate.estimated_revenue_index, 1)}</span>
          </div>
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.payback")}</span>
            <span className="ea-detail__kv-value"><PaybackBadge band={candidate.payback_band} months={candidate.estimated_payback_months} /></span>
          </div>
          <div className="ea-detail__kv">
            <span className="ea-detail__kv-label">{t("expansionAdvisor.nearestBranch")}</span>
            <span className="ea-detail__kv-value">{fmtMeters(candidate.distance_to_nearest_branch_m)}</span>
          </div>
          {candidate.cannibalization_score != null && (
            <div className="ea-detail__kv">
              <span className="ea-detail__kv-label">{t("expansionAdvisor.cannibalization")}</span>
              <ScorePill value={candidate.cannibalization_score} />
            </div>
          )}
        </div>
        <ScoreBreakdownCompact breakdown={breakdown} />
      </div>

      {/* Gate summary */}
      <div className="ea-detail__section">
        <h5 className="ea-detail__section-title">{t("expansionAdvisor.gateChecklist")}</h5>
        <GateSummary gates={gates} unknownGates={gateReasons?.unknown} />
        {gateReasons && (
          <div style={{ fontSize: "var(--oak-fs-xs)", marginTop: 6 }}>
            {gateReasons.passed.length > 0 && (
              <div><span className="ea-badge ea-badge--green">{t("expansionAdvisor.gatesPassed")}</span> {gateReasons.passed.map(humanGateLabel).join(", ")}</div>
            )}
            {gateReasons.failed.length > 0 && (
              <div><span className="ea-badge ea-badge--red">{t("expansionAdvisor.gatesFailed")}</span> {gateReasons.failed.map(humanGateLabel).join(", ")}</div>
            )}
            {gateReasons.unknown.length > 0 && (
              <div><span className="ea-badge ea-badge--amber">{t("expansionAdvisor.gatesNeedVerification")}</span> {gateReasons.unknown.map(humanGateLabel).join(", ")}</div>
            )}
          </div>
        )}
      </div>

      {/* Positives / risks */}
      <div className="ea-detail__section">
        <h5 className="ea-detail__section-title">{t("expansionAdvisor.topPositives")}</h5>
        {(candidate.top_positives_json || []).length > 0 ? (
          <ul style={{ margin: 0, paddingInlineStart: 16 }}>
            {(candidate.top_positives_json || []).map((s, i) => <li key={i} className="ea-detail__text">{s}</li>)}
          </ul>
        ) : <p className="ea-detail__text">—</p>}
      </div>
      <div className="ea-detail__section">
        <h5 className="ea-detail__section-title">{t("expansionAdvisor.topRisks")}</h5>
        {(candidate.top_risks_json || []).length > 0 ? (
          <ul style={{ margin: 0, paddingInlineStart: 16 }}>
            {(candidate.top_risks_json || []).map((s, i) => <li key={i} className="ea-detail__text">{s}</li>)}
          </ul>
        ) : <p className="ea-detail__text">—</p>}
      </div>

      {/* Feature snapshot */}
      {snapshot && (
        <div className="ea-detail__section">
          <h5 className="ea-detail__section-title">{t("expansionAdvisor.featureSnapshot")}</h5>
          <div className="ea-detail__grid">
            <div className="ea-detail__kv">
              <span className="ea-detail__kv-label">{t("expansionAdvisor.dataCompleteness")}</span>
              <span className="ea-detail__kv-value">{fmtPct(snapshot.data_completeness_score)}</span>
            </div>
          </div>
          {snapshot.missing_context.length > 0 && (
            <div>
              <span className="ea-detail__kv-label">{t("expansionAdvisor.missingData")}:</span>{" "}
              <span className="ea-detail__text">{snapshot.missing_context.join(", ")}</span>
            </div>
          )}
        </div>
      )}

      {/* Comparable competitors */}
      {comps.length > 0 && (
        <div className="ea-detail__section">
          <h5 className="ea-detail__section-title">{t("expansionAdvisor.comparableCompetitors")}</h5>
          <table className="ea-comp-table">
            <thead>
              <tr>
                <th>{t("expansionAdvisor.branchName")}</th>
                <th>{t("expansionAdvisor.category")}</th>
                <th>{t("expansionAdvisor.district")}</th>
                <th>{t("expansionAdvisor.nearestBranch")}</th>
              </tr>
            </thead>
            <tbody>
              {comps.map((c, i) => (
                <tr key={c.id || i}>
                  <td>{c.name || "—"}</td>
                  <td>{c.category || "—"}</td>
                  <td>{c.district || "—"}</td>
                  <td>{fmtMeters(c.distance_m)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Debug details (collapsed) */}
      {breakdown && (
        <details className="ea-debug">
          <summary>{t("expansionAdvisor.debugDetails")}</summary>
          <pre>{JSON.stringify({ score_breakdown: breakdown, feature_snapshot: snapshot }, null, 2)}</pre>
        </details>
      )}
    </div>
  );
}
