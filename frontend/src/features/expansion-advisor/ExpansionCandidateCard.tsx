import { useTranslation } from "react-i18next";
import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";

function short(text?: string) {
  if (!text) return "-";
  return text.length > 120 ? `${text.slice(0, 117)}...` : text;
}

export default function ExpansionCandidateCard({ candidate, selected, shortlisted, onSelect, onToggleShortlist, onCompareToggle }: { candidate: ExpansionCandidate; selected: boolean; shortlisted: boolean; onSelect: () => void; onToggleShortlist: () => void; onCompareToggle: () => void }) {
  const { t } = useTranslation();
  const pass = Boolean(candidate.gate_status_json?.overall_pass);
  const comps = (candidate.comparable_competitors_json || []).slice(0, 3);
  return (
    <div style={{ border: selected ? "2px solid #1a9c6c" : "1px solid #d8e1dd", borderRadius: 8, padding: 10, display: "grid", gap: 6 }}>
      <div style={{ display: "flex", justifyContent: "space-between" }}>
        <strong>{candidate.district || t("common.notAvailable")}</strong>
        <div style={{ display: "flex", gap: 6 }}>
          <span>{t("expansionAdvisor.confidenceGrade")} {candidate.confidence_grade || "-"}</span>
          <span>{t("expansionAdvisor.gateVerdict")}: {pass ? t("expansionAdvisor.pass") : t("expansionAdvisor.fail")}</span>
        </div>
      </div>
      <div>{t("expansionAdvisor.candidateParcels")}: {candidate.parcel_id}</div>
      <div>{t("expansionAdvisor.finalScore")}: {candidate.final_score ?? "-"}</div>
      <div>{t("expansionAdvisor.zoningFitScore")}: {candidate.zoning_fit_score ?? "-"}</div>
      <div>{t("expansionAdvisor.frontageScore")}: {candidate.frontage_score ?? "-"}</div>
      <div>{t("expansionAdvisor.accessScore")}: {candidate.access_score ?? "-"}</div>
      <div>{t("expansionAdvisor.parkingScore")}: {candidate.parking_score ?? "-"}</div>
      <div>{t("expansionAdvisor.demandThesis")}: {short(candidate.demand_thesis)}</div>
      <div>{t("expansionAdvisor.costThesis")}: {short(candidate.cost_thesis)}</div>
      <div>
        <strong>{t("expansionAdvisor.comparableCompetitors")}</strong>
        <ul>{comps.map((c, i) => <li key={`${c.id || c.name || i}`}>{c.name || t("common.notAvailable")} ({Math.round(c.distance_m || 0)}m)</li>)}</ul>
      </div>
      <button onClick={onSelect}>{t("expansionAdvisor.decisionMemo")}</button>
      <button onClick={onToggleShortlist}>{shortlisted ? t("expansionAdvisor.removeShortlist") : t("expansionAdvisor.shortlist")}</button>
      <button onClick={onCompareToggle}>{t("expansionAdvisor.compareCandidates")}</button>
    </div>
  );
}
