import { useTranslation } from "react-i18next";
import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";

function short(text?: string) {
  if (!text) return "-";
  return text.length > 120 ? `${text.slice(0, 117)}...` : text;
}

export default function ExpansionCandidateCard({ candidate, selected, shortlisted, compared, onSelect, onToggleShortlist, onCompareToggle }: { candidate: ExpansionCandidate; selected: boolean; shortlisted: boolean; compared: boolean; onSelect: () => void; onToggleShortlist: () => void; onCompareToggle: () => void }) {
  const { t } = useTranslation();
  const pass = Boolean(candidate.gate_status_json?.overall_pass);
  const comps = (candidate.comparable_competitors_json || []).slice(0, 3);
  return (
    <div
      style={{
        border: selected ? "2px solid #1a9c6c" : "1px solid #d8e1dd",
        background: selected ? "#ecfff8" : shortlisted ? "#f6fffb" : "#fff",
        boxShadow: compared ? "0 0 0 2px rgba(33, 111, 255, 0.2)" : undefined,
        borderRadius: 8,
        padding: 10,
        display: "grid",
        gap: 6,
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <strong>{candidate.district || t("common.notAvailable")}</strong>
        <div style={{ display: "flex", gap: 6, alignItems: "center", flexWrap: "wrap" }}>
          {candidate.rank_position ? (
            <span style={{ background: "#1a9c6c", color: "#fff", borderRadius: 10, padding: "2px 8px", fontWeight: 700 }}>#{candidate.rank_position}</span>
          ) : null}
          {selected ? <span style={{ border: "1px solid #1a9c6c", borderRadius: 10, padding: "2px 8px" }}>selected</span> : null}
          {shortlisted ? <span style={{ border: "1px solid #0f766e", borderRadius: 10, padding: "2px 8px" }}>shortlisted</span> : null}
          {compared ? <span style={{ border: "1px solid #1d4ed8", borderRadius: 10, padding: "2px 8px" }}>compared</span> : null}
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
      <div>{t("expansionAdvisor.brandFit")} / {t("expansionAdvisor.economicsScore")}: {candidate.brand_fit_score ?? "-"} / {candidate.economics_score ?? "-"}</div>
      <div>{t("expansionAdvisor.demandThesis")}: {short(candidate.demand_thesis)}</div>
      <div>{t("expansionAdvisor.costThesis")}: {short(candidate.cost_thesis)}</div>
      <div><strong>{t("expansionAdvisor.topPositives")}</strong>: {(candidate.top_positives_json || []).slice(0, 2).join(" • ") || "-"}</div>
      <div><strong>{t("expansionAdvisor.topRisks")}</strong>: {(candidate.top_risks_json || []).slice(0, 2).join(" • ") || "-"}</div>
      <div>
        <strong>{t("expansionAdvisor.comparableCompetitors")}</strong>
        <ul>{comps.map((c, i) => <li key={`${c.id || c.name || i}`}>{c.name || t("common.notAvailable")} ({Math.round(c.distance_m || 0)}m)</li>)}</ul>
      </div>
      <div style={{ display: "flex", gap: 6 }}>
        <button onClick={onSelect}>{t("expansionAdvisor.decisionMemo")}</button>
        <button onClick={onToggleShortlist}>{shortlisted ? t("expansionAdvisor.removeShortlist") : t("expansionAdvisor.shortlist")}</button>
        <button onClick={onCompareToggle}>{compared ? t("expansionAdvisor.removeCompare") : t("expansionAdvisor.compareCandidates")}</button>
      </div>
    </div>
  );
}
