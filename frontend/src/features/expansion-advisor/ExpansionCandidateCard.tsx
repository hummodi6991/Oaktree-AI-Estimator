import { useTranslation } from "react-i18next";
import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";

export default function ExpansionCandidateCard({ candidate, selected, shortlisted, onSelect, onToggleShortlist, onCompareToggle }: { candidate: ExpansionCandidate; selected: boolean; shortlisted: boolean; onSelect: () => void; onToggleShortlist: () => void; onCompareToggle: () => void }) {
  const { t } = useTranslation();
  return (
    <div style={{ border: selected ? "2px solid #1a9c6c" : "1px solid #d8e1dd", borderRadius: 8, padding: 10 }}>
      <strong>{candidate.district || t("common.notAvailable")}</strong>
      <div>{t("expansionAdvisor.candidateParcels")}: {candidate.parcel_id}</div>
      <div>Score: {candidate.final_score ?? "-"}</div>
      <button onClick={onSelect}>{t("expansionAdvisor.decisionMemo")}</button>
      <button onClick={onToggleShortlist}>{shortlisted ? t("expansionAdvisor.removeShortlist") : t("expansionAdvisor.shortlist")}</button>
      <button onClick={onCompareToggle}>{t("expansionAdvisor.compareCandidates")}</button>
    </div>
  );
}
