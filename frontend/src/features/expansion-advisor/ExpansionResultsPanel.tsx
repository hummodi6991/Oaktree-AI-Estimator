import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";
import ExpansionCandidateCard from "./ExpansionCandidateCard";

export default function ExpansionResultsPanel(props: {
  items: ExpansionCandidate[];
  selectedCandidateId: string | null;
  shortlistIds: string[];
  compareIds: string[];
  onSelectCandidate: (candidate: ExpansionCandidate) => void;
  onToggleShortlist: (candidateId: string) => void;
  onToggleCompare: (candidateId: string) => void;
}) {
  return (
    <div style={{ display: "grid", gap: 8 }}>
      {props.items.map((item) => (
        <ExpansionCandidateCard
          key={item.id}
          candidate={item}
          selected={props.selectedCandidateId === item.id}
          shortlisted={props.shortlistIds.includes(item.id)}
          compared={props.compareIds.includes(item.id)}
          onSelect={() => props.onSelectCandidate(item)}
          onToggleShortlist={() => props.onToggleShortlist(item.id)}
          onCompareToggle={() => props.onToggleCompare(item.id)}
        />
      ))}
    </div>
  );
}
