import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";
import ExpansionCandidateCard from "./ExpansionCandidateCard";

export default function ExpansionResultsPanel(props: {
  items: ExpansionCandidate[];
  selectedCandidateId: string | null;
  shortlistIds: string[];
  compareIds: string[];
  localSortActive?: boolean;
  onSelectCandidate: (candidate: ExpansionCandidate) => void;
  onToggleShortlist: (candidateId: string) => void;
  onToggleCompare: (candidateId: string) => void;
  onOpenMemo?: (candidateId: string) => void;
}) {
  return (
    <div className="ea-candidate-list">
      {props.items.map((item) => (
        <ExpansionCandidateCard
          key={item.id}
          candidate={item}
          selected={props.selectedCandidateId === item.id}
          shortlisted={props.shortlistIds.includes(item.id)}
          compared={props.compareIds.includes(item.id)}
          localSortActive={props.localSortActive}
          onSelect={() => props.onSelectCandidate(item)}
          onToggleShortlist={() => props.onToggleShortlist(item.id)}
          onCompareToggle={() => props.onToggleCompare(item.id)}
          onOpenMemo={props.onOpenMemo ? () => props.onOpenMemo!(item.id) : undefined}
        />
      ))}
    </div>
  );
}
