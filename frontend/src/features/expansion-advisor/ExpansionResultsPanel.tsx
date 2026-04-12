import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";
import ExpansionCandidateCard from "./ExpansionCandidateCard";

export default function ExpansionResultsPanel(props: {
  items: ExpansionCandidate[];
  selectedCandidateId: string | null;
  shortlistIds: string[];
  compareIds: string[];
  leadCandidateId?: string | null;
  localSortActive?: boolean;
  onSelectCandidate: (candidate: ExpansionCandidate) => void;
  onToggleCompare: (candidateId: string) => void;
  onOpenMemo?: (candidateId: string) => void;
  onShowOnMap?: (candidate: ExpansionCandidate) => void;
  /** Retained for backward compatibility with tests. Patch 16 removed the
   *  in-card shortlist button so this callback is never invoked. */
  onToggleShortlist?: (candidateId: string) => void;
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
          isLead={item.id === props.leadCandidateId}
          localSortActive={props.localSortActive}
          onSelect={() => props.onSelectCandidate(item)}
          onCompareToggle={() => props.onToggleCompare(item.id)}
          onOpenMemo={props.onOpenMemo ? () => props.onOpenMemo!(item.id) : undefined}
          onShowOnMap={props.onShowOnMap ? () => props.onShowOnMap!(item) : undefined}
        />
      ))}
    </div>
  );
}
