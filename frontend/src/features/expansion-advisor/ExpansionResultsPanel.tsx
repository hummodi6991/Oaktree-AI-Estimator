import { useTranslation } from "react-i18next";
import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";
import ExpansionCandidateCard from "./ExpansionCandidateCard";
import type { MemoDrawerSection } from "./ExpansionMemoPanel";
import { groupCandidatesByTier, type CandidateTier } from "./tiers";

type Props = {
  items: ExpansionCandidate[];
  selectedCandidateId: string | null;
  shortlistIds: string[];
  compareIds: string[];
  leadCandidateId?: string | null;
  localSortActive?: boolean;
  heroPresent?: boolean;
  onSelectCandidate: (candidate: ExpansionCandidate) => void;
  onToggleCompare: (candidateId: string) => void;
  onOpenMemo?: (candidateId: string, options?: { section?: MemoDrawerSection }) => void;
  onShowOnMap?: (candidate: ExpansionCandidate) => void;
  /** Retained for backward compatibility with tests. Patch 16 removed the
   *  in-card shortlist button so this callback is never invoked. */
  onToggleShortlist?: (candidateId: string) => void;
};

export default function ExpansionResultsPanel(props: Props) {
  const { t } = useTranslation();
  const grouped = groupCandidatesByTier(props.items);

  const renderCard = (item: ExpansionCandidate, tier: CandidateTier) => (
    <ExpansionCandidateCard
      key={item.id}
      candidate={item}
      selected={props.selectedCandidateId === item.id}
      shortlisted={props.shortlistIds.includes(item.id)}
      compared={props.compareIds.includes(item.id)}
      isLead={item.id === props.leadCandidateId}
      localSortActive={props.localSortActive}
      tier={tier}
      suppressLeadChip={Boolean(props.heroPresent) && item.id === props.leadCandidateId}
      onSelect={() => props.onSelectCandidate(item)}
      onCompareToggle={() => props.onToggleCompare(item.id)}
      onOpenMemo={props.onOpenMemo ? (options) => props.onOpenMemo!(item.id, options) : undefined}
      onShowOnMap={props.onShowOnMap ? () => props.onShowOnMap!(item) : undefined}
    />
  );

  // When every candidate is Standard (the common case for small shortlists
  // and for brand profiles where no candidate qualifies as Premier or
  // Exploratory), render the flat list with no section headers — the UI
  // looks identical to pre-patch behavior.
  const hasPremier = grouped.premier.length > 0;
  const hasExploratory = grouped.exploratory.length > 0;
  const needsSectionHeaders = hasPremier || hasExploratory;

  if (!needsSectionHeaders) {
    return (
      <div className="ea-candidate-list">
        {grouped.standard.map((item) => renderCard(item, "standard"))}
      </div>
    );
  }

  return (
    <div className="ea-candidate-list">
      {hasPremier && (
        <div className="ea-candidate-list__section ea-candidate-list__section--premier">
          <h3 className="ea-candidate-list__section-header ea-candidate-list__section-header--premier">
            {t("expansionAdvisor.tierPremierHeader")}
          </h3>
          {grouped.premier.map((item) => renderCard(item, "premier"))}
        </div>
      )}
      {grouped.standard.length > 0 && (
        <div className="ea-candidate-list__section ea-candidate-list__section--standard">
          {grouped.standard.map((item) => renderCard(item, "standard"))}
        </div>
      )}
      {hasExploratory && (
        <div className="ea-candidate-list__section ea-candidate-list__section--exploratory">
          <h3 className="ea-candidate-list__section-header ea-candidate-list__section-header--exploratory">
            {t("expansionAdvisor.tierExploratoryHeader")}
          </h3>
          {grouped.exploratory.map((item) => renderCard(item, "exploratory"))}
        </div>
      )}
    </div>
  );
}
