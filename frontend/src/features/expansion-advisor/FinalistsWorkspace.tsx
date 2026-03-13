import { useTranslation } from "react-i18next";
import type { ExpansionCandidate, CandidateMemoResponse } from "../../lib/api/expansionAdvisor";
import { buildFinalistTiles, type FinalistTile } from "./studyAdapters";
import ScorePill from "./ScorePill";
import ConfidenceBadge from "./ConfidenceBadge";
import PaybackBadge from "./PaybackBadge";
import { fmtSAR, fmtScore, isGarbledText } from "./formatHelpers";

type Props = {
  candidates: ExpansionCandidate[];
  shortlistIds: string[];
  leadCandidateId: string | null;
  selectedCandidateId: string | null;
  onSetLead: (candidateId: string) => void;
  onClearLead: () => void;
  onOpenMemo: (candidateId: string) => void;
  onCompare: () => void;
  onRemoveShortlist: (candidateId: string) => void;
  onSelectCandidate: (candidateId: string) => void;
  compareEnabled: boolean;
};

function TileCard({
  tile,
  isSelected,
  onSetLead,
  onClearLead,
  onOpenMemo,
  onRemove,
  onSelect,
  t,
}: {
  tile: FinalistTile;
  isSelected: boolean;
  onSetLead: () => void;
  onClearLead: () => void;
  onOpenMemo: () => void;
  onRemove: () => void;
  onSelect: () => void;
  t: (key: string) => string;
}) {
  const cls = [
    "ea-finalist-tile",
    tile.isLead && "ea-finalist-tile--lead",
    isSelected && "ea-finalist-tile--selected",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <div className={cls} onClick={onSelect} role="button" tabIndex={0} onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") onSelect(); }}>
      {tile.isLead && tile.gateVerdict === "pass" && <div className="ea-finalist-tile__lead-badge">{t("expansionAdvisor.leadSite")}</div>}
      {tile.isLead && tile.gateVerdict !== "pass" && <div className="ea-finalist-tile__lead-badge ea-finalist-tile__lead-badge--exploratory">{t("expansionAdvisor.topExploratoryCandidate")}</div>}
      <div className="ea-finalist-tile__header">
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {tile.rankPosition && <span className="ea-candidate__rank">#{tile.rankPosition}</span>}
          <span className="ea-finalist-tile__district">{tile.district}</span>
        </div>
        <div className="ea-candidate__badges">
          <ScorePill value={tile.finalScore} />
          <ConfidenceBadge grade={tile.confidenceGrade} />
          <span className={`ea-badge ea-badge--${tile.gateVerdict === "pass" ? "green" : tile.gateVerdict === "fail" ? "red" : "amber"}`}>
            {tile.gateVerdict === "pass" ? t("expansionAdvisor.gatePass") : tile.gateVerdict === "fail" ? t("expansionAdvisor.gateFail") : t("expansionAdvisor.gateNeedsValidation")}
          </span>
        </div>
      </div>

      <div className="ea-finalist-tile__metrics">
        <div className="ea-finalist-tile__metric">
          <span className="ea-finalist-tile__metric-label">{t("expansionAdvisor.payback")}</span>
          <PaybackBadge
            band={tile.paybackBand !== "—" ? tile.paybackBand : undefined}
            months={tile.paybackMonths}
          />
        </div>
        <div className="ea-finalist-tile__metric">
          <span className="ea-finalist-tile__metric-label">{t("expansionAdvisor.annualRent")}</span>
          <span className="ea-finalist-tile__metric-value">{fmtSAR(tile.estimatedAnnualRent)}</span>
        </div>
        <div className="ea-finalist-tile__metric">
          <span className="ea-finalist-tile__metric-label">{t("expansionAdvisor.fitoutCost")}</span>
          <span className="ea-finalist-tile__metric-value">{fmtSAR(tile.fitoutCost)}</span>
        </div>
        <div className="ea-finalist-tile__metric">
          <span className="ea-finalist-tile__metric-label">{t("expansionAdvisor.revenueIndex")}</span>
          <span className="ea-finalist-tile__metric-value">{fmtScore(tile.revenueIndex, 1)}</span>
        </div>
      </div>

      <div className="ea-finalist-tile__insights">
        <div className="ea-candidate__insight">
          <span className="ea-candidate__insight-icon ea-candidate__insight-icon--positive">+</span>
          <span>{tile.bestStrength}</span>
        </div>
        <div className="ea-candidate__insight">
          <span className="ea-candidate__insight-icon ea-candidate__insight-icon--risk">!</span>
          <span>{tile.mainRisk}</span>
        </div>
      </div>

      <div className="ea-finalist-tile__actions" onClick={(e) => e.stopPropagation()}>
        {tile.isLead ? (
          <button type="button" className="oak-btn oak-btn--xs oak-btn--secondary" onClick={onClearLead}>
            {t("expansionAdvisor.clearLead")}
          </button>
        ) : (
          <button type="button" className="oak-btn oak-btn--xs oak-btn--primary" onClick={onSetLead}>
            {tile.gateVerdict === "pass" ? t("expansionAdvisor.setAsLead") : t("expansionAdvisor.markExploratoryPick")}
          </button>
        )}
        <button type="button" className="oak-btn oak-btn--xs oak-btn--tertiary" onClick={onOpenMemo}>
          {t("expansionAdvisor.memo")}
        </button>
        <button type="button" className="oak-btn oak-btn--xs oak-btn--tertiary" onClick={onRemove}>
          &times;
        </button>
      </div>
    </div>
  );
}

export default function FinalistsWorkspace({
  candidates,
  shortlistIds,
  leadCandidateId,
  selectedCandidateId,
  onSetLead,
  onClearLead,
  onOpenMemo,
  onCompare,
  onRemoveShortlist,
  onSelectCandidate,
  compareEnabled,
}: Props) {
  const { t } = useTranslation();
  const tiles = buildFinalistTiles(candidates, shortlistIds, leadCandidateId);

  if (tiles.length === 0) return null;

  return (
    <div className="ea-finalists-workspace">
      <div className="ea-finalists-workspace__header">
        <h3 className="ea-finalists-workspace__title">
          {t("expansionAdvisor.finalistsWorkspace")} ({tiles.length})
        </h3>
        <div className="ea-finalists-workspace__header-actions">
          {compareEnabled && (
            <button type="button" className="oak-btn oak-btn--sm oak-btn--primary" onClick={onCompare}>
              {t("expansionAdvisor.compareFinalists")}
            </button>
          )}
        </div>
      </div>
      <div className="ea-finalists-workspace__tiles">
        {tiles.map((tile) => (
          <TileCard
            key={tile.id}
            tile={tile}
            isSelected={tile.id === selectedCandidateId}
            onSetLead={() => onSetLead(tile.id)}
            onClearLead={onClearLead}
            onOpenMemo={() => onOpenMemo(tile.id)}
            onRemove={() => onRemoveShortlist(tile.id)}
            onSelect={() => onSelectCandidate(tile.id)}
            t={t}
          />
        ))}
      </div>
    </div>
  );
}
