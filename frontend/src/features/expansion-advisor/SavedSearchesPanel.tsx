import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { SavedExpansionSearch } from "../../lib/api/expansionAdvisor";
import { extractSavedStudyMeta } from "./studyAdapters";

type Props = {
  items: SavedExpansionSearch[];
  loading: boolean;
  activeSavedId?: string | null;
  onOpen: (savedId: string) => void;
  onDelete?: (savedId: string) => void;
  onRename?: (savedId: string, title: string) => void;
  onEditDescription?: (savedId: string, description: string) => void;
  onChangeStatus?: (savedId: string, status: "draft" | "final") => void;
};

function formatDate(iso?: string | null): string {
  if (!iso) return "";
  try {
    const d = new Date(iso);
    if (isNaN(d.getTime())) return "";
    return d.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
  } catch { return ""; }
}

export default function SavedSearchesPanel({
  items, loading, activeSavedId, onOpen, onDelete, onRename, onEditDescription, onChangeStatus,
}: Props) {
  const { t } = useTranslation();
  const [confirmingDelete, setConfirmingDelete] = useState<string | null>(null);
  const [editingTitleId, setEditingTitleId] = useState<string | null>(null);
  const [editingDescId, setEditingDescId] = useState<string | null>(null);
  const [editTitleValue, setEditTitleValue] = useState("");
  const [editDescValue, setEditDescValue] = useState("");

  if (loading) return <div className="ea-state ea-state--loading">{t("expansionAdvisor.loadingSaved")}</div>;
  if (!items.length) return <div className="ea-state">{t("expansionAdvisor.noSavedStudiesYet")}</div>;

  return (
    <div className="ea-saved-list">
      {items.map((item) => {
        const meta = extractSavedStudyMeta(item);
        const isActive = item.id === activeSavedId;
        const candidateCount = (item.candidates || []).length;
        const shortlistCount = (item.selected_candidate_ids || []).length;
        const isEditingTitle = editingTitleId === item.id;
        const isEditingDesc = editingDescId === item.id;

        return (
          <div
            key={item.id}
            className={`ea-saved-item${isActive ? " ea-saved-item--active" : ""}${meta.isFinal ? " ea-saved-item--final" : ""}`}
            onClick={() => { if (!isEditingTitle && !isEditingDesc) onOpen(item.id); }}
          >
            <div className="ea-saved-item__info">
              {/* Title — inline-editable */}
              {isEditingTitle ? (
                <div className="ea-saved-item__edit-row" onClick={(e) => e.stopPropagation()}>
                  <input
                    className="ea-form__input ea-form__input--inline"
                    value={editTitleValue}
                    onChange={(e) => setEditTitleValue(e.target.value)}
                    autoFocus
                    onKeyDown={(e) => {
                      if (e.key === "Enter" && editTitleValue.trim()) {
                        onRename?.(item.id, editTitleValue.trim());
                        setEditingTitleId(null);
                      } else if (e.key === "Escape") setEditingTitleId(null);
                    }}
                  />
                  <button
                    className="oak-btn oak-btn--xs oak-btn--primary"
                    disabled={!editTitleValue.trim()}
                    onClick={() => { onRename?.(item.id, editTitleValue.trim()); setEditingTitleId(null); }}
                  >
                    {t("expansionAdvisor.saveChanges")}
                  </button>
                  <button className="oak-btn oak-btn--xs oak-btn--tertiary" onClick={() => setEditingTitleId(null)}>
                    {t("expansionAdvisor.cancel")}
                  </button>
                </div>
              ) : (
                <span className="ea-saved-item__title">{item.title}</span>
              )}

              {/* Description — inline-editable */}
              {isEditingDesc ? (
                <div className="ea-saved-item__edit-row" onClick={(e) => e.stopPropagation()}>
                  <input
                    className="ea-form__input ea-form__input--inline"
                    value={editDescValue}
                    onChange={(e) => setEditDescValue(e.target.value)}
                    placeholder={t("expansionAdvisor.studyDescription")}
                    autoFocus
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        onEditDescription?.(item.id, editDescValue.trim());
                        setEditingDescId(null);
                      } else if (e.key === "Escape") setEditingDescId(null);
                    }}
                  />
                  <button
                    className="oak-btn oak-btn--xs oak-btn--primary"
                    onClick={() => { onEditDescription?.(item.id, editDescValue.trim()); setEditingDescId(null); }}
                  >
                    {t("expansionAdvisor.saveChanges")}
                  </button>
                  <button className="oak-btn oak-btn--xs oak-btn--tertiary" onClick={() => setEditingDescId(null)}>
                    {t("expansionAdvisor.cancel")}
                  </button>
                </div>
              ) : item.description ? (
                <span className="ea-saved-item__desc">
                  {item.description.slice(0, 80)}{item.description.length > 80 ? "…" : ""}
                </span>
              ) : null}

              {/* Metadata row */}
              <span className="ea-saved-item__meta">
                <span className={`ea-badge ea-badge--${meta.isFinal ? "green" : "neutral"}`}>
                  {meta.isFinal ? t("expansionAdvisor.savedStudyFinal") : t("expansionAdvisor.savedStudyDraft")}
                </span>
                {candidateCount > 0 && (
                  <span className="ea-saved-item__meta-chip">
                    {t("expansionAdvisor.candidateCountBadge", { count: candidateCount })}
                  </span>
                )}
                {shortlistCount > 0 && (
                  <span className="ea-saved-item__meta-chip">
                    {t("expansionAdvisor.shortlistCountBadge", { count: shortlistCount })}
                  </span>
                )}
                {meta.leadDistrict && (
                  <span className="ea-saved-item__meta-chip">
                    {meta.leadGatesPass ? t("expansionAdvisor.leadSite") : t("expansionAdvisor.topExploratoryCandidate")}: {meta.leadDistrict}
                  </span>
                )}
                {item.updated_at && (
                  <span className="ea-saved-item__meta-date">
                    {t("expansionAdvisor.updatedAt")}: {formatDate(item.updated_at)}
                  </span>
                )}
                {!item.updated_at && item.created_at && (
                  <span className="ea-saved-item__meta-date">
                    {t("expansionAdvisor.createdAt")}: {formatDate(item.created_at)}
                  </span>
                )}
              </span>
            </div>

            {/* Action buttons */}
            <div className="ea-saved-item__actions" onClick={(e) => e.stopPropagation()}>
              <button className="oak-btn oak-btn--sm oak-btn--primary" onClick={() => onOpen(item.id)}>
                {t("expansionAdvisor.reopenStudy")}
              </button>
              {onRename && !isEditingTitle && (
                <button
                  className="oak-btn oak-btn--sm oak-btn--tertiary"
                  onClick={() => { setEditTitleValue(item.title); setEditingTitleId(item.id); setEditingDescId(null); }}
                >
                  {t("expansionAdvisor.renameStudy")}
                </button>
              )}
              {onEditDescription && !isEditingDesc && (
                <button
                  className="oak-btn oak-btn--sm oak-btn--tertiary"
                  onClick={() => { setEditDescValue(item.description || ""); setEditingDescId(item.id); setEditingTitleId(null); }}
                >
                  {t("expansionAdvisor.editDescription")}
                </button>
              )}
              {onChangeStatus && (
                <button
                  className="oak-btn oak-btn--sm oak-btn--tertiary"
                  onClick={() => onChangeStatus(item.id, meta.isFinal ? "draft" : "final")}
                >
                  {meta.isFinal ? t("expansionAdvisor.markAsDraft") : t("expansionAdvisor.markAsFinal")}
                </button>
              )}
              {onDelete && (
                confirmingDelete === item.id ? (
                  <div className="ea-saved-item__confirm-delete">
                    <span className="ea-saved-item__confirm-text">{t("expansionAdvisor.confirmDeleteBody")}</span>
                    <button
                      className="oak-btn oak-btn--sm oak-btn--tertiary"
                      style={{ color: "var(--oak-error)" }}
                      onClick={() => { onDelete(item.id); setConfirmingDelete(null); }}
                    >
                      {t("expansionAdvisor.confirmDeleteAction")}
                    </button>
                    <button
                      className="oak-btn oak-btn--sm oak-btn--tertiary"
                      onClick={() => setConfirmingDelete(null)}
                    >
                      {t("expansionAdvisor.cancel")}
                    </button>
                  </div>
                ) : (
                  <button
                    className="oak-btn oak-btn--sm oak-btn--tertiary"
                    onClick={() => setConfirmingDelete(item.id)}
                  >
                    {t("expansionAdvisor.deleteStudy")}
                  </button>
                )
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}
