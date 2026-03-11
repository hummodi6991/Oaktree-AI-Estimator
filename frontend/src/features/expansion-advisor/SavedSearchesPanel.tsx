import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { SavedExpansionSearch } from "../../lib/api/expansionAdvisor";

type Props = {
  items: SavedExpansionSearch[];
  loading: boolean;
  activeSavedId?: string | null;
  onOpen: (savedId: string) => void;
  onDelete?: (savedId: string) => void;
};

export default function SavedSearchesPanel({ items, loading, activeSavedId, onOpen, onDelete }: Props) {
  const { t } = useTranslation();
  const [confirmingDelete, setConfirmingDelete] = useState<string | null>(null);

  if (loading) return <div className="ea-state ea-state--loading">{t("expansionAdvisor.loadingSaved")}</div>;
  if (!items.length) return <div className="ea-state">{t("expansionAdvisor.savedStudiesEmpty")}</div>;

  return (
    <div className="ea-saved-list">
      {items.map((item) => {
        const shortlistCount = (item.selected_candidate_ids || []).length;
        const isActive = item.id === activeSavedId;
        return (
          <div key={item.id} className={`ea-saved-item${isActive ? " ea-saved-item--active" : ""}`} onClick={() => onOpen(item.id)}>
            <div className="ea-saved-item__info">
              <span className="ea-saved-item__title">{item.title}</span>
              <span className="ea-saved-item__meta">
                <span className={`ea-badge ea-badge--${item.status === "final" ? "green" : "neutral"}`} style={{ marginInlineEnd: 6 }}>
                  {item.status === "final" ? t("expansionAdvisor.savedStudyFinal") : t("expansionAdvisor.savedStudyDraft")}
                </span>
                {item.description && <span style={{ marginInlineEnd: 6 }}>{item.description.slice(0, 60)}{item.description.length > 60 ? "…" : ""}</span>}
                {shortlistCount > 0 && (
                  <span style={{ marginInlineEnd: 6 }}>
                    {t("expansionAdvisor.shortlistedCount", { count: shortlistCount })}
                  </span>
                )}
                {item.updated_at || item.created_at || ""}
              </span>
            </div>
            <div className="ea-saved-item__actions" onClick={(e) => e.stopPropagation()}>
              <button className="oak-btn oak-btn--sm oak-btn--tertiary" onClick={() => onOpen(item.id)}>
                {t("expansionAdvisor.reopenStudy")}
              </button>
              {onDelete && (
                confirmingDelete === item.id ? (
                  <button className="oak-btn oak-btn--sm oak-btn--tertiary" style={{ color: "var(--oak-error)" }} onClick={() => { onDelete(item.id); setConfirmingDelete(null); }}>
                    {t("expansionAdvisor.confirmDeleteStudy")}
                  </button>
                ) : (
                  <button className="oak-btn oak-btn--sm oak-btn--tertiary" onClick={() => setConfirmingDelete(item.id)}>
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
