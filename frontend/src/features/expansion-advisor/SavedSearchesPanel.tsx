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
};

export default function SavedSearchesPanel({ items, loading, activeSavedId, onOpen, onDelete }: Props) {
  const { t } = useTranslation();
  const [confirmingDelete, setConfirmingDelete] = useState<string | null>(null);

  if (loading) return <div className="ea-state ea-state--loading">{t("expansionAdvisor.loadingSaved")}</div>;
  if (!items.length) return <div className="ea-state">{t("expansionAdvisor.savedStudiesEmpty")}</div>;

  return (
    <div className="ea-saved-list">
      {items.map((item) => {
        const meta = extractSavedStudyMeta(item);
        const isActive = item.id === activeSavedId;
        return (
          <div key={item.id} className={`ea-saved-item${isActive ? " ea-saved-item--active" : ""}${meta.isFinal ? " ea-saved-item--final" : ""}`} onClick={() => onOpen(item.id)}>
            <div className="ea-saved-item__info">
              <span className="ea-saved-item__title">{item.title}</span>
              <span className="ea-saved-item__meta">
                <span className={`ea-badge ea-badge--${meta.isFinal ? "green" : "neutral"}`} style={{ marginInlineEnd: 6 }}>
                  {meta.isFinal ? t("expansionAdvisor.savedStudyFinal") : t("expansionAdvisor.savedStudyDraft")}
                </span>
                {meta.leadDistrict && (
                  <span className="ea-saved-item__meta-chip" style={{ marginInlineEnd: 6 }}>
                    {t("expansionAdvisor.leadSite")}: {meta.leadDistrict}
                    {meta.leadParcelId && meta.leadParcelId !== meta.leadDistrict && (
                      <> ({meta.leadParcelId.slice(0, 8)})</>
                    )}
                  </span>
                )}
                {!meta.leadDistrict && meta.leadParcelId && (
                  <span className="ea-saved-item__meta-chip" style={{ marginInlineEnd: 6 }}>
                    {t("expansionAdvisor.leadSite")}: {meta.leadParcelId.slice(0, 8)}
                  </span>
                )}
                {item.description && <span style={{ marginInlineEnd: 6 }}>{item.description.slice(0, 60)}{item.description.length > 60 ? "…" : ""}</span>}
                {meta.shortlistCount > 0 && (
                  <span style={{ marginInlineEnd: 6 }}>
                    {t("expansionAdvisor.shortlistedCount", { count: meta.shortlistCount })}
                  </span>
                )}
                {meta.compareCount > 0 && (
                  <span style={{ marginInlineEnd: 6 }}>
                    {t("expansionAdvisor.smCompared", { count: meta.compareCount })}
                  </span>
                )}
                {meta.lastSort && (
                  <span className="ea-saved-item__meta-chip" style={{ marginInlineEnd: 6 }}>
                    {t("expansionAdvisor.sortLabel")}: {meta.lastSort.replace(/_/g, " ")}
                  </span>
                )}
                {meta.lastFilter && (
                  <span className="ea-saved-item__meta-chip" style={{ marginInlineEnd: 6 }}>
                    {t("expansionAdvisor.filterLabel")}: {meta.lastFilter.replace(/_/g, " ")}
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
