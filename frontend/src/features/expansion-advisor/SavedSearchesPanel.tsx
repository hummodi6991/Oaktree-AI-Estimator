import { useTranslation } from "react-i18next";
import type { SavedExpansionSearch } from "../../lib/api/expansionAdvisor";

type Props = {
  items: SavedExpansionSearch[];
  loading: boolean;
  onOpen: (savedId: string) => void;
};

export default function SavedSearchesPanel({ items, loading, onOpen }: Props) {
  const { t } = useTranslation();

  if (loading) return <div className="ea-state ea-state--loading">{t("expansionAdvisor.loadingSaved")}</div>;
  if (!items.length) return <div className="ea-state">{t("expansionAdvisor.noSavedSearches")}</div>;

  return (
    <div className="ea-saved-list">
      {items.map((item) => (
        <div key={item.id} className="ea-saved-item" onClick={() => onOpen(item.id)}>
          <div className="ea-saved-item__info">
            <span className="ea-saved-item__title">{item.title}</span>
            <span className="ea-saved-item__meta">
              <span className={`ea-badge ea-badge--${item.status === "final" ? "green" : "neutral"}`} style={{ marginInlineEnd: 6 }}>
                {item.status}
              </span>
              {item.updated_at || item.created_at || ""}
            </span>
          </div>
          <button className="oak-btn oak-btn--sm oak-btn--tertiary" onClick={(e) => { e.stopPropagation(); onOpen(item.id); }}>
            {t("expansionAdvisor.openStudy")}
          </button>
        </div>
      ))}
    </div>
  );
}
