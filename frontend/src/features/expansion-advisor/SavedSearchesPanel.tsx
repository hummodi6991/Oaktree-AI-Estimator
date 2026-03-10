import { useTranslation } from "react-i18next";
import type { SavedExpansionSearch } from "../../lib/api/expansionAdvisor";

export default function SavedSearchesPanel({ items, onOpen }: { items: SavedExpansionSearch[]; onOpen: (savedId: string) => void }) {
  const { t } = useTranslation();
  if (!items.length) return <div>{t("expansionAdvisor.noSavedSearches")}</div>;
  return <div style={{ display: "grid", gap: 6 }}>{items.map((item) => <button key={item.id} onClick={() => onOpen(item.id)}>{item.title} ({item.status})</button>)}</div>;
}
