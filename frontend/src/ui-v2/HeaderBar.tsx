import { ChevronDownIcon, GlobeAltIcon } from "@heroicons/react/24/outline";
import { useTranslation } from "react-i18next";
import type { SearchItem } from "../types/search";
import SearchBar from "./SearchBar";

type HeaderBarProps = {
  onSearchSelect: (item: SearchItem) => void;
};

export default function HeaderBar({ onSearchSelect }: HeaderBarProps) {
  const { i18n } = useTranslation();
  const isArabic = i18n.language.startsWith("ar");

  return (
    <header className="oak-topbar">
      <div className="oak-container oak-topbar-inner">
        <div>
          <div className="oak-brand-title">Oaktree Estimator</div>
          <div className="oak-brand-subtitle">Riyadh Commercial Development</div>
        </div>
        <div>
          <SearchBar onSelect={onSearchSelect} />
        </div>
        <div className="oak-top-actions">
          <button
            type="button"
            className="oak-btn oak-btn--secondary oak-btn--md"
            onClick={() => void i18n.changeLanguage(isArabic ? "en" : "ar")}
          >
            <GlobeAltIcon width={16} height={16} />
            <span>{isArabic ? "English" : "العربية"}</span>
          </button>
          <button type="button" className="oak-btn oak-btn--tertiary oak-btn--md ui-v2-header__user-chip">
            <span className="ui-v2-header__avatar" aria-hidden="true">
              OT
            </span>
            <span>Oaktree Team</span>
            <ChevronDownIcon width={14} height={14} />
          </button>
        </div>
      </div>
    </header>
  );
}
