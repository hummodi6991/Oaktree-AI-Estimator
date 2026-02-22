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
  const userDisplayName = "Oaktree User";

  return (
    <header className="oak-topbar ui-v2-header app-topbar">
      <div className="oak-container oak-topbar-inner">
        <div className="app-topbar__left">
          <div className="oak-brand-title app-topbar__title">Oaktree Atlas</div>
        </div>
        <div className="app-topbar__center">
          <SearchBar onSelect={onSearchSelect} />
        </div>
        <div className="oak-top-actions app-topbar__right">
          <button
            type="button"
            className="oak-btn oak-btn--secondary oak-btn--md"
            onClick={() => void i18n.changeLanguage(isArabic ? "en" : "ar")}
          >
            <GlobeAltIcon width={16} height={16} />
            <span>{isArabic ? "English" : "العربية"}</span>
          </button>
          <button type="button" className="oak-btn oak-btn--tertiary oak-btn--md ui-v2-header__user-chip">
            <span className="ui-v2-header__avatar ui-v2-header__avatar--photo" aria-hidden="true">AR</span>
            <span>{userDisplayName}</span>
            <ChevronDownIcon width={14} height={14} />
          </button>
        </div>
      </div>
    </header>
  );
}
