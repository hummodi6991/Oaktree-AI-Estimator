import type { SearchItem } from "../types/search";
import SearchBar from "./SearchBar";

type HeaderBarProps = {
  onSearchSelect: (item: SearchItem) => void;
};

export default function HeaderBar({ onSearchSelect }: HeaderBarProps) {
  return (
    <header className="ui-v2-header">
      <div className="ui-v2-header__brand">
        <h1>Oaktree Estimator</h1>
        <p>Riyadh Commercial Development</p>
      </div>
      <div className="ui-v2-header__search">
        <SearchBar onSelect={onSearchSelect} />
      </div>
      <div className="ui-v2-header__actions">
        <button type="button" className="ui-v2-header__language">
          EN
        </button>
        <button type="button" className="ui-v2-header__user">
          <span className="ui-v2-header__avatar" aria-hidden="true">
            OT
          </span>
          <span>Oaktree Team ▾</span>
        </button>
      </div>
    </header>
  );
}
