import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import type { BranchSuggestion } from "../../lib/api/expansionAdvisor";
import { searchBranchSuggestions } from "../../lib/api/expansionAdvisor";
import { isGarbledText } from "./formatHelpers";

export type BranchEntry = {
  name?: string;
  lat: number;
  lon: number;
  district?: string;
};

type Props = {
  branches: BranchEntry[];
  onChange: (branches: BranchEntry[]) => void;
  disabled?: boolean;
};

/** Debounce helper */
function useDebounce(value: string, ms: number) {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const t = setTimeout(() => setDebounced(value), ms);
    return () => clearTimeout(t);
  }, [value, ms]);
  return debounced;
}

/** Format coords as compact secondary text */
function formatCoords(lat: number, lon: number): string {
  return `${lat.toFixed(6)}, ${lon.toFixed(6)}`;
}

/* ─── Autocomplete Search Input ─── */
function BranchSearchInput({
  onSelect,
  disabled,
}: {
  onSelect: (suggestion: BranchSuggestion) => void;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const [query, setQuery] = useState("");
  const [suggestions, setSuggestions] = useState<BranchSuggestion[]>([]);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const debouncedQuery = useDebounce(query, 250);

  useEffect(() => {
    if (!debouncedQuery || debouncedQuery.trim().length < 2) {
      setSuggestions([]);
      setOpen(false);
      return;
    }
    let cancelled = false;
    setLoading(true);
    searchBranchSuggestions(debouncedQuery)
      .then((items) => {
        if (!cancelled) {
          setSuggestions(items);
          setOpen(items.length > 0);
          setActiveIndex(-1);
        }
      })
      .catch(() => {
        if (!cancelled) setSuggestions([]);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [debouncedQuery]);

  // Close dropdown on outside click
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (wrapperRef.current && !wrapperRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const handleSelect = useCallback(
    (s: BranchSuggestion) => {
      onSelect(s);
      setQuery("");
      setSuggestions([]);
      setOpen(false);
      inputRef.current?.focus();
    },
    [onSelect],
  );

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (!open || suggestions.length === 0) return;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActiveIndex((prev) => Math.min(prev + 1, suggestions.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActiveIndex((prev) => Math.max(prev - 1, 0));
    } else if (e.key === "Enter" && activeIndex >= 0) {
      e.preventDefault();
      handleSelect(suggestions[activeIndex]);
    } else if (e.key === "Escape") {
      setOpen(false);
    }
  };

  return (
    <div className="ea-branch-search" ref={wrapperRef}>
      <div className="ea-branch-search__input-wrap">
        <span className="ea-branch-search__search-icon" aria-hidden="true">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>
        </span>
        <input
          ref={inputRef}
          className="ea-form__input ea-branch-search__input"
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onFocus={() => {
            if (suggestions.length > 0) setOpen(true);
          }}
          onKeyDown={handleKeyDown}
          disabled={disabled}
          placeholder={t("expansionAdvisor.branchSearchPlaceholder", "Search branch name, address, or district…")}
          autoComplete="off"
          role="combobox"
          aria-expanded={open}
          aria-autocomplete="list"
          aria-controls="ea-branch-suggestions"
        />
        {loading && <span className="ea-branch-search__spinner" aria-hidden="true" />}
      </div>
      {open && suggestions.length > 0 && (
        <ul id="ea-branch-suggestions" className="ea-branch-search__dropdown" role="listbox">
          {suggestions.map((s, idx) => (
            <li
              key={s.id}
              className={`ea-branch-search__option${idx === activeIndex ? " ea-branch-search__option--active" : ""}`}
              role="option"
              aria-selected={idx === activeIndex}
              onMouseDown={() => handleSelect(s)}
              onMouseEnter={() => setActiveIndex(idx)}
            >
              <span className="ea-branch-search__option-name">{s.name}</span>
              <span className="ea-branch-search__option-meta">
                {s.district && !isGarbledText(s.district) ? `${s.district} · ` : ""}
                {formatCoords(s.lat, s.lon)}
              </span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

/* ─── Branch Card (compact, polished) ─── */
function BranchCard({
  branch,
  index,
  onRemove,
  onUpdate,
  disabled,
}: {
  branch: BranchEntry;
  index: number;
  onRemove: () => void;
  onUpdate: (updated: BranchEntry) => void;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const [editing, setEditing] = useState(false);
  const [local, setLocal] = useState(branch);

  useEffect(() => setLocal(branch), [branch]);

  const displayName = branch.name || t("expansionAdvisor.unnamedBranch", `Branch ${index + 1}`);
  const subtitle = [branch.district, formatCoords(branch.lat, branch.lon)].filter(Boolean).join(" · ");

  if (editing) {
    return (
      <div className="ea-branch-card ea-branch-card--editing">
        <div className="ea-branch-card__fields">
          <input
            className="ea-form__input"
            placeholder={t("expansionAdvisor.branchName")}
            value={local.name || ""}
            onChange={(e) => setLocal({ ...local, name: e.target.value })}
            disabled={disabled}
          />
          <input
            className="ea-form__input"
            placeholder={t("expansionAdvisor.branchDistrict")}
            value={local.district || ""}
            onChange={(e) => setLocal({ ...local, district: e.target.value })}
            disabled={disabled}
          />
          <div className="ea-branch-card__coord-row">
            <input
              className="ea-form__input"
              type="number"
              step="any"
              placeholder={t("expansionAdvisor.branchLat")}
              value={local.lat ?? ""}
              onChange={(e) => setLocal({ ...local, lat: Number(e.target.value) })}
              disabled={disabled}
            />
            <input
              className="ea-form__input"
              type="number"
              step="any"
              placeholder={t("expansionAdvisor.branchLon")}
              value={local.lon ?? ""}
              onChange={(e) => setLocal({ ...local, lon: Number(e.target.value) })}
              disabled={disabled}
            />
          </div>
        </div>
        <div className="ea-branch-card__actions">
          <button
            type="button"
            className="oak-btn oak-btn--sm oak-btn--primary"
            onClick={() => {
              onUpdate(local);
              setEditing(false);
            }}
            disabled={disabled}
          >
            {t("common.done", "Done")}
          </button>
          <button
            type="button"
            className="oak-btn oak-btn--sm oak-btn--tertiary"
            onClick={() => {
              setLocal(branch);
              setEditing(false);
            }}
            disabled={disabled}
          >
            {t("common.cancel", "Cancel")}
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="ea-branch-card">
      <div className="ea-branch-card__info">
        <span className="ea-branch-card__name">{displayName}</span>
        <span className="ea-branch-card__subtitle">{subtitle}</span>
      </div>
      <div className="ea-branch-card__actions">
        <button
          type="button"
          className="oak-btn oak-btn--sm oak-btn--tertiary"
          onClick={() => setEditing(true)}
          disabled={disabled}
          title={t("common.edit", "Edit")}
        >
          {t("common.edit", "Edit")}
        </button>
        <button
          type="button"
          className="oak-btn oak-btn--sm oak-btn--tertiary ea-branch-card__remove-btn"
          onClick={onRemove}
          disabled={disabled}
          title={t("expansionAdvisor.removeBranch")}
        >
          ×
        </button>
      </div>
    </div>
  );
}

/* ─── Manual Coordinate Entry (collapsible) ─── */
function ManualCoordEntry({
  onAdd,
  disabled,
}: {
  onAdd: (branch: BranchEntry) => void;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState(false);
  const [name, setName] = useState("");
  const [district, setDistrict] = useState("");
  const [lat, setLat] = useState("");
  const [lon, setLon] = useState("");

  const canAdd = lat !== "" && lon !== "";

  const handleAdd = () => {
    if (!canAdd) return;
    onAdd({
      name: name.trim() || undefined,
      lat: Number(lat),
      lon: Number(lon),
      district: district.trim() || undefined,
    });
    setName("");
    setDistrict("");
    setLat("");
    setLon("");
  };

  return (
    <div className="ea-branch-manual">
      <button
        type="button"
        className="ea-branch-manual__toggle"
        onClick={() => setExpanded(!expanded)}
        disabled={disabled}
      >
        <span className={`ea-branch-manual__chevron${expanded ? " ea-branch-manual__chevron--open" : ""}`}>›</span>
        {t("expansionAdvisor.enterManualCoords", "Enter coordinates manually")}
      </button>
      {expanded && (
        <div className="ea-branch-manual__form">
          <div className="ea-branch-manual__row">
            <input
              className="ea-form__input"
              placeholder={t("expansionAdvisor.branchName")}
              value={name}
              onChange={(e) => setName(e.target.value)}
              disabled={disabled}
            />
            <input
              className="ea-form__input"
              placeholder={t("expansionAdvisor.branchDistrict")}
              value={district}
              onChange={(e) => setDistrict(e.target.value)}
              disabled={disabled}
            />
          </div>
          <div className="ea-branch-manual__row">
            <input
              className="ea-form__input"
              type="number"
              step="any"
              placeholder={t("expansionAdvisor.branchLat")}
              value={lat}
              onChange={(e) => setLat(e.target.value)}
              disabled={disabled}
            />
            <input
              className="ea-form__input"
              type="number"
              step="any"
              placeholder={t("expansionAdvisor.branchLon")}
              value={lon}
              onChange={(e) => setLon(e.target.value)}
              disabled={disabled}
            />
            <button
              type="button"
              className="oak-btn oak-btn--sm oak-btn--primary"
              onClick={handleAdd}
              disabled={disabled || !canAdd}
            >
              {t("expansionAdvisor.addBranch")}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

/* ─── Main BranchLocationPicker ─── */
export default function BranchLocationPicker({ branches, onChange, disabled }: Props) {
  const { t } = useTranslation();

  const handleSuggestionSelect = useCallback(
    (s: BranchSuggestion) => {
      const newBranch: BranchEntry = {
        name: s.name,
        lat: s.lat,
        lon: s.lon,
        district: s.district || undefined,
      };
      onChange([...branches, newBranch]);
    },
    [branches, onChange],
  );

  const handleManualAdd = useCallback(
    (entry: BranchEntry) => {
      onChange([...branches, entry]);
    },
    [branches, onChange],
  );

  const handleRemove = useCallback(
    (index: number) => {
      onChange(branches.filter((_, i) => i !== index));
    },
    [branches, onChange],
  );

  const handleUpdate = useCallback(
    (index: number, updated: BranchEntry) => {
      const next = [...branches];
      next[index] = updated;
      onChange(next);
    },
    [branches, onChange],
  );

  return (
    <div className="ea-branch-picker">
      {/* Search autocomplete */}
      <BranchSearchInput onSelect={handleSuggestionSelect} disabled={disabled} />

      {/* Branch cards */}
      {branches.length > 0 && (
        <div className="ea-branch-picker__list">
          {branches.map((branch, index) => (
            <BranchCard
              key={`branch-${index}-${branch.lat}-${branch.lon}`}
              branch={branch}
              index={index}
              onRemove={() => handleRemove(index)}
              onUpdate={(updated) => handleUpdate(index, updated)}
              disabled={disabled}
            />
          ))}
        </div>
      )}

      {branches.length === 0 && (
        <p className="ea-branch-picker__empty">
          {t("expansionAdvisor.noBranchesYet")}
        </p>
      )}

      {/* Manual fallback */}
      <ManualCoordEntry onAdd={handleManualAdd} disabled={disabled} />
    </div>
  );
}
