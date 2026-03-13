import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  CATEGORY_GROUPS,
  CATEGORY_OPTIONS,
  findCategoryOption,
  type CategoryOption,
} from "./categoryOptions";

type Props = {
  value: string;
  onChange: (value: string) => void;
  disabled?: boolean;
  placeholder?: string;
};

/**
 * Searchable single-select combobox for F&B categories.
 *
 * Follows the same visual language and interaction patterns as
 * DistrictMultiSelect but for a single string value.
 *
 * - Supports English and Arabic search (labels + aliases).
 * - Grouped browsing when query is empty.
 * - Unknown legacy values render as a fallback pill.
 * - On selection the normalised `value` string is emitted.
 */
export default function CategorySelect({
  value,
  onChange,
  disabled = false,
  placeholder = "Select a restaurant category",
}: Props) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // ── Lookup helpers ──
  const optionsByValue = useMemo(() => {
    const map = new Map<string, CategoryOption>();
    for (const opt of CATEGORY_OPTIONS) map.set(opt.value, opt);
    return map;
  }, []);

  // Resolve the currently selected option (may be null for legacy values)
  const selectedOption = useMemo(
    () => (value ? findCategoryOption(value) : undefined),
    [value],
  );

  // Is the current value a legacy / unrecognised string?
  const isFallback = Boolean(value) && !selectedOption;

  // ── Filtering ──
  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return CATEGORY_OPTIONS;
    return CATEGORY_OPTIONS.filter((opt) => {
      if (opt.label.toLowerCase().includes(q)) return true;
      if (opt.label_ar.includes(q)) return true;
      if (opt.value.includes(q)) return true;
      return opt.aliases.some((a) => a.toLowerCase().includes(q));
    });
  }, [query]);

  // Group filtered results for display
  const groupedFiltered = useMemo(() => {
    const isSearching = query.trim().length > 0;
    if (isSearching) {
      // Flat list when searching
      return [{ key: "__search__", label: "", label_ar: "", items: filtered }];
    }
    // Grouped when browsing
    return CATEGORY_GROUPS.map((g) => ({
      ...g,
      items: filtered.filter((opt) => opt.group === g.key),
    })).filter((g) => g.items.length > 0);
  }, [filtered, query]);

  // ── Click outside to close ──
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false);
        setQuery("");
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  // ── Handlers ──
  const selectOption = useCallback(
    (opt: CategoryOption) => {
      onChange(opt.value);
      setQuery("");
      setOpen(false);
    },
    [onChange],
  );

  const clearSelection = useCallback(() => {
    onChange("");
    setQuery("");
    inputRef.current?.focus();
  }, [onChange]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Escape") {
        setOpen(false);
        setQuery("");
        inputRef.current?.blur();
      }
      if (e.key === "Backspace" && !query && value) {
        clearSelection();
      }
    },
    [query, value, clearSelection],
  );

  // Display label for the selected value
  const displayLabel = selectedOption
    ? selectedOption.label
    : isFallback
      ? value
      : "";

  return (
    <div
      ref={containerRef}
      className={`ea-category-select${disabled ? " ea-category-select--disabled" : ""}`}
    >
      <div
        className="ea-category-select__control"
        onClick={() => {
          if (!disabled) {
            setOpen(true);
            inputRef.current?.focus();
          }
        }}
      >
        {/* Selected value pill */}
        {value && !open && (
          <span
            className={`ea-category-select__pill${isFallback ? " ea-category-select__pill--fallback" : ""}`}
            title={isFallback ? `Legacy category: ${value}` : value}
          >
            {displayLabel}
            {selectedOption && (
              <span className="ea-category-select__pill-ar">
                {selectedOption.label_ar}
              </span>
            )}
            {!disabled && (
              <button
                type="button"
                className="ea-category-select__pill-remove"
                onClick={(e) => {
                  e.stopPropagation();
                  clearSelection();
                }}
                tabIndex={-1}
                aria-label={`Clear ${displayLabel}`}
              >
                ×
              </button>
            )}
          </span>
        )}

        {/* Search input – visible when open or nothing selected */}
        <input
          ref={inputRef}
          className="ea-category-select__input"
          type="text"
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            if (!open) setOpen(true);
          }}
          onFocus={() => setOpen(true)}
          onKeyDown={handleKeyDown}
          disabled={disabled}
          placeholder={value && !open ? "" : placeholder}
          autoComplete="off"
          role="combobox"
          aria-expanded={open}
          style={value && !open ? { width: 0, minWidth: 0, padding: 0 } : undefined}
        />

        {/* Chevron indicator */}
        <span className="ea-category-select__chevron" aria-hidden="true">
          {open ? "▲" : "▼"}
        </span>
      </div>

      {/* Helper text */}
      {!value && !open && (
        <span className="ea-category-select__helper">
          Choose the closest match for better search quality
        </span>
      )}

      {/* Dropdown */}
      {open && !disabled && (
        <ul className="ea-category-select__dropdown" role="listbox">
          {groupedFiltered.length === 0 && (
            <li className="ea-category-select__no-results">
              No matching categories
            </li>
          )}
          {groupedFiltered.map((group) => (
            <li key={group.key} className="ea-category-select__group">
              {group.label && (
                <div className="ea-category-select__group-header">
                  {group.label}
                  <span className="ea-category-select__group-header-ar">
                    {group.label_ar}
                  </span>
                </div>
              )}
              <ul className="ea-category-select__group-list">
                {group.items.map((opt) => {
                  const isSelected = opt.value === value;
                  return (
                    <li
                      key={opt.value}
                      className={`ea-category-select__option${isSelected ? " ea-category-select__option--selected" : ""}`}
                      onClick={() => selectOption(opt)}
                      role="option"
                      aria-selected={isSelected}
                    >
                      <span className="ea-category-select__option-label">
                        {opt.label}
                      </span>
                      <span className="ea-category-select__option-ar">
                        {opt.label_ar}
                      </span>
                      {isSelected && (
                        <span className="ea-category-select__check">✓</span>
                      )}
                    </li>
                  );
                })}
              </ul>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
