import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { DistrictOption } from "../../lib/api/expansionAdvisor";

type Props = {
  options: DistrictOption[];
  selected: string[];
  onChange: (selected: string[]) => void;
  disabled?: boolean;
  placeholder?: string;
  /** Values in other fields that should be visually flagged if overlapping */
  conflictValues?: string[];
};

/**
 * Searchable multi-select dropdown for Riyadh districts.
 * Selected items render as removable chips.
 * Unknown legacy values (from saved searches) are rendered as fallback chips.
 */
export default function DistrictMultiSelect({
  options,
  selected,
  onChange,
  disabled = false,
  placeholder = "",
  conflictValues,
}: Props) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Build lookup maps once
  const optionsByValue = useMemo(() => {
    const map = new Map<string, DistrictOption>();
    for (const opt of options) {
      map.set(opt.value, opt);
    }
    return map;
  }, [options]);

  // Build alias lookup: alias-normalized → option value
  const aliasMap = useMemo(() => {
    const map = new Map<string, string>();
    for (const opt of options) {
      // Map label_ar and label_en as aliases too
      map.set(opt.label_ar.trim().toLowerCase(), opt.value);
      if (opt.label_en) map.set(opt.label_en.trim().toLowerCase(), opt.value);
      for (const alias of opt.aliases) {
        map.set(alias.trim().toLowerCase(), opt.value);
      }
    }
    return map;
  }, [options]);

  const conflictSet = useMemo(
    () => new Set(conflictValues || []),
    [conflictValues],
  );

  // Filter options based on search query
  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return options;
    return options.filter((opt) => {
      if (opt.label_ar.toLowerCase().includes(q)) return true;
      if (opt.label_en && opt.label_en.toLowerCase().includes(q)) return true;
      if (opt.value.toLowerCase().includes(q)) return true;
      return opt.aliases.some((a) => a.toLowerCase().includes(q));
    });
  }, [options, query]);

  // Close dropdown on outside click
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const toggleOption = useCallback(
    (value: string) => {
      if (selected.includes(value)) {
        onChange(selected.filter((v) => v !== value));
      } else {
        onChange([...selected, value]);
      }
      setQuery("");
      inputRef.current?.focus();
    },
    [selected, onChange],
  );

  const removeChip = useCallback(
    (value: string) => {
      onChange(selected.filter((v) => v !== value));
    },
    [selected, onChange],
  );

  // Resolve display label for a selected value (handles legacy/unknown values)
  const chipLabel = useCallback(
    (value: string): { label: string; fallback: boolean } => {
      const opt = optionsByValue.get(value);
      if (opt) return { label: opt.label_ar, fallback: false };
      // Try alias lookup
      const resolved = aliasMap.get(value.trim().toLowerCase());
      if (resolved) {
        const resolvedOpt = optionsByValue.get(resolved);
        if (resolvedOpt) return { label: resolvedOpt.label_ar, fallback: false };
      }
      // Fallback: render the raw value as-is
      return { label: value, fallback: true };
    },
    [optionsByValue, aliasMap],
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Backspace" && !query && selected.length > 0) {
        onChange(selected.slice(0, -1));
      }
      if (e.key === "Escape") {
        setOpen(false);
        inputRef.current?.blur();
      }
    },
    [query, selected, onChange],
  );

  const selectedSet = useMemo(() => new Set(selected), [selected]);

  return (
    <div
      ref={containerRef}
      className={`ea-district-ms${disabled ? " ea-district-ms--disabled" : ""}`}
    >
      <div
        className="ea-district-ms__control"
        onClick={() => {
          if (!disabled) {
            setOpen(true);
            inputRef.current?.focus();
          }
        }}
      >
        {selected.map((val) => {
          const { label, fallback } = chipLabel(val);
          const isConflict = conflictSet.has(val);
          return (
            <span
              key={val}
              className={`ea-district-ms__chip${fallback ? " ea-district-ms__chip--fallback" : ""}${isConflict ? " ea-district-ms__chip--conflict" : ""}`}
              title={fallback ? `Unknown district: ${val}` : isConflict ? "Also selected in another field" : val}
            >
              {label}
              {!disabled && (
                <button
                  type="button"
                  className="ea-district-ms__chip-remove"
                  onClick={(e) => {
                    e.stopPropagation();
                    removeChip(val);
                  }}
                  tabIndex={-1}
                  aria-label={`Remove ${label}`}
                >
                  ×
                </button>
              )}
            </span>
          );
        })}
        <input
          ref={inputRef}
          className="ea-district-ms__input"
          type="text"
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            if (!open) setOpen(true);
          }}
          onFocus={() => setOpen(true)}
          onKeyDown={handleKeyDown}
          disabled={disabled}
          placeholder={selected.length === 0 ? placeholder : ""}
          autoComplete="off"
          role="combobox"
          aria-expanded={open}
        />
        <span className="ea-district-ms__chevron" aria-hidden="true">
          {open ? "▲" : "▼"}
        </span>
      </div>
      {open && !disabled && (
        <ul className="ea-district-ms__dropdown" role="listbox">
          {filtered.length === 0 && (
            <li className="ea-district-ms__no-results">No matching districts</li>
          )}
          {filtered.map((opt) => {
            const isSelected = selectedSet.has(opt.value);
            return (
              <li
                key={opt.value}
                className={`ea-district-ms__option${isSelected ? " ea-district-ms__option--selected" : ""}`}
                onClick={() => toggleOption(opt.value)}
                role="option"
                aria-selected={isSelected}
              >
                <span className="ea-district-ms__option-label">{opt.label_ar}</span>
                {opt.label_en && (
                  <span className="ea-district-ms__option-en">{opt.label_en}</span>
                )}
                {isSelected && <span className="ea-district-ms__check">✓</span>}
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}
