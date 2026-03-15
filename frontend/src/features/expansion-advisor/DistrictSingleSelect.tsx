import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { DistrictOption } from "../../lib/api/expansionAdvisor";

type Props = {
  options: DistrictOption[];
  value: string;
  onChange: (value: string) => void;
  disabled?: boolean;
  placeholder?: string;
};

/**
 * Compact searchable single-select dropdown for Riyadh districts.
 * Reuses the same DistrictOption data source as DistrictMultiSelect.
 */
export default function DistrictSingleSelect({
  options,
  value,
  onChange,
  disabled = false,
  placeholder = "",
}: Props) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);
  const containerRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Build lookup map once
  const optionsByValue = useMemo(() => {
    const map = new Map<string, DistrictOption>();
    for (const opt of options) map.set(opt.value, opt);
    return map;
  }, [options]);

  // Resolve selected display label
  const selectedLabel = useMemo(() => {
    if (!value) return "";
    const opt = optionsByValue.get(value);
    return opt ? opt.label_ar : value;
  }, [value, optionsByValue]);

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
        setQuery("");
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  // Reset active index when filtered list changes
  useEffect(() => {
    setActiveIndex(-1);
  }, [filtered]);

  const selectOption = useCallback(
    (val: string) => {
      onChange(val);
      setQuery("");
      setOpen(false);
    },
    [onChange],
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (!open) {
        if (e.key === "ArrowDown" || e.key === "Enter") {
          e.preventDefault();
          setOpen(true);
        }
        return;
      }
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setActiveIndex((prev) => Math.min(prev + 1, filtered.length - 1));
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        setActiveIndex((prev) => Math.max(prev - 1, 0));
      } else if (e.key === "Enter" && activeIndex >= 0) {
        e.preventDefault();
        selectOption(filtered[activeIndex].value);
      } else if (e.key === "Escape") {
        setOpen(false);
        setQuery("");
        inputRef.current?.blur();
      }
    },
    [open, filtered, activeIndex, selectOption],
  );

  const handleFocus = () => {
    if (!disabled) setOpen(true);
  };

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setQuery(e.target.value);
    if (!open) setOpen(true);
  };

  const handleClear = (e: React.MouseEvent) => {
    e.stopPropagation();
    onChange("");
    setQuery("");
    inputRef.current?.focus();
  };

  return (
    <div
      ref={containerRef}
      className={`ea-district-ss${disabled ? " ea-district-ss--disabled" : ""}`}
    >
      <div
        className="ea-district-ss__control"
        onClick={() => {
          if (!disabled) {
            setOpen(true);
            inputRef.current?.focus();
          }
        }}
      >
        <input
          ref={inputRef}
          className="ea-district-ss__input"
          type="text"
          value={open ? query : selectedLabel}
          onChange={handleInputChange}
          onFocus={handleFocus}
          onKeyDown={handleKeyDown}
          disabled={disabled}
          placeholder={value ? selectedLabel : placeholder}
          autoComplete="off"
          role="combobox"
          aria-expanded={open}
          aria-autocomplete="list"
          aria-controls="ea-district-ss-options"
        />
        {value && !disabled && (
          <button
            type="button"
            className="ea-district-ss__clear"
            onClick={handleClear}
            tabIndex={-1}
            aria-label="Clear district"
          >
            ×
          </button>
        )}
        <span className="ea-district-ss__chevron" aria-hidden="true">
          {open ? "▲" : "▼"}
        </span>
      </div>
      {open && !disabled && (
        <ul id="ea-district-ss-options" className="ea-district-ss__dropdown" role="listbox">
          {filtered.length === 0 && (
            <li className="ea-district-ss__no-results">No matching districts</li>
          )}
          {filtered.map((opt, idx) => {
            const isSelected = opt.value === value;
            const isActive = idx === activeIndex;
            return (
              <li
                key={opt.value}
                className={`ea-district-ss__option${isSelected ? " ea-district-ss__option--selected" : ""}${isActive ? " ea-district-ss__option--active" : ""}`}
                onMouseDown={() => selectOption(opt.value)}
                onMouseEnter={() => setActiveIndex(idx)}
                role="option"
                aria-selected={isSelected}
              >
                <span className="ea-district-ss__option-label">{opt.label_ar}</span>
                {opt.label_en && (
                  <span className="ea-district-ss__option-en">{opt.label_en}</span>
                )}
                {isSelected && <span className="ea-district-ss__check">✓</span>}
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}
