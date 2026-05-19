import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { buildApiUrl } from "../api";
import type { SearchItem, SearchResponse } from "../types/search";

type SearchBarProps = {
  onSelect: (item: SearchItem) => void;
};

const DEBOUNCE_MS = 250;
const MIN_QUERY_LENGTH = 2;

export default function SearchBar({ onSelect }: SearchBarProps) {
  const { t } = useTranslation();
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [items, setItems] = useState<SearchItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  const requestIdRef = useRef(0);

  const trimmed = query.trim();

  useEffect(() => {
    if (trimmed.length < MIN_QUERY_LENGTH) {
      setItems([]);
      setOpen(false);
      setLoading(false);
      setError(null);
      return;
    }

    const controller = new AbortController();
    const requestId = (requestIdRef.current += 1);

    setLoading(true);
    setError(null);

    const handle = window.setTimeout(async () => {
      try {
        const params = new URLSearchParams({ q: trimmed, limit: "8" });
        const response = await fetch(buildApiUrl(`/v1/search?${params.toString()}`), {
          signal: controller.signal,
        });
        if (!response.ok) {
          throw new Error(`search_failed_${response.status}`);
        }
        const data = (await response.json()) as SearchResponse;
        if (requestId !== requestIdRef.current) return;
        setItems(Array.isArray(data.items) ? data.items : []);
        setOpen(true);
      } catch (fetchError) {
        if (controller.signal.aborted) return;
        if (requestId !== requestIdRef.current) return;
        setItems([]);
        setOpen(true);
        setError(fetchError instanceof Error ? fetchError.message : t("search.unavailable"));
      } finally {
        if (requestId !== requestIdRef.current) return;
        setLoading(false);
      }
    }, DEBOUNCE_MS);

    return () => {
      controller.abort();
      window.clearTimeout(handle);
    };
  }, [trimmed, t]);

  return (
    <div className="ui-v2-search app-topbar__search" onBlur={() => window.setTimeout(() => setOpen(false), 120)}>
      <input
        className="oak-search"
        type="search"
        value={query}
        onFocus={() => setOpen(items.length > 0 || Boolean(error))}
        onChange={(event) => setQuery(event.target.value)}
        placeholder={t("search.placeholder")}
        aria-label={t("search.placeholder")}
      />
      {loading ? <span className="ui-v2-search__loading">{t("search.loading")}</span> : null}
      {open ? (
        <div className="ui-v2-search__results" role="listbox" aria-label={t("search.resultsAriaLabel")}>
          {error ? <div className="ui-v2-search__status">{t("search.unavailable")}</div> : null}
          {!error && !items.length ? <div className="ui-v2-search__status">{t("search.noMatches")}</div> : null}
          {items.map((item) => (
            <button
              key={`${item.type}-${item.id}`}
              type="button"
              className="ui-v2-search__result"
              onMouseDown={(event) => {
                event.preventDefault();
                onSelect(item);
                setQuery(item.label);
                setOpen(false);
              }}
            >
              <span>{item.label}</span>
              {item.subtitle ? <small>{item.subtitle}</small> : null}
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}
