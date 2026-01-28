import { useEffect, useMemo, useRef, useState } from "react";
import type { MutableRefObject, SyntheticEvent } from "react";
import type maplibregl from "maplibre-gl";
import "./MapSearchBar.css";

export type SearchItem = {
  type: string;
  id: string;
  label: string;
  subtitle?: string | null;
  center: [number, number];
  bbox?: [number, number, number, number] | null;
};

type SearchResponse = { items: SearchItem[] };

type MapSearchBarProps = {
  mapRef: MutableRefObject<maplibregl.Map | null>;
};

const MIN_QUERY_LENGTH = 2;
const DEBOUNCE_MS = 250;
const MAX_RESULTS = 12;

const base = (import.meta.env.VITE_API_BASE_URL ?? "").trim();
const normalizedBase = base.replace(/\/+$/, "");

const buildSearchUrl = (query: string) => {
  const params = new URLSearchParams({ q: query, limit: String(MAX_RESULTS) });
  const path = `/v1/search?${params.toString()}`;
  return normalizedBase ? `${normalizedBase}${path}` : path;
};

const getSafeMaxZoom = (map: maplibregl.Map) => {
  const maxZ = map.getMaxZoom?.() ?? 18;
  return Math.min(maxZ, 18);
};

export default function MapSearchBar({ mapRef }: MapSearchBarProps) {
  const [query, setQuery] = useState("");
  const [items, setItems] = useState<SearchItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const requestIdRef = useRef(0);
  const blurTimeoutRef = useRef<number | null>(null);

  const trimmed = query.trim();
  const canSearch = trimmed.length >= MIN_QUERY_LENGTH;

  const resultsLabel = useMemo(() => {
    if (loading) return "Searching…";
    if (!canSearch) return "Type a parcel or street";
    if (error) return error;
    if (!items.length) return "No matches";
    return "";
  }, [canSearch, error, items.length, loading]);

  useEffect(() => {
    if (!canSearch) {
      setItems([]);
      setOpen(false);
      setError(null);
      return;
    }

    const controller = new AbortController();
    const requestId = (requestIdRef.current += 1);
    setLoading(true);
    setError(null);

    const handle = window.setTimeout(async () => {
      try {
        const res = await fetch(buildSearchUrl(trimmed), {
          signal: controller.signal,
        });
        if (!res.ok) {
          const text = await res.text().catch(() => "");
          throw new Error(text || `${res.status} ${res.statusText}`);
        }
        const data = (await res.json()) as SearchResponse;
        if (requestIdRef.current !== requestId) return;
        setItems(data.items || []);
        setOpen(true);
      } catch (err) {
        if (controller.signal.aborted) return;
        if (requestIdRef.current !== requestId) return;
        setError(String((err as Error)?.message || "Search failed"));
        setItems([]);
        setOpen(true);
      } finally {
        if (requestIdRef.current !== requestId) return;
        setLoading(false);
      }
    }, DEBOUNCE_MS);

    return () => {
      controller.abort();
      clearTimeout(handle);
    };
  }, [canSearch, trimmed]);

  useEffect(() => {
    return () => {
      if (blurTimeoutRef.current) {
        window.clearTimeout(blurTimeoutRef.current);
      }
    };
  }, []);

  const focusItem = (item: SearchItem) => {
    const map = mapRef.current;
    if (!map) return;
    const safeMax = getSafeMaxZoom(map);

    if (item.bbox && item.bbox.length === 4) {
      map.fitBounds(
        [
          [item.bbox[0], item.bbox[1]],
          [item.bbox[2], item.bbox[3]],
        ],
        { padding: 80, duration: 800, maxZoom: safeMax },
      );
    } else {
      const desiredZoom = 16;
      map.flyTo({ center: item.center, zoom: Math.min(desiredZoom, safeMax), duration: 800 });
    }
  };

  const handleSelect = (item: SearchItem) => {
    setQuery(item.label);
    setOpen(false);
    focusItem(item);
  };

  const handleClear = () => {
    setQuery("");
    setItems([]);
    setOpen(false);
    setError(null);
    setLoading(false);
  };

  const stopPropagation = (event: SyntheticEvent) => {
    event.stopPropagation();
  };

  const handleClearMouseDown = (event: SyntheticEvent) => {
    event.preventDefault();
    event.stopPropagation();
  };

  const handleBlur = () => {
    if (blurTimeoutRef.current) window.clearTimeout(blurTimeoutRef.current);
    blurTimeoutRef.current = window.setTimeout(() => setOpen(false), 150);
  };

  const handleFocus = () => {
    if (blurTimeoutRef.current) window.clearTimeout(blurTimeoutRef.current);
    setOpen(true);
  };

  return (
    <div
      className="map-search-bar"
      onMouseDown={stopPropagation}
      onPointerDown={stopPropagation}
      onClick={stopPropagation}
      onDoubleClick={stopPropagation}
      onTouchStart={stopPropagation}
    >
      <div className="map-search-bar__input-wrapper" onFocus={handleFocus} onBlur={handleBlur}>
        <input
          type="search"
          placeholder="Search parcels, streets, districts"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          className="map-search-bar__input"
          aria-label="Map search"
        />
        {query.length > 0 && (
          <button
            type="button"
            className="map-search-bar__clear"
            aria-label="Clear search"
            onMouseDown={handleClearMouseDown}
            onClick={handleClear}
          >
            ×
          </button>
        )}
        {loading && <span className="map-search-bar__spinner" aria-hidden="true" />}
      </div>
      {open && (items.length > 0 || resultsLabel) && (
        <div className="map-search-bar__results">
          {resultsLabel && <div className="map-search-bar__status">{resultsLabel}</div>}
          {items.map((item) => (
            <button
              type="button"
              key={`${item.type}-${item.id}`}
              className="map-search-bar__result"
              onClick={() => handleSelect(item)}
            >
              <span className="map-search-bar__result-label">{item.label}</span>
              {item.subtitle && <span className="map-search-bar__result-subtitle">{item.subtitle}</span>}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
