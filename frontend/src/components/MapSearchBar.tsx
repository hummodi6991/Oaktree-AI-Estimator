import { useEffect, useMemo, useRef, useState } from "react";
import type { MutableRefObject, SyntheticEvent } from "react";
import type maplibregl from "maplibre-gl";
import { apiUrl } from "../lib/api";
import type { SearchItem, SearchResponse } from "../types/search";
import "./MapSearchBar.css";

type MapSearchBarProps = {
  mapRef: MutableRefObject<maplibregl.Map | null>;
};

const MIN_QUERY_LENGTH = 2;
const DEBOUNCE_MS = 250;
const MAX_RESULTS = 12;

const boundsToViewportParam = (bounds: maplibregl.LngLatBounds) => {
  return [bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth()].join(",");
};

const buildSearchParams = (query: string, viewport: string | null) => {
  const params = new URLSearchParams({ q: query, limit: String(MAX_RESULTS) });
  if (viewport) {
    params.set("viewport_bbox", viewport);
  }
  return params;
};

const fetchSearch = async (query: string, viewport: string | null, signal: AbortSignal) => {
  const params = buildSearchParams(query, viewport);
  const res = await fetch(apiUrl(`/v1/search?${params.toString()}`), { signal });
  if (!res.ok) {
    throw new Error(`search_failed_${res.status}`);
  }
  const data = (await res.json()) as SearchResponse;
  return Array.isArray(data.items) ? data.items : [];
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
    if (!canSearch) return "";
    if (loading) return "Searching…";
    if (error) return "Search unavailable";
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
    const viewport = mapRef.current ? boundsToViewportParam(mapRef.current.getBounds()) : null;

    setLoading(true);
    setError(null);

    const handle = window.setTimeout(async () => {
      try {
        const results = await fetchSearch(trimmed, viewport, controller.signal);
        if (requestIdRef.current !== requestId) return;
        setItems(results);
        setOpen(true);
      } catch (err) {
        if (controller.signal.aborted) return;
        if (requestIdRef.current !== requestId) return;
        setError("Search unavailable");
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
        { padding: 40, duration: 600, maxZoom: safeMax },
      );
    } else {
      const desiredZoom = 16;
      map.flyTo({ center: item.center, zoom: Math.min(desiredZoom, safeMax), duration: 600 });
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
    setOpen(canSearch);
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
