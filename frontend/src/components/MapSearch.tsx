import { useEffect, useMemo, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import { buildApiUrl, fetchWithAuth } from "../api";

export type SearchItem = {
  type: string;
  id: string;
  label: string;
  subtitle?: string | null;
  center: [number, number];
  bbox?: [number, number, number, number] | null;
};

type SearchResponse = { items: SearchItem[] };

type MapSearchProps = {
  mapRef: React.MutableRefObject<maplibregl.Map | null>;
};

const MIN_QUERY_LENGTH = 2;
const DEBOUNCE_MS = 250;
const MAX_RESULTS = 12;

export default function MapSearch({ mapRef }: MapSearchProps) {
  const [query, setQuery] = useState("");
  const [items, setItems] = useState<SearchItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const markerRef = useRef<maplibregl.Marker | null>(null);
  const requestIdRef = useRef(0);

  const trimmed = query.trim();
  const canSearch = trimmed.length >= MIN_QUERY_LENGTH;

  const resultsLabel = useMemo(() => {
    if (loading) return "Searching…";
    if (!canSearch) return "Type a street or POI";
    if (error) return error;
    if (!items.length) return "No matches";
    return "";
  }, [canSearch, error, items.length, loading]);

  useEffect(() => {
    if (!canSearch) {
      setItems([]);
      setOpen(false);
      setError(null);
      if (markerRef.current) {
        markerRef.current.remove();
        markerRef.current = null;
      }
      return;
    }

    const controller = new AbortController();
    const requestId = (requestIdRef.current += 1);

    setLoading(true);
    setError(null);

    const handle = window.setTimeout(() => {
      const params = new URLSearchParams({ q: trimmed, limit: String(MAX_RESULTS) });
      fetchWithAuth(buildApiUrl(`/v1/search?${params.toString()}`), { signal: controller.signal })
        .then((res) => res.json() as Promise<SearchResponse>)
        .then((data) => {
          if (requestIdRef.current !== requestId) return;
          setItems(data.items || []);
          setOpen(true);
        })
        .catch((err) => {
          if (controller.signal.aborted) return;
          setError(String(err?.message || "Search failed"));
          setItems([]);
          setOpen(true);
        })
        .finally(() => {
          if (requestIdRef.current !== requestId) return;
          setLoading(false);
        });
    }, DEBOUNCE_MS);

    return () => {
      controller.abort();
      clearTimeout(handle);
    };
  }, [canSearch, trimmed]);

  useEffect(() => {
    return () => {
      if (markerRef.current) {
        markerRef.current.remove();
        markerRef.current = null;
      }
    };
  }, []);

  const focusItem = (item: SearchItem) => {
    const map = mapRef.current;
    if (!map) return;

    const marker = markerRef.current;
    if (marker) {
      marker.setLngLat(item.center);
    } else {
      const el = document.createElement("div");
      el.className = "map-search-marker";
      markerRef.current = new maplibregl.Marker({ element: el }).setLngLat(item.center).addTo(map);
    }

    if (item.bbox && item.bbox.length === 4) {
      const [[minLng, minLat], [maxLng, maxLat]] = [
        [item.bbox[0], item.bbox[1]],
        [item.bbox[2], item.bbox[3]],
      ];
      map.fitBounds(
        [
          [minLng, minLat],
          [maxLng, maxLat],
        ],
        { padding: 80, maxZoom: 17, duration: 800 },
      );
    } else {
      map.flyTo({ center: item.center, zoom: 16, duration: 800 });
    }
  };

  const handleSelect = (item: SearchItem) => {
    setQuery(item.label);
    setOpen(false);
    focusItem(item);
  };

  return (
    <div className="map-search" onFocus={() => setOpen(true)}>
      <div className="map-search__input-wrapper">
        <input
          type="search"
          placeholder="Search Riyadh streets, districts, POIs"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          className="map-search__input"
          aria-label="Map search"
        />
        {loading && <span className="map-search__spinner" aria-hidden="true" />}
      </div>
      {open && (items.length > 0 || resultsLabel) && (
        <div className="map-search__results">
          {resultsLabel && <div className="map-search__status">{resultsLabel}</div>}
          {items.map((item) => (
            <button
              type="button"
              key={`${item.type}-${item.id}`}
              className="map-search__result"
              onClick={() => handleSelect(item)}
            >
              <span className="map-search__result-label">{item.label}</span>
              {item.subtitle && <span className="map-search__result-subtitle">{item.subtitle}</span>}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
