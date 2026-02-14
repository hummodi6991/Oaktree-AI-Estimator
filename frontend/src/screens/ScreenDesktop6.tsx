import React, { useMemo, useState } from "react";
import type { Geometry } from "geojson";
import { apiIdentify, apiCreateEstimate, estimatePdfUrl } from "../lib/api";
import { useMapLibre } from "../lib/map/useMapLibre";
import Desktop6Figma from "../ui/figma/Desktop6Figma";

/**
 * End-to-end wiring for the pixel-accurate Figma Desktop-6 UI.
 * We render the Figma component and inject behavior through callbacks/props.
 */
export function ScreenDesktop6() {
  const [selectedGeom, setSelectedGeom] = useState<Geometry | null>(null);
  const [parcelLabel, setParcelLabel] = useState<string>(
    "Parcel: — | Area: — | Land-use: — | Method: —"
  );
  const [estimateId, setEstimateId] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  // Mount MapLibre under the exact Figma map rectangle.
  useMapLibre({
    containerId: "oaktree-map-underlay",
    onClick: async (lng, lat) => {
      setErr(null);
      setEstimateId(null);
      try {
        const res = await apiIdentify(lng, lat);
        if (!res?.found || !res?.parcel?.geometry) {
          setSelectedGeom(null);
          setParcelLabel("Parcel: — | Area: — | Land-use: — | Method: —");
          return;
        }
        setSelectedGeom(res.parcel.geometry as Geometry);
        const p = res.parcel;
        setParcelLabel(
          `Parcel: ${p.parcel_id ?? "—"} | Area: ${p.area_m2 ? Number(p.area_m2).toLocaleString() : "—"} m² | Land-use: ${p.landuse_code ?? "—"} | Method: ${p.landuse_method ?? "—"}`
        );
      } catch (e: any) {
        setErr(e?.message || "Identify failed");
      }
    },
    selectedGeometry: selectedGeom
  });

  const exportPdfHref = useMemo(() => (estimateId ? estimatePdfUrl(estimateId) : null), [estimateId]);

  async function onCalculateEstimate() {
    setErr(null);
    if (!selectedGeom) {
      setErr("Select a parcel first (click on the map).");
      return;
    }
    setBusy(true);
    try {
      const res = await apiCreateEstimate(selectedGeom);
      // Backend returns an estimate header with id
      setEstimateId(res?.id || res?.estimate_id || null);
    } catch (e: any) {
      setErr(e?.message || "Estimate failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <Desktop6Figma
      parcelLabel={parcelLabel}
      onCalculateEstimate={onCalculateEstimate}
      exportPdfHref={exportPdfHref}
      busy={busy}
      error={err}
      mapUnderlayId="oaktree-map-underlay"
    />
  );
}
