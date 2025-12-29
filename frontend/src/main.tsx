import React, { useState } from "react";
import { createRoot } from "react-dom/client";

import Map from "./components/Map";
import ExcelForm from "./components/ExcelForm";
import "./index.css";
import type { ParcelSummary } from "./api";

function App() {
  const [parcel, setParcel] = useState<ParcelSummary | null>(null);

  const codeLabel = (() => {
    const selectedLandUse = parcel?.landuse_code?.trim().toLowerCase();
    if (selectedLandUse === "s") return "s — Residential";
    if (selectedLandUse === "m") return "m — Mixed/Commercial";
    return "—";
  })();

  return (
    <>
      <Map
        onParcel={(selectedParcel) => {
          setParcel(selectedParcel);
        }}
      />
      <div style={{ padding: 12 }}>
        <div style={{ display: "flex", flexDirection: "column", gap: 6, color: "var(--sand-200)", marginBottom: 10 }}>
          {!parcel && <em>Click a parcel on the map to begin…</em>}
          <span style={{ fontSize: "0.95rem", color: "rgba(247, 241, 230, 0.85)" }}>
            <strong>Tip:</strong> Some parcel outlines appear only when you zoom in. If parcels look missing, zoom closer to reveal the blue parcel boundaries.
          </span>
        </div>
        {parcel ? (
          <>
            <div>
              <b>Parcel:</b> {parcel.parcel_id} | <b>Area:</b> {parcel.area_m2?.toFixed(0)} m² | <b>Land-use:</b>{" "}
              {codeLabel}
            </div>
            <ExcelForm parcel={parcel} />
          </>
        ) : null}
      </div>
    </>
  );
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
