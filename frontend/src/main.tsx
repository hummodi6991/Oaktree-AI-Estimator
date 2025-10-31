import React, { useState } from "react";
import { createRoot } from "react-dom/client";

import Map from "./components/Map";
import ExcelForm from "./components/ExcelForm";
import "./index.css";
import type { ParcelSummary } from "./api";

function App() {
  const [parcel, setParcel] = useState<ParcelSummary | null>(null);

  return (
    <>
      <Map onParcel={(parcel) => setParcel(parcel)} />
      <div style={{ padding: 12 }}>
        {parcel ? (
          <>
            <div>
              <b>Parcel:</b> {parcel.parcel_id} | <b>Area:</b> {parcel.area_m2?.toFixed(0)} m² | <b>Land-use:</b>{" "}
              {parcel.landuse_code || parcel.landuse_raw}
            </div>
            <ExcelForm parcel={parcel} />
          </>
        ) : (
          <em>Click a parcel on the map to begin…</em>
        )}
      </div>
    </>
  );
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
