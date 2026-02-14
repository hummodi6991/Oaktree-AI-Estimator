import type { ParcelSummary } from "../../api";
import styles from "./ParcelStrip.module.css";

type Props = {
  parcel: ParcelSummary;
  mapVisible: boolean;
  onToggleMap: () => void;
};

function formatLandUse(code?: string | null) {
  const normalized = code?.trim().toLowerCase();
  if (normalized === "m") return "m — Mixed/Commercial";
  if (normalized === "s") return "s — Residential";
  return "—";
}

function formatLanduseMethod(method?: string | null) {
  switch (method) {
    case "parcel_label":
      return "ArcGIS parcel label";
    case "suhail_overlay":
      return "Suhail zoning";
    case "osm_overlay":
      return "OSM overlay";
    default:
      return "—";
  }
}

export default function ParcelStrip({ parcel, mapVisible, onToggleMap }: Props) {
  const area = parcel.parcel_area_m2 ?? parcel.area_m2;

  return (
    <div className={styles.strip}>
      <div className={styles.text}>
        Parcel: {parcel.parcel_id ?? "—"} | Area: {area != null ? area.toLocaleString() : "—"} m² | Land-use: {formatLandUse(parcel.landuse_code)} | Method: {formatLanduseMethod(parcel.landuse_method)}
      </div>
      <div className={styles.actions}>
        <button className={styles.btn} type="button">Export PDF</button>
        <button className={styles.btn} type="button" onClick={onToggleMap}>{mapVisible ? "Hide Map" : "Show Map"}</button>
      </div>
    </div>
  );
}
