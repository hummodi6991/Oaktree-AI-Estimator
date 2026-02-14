import { useState } from "react";
import type { ParcelSummary } from "../api";
import Map from "../components/Map";
import ExcelForm from "../components/ExcelForm";
import EmptyState from "../components/v2/EmptyState";
import TopNav from "../components/v2/TopNav";
import ParcelStrip from "../components/v2/ParcelStrip";
import ControlsRow from "../components/v2/ControlsRow";
import EstimatedTabs from "../components/v2/EstimatedTabs";
import FinancialSummaryCard from "../components/v2/FinancialSummaryCard";
import "../styles/tokens.css";
import styles from "./DashboardV2.module.css";

export default function DashboardV2() {
  const [parcel, setParcel] = useState<ParcelSummary | null>(null);
  const [mapVisible, setMapVisible] = useState(true);

  return (
    <div className={`uiV2 ${styles.page}`}>
      <TopNav />
      <div className={styles.container}>
        <main className={styles.main}>
          {mapVisible && (
            <div className={styles.mapWrap}>
              <Map onParcel={(selectedParcel) => setParcel(selectedParcel)} />
            </div>
          )}
          {!parcel ? (
            <EmptyState />
          ) : (
            <>
              <ParcelStrip parcel={parcel} mapVisible={mapVisible} onToggleMap={() => setMapVisible((v) => !v)} />
              <ControlsRow />
              <EstimatedTabs summaryContent={<ExcelForm parcel={parcel} />} />
            </>
          )}
        </main>
        <aside className={styles.sidebar}>
          <FinancialSummaryCard />
        </aside>
      </div>
    </div>
  );
}
