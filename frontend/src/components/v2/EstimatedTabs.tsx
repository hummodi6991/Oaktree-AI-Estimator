import { useState } from "react";
import styles from "./EstimatedTabs.module.css";

type Props = { summaryContent?: React.ReactNode };

const tabs = ["Summary", "Financial Breakdown", "Revenue Breakdown", "Parking"];

export default function EstimatedTabs({ summaryContent }: Props) {
  const [active, setActive] = useState("Summary");
  return (
    <section className={styles.wrap}>
      <div className={styles.title}>Estimated Calculations</div>
      <div className={styles.tabs}>
        {tabs.map((tab) => (
          <button key={tab} type="button" className={`${styles.tab} ${active === tab ? styles.active : ""}`} onClick={() => setActive(tab)}>{tab}</button>
        ))}
      </div>
      <div className={styles.content}>{active === "Summary" ? summaryContent ?? "Summary placeholder" : `${active} placeholder`}</div>
    </section>
  );
}
