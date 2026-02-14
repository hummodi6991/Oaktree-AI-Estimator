import styles from "./EmptyState.module.css";

export default function EmptyState() {
  return (
    <section className={styles.wrap}>
      <h2 className={styles.headline}>Select one or multiple parcel(s) to begin analysis</h2>
      <p className={styles.sub}>Choose parcel to start development assumptions and view financial projects.</p>
      <div className={styles.tiles}>
        <div className={styles.tile}>Cost & Revenue Calculations</div>
        <div className={styles.tile}>Detailed Breakdown</div>
        <div className={styles.tile}>Scenario Comparison</div>
      </div>
    </section>
  );
}
