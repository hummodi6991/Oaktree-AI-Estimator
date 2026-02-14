import styles from "./FinancialSummaryCard.module.css";

export default function FinancialSummaryCard() {
  const fields = ["Total Capex", "Annual Revenue", "Net Operating Income - NOI", "ROI", "Yield", "Key Ratios"];
  return (
    <aside className={styles.card}>
      <h3 className={styles.title}>Financial Summary</h3>
      {fields.map((field) => (
        <div className={styles.row} key={field}>
          <span>{field}</span>
          <strong>—</strong>
        </div>
      ))}
    </aside>
  );
}
