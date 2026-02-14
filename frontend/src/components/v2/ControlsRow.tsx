import styles from "./ControlsRow.module.css";

export default function ControlsRow() {
  const costs = [
    ["Residential (SAR/m²)", "2200 SAR"],
    ["Retail (SAR/m²)", "2600 SAR"],
    ["Office (SAR/m²)", "2400 SAR"],
    ["Basement (SAR/m²)", "2200 SAR"],
    ["Upper annex (non-FAR) unit cost (SAR/m²)", "2200 SAR"],
  ];

  return (
    <div className={styles.grid}>
      <div className={styles.card}>
        <label className={styles.label}>Provider</label>
        <select className={styles.select} defaultValue="Blended v1 (Suhail + Aqar)">
          <option>Blended v1 (Suhail + Aqar)</option>
        </select>
        <label className={styles.label}>Override land use (optional)</label>
        <input className={styles.input} defaultValue="auto: use parcel" />
        <label className={styles.label}>Override land price (SAR/m2, optional)</label>
        <input className={styles.input} defaultValue="---" />
        <div className={styles.buttons}>
          <button className={styles.btn} type="button">Fetch land price</button>
          <button className={styles.btn} type="button">Calculate Estimate</button>
          <button className={styles.btn} type="button">Scenario</button>
        </div>
      </div>
      <div className={styles.card}>
        <h3>Construction unit costs (SAR/m²)</h3>
        {costs.map(([label, value]) => (
          <div className={styles.costRow} key={label}>
            <span>{label}</span>
            <strong>{value}</strong>
          </div>
        ))}
      </div>
    </div>
  );
}
