import styles from "./TopNav.module.css";

export default function TopNav() {
  return (
    <header className={styles.nav}>
      <div>
        <div className={styles.title}>Oaktree Estimator</div>
        <div className={styles.subtitle}>Riyadh Commercial Development</div>
      </div>
      <input className={styles.search} placeholder="Search by parcels, streets, districts" />
      <div className={styles.right}>
        <span>العربية</span>
        <span>Asad ur rehman</span>
      </div>
    </header>
  );
}
