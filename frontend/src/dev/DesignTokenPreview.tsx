export default function DesignTokenPreview() {
  return (
    <section
      style={{
        position: "fixed",
        right: 16,
        bottom: 16,
        width: 320,
        padding: 12,
        zIndex: 1000,
      }}
    >
      <div className="ot-card" style={{ padding: 12, display: "grid", gap: 10 }}>
        <strong style={{ fontFamily: "var(--font-sans)" }}>Design Token Preview</strong>
        <div style={{ display: "flex", gap: 8 }}>
          <button type="button" className="ot-btn-primary">
            Primary
          </button>
          <button type="button" className="ot-btn-secondary">
            Secondary
          </button>
        </div>
        <input className="ot-input" placeholder="Tokenized input" />
      </div>
    </section>
  );
}
