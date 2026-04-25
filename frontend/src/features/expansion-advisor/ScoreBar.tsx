type ScoreBarProps = {
  label: string;
  value: number | null | undefined;
};

/**
 * Neutral horizontal bar for 0–100 scores in the Breakdown tab.
 * No traffic-light coloring — Phase 1A keeps the breakdown chrome
 * deliberately neutral so the user reads the value, not the color.
 */
export default function ScoreBar({ label, value }: ScoreBarProps) {
  const numeric = typeof value === "number" && Number.isFinite(value) ? value : null;
  const clamped = numeric == null ? 0 : Math.max(0, Math.min(100, numeric));
  return (
    <div className="ea-score-bar">
      <div
        className="ea-score-bar__head"
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "baseline",
          gap: 8,
          marginBottom: 4,
        }}
      >
        <span
          className="ea-score-bar__label"
          style={{
            fontSize: "var(--oak-fs-xs, 12px)",
            color: "var(--oak-text-light, #828282)",
            fontWeight: "var(--oak-fw-medium, 500)",
          }}
        >
          {label}
        </span>
        <span
          className="ea-score-bar__value"
          style={{
            fontSize: "var(--oak-fs-sm, 14px)",
            fontWeight: "var(--oak-fw-semibold, 600)",
            color: "var(--oak-text-dark, #171717)",
          }}
        >
          {numeric == null ? "" : Math.round(numeric)}
        </span>
      </div>
      <div
        className="ea-score-bar__track"
        role="progressbar"
        aria-label={label}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={numeric == null ? undefined : Math.round(numeric)}
        style={{
          height: 6,
          borderRadius: 3,
          background: "var(--oak-track-bg, rgba(0,0,0,0.08))",
          overflow: "hidden",
        }}
      >
        <div
          className="ea-score-bar__fill"
          style={{
            width: `${clamped}%`,
            height: "100%",
            background: "var(--oak-accent, #4b5563)",
          }}
        />
      </div>
    </div>
  );
}
