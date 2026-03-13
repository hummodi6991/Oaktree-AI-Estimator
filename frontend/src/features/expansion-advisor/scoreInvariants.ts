/**
 * Lightweight invariants for score breakdown formatting.
 *
 * Prevents weighted_points from being accidentally formatted as
 * percentages (e.g. 2500% instead of 25%).
 */

/**
 * Normalize a weight value to a display-ready percentage (0-100 integer).
 *
 * The backend sends weight_percent as whole integers (e.g. 25 for 25%).
 * If a fractional representation slips through (e.g. 0.25), we convert it.
 * Values already > 1 are assumed to be percent and passed through.
 *
 * Returns a string suitable for rendering: "25", "20", etc.
 */
export function normalizeWeightPercent(weight: number): string {
  if (weight == null || !Number.isFinite(weight)) return "0";
  // If the value is in 0..1 range (fractional), scale to percent.
  // If it's already > 1, assume it's already in percent.
  const pct = weight > 0 && weight <= 1 ? weight * 100 : weight;
  return Math.round(pct).toFixed(0);
}

/**
 * Assert that a weighted_points value is sane (not accidentally a percent).
 *
 * weighted_points = raw_input_score * (weight / 100), so it should never
 * exceed the raw_input_score. This is a dev-time assertion, not a runtime
 * guard — if it fires, the scoring pipeline has a formatting bug.
 */
export function assertWeightedPointsSane(
  weighted: number,
  rawInput: number,
  label: string,
): void {
  if (!import.meta.env.DEV) return;
  if (weighted > rawInput + 0.5) {
    console.warn(
      `[score-invariant] ${label}: weighted_points (${weighted}) exceeds ` +
      `raw_input_score (${rawInput}). This likely means weighted_points ` +
      `is being treated as a percentage instead of points.`,
    );
  }
}
