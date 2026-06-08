# Whitespace Probe — 1000 m Percentile Block (Recalibration Reference)

**Scope:** READ-ONLY diagnostic. Additive one-block edit to
`scripts/diagnostics/whitespace_input_distribution.sql`. No `app/` changes, no
schema changes, no production behavior changed.

**Branch:** `claude/whitespace-floor-investigation-kgcw3k`
**Commit:** `diag: add city-wide 1000m percentile block to whitespace probe`

---

## 1. Why this block exists

The `competition_whitespace` leg input is produced by
`_competition_whitespace_score(competitor_count, …)`
(`app/services/expansion_advisor.py:2340`). Its curve is designed over a
**0–25 same-category competitor** domain and **floors at 15.00** once the count
is high enough (raw `== 15` at count ≈ 14.95, i.e. `count >= 15` floors;
raw `<= 0` at `count >= 25`).

The earlier whitespace probe established two things for dine-in candidates:

1. **The floor is pervasive.** ~97.4% of sampled dine-in candidates are floored
   at `15.00`; the *scored* competitor count (3000 m competition radius) has
   p50 ≈ 230 and max ≈ 339 — an order of magnitude past the curve's 25-count
   design ceiling.
2. **A tighter trade area lands the count back in-range.** The per-district
   radius ladder (block D of the probe) showed the same-category count collapses
   into the curve's 0–25 design range at **~1000 m**.

That makes **1000 m** the natural radius at which to re-anchor the curve. But to
set the new reference *non-arbitrarily*, we need the **city-wide percentile
distribution** of the same-category count at 1000 m — not just the per-district
averages the probe already printed (block D's `avg_recompute_1000m`) and not the
3000 m full-percentile block (block A, which is scored at the wrong radius for
recalibration). This new block fills exactly that gap.

---

## 2. What was added (block E)

A single labeled result row appended after the per-district radius-sensitivity
block (D) and before the `DROP TABLE` cleanup. It reads the **existing**
`ws_recompute.recompute_1000m` column — so it does **not** re-sample and does
**not** re-implement the recompute. It inherits, verbatim from block D's LATERAL:

- **Same category-match approximation:** `lower(rp.category) = lower(search_category)`
  for POIs, plus a delivery `category_raw` / `cuisine_raw` ILIKE.
- **Same competitor universe:** `restaurant_poi` (OPERATIONAL-or-NULL)
  `UNION ALL` `delivery_source_record`.
- **Same radius predicate:** `ST_DWithin(…::geography, …::geography, 1000)`.
- **Same sampled candidate set:** the up-to-150-row `ws_recompute` temp table.

### Columns emitted

| Column | Meaning |
|---|---|
| `label` | literal `'recompute_1000m_citywide'` (row identifier) |
| `n` | candidates contributing (rows in `ws_recompute`) |
| `n_zero` | candidates with `recompute_1000m = 0` (true greenfield at 1 km) |
| `p5 … p99` | percentile_cont of the 1000 m same-category count |
| `max` | max 1000 m count |
| `pct_in_design_range` | share with count **≤ 25** — the curve's current domain |
| `pct_le_15` | share with count **≤ 15** — where the curve still has headroom above the 15.00 floor |

### How to read the output (for recalibration)

- **`p50` / `p75` / `p90`** are the candidate anchors for the recalibrated
  curve's "competitive" knee at 1000 m. If, e.g., p90 ≈ 18 and p50 ≈ 6, a curve
  whose floor engages near the p90 (rather than at count 15 / 25) keeps spread
  across the bulk of the distribution.
- **`pct_in_design_range` near 100%** confirms 1000 m brings essentially the
  whole city back inside the curve's 0–25 domain (the fork-(a) hypothesis: the
  radius, not the curve, was the problem).
- **`pct_le_15`** is the share that is *already* on the non-floored part of the
  current curve at 1000 m — i.e. how much of the population would regain ranking
  spread without any curve change, purely from the radius move.
- **`n_zero`** flags how many candidates would map to the *unconfident-zero*
  neutral-50 path vs. the confident-zero `100.0` path — relevant because the
  leg's missing-data semantics differ at the zero boundary
  (`_competition_whitespace_score:2372`).

---

## 3. The added SQL

```sql
-- ── E) City-wide 1000 m percentile distribution (recalibration reference) ──
-- Exists to set the recalibrated _competition_whitespace_score reference: the
-- per-district ladder (block D) showed the same-category count collapses into
-- the curve's 0-25 design range at ~1000 m, so this block reports the city-wide
-- percentile distribution of that SAME recompute_1000m column ...
SELECT
    'recompute_1000m_citywide'                                              AS label,
    COUNT(*)                                                                AS n,
    COUNT(*) FILTER (WHERE recompute_1000m = 0)                             AS n_zero,
    round(percentile_cont(0.05) WITHIN GROUP (ORDER BY recompute_1000m)::numeric,1) AS p5,
    round(percentile_cont(0.25) WITHIN GROUP (ORDER BY recompute_1000m)::numeric,1) AS p25,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY recompute_1000m)::numeric,1) AS p50,
    round(percentile_cont(0.75) WITHIN GROUP (ORDER BY recompute_1000m)::numeric,1) AS p75,
    round(percentile_cont(0.90) WITHIN GROUP (ORDER BY recompute_1000m)::numeric,1) AS p90,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY recompute_1000m)::numeric,1) AS p95,
    round(percentile_cont(0.99) WITHIN GROUP (ORDER BY recompute_1000m)::numeric,1) AS p99,
    MAX(recompute_1000m)                                                    AS max,
    round(100.0*COUNT(*) FILTER (WHERE recompute_1000m <= 25)/NULLIF(COUNT(*),0),1) AS pct_in_design_range,
    round(100.0*COUNT(*) FILTER (WHERE recompute_1000m <= 15)/NULLIF(COUNT(*),0),1) AS pct_le_15
FROM ws_recompute;
```

---

## 4. Provenance & caveats

- **`recompute_1000m` is an approximation, not the app's exact scored value.**
  The app's same-category filter uses `_expand_category()` alias keys/regex;
  this block uses a plain `lower()=lower()` + delivery ILIKE. For multi-alias
  categories the absolute counts can differ. Treat the distribution as a
  **radius-sensitivity / recalibration signal**, not an exact reproduction of
  the production count. (This caveat is already documented in the script header.)
- **"City-wide" = over the sampled candidates in `ws_recompute`.** Block D caps
  that temp table at `LIMIT 150` to bound cost (the delivery join is on
  lat/lon points and is **not** index-accelerated). The percentiles are
  therefore over up to 150 candidates of the most-recent dine-in search, the
  same set block D's `avg_recompute_1000m` summarizes. To widen to the full
  ~500-candidate `ws_sample`, raise block D's `LIMIT 150` (cost grows with the
  un-indexed delivery scan).
- **Scored radius for dine-in is 3000 m** (`_CATCHMENT_RADII_M`,
  `expansion_advisor.py:818`); 1000 m here is a *hypothetical* tighter trade
  area being evaluated, not the current production radius.

---

## 5. Validation

This is a `psql -f`-safe diagnostic (no `\set`, no heredocs, no prompts). Run:

```bash
psql "$DATABASE_URL" -f scripts/diagnostics/whitespace_input_distribution.sql
```

The new block prints as the **last labeled SELECT** before the temp tables are
dropped. Sanity checks when reading results:

- `pct_in_design_range` should be high (the whole premise is that 1000 m pulls
  counts back under 25). If it is *low*, 1000 m is not tight enough and the
  recalibration radius needs reconsidering.
- `n` should match block D's `n` (same temp table, no extra sampling).
- `max` at 1000 m should be far below the 3000 m scored `max` (~339) from
  block A.

---

## 6. Risk

**Minimal.** Read-only SQL diagnostic; reuses an already-materialized temp
column; touches no other block, no `app/` code, no schema. Worst case is a
mis-read of the approximate counts — mitigated by the header caveat and the
`pct_in_design_range` self-check above.

**Merge recommendation:** safe to keep on the investigation branch as a probe
artifact. Next step is Ahmed re-running it in Codespace and feeding the 1000 m
percentiles into the `_competition_whitespace_score` recalibration.
