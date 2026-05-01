# Market-viability conjunction — validation playbook

This document covers both the existing 2-of-3 form (rent x population, shipped
2026-05-01 in commit `59320ae2a`) and the new 3-of-3 form that adds NASA
Black Marble VNP46A3 monthly nighttime-radiance growth as the third leg.

The pass is implemented in
`app/services/expansion_advisor.py::_apply_market_viability_pass` and runs
after rerank. It is a **positional demotion** (does not mutate `final_score`)
that fires only when each leg has a confident signal.

## How the conjunction works

A candidate is flagged when **all three** are true and confident:

1. **Rent leg.** `economics_detail.rent_burden.percentile >=
   EXPANSION_VIABILITY_RENT_PCT_THRESHOLD` and `rent_burden.source_label`
   is **not** in the deny-list `("city_band_type", "city")` (those scopes
   are too coarse to be confident).
2. **Population leg.** `population_reach > 0` and below the cohort's
   `EXPANSION_VIABILITY_POP_PERCENTILE` cutoff.
3. **Growth leg (third leg).** *Inverted* — the candidate's district has
   `radiance_growth.confident == false` **or** `value_yoy_pct <
   EXPANSION_VIABILITY_RADIANCE_YOY_THRESHOLD`. Confident positive growth
   **rescues** the candidate from the conjunction (no flag).

If a confident signal is missing on any leg, that leg falls through; the
pre-existing 2-of-3 behavior governs.

## Source labels

- Rent: `district`, `district_band_type`, `district_type`, `city`,
  `city_band_type`, `conservative_default`. Confident scopes are everything
  except `city_band_type` and `city`.
- Radiance: `blackmarble_district_yoy_simple` (current, single-month YoY)
  or `blackmarble_district_yoy_12mo` (future, trailing-12-month average
  once 24 months of backfill is loaded).

## SQL recipes

### 1. Flag rate by rent scope (applies to both 2-of-3 and 3-of-3)

```sql
SELECT
  score_breakdown_json #>> '{economics_detail,rent_burden,source_label}' AS rent_scope,
  COUNT(*) FILTER (WHERE score_breakdown_json ? 'market_viability_flag') AS flagged,
  COUNT(*) AS total,
  ROUND(100.0 * COUNT(*) FILTER (WHERE score_breakdown_json ? 'market_viability_flag') / COUNT(*), 1) AS pct
FROM expansion_candidate
WHERE computed_at > NOW() - INTERVAL '24 hours'
GROUP BY 1
ORDER BY 1;
```

Expected: 5-15% flag rate among district-confident scopes
(`district`, `district_band_type`, `district_type`); 0% among
`city_band_type` / `city` (those scopes never enter the conjunction).

### 2. Third-leg rescue rate (3-of-3 only)

```sql
SELECT
  COUNT(*) AS total_district_confident,
  COUNT(*) FILTER (WHERE score_breakdown_json ? 'market_viability_flag') AS still_flagged,
  COUNT(*) FILTER (
    WHERE feature_snapshot_json ? 'radiance_growth'
      AND (feature_snapshot_json #>> '{radiance_growth,confident}')::boolean = true
  ) AS has_radiance_signal,
  COUNT(*) FILTER (
    WHERE feature_snapshot_json ? 'radiance_growth'
      AND (feature_snapshot_json #>> '{radiance_growth,confident}')::boolean = true
      AND (feature_snapshot_json #>> '{radiance_growth,value_yoy_pct}')::numeric > 0
  ) AS has_positive_growth
FROM expansion_candidate
WHERE computed_at > NOW() - INTERVAL '24 hours'
  AND score_breakdown_json #>> '{economics_detail,rent_burden,source_label}'
      NOT IN ('city_band_type', 'city');
```

Compare `still_flagged / total_district_confident` between the pre-3-of-3
baseline (~5-15%) and post-3-of-3 (expected lower by some margin reflecting
growth rescues).

### 3. Per-district radiance distribution sanity check

```sql
SELECT
  district_key,
  year_month,
  pixel_count_valid,
  ROUND(radiance_mean::numeric, 2) AS rad_mean
FROM district_radiance_monthly
WHERE source = 'nasa_blackmarble_vnp46a3_c2'
  AND year_month = (SELECT MAX(year_month) FROM district_radiance_monthly)
ORDER BY radiance_mean DESC NULLS LAST
LIMIT 20;
```

Expected (per POC, 2026-05-01): top 10-20 districts include Hittin, Olaya,
KKIA airport, Qurtubah. Pixel counts should mostly be > 10 in the lenient
(`quality < 2`) regime; below-floor districts produce
`confident=false` and silently fall through.

## Gate status surface

`_candidate_gate_status` exposes `radiance_growth_pass` alongside the existing
gates. It is **advisory**, not in `hard_fail_gates`.

- `True`: confident signal, YoY >= threshold (rescue would apply).
- `False`: confident signal, YoY < threshold.
- `None`: no radiance data for the district (advisory unknown).

## Tuning the rescue threshold

`EXPANSION_VIABILITY_RADIANCE_YOY_THRESHOLD` defaults to `0.0` — any positive
growth rescues. If the false-rescue rate is high (i.e., flagged-and-rescued
candidates that under-perform after launch), raise to `5.0` or `10.0` to
require meaningful growth.

## Operational notes

- The monthly cron (`.github/workflows/ingest-blackmarble.yml`) fires on the
  15th of each month, ingesting the latest available month of VNP46A3.
- Black Marble has a ~6-8 week production lag; the workflow defaults to
  `2 months ago` if no `year_month` input is provided.
- Allowlisted NASA domains: `ladsweb.modaps.eosdis.nasa.gov`,
  `urs.earthdata.nasa.gov`. The patch can merge before the allowlist lands;
  only the workflow's ingest step will fail until the allowlist is active.
