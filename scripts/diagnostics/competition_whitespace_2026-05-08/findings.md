# competition_whitespace flat-at-100 — investigation

Search: Burger, search_id `dce2d7dc-e8a8-486a-8656-008f23f717f4`, cohort 46.
Symptom: `inputs.competition_whitespace = 100`, `weighted_components.competition_whitespace = 5.76` for every candidate; raw stddev 0.

Investigation is read-only. No code, weights, or DB state were modified.

---

## 1. Producer location and signature

The raw `competition_whitespace` value plumbed into `_score_breakdown` (assignment at
`app/services/expansion_advisor.py:2912`) is the local `whitespace_score` returned by:

```python
def _competition_whitespace_score(
    competitor_count: int,
    *,
    confident: bool | None = None,
) -> float:
```

- File: `app/services/expansion_advisor.py`
- Definition: lines **2222–2259**
- Call site (search loop): lines **7393–7395**
  ```python
  whitespace_score = _competition_whitespace_score(
      competitor_count, confident=competitor_count_confident
  )
  ```
- Both inputs are read off the candidate row at lines **7321–7328**:
  - `competitor_count = _safe_int(row.get("competitor_count"))`
  - `competitor_count_confident` ← `row.get("competitor_count_confident")` (tri-state: True/False/None)

Upstream of that, both fields are populated by `_bulk_enrich_competitors`
(`app/services/expansion_advisor.py:6005–6159`), invoked at:
- candidate_location path: line **6781** (this is the path Burger / cohort 46 took, since 46 ≥ 10)
- direct commercial_unit fallback: line **6824**

The legacy ARCGIS-pool SQL paths at lines **6411–6419** (`_build_candidate_sql`) and
**6510–6521** (`_build_candidate_sql_no_district`) also produce a `competitor_count` column
using the same matching predicate and bypass `_bulk_enrich_competitors` entirely
(in that branch `competitor_count_confident` falls through as `None`, see comment at 7322–7328).

---

## 2. Exact code path that produces the saturation value

The saturation value is **100**, produced at line **2254**:

```python
2251      if confident is False and competitor_count <= 0:
2252          return 50.0
2253      if competitor_count <= 0:
2254          return 100.0                  # ← always-fires path for the Burger cohort
2255      # Log-scaled decay: steeper at low counts, gentler at high counts.
2256      raw = 100.0 * (1.0 - (math.log1p(competitor_count) / math.log1p(25)))
2257      # Floor at 15 — even saturated areas get some score so rankings remain
2258      # distinguishable.
2259      return _clamp(max(15.0, raw))
```

For every Burger-cohort candidate, the producer is being called with
`competitor_count == 0` and `confident == True` (or `None`). The `confident is False` F4
guard at 2251–2252 does **not** fire, so the function returns the literal 100.0.

Confirmation that 100 → weighted 5.76:
- weight = `8.7640 - EXPANSION_CHAIN_STRENGTH_WEIGHT` (line 2884). With the default
  `EXPANSION_CHAIN_STRENGTH_WEIGHT=3.0`, weight = 5.7640.
- `weighted_components["competition_whitespace"]` = `100 * (5.7640 / 100)` = **5.7640** ⇒
  rounds to 5.76 (lines 2924–2926). Matches the observed value exactly.

Why `competitor_count == 0` for every candidate is the next question (Section 4).

Why `confident == True` (so 50.0 fallback not used): in `_bulk_enrich_competitors`
the SQL emits `broader_count = COUNT(*)` over **all** POIs in the radius, regardless of
category, then sets `confident = (broader_count > 0)` (line 6148). Riyadh districts are
POI-dense, so `broader_count > 0` virtually always — the F4 floor only protects true
greenfield (no POI of any kind in radius). It cannot rescue a category-matching bug.

---

## 3. Intended semantic

From the docstring (2227–2249) and the surrounding comment block (2842–2855, 2879–2884):

`competition_whitespace` is meant to measure **how uncontested the candidate's site is for
the searched category**, on a 0–100 scale where higher = more open. The intended signal
is the count of *same-category* competitors inside the candidate's competition radius
(service-model-tuned via `_catchment_radii(service_model)["competition"]`). The curve
calibrates to Riyadh F&B densities:

```
0 competitors -> 100  (wide open)
1              -> 88
2              -> 78
3              -> 69
5              -> 55
8              -> 40
12             -> 28
20+            -> 15  (floor)
```

The F4 defensive branch (added later) returns the neutral midpoint 50.0 when the radius
returned **no POIs at all in any category** — i.e. the zero same-category count is
unsupported by data, not evidence of a true greenfield. Patch B carved 3.00 points out of
this component into the new `chain_strength` leg (pro-presence: validation by established
brands), so the combined Pillar-2 budget is preserved.

The component is anti-presence (more competitors → lower score); `chain_strength` is its
pro-presence counterpart.

---

## 4. Data-source diagnosis

Producer is broken on the matching predicate. The underlying signal is varied; the
producer collapses it to zero for every candidate before scoring.

`_bulk_enrich_competitors` builds `competitor_count` from a UNION of two sources, gated
by an `in_category` flag computed differently for each side
(`app/services/expansion_advisor.py:6094–6138`):

```sql
-- Source 1: restaurant_poi  (Google Places)
SELECT (lower(rp.category) = ANY(:category_keys)) AS in_category, ...

-- Source 2: delivery_source_record  (HungerStation etc.)
SELECT (lower(COALESCE(dsr.category_raw, '')) ~* :category_regex
        OR lower(COALESCE(dsr.cuisine_raw, '')) ~* :category_regex) AS in_category, ...
```

For `category="burger"`, `_expand_category("burger")` (lines 550–566 + alias map at 155–158) returns:
- `keys = ["burger"]`
- `regex = "burger|hamburger|برجر"`

So:

- **restaurant_poi side:** `in_category` is true only when `lower(rp.category)` **exactly equals** the literal `"burger"`. That is an unrealistic match for Google-Places-style category strings (they're typically broader buckets like `restaurant`, `fast_food_restaurant`, `meal_takeaway`, full names like `Burger Joint`, or Arabic strings; see `restaurant_poi` ingest pipelines). If the column rarely or never holds the bare token `burger`, this side contributes 0 in-category rows for every candidate.
- **delivery_source_record side:** uses `~*` regex on `category_raw` / `cuisine_raw`. If `dsr` is largely keyed on normalized buckets (`expansion_delivery_market` normalizes to `international|traditional|coffee_bakery|seafood`, see lines 247–251) rather than verbatim `burger`/`hamburger`/`برجر`, the regex misses them all.

End result: `COUNT(*) FILTER (WHERE in_category) = 0` for every parcel in the cohort, even
though the radius almost certainly contains burger-relevant restaurants and burger
listings. The same matching predicate is used in the two legacy SQL paths
(lines 6413, 6515), so this is not a bulk-vs-legacy asymmetry — it's a single
matching contract.

The legacy fallback paths also have a *second* failure mode: their competitor_count subquery
queries `restaurant_poi` only — no UNION with `delivery_source_record` — so even if
`dsr.category_raw` did contain `burger`, those candidates would still see 0.

The data-feed itself is fine: the same SQL returns useful values for `chain_strength`
(MAX of `expansion_competitor_quality.chain_strength_score` across same-category POIs),
`provider_listing_count`, `delivery_listing_count`, etc., from the same rows — so
restaurant_poi and dsr have varied content per candidate. The bug is the
`in_category` predicate for `restaurant_poi`, which is a strict equality on a low-cardinality
column whose values do not correspond to category keys like `burger`/`pizza`/`chicken`.

### Counterfactual SQL the user can run

Run from a psql session against the production read replica. Replace the
search_id literal as needed; the cohort UUID is the one in the brief.

#### 4.a — Confirm the breakdown is flat at 100 across the cohort

```sql
-- One row per candidate with the breakdown's stored input.
SELECT
    candidate_id,
    score_breakdown_json -> 'inputs' ->> 'competition_whitespace'           AS ws_input,
    score_breakdown_json -> 'inputs' ->> 'competition_whitespace_confident' AS ws_confident,
    score_breakdown_json -> 'weighted_components' ->> 'competition_whitespace' AS ws_weighted
FROM expansion_search_candidate
WHERE search_id = 'dce2d7dc-e8a8-486a-8656-008f23f717f4'
ORDER BY rank ASC;

-- Aggregate distribution.
SELECT
    COUNT(*)                                                              AS n,
    MIN((score_breakdown_json -> 'inputs' ->> 'competition_whitespace')::numeric) AS ws_min,
    MAX((score_breakdown_json -> 'inputs' ->> 'competition_whitespace')::numeric) AS ws_max,
    STDDEV((score_breakdown_json -> 'inputs' ->> 'competition_whitespace')::numeric) AS ws_stddev,
    BOOL_AND((score_breakdown_json -> 'inputs' ->> 'competition_whitespace_confident')::boolean) AS all_confident
FROM expansion_search_candidate
WHERE search_id = 'dce2d7dc-e8a8-486a-8656-008f23f717f4';
```

Expected: `ws_input = 100` for every row, `ws_stddev = 0`, `all_confident = true`.

#### 4.b — Show the underlying signal IS varied per candidate

If `feature_snapshot_json` is persisted, the brand/competitor evidence per parcel is
visible there; otherwise look at the sibling fields written next to the breakdown.

```sql
SELECT
    candidate_id,
    rank,
    -- brand presence aggregations the snapshot writes
    feature_snapshot_json -> 'brand_presence'                              AS brand_presence,
    jsonb_array_length(COALESCE(comparable_competitors_json, '[]'::jsonb)) AS comparable_n,
    feature_snapshot_json ->> 'competitor_density_score'                   AS density,
    feature_snapshot_json ->> 'competitor_count'                           AS cc_snapshot
FROM expansion_search_candidate
WHERE search_id = 'dce2d7dc-e8a8-486a-8656-008f23f717f4'
ORDER BY rank ASC;
```

Expected: variance across rows in `brand_presence`, `comparable_n`,
`competitor_density_score`, despite `competition_whitespace` being uniform — i.e.
the data feed *does* differentiate candidates; only the `in_category` predicate fails.

#### 4.c — Show that `restaurant_poi.category` rarely equals "burger" exactly

This is the smoking-gun query for the matching predicate.

```sql
-- How many same-category rows exist anywhere in the corpus under the current
-- equality predicate, and how many would match a substring/regex?
SELECT
    COUNT(*) FILTER (WHERE lower(category) = 'burger')                 AS exact_burger,
    COUNT(*) FILTER (WHERE lower(category) ~* 'burger|hamburger|برجر') AS regex_burger,
    COUNT(*)                                                            AS total_pois
FROM restaurant_poi;

-- What does category actually look like for the restaurants we'd want to count?
SELECT lower(category) AS cat_lc, COUNT(*) AS n
FROM restaurant_poi
WHERE lower(name) ~* 'burger|hamburger|برجر'
   OR lower(COALESCE(category, '')) ~* 'burger|hamburger|برجر'
GROUP BY 1
ORDER BY n DESC
LIMIT 30;
```

Expected: `exact_burger` is small or 0; `regex_burger` is much larger; the
distribution shows category values like `restaurant`, `fast_food_restaurant`,
`meal_takeaway`, brand-name strings, etc. — none of which equal `burger`.

#### 4.d — Per-candidate bulk-enrichment replay

For one or two candidate parcels in the cohort, replay the exact subquery
`_bulk_enrich_competitors` runs (so the producer's input is fully visible,
not just the persisted output). `:radius_m` should be the burger /
delivery_first competition radius — confirm by reading
`_catchment_radii("delivery_first")["competition"]` in `expansion_advisor.py`.

```sql
-- Pick one parcel
WITH inputs AS (
    SELECT 'PARCEL_ID_HERE'::text AS parcel_id,
           CAST(LON_HERE  AS double precision) AS lon,
           CAST(LAT_HERE  AS double precision) AS lat
)
SELECT
    COUNT(*) FILTER (WHERE in_category)                       AS competitor_count,
    COUNT(*)                                                  AS broader_count,
    SUM(CASE WHEN src='rp' AND in_category THEN 1 ELSE 0 END) AS rp_in_cat,
    SUM(CASE WHEN src='rp' THEN 1 ELSE 0 END)                 AS rp_total,
    SUM(CASE WHEN src='dsr' AND in_category THEN 1 ELSE 0 END) AS dsr_in_cat,
    SUM(CASE WHEN src='dsr' THEN 1 ELSE 0 END)                AS dsr_total
FROM inputs i
LEFT JOIN LATERAL (
    SELECT 'rp'::text AS src,
           (lower(rp.category) = ANY(ARRAY['burger']::text[])) AS in_category
    FROM restaurant_poi rp
    WHERE ST_DWithin(
        rp.geom::geography,
        ST_SetSRID(ST_MakePoint(i.lon, i.lat), 4326)::geography,
        1000  -- replace with actual competition radius
    )
    UNION ALL
    SELECT 'dsr'::text AS src,
           (lower(COALESCE(dsr.category_raw, '')) ~* 'burger|hamburger|برجر'
         OR lower(COALESCE(dsr.cuisine_raw,  '')) ~* 'burger|hamburger|برجر') AS in_category
    FROM delivery_source_record dsr
    WHERE dsr.lat IS NOT NULL AND dsr.lon IS NOT NULL
      AND ST_DWithin(
          ST_SetSRID(ST_MakePoint(dsr.lon::double precision,
                                  dsr.lat::double precision), 4326)::geography,
          ST_SetSRID(ST_MakePoint(i.lon, i.lat), 4326)::geography,
          1000
      )
) combined ON TRUE;
```

Expected: `rp_in_cat = 0`, `rp_total > 0`, `dsr_in_cat ≈ 0`, `dsr_total > 0`,
`broader_count > 0`, `competitor_count = 0`.

That triple `(broader_count > 0, competitor_count = 0)` is exactly the pre-condition
for the producer's `competitor_count <= 0 → 100.0` branch with `confident=True`.

---

## 5. Smallest patch surface — two viable shapes

### Patch A — fix `restaurant_poi` matching to share semantics with the dsr side

Lowest-risk shape. Change the `in_category` predicate on the restaurant_poi UNION arm
to mirror what the dsr arm does — substring/regex match against the same patterns
returned by `_expand_category`. Sites:

1. `_bulk_enrich_competitors`, line 6111
   - Before: `(lower(rp.category) = ANY(:category_keys)) AS in_category`
   - After:  also tolerate `name`/`cuisine`-style columns and use `~* :category_regex`,
     OR widen the keys list to the alias regex tokens. Concretely the smallest version
     is: `lower(COALESCE(rp.category, '')) ~* :category_regex` (and continue passing the
     same `:category_regex` already bound for the dsr arm).
2. `_build_candidate_sql` competitor LATERAL, line 6413 — same change.
3. `_build_candidate_sql_no_district` competitor LATERAL, line 6515 — same change.

This restores discriminative power without touching:
- the producer function `_competition_whitespace_score`,
- the F4 confident gate,
- weights / Patch B chain_strength,
- the `_expand_category` alias map.

Risk: regex match is broader than equality — for densely-categorized burger sites,
`competitor_count` will jump from 0 to a real number, which slightly *lowers*
`competition_whitespace` for candidates that were previously getting the free 100. That
is the intended correction. Validate with the per-candidate replay query (4.d).
Performance: existing GIST index on `restaurant_poi.geom` still drives selectivity;
the predicate change is on a non-indexed column already filtered post-spatial. No new index
needed.

Note on the legacy paths: the legacy `_COMP_LATERAL` doesn't UNION dsr at all (lines
6411 and 6513 query restaurant_poi only), so even after Patch A those paths still
underweight burger-only delivery brands. If the legacy paths are exercised in the
production cohort path (they are not for cohort 46, but they are the fallback when
candidate_location is sparse), consider also UNION-ing dsr there. That is a strictly
larger surface and should be a follow-up, not in the same patch.

### Patch B — surface `_expand_category` keys that match how `restaurant_poi.category` actually stores values

Higher-leverage but wider blast radius. If the data audit (query 4.c) shows
`restaurant_poi.category` stores Google-Places-style buckets (`fast_food_restaurant`,
`meal_takeaway`, etc.), then keep the equality predicate but expand
`_CATEGORY_ALIAS_MAP["burger"]["keys"]` to include the bucket(s) Google actually emits
for burger venues — e.g. `["burger", "hamburger_restaurant", "fast_food_restaurant",
"meal_takeaway"]`. Single-file patch in `app/services/expansion_advisor.py:155–158`
(and parallel entries for `pizza`/`chicken`/`fast food`/etc. with the same audit).

Trade-off vs. Patch A:
- Pro: keeps the equality predicate (cheaper, more selective), preserves the existing
  symmetry between the bulk path and the two legacy paths.
- Con: cross-cuts every category, so requires the data audit per category to avoid
  over-counting non-burger fast food as burger competitors. Less defensible than the
  explicit-regex shape when the source column is genuinely free-form.

Patch A is the recommended default; Patch B becomes preferable only if `restaurant_poi.category`
is a closed taxonomy in production. The two are not mutually exclusive — Patch A alone is
sufficient to remove the always-100 saturation; Patch B alone fixes it only if the equality
predicate's misses are entirely due to alias-map gaps.

---

## Open questions (product judgment needed before patching)

1. **Should restaurant_poi match be regex or extended-equality?** Depends on whether
   `restaurant_poi.category` is a closed taxonomy (Google Places enum) or a free-form
   string. Run query 4.c first; choose Patch A vs Patch B accordingly. If the column is
   genuinely free-form (mixed Arabic/English, brand names), Patch A is the only honest fix.

2. **Should the legacy `_COMP_LATERAL` (lines 6411, 6515) also UNION delivery_source_record?**
   The current bulk path does; the legacy paths do not. This is a pre-existing asymmetry,
   not a regression of this bug, but it means burger-on-delivery-only brands are
   systematically under-counted on the ARCGIS fallback. Likely yes, but out of scope for
   the minimum fix.

3. **F4 calibration after Patch A.** Once `competitor_count` becomes non-zero for most
   Riyadh candidates, the F4 path (`confident is False AND competitor_count <= 0 → 50.0`)
   will fire less often, which is correct. But monitor for any cohorts where Patch A drops
   the score below the current 100 floor without introducing meaningful rank changes — if
   so, the calibration table at 2233–2241 may want a small refit. Not a prerequisite to
   merging Patch A.

4. **Ranking impact for already-shipped Burger searches.** The cohort-46 ranking is
   currently being driven by other Pillar-2 components and the residual 5.76 constant
   contributes nothing to ordering. After Patch A, candidates with denser burger
   environments will see their final_score drop by up to ~5 points (5.76 × (1 - 15/100));
   conversely, genuinely greenfield candidates retain their 5.76. Confirm with product
   that this re-ranking is desired and not a regression mask.
