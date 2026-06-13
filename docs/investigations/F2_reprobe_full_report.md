# F2 Re-Probe — Cross-Source Competitor Overlap (with REAL category terms)

**Investigation report — Oaktree Atlas / Expansion Advisor**
**Date:** 2026-06-13
**Branch:** `claude/investigate-ea-f2-reprobe-z3qjdb`
**Mode:** READ-ONLY sizing. No app code changed.
**Artifacts:**
- Probe: `scripts/diagnostics/competitor_cross_source_overlap_v2.sql` (`psql -x -f`)
- Findings note: `docs/investigations/ea_audit_2026-06-13.md`

---

## 1. Executive summary

The first probe (`competitor_cross_source_overlap.sql`) sized cross-source
competitor double-counting at overlap-share **p50 0.379 / p75 0.433 / p90 0.500**,
but it matched competitors with a **single literal token `'burger'`** — which is
**not** how production counts. That biases the `_WHITESPACE_LOG_REF` re-anchor.

This re-probe measures the overlap **exactly the way production
(`_bulk_enrich_competitors`) matches the in-category set**: multi-key POI
matching + alias regex on the delivery side. It is a **sizing-only** probe —
it does not patch anything. Ahmed runs it in Codespace and pastes the numbers
into the results tables (Section 6).

**Three findings are already firm from the code (do not need the probe run):**

1. **The `cafe` POI signal is structurally dark.** The `cafe` category's POI
   keys `{coffee_bakery}` match **~0** real `restaurant_poi.category` rows,
   because `restaurant_poi.category` is never assigned the meta-bucket
   `coffee_bakery`. Any cafe POI/overlap number is an **artifact**, not a real
   "no competitors." (Detail in §4.1.)
2. **`expansion_candidate.source_type` no longer exists** — dropped by migration
   `20260330_exp_adv_commercial_units`. The prompt listed it; the probe does
   not reference it. (§4.3.)
3. **The probe's term sets match the live `_CATEGORY_ALIAS_MAP` exactly** — no
   discrepancy between the prompt's copies and the live tree. (§3.)

---

## 2. Why a re-probe — how production actually counts

Production counts same-category competitors in `_bulk_enrich_competitors`
(`app/services/expansion_advisor.py:7503+`), in the `combined` CTE that
`UNION ALL`s two sources:

```sql
-- Source 1: restaurant_poi (Google Places)
SELECT (lower(rp.category) = ANY(:category_keys)) AS in_category, ...
FROM restaurant_poi rp
WHERE (rp.business_status IS NULL OR rp.business_status = 'OPERATIONAL')
  AND ST_DWithin(rp.geom::geography, <candidate>, :radius_m)
UNION ALL
-- Source 2: delivery_source_record (HungerStation etc.)
SELECT (lower(COALESCE(dsr.category_raw,'')) ~* :category_regex
        OR lower(COALESCE(dsr.cuisine_raw,'')) ~* :category_regex) AS in_category, ...
FROM delivery_source_record dsr
WHERE <dsr_geom_or_latlon_guard>
  AND ST_DWithin(<dsr_geo>, <candidate>, :radius_m)
```

`competitor_count = COUNT(*) FILTER (WHERE in_category)` over the union — i.e.
**`poi_in_category + dsr_in_category`, with no cross-source dedupe.** That is
the inflated count we are sizing.

`category_keys` and `category_regex` come from `_expand_category(category)`
(`:558-574`):

```python
keys  = aliases["keys"]
regex = "|".join(re.escape(p).replace(r"\.", ".") for p in aliases["raw_patterns"])
```

So `regex` is just the `raw_patterns` joined by `|` with `.` kept as a wildcard.
The old `'burger'`-token probe ignored both the multi-key POI side and the
alias regex DSR side, undercounting the true in-category set.

---

## 3. Term sets — verified verbatim against the live tree

Quoted from `_CATEGORY_ALIAS_MAP` (`app/services/expansion_advisor.py:154-223`).
**These match the prompt's table exactly.**

| Probe category | POI keys (`= ANY`)                 | DSR regex (`~*`) |
|----------------|------------------------------------|------------------|
| `burger`       | `{burger}`                         | `burger\|hamburger\|برجر` |
| `fast food`    | `{burger,pizza,chicken,fast_food}` | `fast.food\|fast_food\|qsr\|burger\|hamburger\|chicken\|broasted\|fried.chicken\|pizza\|pizzeria\|وجبات سريعة\|برجر\|دجاج\|بيتزا\|فاست فود` |
| `cafe`         | `{coffee_bakery}`                  | `cafe\|coffee\|bakery\|dessert\|pastry\|قهوة\|مقهى\|كافيه\|مخبز\|حلويات` |
| `chicken`      | `{chicken}`                        | `chicken\|broasted\|fried.chicken\|wings\|دجاج` |

`_CHAIN_NAME_NORM_SQL` (`app/ingest/expansion_advisor_competitors.py:54-66`) is
mirrored inline in the probe for the normalized-name overlap rule (lower →
Arabic Alef/Ya/tatweel TRANSLATE → strip non-alnum/non-Arabic → collapse
whitespace → trim).

---

## 4. Code-verified findings (firm before the probe runs)

### 4.1 `cafe` → `{coffee_bakery}` matches ~0 real POI rows (artifact)

`restaurant_poi.category` is assigned **only** through `normalize_category()`
(`app/services/restaurant_categories.py:106`, driven by
`app/ingest/restaurant_pois.py` for every source — Overture/OSM/delivery).
`normalize_category()` returns one of the **granular** keys:

```
burger, pizza, chicken, shawarma, grills, traditional, japanese, chinese,
indian, korean, thai, italian, asian, seafood, coffee, bakery, dessert,
juice, sandwich, healthy, breakfast, international
```

It **never returns `coffee_bakery`** — that token is a downstream *meta-bucket*
(used by delivery-bucket mapping and the category display map), not a value
ever stored in `restaurant_poi.category`.

**Consequence:** for every `cafe`/`coffee` search, the production POI side of
`_bulk_enrich_competitors` matches **zero** rows. The entire cafe competitor
signal comes from the **DSR (delivery) side only**. Therefore:

- The cafe `poi_count` in this probe will be ~0 (confirmed by result set **[C]**).
- Cafe overlap is effectively undefined — there is no POI set to dedupe DSR rows
  against, so `overlap_share ≈ 0` and `deduped ≈ union` **by construction**, not
  because cafes have no real cross-source duplication.

This is **flagged, not silently reported as 0**. It is a real production
observation worth a **separate follow-up** (the cafe POI leg is dark), but it
is **out of scope** for this sizing-only re-probe.

> Note: `fast_food` (one of the four `fast food` keys) is *also* never emitted
> by `normalize_category()`, but `burger`/`pizza`/`chicken` are, so the
> `fast food` POI side stays well covered; only the redundant `fast_food` key
> contributes nothing.

### 4.2 Radius

The probe uses the **1000 m** qsr/dine_in/delivery_first tight competition
radius (`_catchment_radii`). Note the `cafe` **service model** (distinct from
the `cafe` *category*) uses an **800 m** competition radius in production
(`app/services/expansion_advisor.py:837`) and sits on the **default REF=25**.
The 1000 m probe radius intentionally matches the three models that carry a
non-default `_WHITESPACE_LOG_REF`.

### 4.3 `expansion_candidate.source_type` was dropped

Dropped by migration `20260330_exp_adv_commercial_units` and not re-added. The
prompt listed it among candidate columns; the probe does **not** reference it.
Columns actually used: `expansion_candidate.{parcel_id, lat, lon, search_id,
computed_at}` joined to `expansion_search.{id, category, service_model,
created_at}`.

### 4.4 DSR `geom`

Confirmed present (migration `20260322_geom_indexes_dsr_pop`, trigger-synced
from lat/lon). The probe resolves the `_dsr_has_geom` branch the same way
production does and uses `geom` when present, else the lat/lon construct.

### 4.5 Other column/table verification

| Table | Columns confirmed |
|-------|-------------------|
| `restaurant_poi` | `name`, `category`, `business_status`, `geom` (Point/4326, NOT NULL, trigger from lat/lon — migration 0010), `lat`, `lon` |
| `delivery_source_record` | `restaurant_name_raw`, `category_raw`, `cuisine_raw`, `lat`, `lon`, `geom` (migration 20260322) |
| `expansion_candidate` | `id`, `search_id` (FK → `expansion_search.id`), `parcel_id`, `lat`, `lon`, `computed_at` |
| `expansion_search` | `id`, `created_at`, `category`, `service_model` |

---

## 5. Methodology (what the probe computes)

For each of the four categories, over **≤60 most-recent distinct candidate
centroids** pulled from `expansion_candidate` joined to category-matched
`expansion_search`, at a **1000 m** radius:

1. `n_candidates` — sampled centroids.
2. `poi_count` — same-category POI within radius (production match:
   `lower(category)=ANY(keys)`, `business_status` NULL or `OPERATIONAL`).
3. `dsr_count` — same-category DSR within radius (production regex match on
   `category_raw`/`cuisine_raw`, geom-or-lat/lon guard).
4. `union_count = poi_count + dsr_count` — today's inflated count.
5. **Overlap (cross-source duplicate):** a DSR row is counted as a duplicate
   when, within the same candidate radius, it matches **any** in-category POI by
   **either**:
   - normalized-name equality (`_CHAIN_NAME_NORM_SQL`, non-empty), **or**
   - spatial proximity **≤ 75 m**.

   Each DSR row is counted at most once (`DISTINCT`).
6. `deduped_count = union_count − duplicates`.
7. `overlap_share = duplicates / union_count` (NULL when no competitors).

**Outputs:**

- **[A] Per-category:** `union`, `deduped`, `overlap_share` at **p50/p75/p90**,
  plus dominant service_model and its current REF.
- **[B] Pooled (all four categories):** blended `union`/`deduped`/`overlap_share`
  p50/p75/p90.
- **[C] POI keys-match diagnostic:** `poi_rows_matching_keys` and the distinct
  matched `restaurant_poi.category` values per category (proves the cafe caveat).
- **[C2]** Top `restaurant_poi.category` literals actually stored (context).
- **[D] Anchor sizing:** per-`service_model` **current vs suggested**
  `_WHITESPACE_LOG_REF`, where **suggested = deduped p90 (rounded)** — clearly
  labeled *for review, NOT applied*.

### Anchor-sizing rationale

`_WHITESPACE_LOG_REF` (`app/services/expansion_advisor.py:2789`) is the
per-service-model count where the whitespace log curve reaches its 15.0 floor
(`raw = 100·(1 − log1p(count)/log1p(REF))` floors structurally at `count = REF`).
Current values: **qsr=75, dine_in=50, delivery_first=50, default=25.** These
were sized against the *un-deduped* tail. After cross-source dedupe the counts
shrink, so the re-anchored REF should track the **deduped p90 tail** (same rule
the existing comment uses). The probe prints both side by side so the target is
readable directly.

---

## 6. Results — TO FILL IN after running the probe

> Run: `psql -x -f scripts/diagnostics/competitor_cross_source_overlap_v2.sql`
> `current_ref` is informational; `suggested_ref` is **for review, NOT applied.**

### [A] Per-category (overlap share + union/deduped p50/p75/p90)

| Category | n | n>0 | dom. SM | union p50/75/90 | deduped p50/75/90 | overlap p50/75/90 |
|----------|---|-----|---------|-----------------|-------------------|-------------------|
| burger    |  |  |  |  |  |  |
| cafe      |  |  |  |  |  | (POI≈0 — see [C]) |
| chicken   |  |  |  |  |  |  |
| fast food |  |  |  |  |  |  |

### [B] Pooled (all categories)

| scope | n | n>0 | union p50/75/90 | deduped p50/75/90 | overlap p50/75/90 |
|-------|---|-----|-----------------|-------------------|-------------------|
| ALL   |  |  |  |  |  |

### [C] POI keys-match diagnostic (confirms the cafe caveat)

| Category | poi_rows_matching_keys | matched_category_values |
|----------|------------------------|-------------------------|
| burger    |  |  |
| cafe      | (expect ≈0) | (expect `{}` / NULL) |
| chicken   |  |  |
| fast food |  |  |

### [D] Anchor sizing — current vs suggested `_WHITESPACE_LOG_REF` (per service_model)

| service_model | current_ref | n | union_p90 | deduped_p90 | suggested_ref (review) |
|---------------|-------------|---|-----------|-------------|------------------------|
| qsr            | 75 |  |  |  |  |
| dine_in        | 50 |  |  |  |  |
| delivery_first | 50 |  |  |  |  |
| (other)        | 25 |  |  |  |  |

---

## 7. Interpreting the numbers (decision guide)

- **If deduped p90 ≪ current REF** for a model → the curve is flooring too early
  on inflated counts; re-anchor REF down toward deduped p90 to restore spread.
- **If deduped p90 ≈ current REF** → the current REF already approximates the
  true (deduped) tail; the `'burger'`-probe bias was not material for that model.
- **Cafe row:** treat overlap/POI as N/A — the POI leg is dark (§4.1); any cafe
  re-anchor must be reasoned from the DSR-only count, and the dark POI leg is a
  separate follow-up.
- The **pooled [B]** row is the blended cross-check against the old probe's
  0.379 / 0.433 / 0.500 share — but the **per-service_model [D]** table is the
  authoritative anchor input, since REF is service-model-keyed.

---

## 8. Scope guardrails (what was NOT done)

- No patch to `_bulk_enrich_competitors`, `_WHITESPACE_LOG_REF`, or
  `_CHAIN_NAME_NORM_SQL`.
- No dedupe implementation, no REF edit, no cleanups.
- Sizing numbers only. **STOP** — Ahmed runs the probe and pastes results; the
  REF re-anchor decision (and the separate cafe POI-key follow-up) are made from
  the tables above.
