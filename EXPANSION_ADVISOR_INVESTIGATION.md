# Expansion Advisor — High-Value Fixes & Improvements (Read-Only Investigation)

**Scope:** `app/services/expansion_advisor.py` (11,085 lines) + the ingest/economics/memo modules it calls. Riyadh-only F&B site selection. All findings are READ-ONLY; no application code was changed.

**Headline:** Candidate *sourcing* is sound — the live pipeline draws candidates **exclusively from listings** (`candidate_location` Tier-1 → `commercial_unit`), and the ArcGIS-parcel candidate SQL is **dead code**. The accuracy problems are downstream: a **fabricated zoning verdict on every listing**, **gates that don't actually gate**, and **NULL-rent listings scored as if their estimated rent were real**. Several user-facing preference toggles also barely move rankings.

---

## Prioritized findings

| # | Severity | Finding | Confidence | Effort |
|---|----------|---------|-----------|--------|
| 1 | **P1** | Every listing gets hardcoded zoning `commercial/100/pass`; real ArcGIS zoning is never spatially joined. Defeats the industrial/residential exclusion + zoning gate; kills 45% of `fit_score` discrimination. | CONFIRMED | M |
| 2 | **P1** | Gates don't gate: no `overall_pass` filter; `excluded_districts` is advisory; final sort is strict `-final_score` with no gate term → a gated/excluded candidate can rank #1. | CONFIRMED | S–M |
| 3 | **P1** | NULL-rent listings get a *district-estimate* rent but are percentile-scored at **full confidence** as if it were the listing's own rate, corrupting the 26%-weight economics + value badges. | CONFIRMED-IN-CODE (prevalence NEEDS-DB-VERIFY) | M |
| 4 | **P2** | `value_score` never enters the weighted total — only a post-hoc ±4/−6 nudge, and only in percentile mode. Several brief preferences are structurally near-zero (`price_tier` 0% for non-premium; `primary_channel` ≈1.35%; `parking_sensitivity` ≈0.35 pt). | CONFIRMED | S–M |
| 5 | **P2** | Memo read-cache is hard-disabled → every POST/pre-warm fires a full-cost LLM call; EN/AR share one column and mutually evict; first AR view regenerates inside a GET handler. | CONFIRMED | S |
| 6 | **P2** | Deterministic memo builders assert facts on absence-as-zero ("0 SAR rent", "open whitespace" when unmeasured); `_build_explanation` is 100% hardcoded English (untranslated in AR memos). | CONFIRMED | S–M |
| 7 | **P3** | Rank/score display divergence under LLM rerank: `rank_position` follows `final_rank`, `display_score` follows `final_score`. Dormant (flag off by default). | CONFIRMED | S |
| 8 | **P3** | Dead parcel-candidate SQL (`_build_candidate_sql*`) is a latent re-leak trap; fallback path drops `platform`; Wasalt unhandled end-to-end. | CONFIRMED | S |
| 9 | **P3** | `_dedupe_score_clones` can drop distinct candidates in the missing-district / zero-rent corner. | CONFIRMED | S |

---

## Pipeline overview (what's verified correct)

The live retrieval path in `run_expansion_search`:

- **Primary pool** — `_query_candidate_location_pool` (`app/services/expansion_advisor.py:5834`): `FROM candidate_location cl INNER JOIN commercial_unit cu ON cl.source_tier = 1 AND cl.source_id = cu.aqar_id` with `WHERE cl.source_tier = 1` (`:5995`, `:6019`). No `UNION`.
- **Fallback pool** — `_query_commercial_unit_candidates` (`:6080`): `FROM commercial_unit cu` only. Selected when `use_candidate_location = (_cl_count >= 10)` is false (`:6963`, `:7129`).
- Tier semantics (migration `alembic/versions/0020_candidate_location.py:28-29`): `source_tier 1=aqar, 2=delivery/poi, 3=arcgis`; `source_type ∈ {aqar, hungerstation, restaurant_poi, arcgis_parcel}`. The `candidate_location` table physically holds all three tiers, so **exclusivity rests entirely on the `source_tier = 1` predicate + the `INNER JOIN commercial_unit`** — correct today, but fragile (no test guard).
- The ArcGIS-parcel candidate builders `_build_candidate_sql` (`:6619`) and `_build_candidate_sql_no_district` (`:6814`) read `FROM ARCGIS_PARCELS_TABLE` but have **no call site** — dead code (see Finding 8).
- Enrichment joins are coordinate-based (`ST_DWithin` on candidate lon/lat) for population (`_bulk_enrich_population:6214`), competitors (`_bulk_enrich_competitors:6310`), delivery, roads (`:8136`), and parking (`:8243`). Competitor/delivery defaults are guarded by `confident` / `_delivery_observed` flags that separate "measured zero" from "no coverage." The exception is zoning (Finding 1).

Operation sequence after scoring (`:9468-9555`):

```
candidates.sort(_rank_sort_key)          # 9468  (pre-order for dedupe/balancing; gate_rank here is discarded)
_dedupe_candidates(...)                   # 9470
_dedupe_score_clones(...)                 # 9473
district balancing                        # 9484-9511
_apply_market_viability_pass(...)         # 9521  (stashes viability_delta = -10 * len(reasons))
_apply_score_deltas_and_sort(...)         # 9527  (folds deltas into final_score; sorts strict (-final_score, parcel_id))
candidates = candidates[:limit]           # 9529
_apply_rerank_to_candidates(...)          # 9540  (no-op unless EXPANSION_LLM_RERANK_ENABLED; default OFF)
enumerate -> rank_position/compare_rank   # 9542-9544
display_score = clamp(final_score,1,99)   # 9553-9555
```

The viability penalty is correctly **baked into `final_score`** and cannot be undone by the strict re-sort. The only positional reversal path is the LLM rerank, which is off by default (`config.py:148`).

---

## P1 findings

### 1. Every listing is assigned a fabricated `commercial / 100 / pass` zoning — real parcel zoning is never spatially joined

**Evidence.** The dominant query hardcodes the COALESCE default, and the fallback query hardcodes the literal:

- `app/services/expansion_advisor.py:5945-5946` — `COALESCE(cl.landuse_label, 'commercial')`, `COALESCE(cl.landuse_code, 2000)`. For Tier-1 listings `cl.landuse_*` is **always NULL** — the Tier-1 ingest insert (`app/ingest/candidate_locations.py:58-105`) omits the `landuse_*` columns entirely (contrast Tier-3 `_ingest_tier3_arcgis:242`, which carries real `p.landuse_code/label`).
- `app/services/expansion_advisor.py:6189-6190` — fallback: `'commercial' AS landuse_label, 2000 AS landuse_code`.
- The code map turns 2000 into a perfect pass: `app/services/expansion_advisor.py:1606` — `2000: ("commercial", 100, "pass")`, while `4000: (...,"fail")` (industrial) and `1000` (residential) are never assigned to a listing.

There is **no `ST_Contains/ST_Within`** point-in-polygon of the listing against `riyadh_parcels_arcgis_proxy` for zoning anywhere in the query path or ingest.

**Why it degrades accuracy.** `zoning_fit` is 45% of `fit_score` (≈`:7730`); `fit_score` feeds both `brand_fit` and `economics`. Because the value is a **constant 100 for all listings**, it (a) contributes zero discriminating power — 45% of `fit_score` is dead, (b) forces `zoning_verdict="pass"` so the **zoning gate always passes** (`:9074`, `_candidate_gate_status`), and (c) makes the **industrial hard-exclusion at `:7735` unreachable** for listings. A listing physically on a residential or industrial parcel is surfaced and badged as zoning-clean.

**Confidence.** CONFIRMED-IN-CODE (constant + map). Magnitude of mislabeled sites: NEEDS-DB-VERIFY (Q1, Q2).

**Effort / blast radius.** Medium. Touches either the Tier-1 ingest insert (add a point-in-polygon zoning lookup, mirroring Tier-3) or a bulk enrichment in the scoring loop (the road/parking bulk pattern at `:8136` is a ready template). Risk: changing zoning from "always pass" to real values will start **failing** some candidates — intended, but it moves results, so ship with the gate treating unknown zoning as `unknown`/neutral, not a silent pass.

**Suggested direction.** Spatially join listings to `riyadh_parcels_arcgis_proxy` for landuse at ingest; until then, treat NULL zoning as `verdict=unknown` + neutral score rather than `commercial/100/pass`, so the industrial exclusion and zoning gate behave honestly.

### 2. Gates don't gate — no `overall_pass` filter, `excluded_districts` is advisory, and the final sort ignores gate rank

**Evidence.**

- Only `zoning_fit_pass` and `area_fit_pass` are hard-fail (`_HARD_FAIL_GATES_BASE`, `:92-95`); everything else — including `district_pass` (driven by the user's `excluded_districts`, `:2787`), `economics_pass`, `cannibalization_pass`, `delivery_market_pass` — is advisory and can only set `overall_pass=None`, never `False`.
- There is **no filter on `overall_pass`** between gate computation (`:9077`) and persistence — `grep overall_pass` shows it used only as a sort tiebreak and in report summaries.
- The authoritative final ordering is `_apply_score_deltas_and_sort`, which sorts **strictly** `(-final_score, parcel_id)` (`:4791-4796`) — **no gate term**. The earlier `_rank_sort_key` (`:9439`) does include `gate_rank`, but it runs *before* the viability/score-delta pass (`:9468`) and is discarded.
- Population/commercial/construction hard floors exist (`:97-102`) but default to `0` (disabled) in `config.py`.

Net: with `area_fit` effectively always true (the candidate SQL pre-filters area BETWEEN min/max) and zoning always passing for listings (Finding 1), **no gate removes anything in production**, and a candidate in an explicitly excluded district (or any advisory failure) can sit at rank #1 on score alone.

**Why it degrades accuracy.** The operator's own constraints (excluded districts, economics floor, cannibalization tolerance) are silently non-binding. Wrong candidates appear and can outrank compliant ones.

**Confidence.** CONFIRMED-IN-CODE.

**Effort / blast radius.** Small–Medium. **Product decision (resolved with stakeholder): excluded districts should HARD-EXCLUDE** (remove from results, not flag/demote). Then either filter `overall_pass=False` out of the shortlist or add `gate_rank` as the primary sort key in `_apply_score_deltas_and_sort`. Blast radius: changes which candidates appear and their order; update the gate-summary UI copy accordingly.

**Suggested direction.** Make `district_pass` (explicit exclusion) hard-fail and remove failing candidates; fold a gate term into the final sort so remaining advisory-failed candidates can't outrank clean ones.

### 3. NULL-rent listings are percentile-scored at full confidence using a district estimate

**Evidence.**

- Rent *value* is real only when present: `:7861` — `if row.get("commercial_unit_id") and _cu_actual_rent and _cu_actual_area > 0:` … else `_estimate_rent_sar_m2_year(db, district)` (district estimate; default 900 SAR/m²/yr at `:3966`/`:797`).
- But `_is_listing = bool(row.get("commercial_unit_id"))` (`:7840`, `:8730`) is `True` regardless, so `_economics_score` takes the **percentile branch** (`:4506`) and passes the *estimated* rent as `listing_monthly_rent_per_m2` (`:4509`).
- `_rent_burden_confidence` only sees the percentile `source_label`, never `rent_source` (`:4537-4540`), so a `conservative_default`/estimated rent is **not damped** — it scores at the same confidence as a real listing rate. `commercial_unit.price_sar_annual` is nullable (`app/models/tables.py:410`); `candidate_location.rent_sar_annual` is nullable (`0020_candidate_location.py:44`).

**Why it degrades accuracy.** Economics is the **largest component (26.29%)**. For every NULL-rent listing, the burden score (and `best_value`/`above_market` badge) is computed against peers as if the estimate were observed — silently misordering by the heaviest weight. Note the weight-absorption coupling: `revenue_weight = 0.38 + (0.20 − rb_weight)` (`:4541-4542`), so any rent-confidence change re-weights revenue too — a fix must thread `rent_source` provenance into `_rent_burden_confidence`, not just patch the ledger at `:7861`.

**Confidence.** CONFIRMED-IN-CODE; production exposure NEEDS-DB-VERIFY (Q3).

**Effort / blast radius.** Medium. Thread rent provenance into `_economics_score`; when rent is estimated, either damp `rb_confidence` or mark the value band low-confidence. Blast radius: economics scores + value badges shift for the affected fraction.

---

## P2 findings

### 4. `value_score` never enters the weighted total; several brief preferences are structurally near-zero

**Evidence (weights confirmed in `_score_breakdown`, `:3012-3062`, sum asserted = 100):**
`occupancy_economics 26.29 · listing_quality 22.0 · brand_fit 9.64 · landlord_signal 7.01 · competition_whitespace 5.76 · demand_potential 8.76 · access_visibility 8.76 · delivery_demand 4.38 · chain_strength 3.0 · confidence 4.38`.

- `value_score` is computed only when `EXPANSION_VALUE_SCORE_ENABLED` **and** rent mode == `percentile` (`:4562-4566`); it never enters the economics `score` (`:4543-4549`). Its only ranking effect is a post-hoc `+4 / −6` delta in `_apply_score_deltas_and_sort` (`:4800-4815`).
- `price_tier` only acts as a one-sided penalty for `"premium"` (`:1566-1569`) → **exactly 0 effect for mid/value/budget/None** (and the default profile sets `price_tier=None`, `:1483`).
- `primary_channel` only enters via `_channel_fit_score × 0.14` inside `brand_fit` → effective ≈ **9.64% × 0.14 = 1.35%**.
- `parking_sensitivity` reaches only the brand_fit parking leg (no `access_visibility` path) → ≈ **0.35 pt** max swing. (`frontage_/visibility_sensitivity` do reach the 8.76% `access_visibility` leg and are meaningful.)
- Latent drift hazard: 8 of 10 `weighted_components` multipliers are hardcoded float literals (`:3046-3061`) rather than read from `component_weights`; re-tuning the dict without the literals would silently diverge displayed vs applied weights.

**Why it degrades accuracy.** Users toggle preferences (price tier, primary channel, parking sensitivity) expecting them to shape the ranking; they essentially never reorder a shortlist. `value_score`'s influence is a coarse ±-nudge that vanishes outside percentile mode.

**Confidence.** CONFIRMED-IN-CODE. **Effort:** Small–Medium (re-weight, or fold `value_score`/channel into the total). **Blast radius:** global re-tuning — guard with the existing weight-sum assert at `:3028`.

### 5. Memo read-cache is hard-disabled → repeated full-cost LLM calls; EN/AR mutually evict

**Evidence.** `app/api/expansion_advisor.py:1503` — `_decision_memo_cache_lookup` *"Always return None — memo cache reads are disabled."* So `post_decision_memo` (`:1622`) and `_prewarm_decision_memos` (`:767`) always fall through to `generate_structured_memo` (a live LLM call). Writes *do* persist (`:1545`) but `MEMO_PROMPT_VERSION` is written and never read. There is one `(decision_memo, decision_memo_json, decision_memo_lang)` triple per candidate, so switching locale overwrites the other (`:10804-10816`), and the first AR view regenerates **synchronously inside a GET handler** (`get_candidate_memo`, "COST NOTE" comment at `:10801`). LLM path is OpenAI `gpt-4o-mini` (`llm_decision_memo.py:2549`) with graceful `None` fallbacks; no deterministic non-LLM generator (POST 503s if both structured + legacy fail).

**Why it matters (cost, explicitly in scope).** Every memo view/pre-warm pays full LLM cost even when a fresh, version-matching memo exists. **Confidence:** CONFIRMED-IN-CODE; "0 memos persisted" claim NEEDS-DB-VERIFY (Q4). **Effort:** Small — re-enable the read with a prompt-version + lang key, and store per-locale.

### 6. Deterministic memo builders assert facts on absence-as-zero; `_build_explanation` is untranslated

**Evidence.** `_top_positives_and_risks:3136` flags "Economics score is below preferred threshold" when `_safe_float(economics_score)` is a *missing→0.0*; `_build_explanation:3663` emits "Relatively open competitive whitespace" on `competitor_count <= 3` with no observed-vs-inferred guard (unlike `_top_positives_and_risks`, which computes `delivery_observed`); `_build_cost_thesis:3426` templates "Estimated rent is {…:.0f} SAR/m²/year" → renders "0 SAR" on a default. `_build_explanation` (`:3655-3688`) is entirely hardcoded English with no i18n/structured sibling → leaks English into AR memos. Headline + watchout in `get_candidate_memo` (`:10673`, `:10778`) are also English regardless of `lang`.

**Why it degrades output.** The memo can assert confidently-wrong facts ("0 SAR rent", "open whitespace") and renders mixed-language in Arabic. **Confidence:** CONFIRMED-IN-CODE. **Effort:** Small–Medium — guard claims on data presence; route `_build_explanation` + headline through i18n.

---

## P3 findings

### 7. Rank/score display divergence under LLM rerank
`rank_position`/`compare_rank` follow `final_rank` (`:9542-9544`; rerank sorts by `final_rank` at `expansion_rerank` → service `:1109`), while `display_score` follows `final_score` (`:9553-9555`). With `EXPANSION_LLM_RERANK_ENABLED` (default **off**, `config.py:148`) a viability-demoted candidate can be rank-promoted above a higher-scoring peer (its −10 score penalty survives, but its position doesn't reflect it). Dormant today; verify the flag per environment (Q7).

### 8. Dead parcel SQL + platform gaps
`_build_candidate_sql`/`_build_candidate_sql_no_district` (`:6619`, `:6814`) read `FROM ARCGIS_PARCELS_TABLE` but have **no call site** — a latent re-leak trap if rewired (those rows would be `_is_listing=False`, ceiling-180 economics, real parcel zoning — inconsistent with the listing pool). The fallback `_query_commercial_unit_candidates` selects no `source_type` → `platform` silently `None` (`:9394-9398`). **Wasalt** is unreferenced anywhere despite the product framing; the platform allow-list is `("aqar","bayut")` only (`:9396`). Dedup keys are otherwise consistent — `parcel_id`/`source_id`/`aqar_id` stay prefixed; `_strip_platform_prefix` touches only `display_id` (`:9401`), so cross-platform double-counting was **not** found.

### 9. `_dedupe_score_clones` corner
Collapses on `district + final_score(±0.3) + exact rent + area(±5%)` (`:956-962`); two distinct candidates both missing district and both with zero/unmeasured rent can be merged. Bounded but real for rent-less listings.

### Context (not bugs)
Fit-out cost, implied check, and revenue index are heuristic constants (`:3983`, `:3998-4057`, `:4085`) — deterministic, so they don't misorder, but revenue (the heaviest economics input) is never real revenue. `listing_quality`→50 for non-listings (`:2592`) is moot while the pool is 100% listings. `_percentile_rent_burden` has sane envelope guards (`:4310-4341`) but district cohorts can be as small as `min_n=8` while still scoring at full confidence (`:4257-4260`).

---

## Top 3 by ROI

1. **Fix listing zoning (Finding 1).** Highest confidence, affects 100% of listings, and it's the only change that restores a *safety* gate (industrial/residential exclusion) while also recovering 45% of `fit_score`'s discriminating power. Self-contained (ingest spatial join or one bulk enrichment).
2. **Make gates bind (Finding 2).** Small diff, large correctness payoff: hard-exclude excluded districts (confirmed product decision) and add a gate term to the final sort so advisory-failed candidates can't outrank clean ones. Pairs naturally with #1 (once zoning can fail, the gate must actually gate).
3. **Damp estimated-rent economics (Finding 3).** Protects the heaviest score component from treating district estimates as observed listing rents. Thread `rent_source` into `_rent_burden_confidence` so the weight-absorption math stays consistent.

---

## NEEDS-DB-VERIFY — run together in one Codespace `psql` session

```sql
-- Q1 (Finding 1): confirm Tier-1 listings have NULL landuse (so the COALESCE->'commercial' always fires)
SELECT source_tier, COUNT(*) AS n, COUNT(landuse_code) AS has_code, COUNT(landuse_label) AS has_label
FROM candidate_location GROUP BY source_tier ORDER BY source_tier;
-- expect tier 1: has_code = has_label = 0; tier 3 fully populated.

-- Q2 (Finding 1): quantify mislabel impact -- listings physically on non-commercial parcels
SELECT p.landuse_code, COUNT(*) AS listings_on_this_zoning
FROM candidate_location cl
JOIN public.riyadh_parcels_arcgis_proxy p ON ST_Contains(p.geom, cl.geom)
WHERE cl.source_tier = 1 AND cl.is_cluster_primary AND cl.geom IS NOT NULL
GROUP BY p.landuse_code ORDER BY listings_on_this_zoning DESC;
-- rows with code IN (1000 residential, 4000 industrial, 3000 public) are wrongly scored commercial/pass.

-- Q3 (Finding 3): NULL-rent exposure on the dominant path
SELECT COUNT(*) FILTER (WHERE rent_sar_annual IS NULL) AS null_rent_tier1, COUNT(*) AS tier1_total
FROM candidate_location WHERE source_tier = 1 AND is_cluster_primary = TRUE;
SELECT COUNT(*) FILTER (WHERE price_sar_annual IS NULL OR price_sar_annual <= 0) AS null_rent, COUNT(*) AS total
FROM commercial_unit WHERE restaurant_suitable = TRUE AND status = 'active';

-- Q3b (Finding 3): how small are the district percentile cohorts (min_n=8 scores at full confidence)
SELECT neighborhood, listing_type, width_bucket(area_sqm,0,1000,5) AS band, COUNT(*) n
FROM commercial_unit
WHERE restaurant_suitable=TRUE AND status='active' AND price_sar_annual>0 AND area_sqm>0 AND area_sqm<=1000
  AND (price_sar_annual/area_sqm/12.0) BETWEEN 15 AND 350
  AND (property_type IS NULL OR lower(property_type) NOT IN ('warehouse','building','land','rest_house','farm'))
GROUP BY 1,2,3 ORDER BY n ASC;  -- look for many bands with 8 <= n < ~15

-- Q4 (Finding 5): are any memos actually persisted, and AR coverage
SELECT COUNT(*) FILTER (WHERE decision_memo_json IS NOT NULL) AS json_memos,
       COUNT(*) FILTER (WHERE decision_memo IS NOT NULL)      AS text_memos,
       COUNT(*) FILTER (WHERE decision_memo_lang = 'ar')      AS ar_memos
FROM expansion_candidate;

-- Q5 (sanity): source-tier primary distribution (proves Tier 2/3 exist but are excluded as candidates)
SELECT source_tier, COUNT(*) FILTER (WHERE is_cluster_primary) AS primaries
FROM candidate_location GROUP BY source_tier ORDER BY source_tier;

-- Q6 (Finding 8): does any active unit carry platform='wasalt' (whose label would be dropped)?
SELECT platform, COUNT(*) FROM commercial_unit WHERE status='active' GROUP BY platform;

-- Q7 (Finding 7): did the LLM rerank actually move anything (confirms flag state in prod)
SELECT rerank_status, COUNT(*) FROM expansion_candidate GROUP BY rerank_status ORDER BY 2 DESC;
```

> Adjust the persisted table name if it differs from `expansion_candidate` in `app/models/tables.py`.

---

*Read-only investigation. No application code, branches, or commits were created. Implementation is a separate, explicitly-approved pass.*
