# Expansion Advisor — Weighted Blast-Radius Ranking

> **READ-ONLY investigation.** No files edited, no branch created, no commit/push. This document is the deliverable.

**Scope confirmed against the live tree.** `final_score` is computed in `_score_breakdown` (`app/services/expansion_advisor.py:2943`), called once per candidate at `:7981`. The ten top-level component weights are read at `:3009-3020` (sum-to-100 asserted at `:3025`). Everything below is decomposed from those weights × the sub-weights inside each `_*_score`.

---

## 1. Effective-weight table (raw signal → effective % of `final_score`)

Assumption set for the numbers (stated so they're reproducible): medium brand sensitivity (`_sensitivity_weight` default 0.6, `:1500`), `balanced` expansion goal, percentile rent_burden at full weight (`revenue_weight = 0.38`, `:4539`), `EXPANSION_CHAIN_STRENGTH_WEIGHT=3.0` (`config.py:336`). Ranges given where a brand profile or a damping path moves the number.

| Raw signal | Components it feeds (file:line) | Effective % of `final_score` |
|---|---|---|
| **`street_width_m`** (measured Aqar frontage) | access_visibility (100% of it: `_frontage_score`→`_frontage_score_from_street_width` `:1750`/`:1709`, `_access_score`→`:1768`/`:1726`, blended `:1894`); revenue_index street_signal 35% inside occupancy_economics (`:4113-4114` → `_economics_score` revenue leg `:4541`); brand_fit visibility_signal (`:1578`); confidence +15 block (`:2424`) | **≈ 14% (13–18%)** — see breakdown below |
| district momentum | listing_quality sub-weight 0.35 (`:2664`), top weight 22% (`:3011`) → `_district_momentum_score` `:383` | **7.70%** |
| freshness (aqar dates) | listing_quality 0.30 (`:2660`); `_effective_listing_age_days` `:2448` | **6.60%** |
| rent comps (percentile rent_burden) | occupancy_economics rb_weight 0.20 (`:4538`), `_percentile_rent_burden` `:4504` | **0–5.26%** (damped, see §2) |
| population_reach | demand_potential 8.764% × pop_w (0.6 qsr, `:2337`/`:7718`); `_population_score` `:2254` | **≈ 5.26%** (qsr); 2.2–6.6% by model |
| LLM suitability | listing_quality 0.20 (`:2661`); `:2620` | **4.40%** |
| delivery supply/realized demand | delivery_demand 4.382% (`provider_intelligence_composite` `:7958`) + demand_potential ×del_w ~3.5% (`:7718`); `_delivery_score` `:2280` | **≈ 4–8%** |
| landlord_signal (LLM) | own component 7.0112% (`:3013`); `_landlord_signal_component` `:2929` | **7.01%** |
| competitor_count (whitespace) | competition_whitespace 5.764% (`:3008`); `_competition_whitespace_score` `:2340` | **5.76%** |
| area_m2 (revenue throughput) | occupancy_economics revenue leg 0.38×0.20 (`:4173`) + brand_fit/fit_score | **≈ 2.0% +** |
| LLM listing quality / image | listing_quality 0.10 (`:2662`); `:2631` | **2.20%** |
| max_chain_strength | chain_strength 3.0% (`:3015`); `_chain_strength_score` `:2380` | **3.00%** |
| road_context (touches/arterial) | only rent multiplier `_road_signal_from_context` `:3707` → rent_burden | **< 0.5%** (indirect) |

### `street_width_m` total — explicit sum

- access_visibility: `8.764% × 1.00` = **8.76%** (for a listing with measured width, *both* frontage and access derive 100% from it; `:1750`, `:1768`)
- occupancy_economics: `26.2924% × 0.38 × 0.35` = **3.50%** (rises to ~5.3% when rent_burden is damped and `revenue_weight`→0.58, `:4539`)
- brand_fit: `9.6404% × (0.08 + 0.6×0.05)=0.11` = **1.06%** (+~0.77% if goal=`flagship`, `:1547`)
- confidence: `4.382% × 0.15` = **0.66%** (the +15 block, `:2424`)

**Total ≈ 13.98% ≈ 14% of `final_score`, range ~13–18%.** This is by far the single largest dependency on one raw input — confirming the leading suspect.

---

## 2. Failure mode per signal (honest vs silent-fiction)

| Signal | What fires when missing (file:line) | Verdict |
|---|---|---|
| **`street_width_m`** | frontage/access → `_frontage_score`/`_access_score` get `road_context_available=False` for listings (`:7916`,`:7923`) → return **50.0 neutral** (`:1757`,`:1771`); revenue street_signal → **50.0** (`:4116`); confidence → **no +15** (honest downweight, `:2424`) | **Honest-neutral.** Not fiction — missing width regresses to mid, and confidence *does* drop. Blast radius is therefore gated by miss-frequency, not by fabrication. **The curves themselves (`:1709`,`:1726`) are hand-set "Riyadh norms," uncalibrated to any outcome.** |
| district momentum | districts < `_MOMENTUM_SAMPLE_FLOOR=20` (`:2508`) → `.get()` None → **50.0 neutral** (`:2656`) | Honest-neutral (tri-state). But comment claims only 69% of rows covered (`:2507`) → ~31% inert. |
| freshness | all 3 dates null → `(None,"unknown")` (`:2480`) → **50.0** (`:2595`); future-date guard `:2476` | Honest-neutral |
| rent comps | thin district pool → city pool, **and `_rent_burden_confidence` damps weight to 0.25/0.15/0.0** (`:4259-4262`); comp None → `absolute_fallback` 100−rent/220 at full weight (`:4516`) | **Honest** — the rare *correct* design here: it lowers its own weight, deficit absorbed by revenue (`:4539`) |
| population_reach | `reach<=0` → **score 0.0** (`:2275`) — a **penalty**, not neutral | Honest-but-harsh: missing population *crushes* demand rather than neutralizing |
| competitor_count | `confident=False & count=0` → **50.0** (`:2369`); but `confident=None & count=0` → **100.0** (`:2371`) | **Silent-fiction risk** when `confident` is None — absence scores as "wide open." Mitigated on the listings path (`confident` passed, `:7721`) but `None` on the ArcGIS-fallback pool (`:7654`) |
| landlord/suitability/quality (LLM) | None → **50.0** neutral (`:2939`, `:2625`, `:2634`) | Honest-neutral |
| chain_strength | None → **50.0** (`:2395`) | Honest-neutral |
| road_context | missing or `>= _ROAD_DISTANCE_SENTINEL_M` → **0.5 neutral** (`:3723`,`:3736`) | Honest-neutral (the cited "not a real measurement" sentinel is, on reading, a documented neutral — *not* fiction) |
| realized_demand | flag off or `branches<3` → falls to listing-count supply proxy (`:7704`,`:2309`) | Honest (different, weaker signal) |

**Net:** there is far less silent-fiction here than the prior pass implied. The codebase consistently uses honest neutral-50 fallbacks. The two genuine integrity concerns are (a) `competition_whitespace` 0→100 on the `confident=None` path, and (b) the categorical pool issue in §4.

---

## 3. Ranked shortlist by weighted blast radius (top 5)

1. **`street_width_m`** · eff. weight **~14%** · failure = honest-neutral substitution + **uncalibrated hand-set curves** · miss-freq **UNCONFIRMED — needs probe** (code comment claims 95% coverage `:1703`, never verified) · right tool: **deterministic — backfill coverage + (separately) calibrate curves against a label we partly have (rent/area)**; *no AI needed* · effort: low (coverage audit) / medium (curve recalibration) · proof metric: **% of active Tier-1 candidates with `street_width_m>0`**, and rank-correlation stability when the curve is reshaped.
2. **REGA license-expiry pool gate** (see §4) · eff. weight = N/A (categorical) · failure = **expired/invalid listings can rank #1** · freq **needs probe** · right tool: **deterministic SQL gate + denormalize `aqar_license_expiry`** · effort: low · proof metric: count of top-ranked candidates with expired/null license before vs after.
3. **district momentum** · eff. weight **7.70%** · honest-neutral · ~**31% of rows neutral-50** (claimed `:2507`, needs probe) · right tool: **product/data — can't manufacture density; consider lowering the floor or a city-level prior**; no AI · effort: medium · proof metric: share of candidates resolving to neutral-50 momentum.
4. **population_reach** · eff. weight **~5.3%** · failure = **0-penalty** when missing (`:2275`) · freq needs probe · right tool: **deterministic spatial join (population_density)**; fix is to neutral-50 the missing case OR confirm full coverage · effort: low · proof metric: % candidates with `population_reach=0`.
5. **competition_whitespace `confident=None`→100** · eff. weight **5.76%** · **silent-fiction** on the ArcGIS-fallback pool (`:2371`,`:7654`) · freq = fraction of pool on the non-bulk-enriched path · right tool: **deterministic — pass `confident=False` (or default it) on all paths**; no AI · effort: trivial · proof metric: count of candidates with whitespace=100 & competitor_count=0 & `confident IS NULL`.

---

## 4. Probes I need run (verified column names, paste-ready)

Columns verified against `app/models/tables.py`: `commercial_unit.aqar_id/street_width_m/status/restaurant_suitable/aqar_advertisement_license/aqar_license_expiry` (`:404-447`); `candidate_location.street_width_m/source_tier/is_cluster_primary/population_run_id/rega_advertisement_license` (`:508-572`). Note: **`candidate_location` has no `population_reach` and no `aqar_license_expiry` column** — population is computed at query time; expiry lives only on `commercial_unit`.

```sql
-- P1 (decides #1): street_width coverage on the LATEST candidate pool, Tier-1 listings, dedup-primary.
WITH r AS (SELECT population_run_id FROM candidate_location
           ORDER BY created_at DESC LIMIT 1)
SELECT count(*) AS candidates,
       count(*) FILTER (WHERE street_width_m IS NOT NULL AND street_width_m > 0) AS with_width,
       round(100.0*count(*) FILTER (WHERE street_width_m IS NOT NULL AND street_width_m>0)/nullif(count(*),0),1) AS pct_with_width
FROM candidate_location
WHERE population_run_id = (SELECT population_run_id FROM r)
  AND source_tier = 1 AND is_cluster_primary = TRUE;

-- P2 (pool integrity / REGA): among candidate-ELIGIBLE listings, how many have expired or no license?
SELECT count(*) AS eligible,
       count(*) FILTER (WHERE aqar_advertisement_license IS NULL OR btrim(aqar_advertisement_license)='') AS no_license,
       count(*) FILTER (WHERE aqar_license_expiry IS NOT NULL AND aqar_license_expiry < now()::date) AS expired,
       count(*) FILTER (WHERE aqar_license_expiry IS NULL) AS expiry_unknown
FROM commercial_unit
WHERE status='active' AND restaurant_suitable = TRUE;

-- P3 (momentum): verify the "37 districts / 69% rows" claim at floor=20.
WITH ld AS (
  SELECT cu.aqar_id, dp.district_label
  FROM commercial_unit cu
  JOIN external_feature_polygons_mat dp
    ON ST_Contains(dp.geom, ST_SetSRID(ST_MakePoint(cu.lon,cu.lat),4326))
  WHERE cu.status='active' AND cu.lat IS NOT NULL AND cu.lon IS NOT NULL AND dp.district_label IS NOT NULL),
dc AS (SELECT district_label, count(*) n FROM ld GROUP BY district_label)
SELECT count(*) FILTER (WHERE n>=20) AS qualifying_districts,
       round(100.0*sum(n) FILTER (WHERE n>=20)/nullif(sum(n),0),1) AS pct_rows_covered
FROM dc;

-- P4 (population): % of candidate pool that would hit the 0-penalty.
-- candidate_location has no population_reach column; confirm the pool-SQL join source first.
-- If population is persisted on location_score, probe there; otherwise this must be measured
-- inside the expansion_search path. Marked NEEDS-SOURCE-CONFIRM.

-- P5 (whitespace fiction): pool rows that score whitespace=100 from absence.
WITH r AS (SELECT population_run_id FROM candidate_location ORDER BY created_at DESC LIMIT 1)
SELECT source_tier, count(*) FROM candidate_location
WHERE population_run_id=(SELECT population_run_id FROM r)
GROUP BY source_tier ORDER BY source_tier;  -- size of non-Tier-1 (confident=None) pool
```

---

## 5. The single recommendation

**Do the `street_width_m` work — and run probe P1 first, because that one number decides the shape of the fix.**

**Evidence chain.** `street_width_m` carries **~14% of `final_score`** — `8.76` (access_visibility, 100% derived) + `3.50` (revenue index) + `1.06` (brand_fit) + `0.66` (confidence) — more than 2× the next-largest single-input dependency (momentum, 7.7%). No other signal concentrates this much score on one scraped field. Its fallback is honest-neutral, so **blast radius = 14% × miss-rate**, and the miss-rate is the great unknown: the only basis for "95% covered" is an *unverified code comment* (`:1703`).

**What flips the pick — the one number:** P1's `pct_with_width`.

- If **< ~85%**: street_width is unambiguously #1. The fix is *deterministic*, no AI: (a) backfill street width from the detail scraper / a parcel-frontage spatial join, and (b) since 14% of score rides on a hand-set curve, sanity-check that the curve's neutral-50 miss path isn't silently advantaging width-less listings in access_visibility (it shouldn't, but confidence is the only honest penalty today).
- If **≥ ~95%** as claimed: street_width's *coverage* is fine, the highest-value fix **flips to the REGA license-expiry gate (#2)** — a categorical trust failure (an expired/delisted listing can rank #1), fixable with a cheap deterministic SQL gate + one denormalized column, no model and no labels.

**Smallest first step:** run P1 and P2. Two counts settle the entire ranking — whether the next patch is a street-width coverage/curve fix or a license-validity pool gate. I recommend not writing either patch until those two numbers are in hand.

**What I deliberately did *not* recommend:** an AI/model approach anywhere in the top 5. Every top item is a join, a SQL gate, a coverage backfill, or a curve recalibration. The only place a model could help (calibrating the street-width→frontage curve to realized revenue) needs outcome labels the repo does not have.
