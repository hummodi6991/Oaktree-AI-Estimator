# Expansion Advisor — `chain_strength` leg: MAX-saturation → strong-chain SHARE

**Status:** Implemented, committed, and pushed to
`claude/scoring-ranking-integrity-investigation-fxrOq` (commit `66783b7e7`).
**Not merged, no PR** — for diff review.

**Files changed:** `app/core/config.py` (+17), `app/services/expansion_advisor.py` (+75/-10).
**Untouched by request:** `_chain_strength_score`, the ECQ ingest ladder in
`app/ingest/expansion_advisor_competitors.py`, and the `_score_breakdown`
weight math (`chain_strength` stays 3.0%).

---

## 1. What was wrong

The `chain_strength` leg input was
`MAX(chain_strength) FILTER (WHERE in_category)` over same-category POIs in the
competition radius, where `chain_strength = expansion_competitor_quality.chain_strength_score`.

On the ingest ladder (`LEAST(100.0, COALESCE(chain_size,1)*12.0)`), any brand
with **≥9 operational citywide locations** scores **100**. Because the leg input
is a **MAX**, a single such chain anywhere in the radius pins the input to 100.

In Riyadh QSR/burger, ≥9-branch chains (McDonald's, Burger King, Hardee's,
Herfy, Kudu, …) are ubiquitous, so virtually every populated radius returned
`max_chain_strength = 100`. The leg therefore contributed a flat
`3.0% × 100 = ~3` points to nearly every candidate — **non-discriminative**
(degenerate in practice) for chain-dense categories.

## 2. Why it happens

`MAX` is an extreme-value aggregate: it answers "is there *any* strong chain in
radius?", which is ~always "yes" in a dense F&B market. It cannot distinguish a
district with one big chain among many independents from a district that is
wall-to-wall established brands.

## 3. The fix (smallest targeted change)

Replace the leg **input** with a **strong-chain SHARE**: among same-category,
ECQ-matched POIs in the radius (`chain_strength IS NOT NULL` excludes the NULL
delivery side), the **percentage** whose `chain_strength_score` is "strong"
(`>= EXPANSION_CHAIN_STRONG_THRESHOLD`). Computed only when at least
`EXPANSION_CHAIN_MIN_MATCHED` matched POIs exist; otherwise `NULL`.

This measures *how saturated with established operators* the radius is, on a
continuous 0–100 scale, instead of a binary "≥1 big chain present".

The radius `MAX` is **retained** (not removed) purely to keep feeding the
`inputs.chain_strength_max` JSON diagnostic in `score_breakdown_json`.

### New env vars (`app/core/config.py`, mirroring the `EXPANSION_CHAIN_STRENGTH_WEIGHT` pattern)

| Env var | Default | Meaning |
|---|---|---|
| `EXPANSION_CHAIN_STRONG_THRESHOLD` | `60.0` | An ECQ `chain_strength_score` at/above this counts as a strong/established chain (≈ a 5+ branch chain: `chain_size 5 → 60`). |
| `EXPANSION_CHAIN_MIN_MATCHED` | `3` | Minimum in-category ECQ-matched POIs before a share is trustworthy; below it the leg input is `NULL` → Python `None` → neutral `50.0`. |

## 4. The diff

```diff
diff --git a/app/core/config.py b/app/core/config.py
@@ class Settings:
     EXPANSION_CHAIN_STRENGTH_WEIGHT: float = float(
         os.getenv("EXPANSION_CHAIN_STRENGTH_WEIGHT", "3.0")
     )
+
+    # Strong-chain SHARE calibration for the chain_strength leg input.
+    # Replaces the MAX-over-radius leg input (which saturated at 100 for any
+    # radius containing a single big chain) with the SHARE of same-category,
+    # ECQ-matched POIs in the radius whose chain_strength_score is "strong".
+    # EXPANSION_CHAIN_STRONG_THRESHOLD: an ECQ chain_strength_score at/above
+    #   this counts as a strong/established chain (default 60.0 ≈ a 5+ branch
+    #   chain on the ingest ladder LEAST(100, chain_size*12)).
+    # EXPANSION_CHAIN_MIN_MATCHED: minimum number of in-category ECQ-matched
+    #   POIs required before a share is trustworthy; below it the leg input is
+    #   NULL → Python None → _chain_strength_score keeps the neutral 50.0.
+    EXPANSION_CHAIN_STRONG_THRESHOLD: float = float(
+        os.getenv("EXPANSION_CHAIN_STRONG_THRESHOLD", "60.0")
+    )
+    EXPANSION_CHAIN_MIN_MATCHED: int = int(
+        os.getenv("EXPANSION_CHAIN_MIN_MATCHED", "3")
+    )
```

```diff
diff --git a/app/services/expansion_advisor.py b/app/services/expansion_advisor.py
@@ _bulk_enrich_competitors docstring
     Returns ``{parcel_id: {"competitor_count": int, "confident": bool,
-    "max_chain_strength": float | None}}`` for all rows that have lat/lon.
+    "max_chain_strength": float | None, "chain_strength_share": float | None,
+    "top_chain_strength_name": str | None}}`` for all rows that have lat/lon.
@@ docstring (Patch B paragraph)
-    to chain quality), so the leg measures established-brand validation
-    via the Google Places side only.
+    to chain quality), so the signal is from the Google Places side only.
+    ``max_chain_strength`` is RETAINED only for the ``chain_strength_max``
+    JSON diagnostic — it is no longer the leg input because a MAX saturates
+    at 100 for any radius containing a single big chain.
+
+    ``chain_strength_share`` is the chain_strength leg input: among same-
+    category, ECQ-matched POIs in the radius (``chain_strength IS NOT NULL``
+    excludes the NULL delivery side), the percentage whose
+    ``chain_strength_score >= EXPANSION_CHAIN_STRONG_THRESHOLD``. NULL when
+    fewer than ``EXPANSION_CHAIN_MIN_MATCHED`` matched POIs are in radius, so
+    thin-evidence radii flow to Python None and ``_chain_strength_score``
+    keeps returning the neutral 50.0 (never COALESCE'd to 0).
@@ LATERAL outer SELECT
                         comp.max_chain_strength AS max_chain_strength,
+                        comp.chain_strength_share AS chain_strength_share,
                         comp.top_chain_strength_name AS top_chain_strength_name
@@ LATERAL inner aggregates
                             MAX(chain_strength) FILTER (WHERE in_category) AS max_chain_strength,
+                            -- Strong-chain SHARE: among same-category POIs that
+                            -- carry an ECQ chain_strength_score (chain_strength
+                            -- IS NOT NULL excludes the NULL delivery side), the
+                            -- percentage that are "strong" (>= threshold). This
+                            -- is the chain_strength leg input; it replaces the
+                            -- MAX above, which saturated at 100 for any radius
+                            -- containing a single big chain. NULL when fewer
+                            -- than :chain_min_matched matched POIs exist, so
+                            -- thin-evidence radii flow to Python None and
+                            -- _chain_strength_score keeps its neutral 50.0 (no
+                            -- COALESCE). MAX is retained above purely for the
+                            -- chain_strength_max JSON diagnostic.
+                            CASE
+                              WHEN COUNT(*) FILTER (WHERE in_category AND chain_strength IS NOT NULL)
+                                   >= :chain_min_matched
+                              THEN 100.0
+                                   * COUNT(*) FILTER (WHERE in_category AND chain_strength >= :chain_strong_threshold)
+                                   / COUNT(*) FILTER (WHERE in_category AND chain_strength IS NOT NULL)
+                              ELSE NULL
+                            END AS chain_strength_share,
@@ query bind params
                 {"pids": pids, "lons": lons, "lats": lats,
                  "category_keys": category_keys, "category_regex": category_regex,
-                 "radius_m": competition_radius_m},
+                 "radius_m": competition_radius_m,
+                 "chain_strong_threshold": float(settings.EXPANSION_CHAIN_STRONG_THRESHOLD),
+                 "chain_min_matched": int(settings.EXPANSION_CHAIN_MIN_MATCHED)},
@@ return dict
                     if r["max_chain_strength"] is not None
                     else None
                 ),
+                "chain_strength_share": (
+                    float(r["chain_strength_share"])
+                    if r["chain_strength_share"] is not None
+                    else None
+                ),
                 "top_chain_strength_name": (
@@ bulk-unpack site 1 (candidate_location path)
                     _r["max_chain_strength"] = _entry.get("max_chain_strength")
+                    _r["chain_strength_share"] = _entry.get("chain_strength_share")
                     _r["top_chain_strength_name"] = _entry.get("top_chain_strength_name")
@@ bulk-unpack site 2 (commercial_unit path)
                         _r["max_chain_strength"] = _entry.get("max_chain_strength")
+                        _r["chain_strength_share"] = _entry.get("chain_strength_share")
                         _r["top_chain_strength_name"] = _entry.get("top_chain_strength_name")
@@ final-pass extraction (~:7812)
         # Patch B: max chain_strength_score across same-category POIs in
-        # the candidate's competition radius. None when the bulk enrichment
-        # path was bypassed OR when no same-category POI rows joined to
-        # expansion_competitor_quality. _chain_strength_score() converts
-        # None to a neutral 50 so thin-data candidates aren't penalized.
+        # the candidate's competition radius. Retained ONLY for the
+        # chain_strength_max JSON diagnostic; it is NOT the leg input. None
+        # when the bulk enrichment path was bypassed OR when no same-category
+        # POI rows joined to expansion_competitor_quality.
         _max_chain_strength_raw = row.get("max_chain_strength")
         max_chain_strength: float | None = (
             float(_max_chain_strength_raw)
             if _max_chain_strength_raw is not None
             else None
         )
+        # Strong-chain SHARE: the chain_strength leg input. None when the
+        # bulk enrichment path was bypassed OR fewer than
+        # EXPANSION_CHAIN_MIN_MATCHED in-category ECQ-matched POIs were in
+        # radius. _chain_strength_score() converts None to a neutral 50 so
+        # thin-data candidates aren't penalized.
+        _chain_strength_share_raw = row.get("chain_strength_share")
+        chain_strength_share: float | None = (
+            float(_chain_strength_share_raw)
+            if _chain_strength_share_raw is not None
+            else None
+        )
@@ leg input (~:7892)
-        chain_strength_score = _chain_strength_score(max_chain_strength)
+        chain_strength_score = _chain_strength_score(chain_strength_share)
@@ prepared dict
                 "max_chain_strength": max_chain_strength,
+                "chain_strength_share": chain_strength_share,
                 "chain_strength_score": chain_strength_score,
@@ final-pass fallback (~:8817)
         max_chain_strength = prepared_item.get("max_chain_strength")
+        chain_strength_share = prepared_item.get("chain_strength_share")
         chain_strength_score = prepared_item.get("chain_strength_score")
         if chain_strength_score is None:
-            chain_strength_score = _chain_strength_score(max_chain_strength)
+            # Leg input is the strong-chain SHARE (None → neutral 50.0).
+            chain_strength_score = _chain_strength_score(chain_strength_share)
```

## 5. Consistency & contract preservation

- **Both scoring passes feed `chain_strength_share`**: coarse pass
  (`_chain_strength_score(chain_strength_share)`) and final-pass fallback
  (same). The computed `chain_strength_score` is carried in `prepared`, so the
  final pass normally reuses the coarse value; the fallback now recomputes from
  the share, not the MAX.
- **`chain_strength_max=max_chain_strength` still passed to `_score_breakdown`**
  at both call sites, so `inputs.chain_strength_max` keeps recording the true
  radius MAX diagnostic.
- **`NULL → None → 50.0` preserved**: SQL `ELSE NULL` (no `COALESCE`) → the
  `None`-guard in the return dict → `_chain_strength_score(None) → 50.0`. The
  ArcGIS-fallback pool path never sets `chain_strength_share`, so
  `row.get(...) → None → 50.0` — identical to prior behavior.
- **Weight math untouched**: `chain_strength` weight stays 3.0%; the
  `assert sum(component_weights) == 100` invariant and `_score_breakdown`
  arithmetic are unchanged.
- **SQL division is numeric, not integer**: `100.0 * COUNT(...) / COUNT(...)` —
  the `100.0` numeric literal promotes the expression, so e.g. 2 strong of 5
  matched → `40.0`, not truncated.

## 6. Validation performed (in this container)

```
ast.parse OK                              app/services/expansion_advisor.py, app/core/config.py
settings.EXPANSION_CHAIN_STRONG_THRESHOLD = 60.0
settings.EXPANSION_CHAIN_MIN_MATCHED      = 3
_chain_strength_score(None) = 50.0   _chain_strength_score(0) = 0.0
_chain_strength_score(40)   = 40.0   _chain_strength_score(100) = 100.0
score_component_probe.py: PART B.2 sum(weights)=100.0 → PASS; OVERALL ALL PASS
```

> Only minimal deps (sqlalchemy/pydantic) are installed in this sandbox, so the
> full `make lint` / `pytest` suite was **not** run here.

## 7. Recommended pre-merge validation

```bash
make fmt && make lint
python -m pytest tests/test_expansion_advisor_service.py -q
python scripts/diagnostics/score_component_probe.py
```

Live sanity check: for a QSR/burger search, confirm `chain_strength_share`
spreads across [0,100] across candidates where the old `max_chain_strength`
was pinned at 100. Useful DB probe (Codespace):

```sql
-- Distribution of the new leg input would mirror this radius computation per candidate:
SELECT
  100.0 * count(*) FILTER (WHERE ecq.chain_strength_score >= 60.0)
        / NULLIF(count(*), 0) AS strong_chain_share_pct,
  count(*) AS matched_pois
FROM restaurant_poi rp
JOIN expansion_competitor_quality ecq
  ON ecq.restaurant_poi_id = rp.id AND ecq.city = 'riyadh'
WHERE lower(rp.category) = ANY(ARRAY['burger','fast food','...'])  -- category_keys
  AND (rp.business_status IS NULL OR rp.business_status = 'OPERATIONAL')
  AND ST_DWithin(rp.geom::geography,
                 ST_SetSRID(ST_MakePoint(:lon,:lat),4326)::geography, :radius_m);
-- matched_pois < 3 (EXPANSION_CHAIN_MIN_MATCHED) ⇒ leg input NULL ⇒ neutral 50.
```

## 8. Merge recommendation

**Low risk.** Additive SQL column + two read-site swaps; missing-data and
weight invariants preserved; only the chain_strength leg input semantics
change (now discriminates by *share of strong chains* rather than saturating
at the radius MAX). Recommend merging after `make lint` + the service test
suite pass and a one-search live spot-check of the share distribution.
