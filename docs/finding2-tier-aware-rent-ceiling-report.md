# Finding 2 — Tier-aware rent ceiling (absolute / fallback path only)

> **GATE: Implemented on a feature branch. NOT merged. Held for Ahmed's review.**
> Branch: `claude/finding2-tier-aware-rent-ceiling-W7peX` (rebased on `main`, post–Finding-1).
> Riyadh-only. No PR opened.

---

## 1. Summary

`price_tier` now reaches `_economics_score` and scales the **absolute** rent ceilings only
(`absolute_fallback` 220 SAR/m²/mo, `absolute_legacy` 180 SAR/m²/mo). A premium operator paying
premium rent/m² is no longer penalized identically to a value operator at the same rent/m². The
**percentile** branch — which dominates real Aqar/Bayut inventory and feeds `value_score` — is left
completely tier-blind, so `value_score` / `value_band` output is unchanged.

`price_tier` unset / `"mid"` / unknown → multiplier `1.0` → **byte-identical to pre-patch on every
path.**

---

## 2. What is wrong now

In `_economics_score`, rent-burden has three modes:

| Mode | Trigger | Ceiling (pre-patch) | Tier-aware? |
|------|---------|---------------------|-------------|
| `percentile` | `is_listing` **and** comparables found | peer-relative (no fixed ceiling) | n/a — correct, tier-blind by design |
| `absolute_fallback` | `is_listing`, **no** comparables | fixed **220** SAR/m²/mo | ❌ was tier-blind |
| `absolute_legacy` | non-listing | fixed **180** SAR/m²/mo | ❌ was tier-blind |

On the two absolute paths, a **premium** brand paying premium rent/m² received the same rent-burden
penalty as a **value** brand at the same rent/m². `price_tier` already moved revenue
(`_implied_average_check` / `_estimate_revenue_index`) but not affordability — an internal
inconsistency in how the same signal is treated across the economics composite.

---

## 3. Why it happens

The two `_economics_score(...)` call sites passed every economics input **except** `price_tier`, even
though `effective_brand_profile` (which carries `price_tier`) was already in scope at both. The
absolute ceilings were hard-coded literals (`220.0`, `180.0`) inside the branch bodies, so there was
no seam through which tier could influence them.

---

## 4. Smallest safe fix

A `price_tier → multiplier` map applied **only** to the two absolute ceilings:

| Tier | Multiplier | absolute_legacy ceiling | absolute_fallback ceiling |
|------|-----------|-------------------------|---------------------------|
| `value` | 0.85 | 153.0 | 187.0 |
| `mid` / unset / unknown | 1.00 | **180.0** (unchanged) | **220.0** (unchanged) |
| `premium` | 1.30 | 234.0 | 286.0 |

The percentile branch and `_percentile_rent_burden` are **untouched**, so `value_score` /
`value_band` (published only in percentile mode) stay tier-blind.

### 4.1 Diff — `app/services/expansion_advisor.py`

```diff
@@ Economics composite score
+# Finding 2: price-tier multiplier applied to the ABSOLUTE rent ceilings only
+# (absolute_fallback 220 / absolute_legacy 180). A premium brand can sustain a
+# higher rent/m² before the burden penalty bites; a value brand less. The
+# percentile path is intentionally NOT tier-adjusted — it is peer-relative and
+# feeds value_score, which must stay tier-blind. Tier vocab matches
+# _IMPLIED_CHECK_SAR ("value"/"mid"/"premium"); unknown/None -> 1.0 (no change).
+_RENT_CEILING_TIER_MULT: dict[str, float] = {
+    "value": 0.85,
+    "mid": 1.0,
+    "premium": 1.30,
+}
+
+
+def _rent_ceiling_tier_multiplier(price_tier: str | None) -> float:
+    return _RENT_CEILING_TIER_MULT.get(str(price_tier or "").lower().strip(), 1.0)
+
+
 def _economics_score(
     *,
     ...
     unit_neighborhood_raw: str | None = None,
+    price_tier: str | None = None,
 ) -> tuple[float, dict[str, Any]]:
     monthly_rent_per_m2 = estimated_annual_rent_sar / max(area_m2 * 12.0, 1.0)
 
+    _tier_mult = _rent_ceiling_tier_multiplier(price_tier)
+    _fallback_ceiling = 220.0 * _tier_mult
+    _legacy_ceiling = 180.0 * _tier_mult
+
     rent_burden_meta: dict[str, Any] = {"mode": "absolute_legacy"}
@@ absolute_fallback branch
-            rent_burden_score = _clamp(100.0 - (monthly_rent_per_m2 / 220.0) * 100.0)
+            rent_burden_score = _clamp(100.0 - (monthly_rent_per_m2 / _fallback_ceiling) * 100.0)
             rent_burden_meta = {
                 "mode": "absolute_fallback",
                 "listing_monthly_rent_per_m2": round(monthly_rent_per_m2, 2),
-                "ceiling": 220.0,
+                "ceiling": round(_fallback_ceiling, 2),
+                "ceiling_base": 220.0,
+                "ceiling_tier_multiplier": round(_tier_mult, 3),
+                "price_tier": (str(price_tier).lower().strip() if price_tier else None),
             }
@@ absolute_legacy branch
-        rent_burden_score = _clamp(100.0 - (monthly_rent_per_m2 / 180.0) * 100.0)
+        rent_burden_score = _clamp(100.0 - (monthly_rent_per_m2 / _legacy_ceiling) * 100.0)
         rent_burden_meta = {
             "mode": "absolute_legacy",
             "monthly_rent_per_m2": round(monthly_rent_per_m2, 2),
-            "ceiling": 180.0,
+            "ceiling": round(_legacy_ceiling, 2),
+            "ceiling_base": 180.0,
+            "ceiling_tier_multiplier": round(_tier_mult, 3),
+            "price_tier": (str(price_tier).lower().strip() if price_tier else None),
         }
@@ call site 1 (run_expansion_search, first scoring pass)
             unit_neighborhood_raw=row.get("unit_neighborhood_raw"),
+            price_tier=effective_brand_profile.get("price_tier"),
         )
@@ call site 2 (run_expansion_search, final re-score with road signal)
             unit_neighborhood_raw=row.get("unit_neighborhood_raw"),
+            price_tier=effective_brand_profile.get("price_tier"),
         )
```

### 4.2 New regression test — `tests/test_expansion_advisor_regression.py`

`test_economics_score_tier_aware_absolute_ceiling` asserts:

- multiplier vocab + case/space-insensitivity + unknown/None → `1.0`;
- absolute path, same rent/m²: `premium (234) > none (180) > value (153)` for `rent_burden_score`;
- `price_tier=None` / `"mid"` reproduce the pre-patch ceiling (`180.0`) exactly;
- percentile mode (via a stubbed `_percentile_rent_burden`) is invariant to `price_tier` —
  `rent_burden_score`, `value_score`, `value_band` unchanged, and **no tier metadata leaks** onto the
  percentile `rent_burden` block.

---

## 5. Invariants

- `price_tier` unset / `"mid"` / unknown → multiplier `1.0` → identical to today on every path.
- Percentile path output (`rent_burden_score`, `value_score`, `value_band`) unchanged — no
  `price_tier` flows into it.
- Only `absolute_fallback` / `absolute_legacy` rows change, and only for `price_tier ∈ {value, premium}`.

---

## 6. Validation

### 6.1 Ran in-sandbox (green)

| Command | Result |
|---------|--------|
| `pytest tests/test_expansion_advisor_regression.py -k "economics or expansion"` | **120 passed** |
| `pytest tests/test_expansion_advisor_regression.py::test_economics_score_tier_aware_absolute_ceiling tests/test_expansion_advisor_service.py` | **157 passed** |
| `pytest tests/test_sample_regression_memos.py tests/test_llm_decision_memo.py tests/test_expansion_advisor.py tests/test_expansion_advisor_regression.py` | **290 passed** (golden memo regression incl.) |

**Lint note:** `black --check` / `flake8` report churn, but the file is **already** black-dirty with
~1011 pre-existing flake8 hits on `main` (e.g. E501 at lines 4578/4627 are untouched legacy lines).
The repo does not enforce these in CI; the diff follows surrounding style, so no cosmetic reformatting
was added — keeping the patch minimal.

### 6.2 For Ahmed (CC cannot run — `/searches` auth-gated, RDS unreachable from CC)

**Confirmed JSON path.** Verified in code: `economics_meta` is merged into
`score_breakdown_json["economics_detail"]` at `app/services/expansion_advisor.py:9180`, and
`rent_burden_meta` is nested under key `rent_burden` with field `mode`. So
`#>> '{economics_detail,rent_burden,mode}'` is correct.

```sql
-- Size the affected population FIRST, before concluding anything.
SELECT score_breakdown_json #>> '{economics_detail,rent_burden,mode}' AS rent_mode,
       COUNT(*)
FROM expansion_candidate
WHERE computed_at > now() - interval '7 days'
GROUP BY 1 ORDER BY 2 DESC;
```

> **⚠️ Caveat — state plainly.** If production listings are overwhelmingly `percentile` mode
> (expected for real Aqar/Bayut inventory), a premium-vs-value brief will **not** move current
> rankings. This fix is a **latent correctness guard** that only fires on `absolute_fallback` /
> `absolute_legacy` rows. Any live A/B check must **isolate `absolute_*` rows**, or it will look like
> a no-op.

**Live recipe (UI + psql, mirroring the Finding 1 flow).** Run two app searches identical except
`price_tier` (one **premium**, one **value**). Then, for the SAME listing across both searches,
compare:

- `economics_detail.rent_burden.mode`
- `economics_detail.rent_burden.ceiling`
- `economics_detail.rent_burden_score`

Expect a difference **only** on `absolute_*` rows (premium ceiling 234/286, value 153/187; premium
`rent_burden_score` higher) and **zero change on `percentile` rows**.

```sql
-- Compare one listing across the two searches (substitute the two search ids + parcel/listing id).
SELECT search_id,
       score_breakdown_json #>> '{economics_detail,rent_burden,mode}'    AS mode,
       score_breakdown_json #>> '{economics_detail,rent_burden,ceiling}' AS ceiling,
       score_breakdown_json #>> '{economics_detail,rent_burden_score}'   AS rent_burden_score
FROM expansion_candidate
WHERE search_id IN (:premium_search_id, :value_search_id)
  AND parcel_id = :parcel_id
ORDER BY search_id;
```

---

## 7. Merge recommendation

**Risk: Low.**

- Additive signature kwarg (`price_tier`, defaulted `None`); no schema change; no contract removal.
- The percentile path — which dominates production — is provably untouched.
- Multiplier is identity for `mid` / unset / unknown, so unchanged for the vast majority of rows.
- Behavior changes only for `absolute_*` rows under `value` / `premium` briefs.

**Recommendation:** merge after Ahmed runs the live `absolute_*`-isolated check in §6.2. Holding per
the gate — no PR opened, not merged.

---

## 8. Files changed

| File | Change |
|------|--------|
| `app/services/expansion_advisor.py` | tier→multiplier constant + helper; `price_tier` kwarg on `_economics_score`; scaled absolute ceilings + richer `rent_burden_meta`; pass `price_tier` at both call sites |
| `tests/test_expansion_advisor_regression.py` | new `test_economics_score_tier_aware_absolute_ceiling` |
