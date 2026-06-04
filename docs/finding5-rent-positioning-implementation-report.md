# Finding 5 — Implementation Report: deterministic rent-positioning phrase (EN + AR inversion fix)

**Branch:** `claude/finding5-rent-positioning-deterministic` (committed + pushed; **not merged, no PR**)
**Gate status:** Implemented on a feature branch. STOPPED after the diff for Ahmed's review. Riyadh-only.

---

## Root cause (accepted from the F5 read-only investigation)

The "cheaper than ~38%" inversion was **LLM-authored prose**. The payload handed the model the raw
fraction `rent_percentile = 0.375` and the prompt told it to compute `N = round((1 − fraction) × 100)`
itself (EN rule + AR Rule 8). The model performed that inversion unreliably and emitted `0.375 → 38`
instead of the correct `1 − 0.375 → 62`. Both EN and AR delegated the arithmetic, so both were exposed.
All *deterministic* surfaces (frontend `pctFromFraction`, i18n) were already correct.

Ground truth for sample `parcel_id 6545795`: listing `141.47 < median 164.72` (14% below median),
`burden_score 70`, `percentile 0.375` → correct phrase is **"cheaper than ~62%"**.

## Fix strategy

Move the inversion out of the LLM. Compute the inverted display number + zone **once, deterministically**
in the payload builder, mirroring the frontend `pctFromFraction` EXACTLY. Expose a structured
`{zone, pct_value, scope}` (not a ready-made English phrase) so each locale renders its own sentence
around the same pre-inverted number. Keep the raw fraction field unchanged for the frontend.

---

## Changes (3 files, +174 / −63)

### 1. `app/services/llm_decision_memo.py`

**a. New helper `_rent_positioning(percentile, scope)`** (after `_scope_from_source_label`)

Mirrors `frontend/src/features/expansion-advisor/AdvisorySectionCards.tsx:28-39` exactly:
- clamp to `[0, 1]`
- `pct = int(math.floor(clamped * 100.0 + 0.5))` — **JS `Math.round` (round-half-up)**, NOT Python
  `round()` (banker's), which diverges at `.5` boundaries (e.g. `0.125` → JS `13`, Python `12`)
- zones: MID `40 <= pct <= 60` (inclusive, no number) → LOW `pct < 40` (`pct_value = 100 - pct`) →
  HIGH `pct > 60` (`pct_value = pct`)
- returns `{zone, pct_value, scope}`, or `None` when percentile is absent (caller omits the clause)

```python
def _rent_positioning(percentile: Any, scope: str | None) -> dict | None:
    frac = _safe_float(percentile)
    if frac is None:
        return None
    clamped = max(0.0, min(1.0, frac))
    pct = int(math.floor(clamped * 100.0 + 0.5))  # JS Math.round, not banker's round()
    if 40 <= pct <= 60:
        return {"zone": "mid", "pct_value": None, "scope": scope}
    if pct < 40:
        return {"zone": "low", "pct_value": 100 - pct, "scope": scope}
    return {"zone": "high", "pct_value": pct, "scope": scope}
```

Added `import math`.

**b. Payload** — `build_memo_advisory_sections` now emits, in `financial_framing`:
```python
"rent_percentile_vs_comparables": rent_percentile,                      # UNCHANGED (raw 0.375)
"rent_positioning": _rent_positioning(rent_percentile, comparable_scope),  # NEW {zone, pct_value, scope}
```
The raw field is untouched, so the frontend renderer is unaffected.

**c. EN prompt rule** — rewritten to *consume, not compute*. Removed
`N = round((1 − fraction) × 100)` and the `0.28 → 72%` / `0.88 → 88%` arithmetic examples that invited
recomputation. New rule: COPY `rent_positioning.pct_value` verbatim; three templates by `zone`
(`low` → "cheaper than about {pct_value}% of {comparables}", `mid` → "around the district median rent"
no number, `high` → "more expensive than about {pct_value}% of {comparables}"); `{comparables}` chosen
from `rent_positioning.scope` (`district` / `city_band` / `city`).

**d. AR Rule 8** — rewritten symmetrically, in Arabic, consuming the **same**
`rent_positioning.pct_value` (no Arabic-side arithmetic). Removed `حيث N = (100 − المئوية)` and the
`0.28 → 72%` / `0.88 → 88%` examples. Reused the existing Arabic template fragments verbatim to preserve
bytes; zone selection now keys off `rent_positioning.zone`.

**e. Version bump** — `MEMO_PROMPT_VERSION`: `v10-competitor-econ-guardrail-2026-06` →
`v11-rent-positioning-deterministic-2026-06` (forces cache-miss regeneration, incl. listing 6545795).

### 2. `tests/data/pr4a_structured_memo_system_prompt_en_head.txt`

EN byte-identity snapshot regenerated from `_compose_structured_system_prompt("en")`.

### 3. `tests/test_llm_decision_memo.py`

New `TestRentPositioning` + a JS reference re-implementation `_pct_from_fraction_js`:
- `test_known_anchor_fractions`: `0.28→(low,72)`, `0.375→(low,62)`, `0.50→(mid,None)`, `0.70→(high,70)`
- `test_agrees_with_frontend_pct_from_fraction_on_every_fraction`: sweeps `0..1` at 0.001 and asserts
  equality with the JS round-half-up reference on **every** value
- `test_banker_rounding_boundary_uses_round_half_up`: `0.125 → (low, 87)` (not 88)
- `test_none_percentile_returns_none`
- `test_out_of_range_fractions_are_clamped`: `-0.2 → low`, `1.7 → (high, 100)`
- `test_median_invariant_listing_below_median_is_cheaper_than_over_50pct`: `0.375 → low, pct_value > 50`

---

## Invariants honoured

- Raw `rent_percentile_vs_comparables` unchanged → frontend `pctFromFraction` output unchanged.
- Backend `_rent_positioning` agrees with `pctFromFraction` on every fraction (same thresholds + JS
  round-half-up) — enforced by the sweep test.
- `listing_monthly_rent_per_m2 < median_monthly_rent_per_m2` ⟺ rendered "cheaper than > 50%".

---

## Validation (in-sandbox)

| Check | Result |
|-------|--------|
| `TestRentPositioning` (all cases incl. `0.375 → low,62`) | ✅ |
| EN byte-identity snapshot test | ✅ |
| AR composition tests (both locales carry new rule; old arithmetic gone) | ✅ |
| Broad `pytest -k "memo or decision or grounding or prompt or byte or positioning"` | ✅ 446 passed, 6 skipped |
| Payload smoke for sample 6545795 | ✅ `rent_positioning = {zone: low, pct_value: 62, scope: district}`; raw fraction still `0.375` |
| Arabic bytes (yeh U+064A, heh U+0647, no Farsi variants in AR block) | ✅ |
| flake8 on my added line ranges | ✅ no new issues (pre-existing E302/E305/F402/E501 unchanged) |

**Formatting note:** `main`'s `llm_decision_memo.py` is itself not `black`-clean — running whole-file
`black` would reformat large amounts of pre-existing code and balloon the diff, contrary to "smallest
patch." My additions were verified to be `black`-consistent in isolation; I deliberately did not
reformat the file.

---

## Left for Ahmed (CC cannot call the LLM / DB)

1. After deploy, regenerate listing 6545795's memo in **both `en` and `ar`**; expect "cheaper than ~62%"
   and internal consistency with its "14% below median" line and `burden_score 70`. Read the AR figure
   from the returned **JSON / psql bytes**, NOT the rendered UI or a diff PDF (BiDi distortion).
2. Spot-check one MID and one HIGH percentile-mode listing in both locales.

---

## Scope notes

- Does **not** cover Finding 4.1 (the "favorable vs peers / competitors in the area" HOW-IT-COMPARES
  phrasing) — that is a separate one-line prompt tweak. It does make the rent line explicitly
  "vs {scope} comparables", narrowing the pool-vs-named-brand ambiguity for that one claim.
- Frontend `pctFromFraction` and i18n keys were already correct and are left untouched.

**Merge recommendation:** Low risk — additive payload field + prompt "copy, don't compute" instruction +
version bump; deterministic surfaces unchanged; new tests lock the backend/frontend rounding parity.
**Do NOT merge yet — awaiting Ahmed's post-deploy en/ar regeneration check.**
