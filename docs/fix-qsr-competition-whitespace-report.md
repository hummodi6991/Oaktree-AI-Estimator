# PATCH PR-B — qsr competition radius + whitespace curve recalibration (Item 1)

**Branch:** `claude/weight-audit-pr-b-1yvztx` (commit `2cca3509a`)
**Status:** Pushed — no PR opened, no merge, no dispatch. **Ahmed reviews the diff (especially the
REF=75 constant and the 1000 m radius) and gives explicit merge approval.**
**Type:** Real scoring change (not emit-only, not flag-gated). **Changes qsr rankings on deploy** —
same class as the merged dine_in (#1288) and delivery_first (#1291) whitespace fixes; this PR follows
that bug-fix precedent exactly. Single-purpose: ONLY the two constants + tests + comments. Out of
scope (deliberately): scope-aware REF, cafe values, Items 2–6 of the weight audit.

---

## 1. What is wrong now

Production scores qsr same-category competitors over a **1200 m** competition radius and lets qsr
fall through to the **default curve REF=25**. A city-wide probe (548 QSR candidates, exact production
count predicate) showed this floors:

- **67.2%** of burger-scope candidates at 15.00, and
- **81.6%** of fast_food-scope candidates at 15.00,

confirmed live by search `d4ca314b` — **10 of 15** shortlist candidates flat at 15.00. A
near-constant component output means `competition_whitespace` carries close to **zero discriminating
signal** for qsr, exactly the signature already fixed for dine_in and delivery_first.

## 2. Why it happens — both levers are required

The radius and the curve reference were never co-calibrated for qsr:

- **Why not REF-only.** At 1200 m the counts run **p50 35 / p75 62** on broad scopes — they overflow
  the log-decay domain. Raising REF alone would un-floor some of the band but leaves the curve
  discriminating over a count range where the distribution is fat and compressed.
- **Why not radius-only.** The discriminating variation lives at **1000 m** (burger-scope probe
  **p25 4 / p50 16 / p75 24 / p90 34**), but under REF=25 the p50 (count 16) still floors
  (16 → raw 13.0 → floored 15.0). Tightening the radius is necessary but not sufficient.

Same settlement shape as dine_in and delivery_first (both 1000 m). Their counts settled lower, so
they took REF=50; **qsr counts run higher → REF=75**.

## 3. The fix (two coupled constants + tests + comments — qsr blast radius only)

### Change 1 — qsr competition radius 1200 → 1000 m

`app/services/expansion_advisor.py:836` — `_CATCHMENT_RADII_M['qsr']['competition']`:
`1200.0 → 1000.0`. The model's **`demand` (1500) and `provider` (1500) radii are unchanged**
(convenience walk/drive-thru catchment). dine_in / delivery_first / cafe rows untouched. Block
comment extended in the same style as the dine_in/delivery_first entries
(`expansion_advisor.py:830-835`).

### Change 2 — add a qsr curve reference

`app/services/expansion_advisor.py:2715` — `_WHITESPACE_LOG_REF`: add `"qsr": 75.0`.
`dine_in: 50.0`, `delivery_first: 50.0`, and `_WHITESPACE_LOG_REF_DEFAULT = 25.0` are unchanged;
cafe and unknown models still resolve to the 25 default. No call-site change needed — the scorer is
already called with `service_model=service_model` (`expansion_advisor.py:8270`), and both
`_bulk_enrich_competitors` call sites (`:7637`, `:7682`) already pass `service_model`, so the radius
flows through `_catchment_radii(service_model)["competition"]` (`:6857`).

Comment at the dict entry (`expansion_advisor.py:2704-2711`) records: floors only count ≥ 39
(c\* exact 38.69); at the 1000 m counts this spreads the p25–p75 band to ~26–63 (burger scope) and
floors 4.9%.

> **KNOWN LIMITATION (recorded verbatim in the code comment):** broad `fast_food`-scope briefs still
> floor ~32% at this setting (probe record 15); acceptable because production briefs are
> narrow-scope; the structural fix would be scope-aware REF — future work, not this PR.

### Change 3 — update the blast-radius guard tests

The delivery_first fix had pinned **qsr** bit-for-bit to the legacy REF=25 curve
(`test_competition_whitespace_cafe_qsr_reference_unchanged`). That guard now encoded the bug. Updated
to:

- **drop qsr** from the legacy REF=25 loop (now guards cafe / `None` only), same representative
  counts `1/3/6/8/12/16/24/25/40/50/145` (`tests/test_expansion_advisor_service.py:547-575`);
- new `test_competition_whitespace_qsr_reference_varies_off_floor` pins qsr **bit-for-bit to the
  REF=75 curve** (via a `_ref75` helper) across counts `1/3/4/6/16/24/32/38/39/50/75/145`, plus exact
  curve pins — see §4 (`tests/test_expansion_advisor_service.py:622-680`);
- `test_competition_whitespace_f4_path_unchanged_per_model` already covered qsr — unchanged;
- the dine_in and delivery_first dedicated tests are **byte-identical** (verified: no diff hunk
  touches either function body).

The guard's point is unchanged — pin per-model whitespace behaviour — it now encodes the fixed qsr
curve instead of the buggy one. It is **not** weakened to a no-op.

### Change 4 — chain-share side-effect comment (comment-only)

`app/services/expansion_advisor.py:6935-6941` — appended to the **existing** chain-share comment
anchor inside `_bulk_enrich_competitors`'s SQL (no code restructure): the competition radius also
bounds the chain_strength share counts; at 1000 m matched counts shrink and a few more candidates
fall under `EXPANSION_CHAIN_MIN_MATCHED=3` into the neutral 50 — expected and accepted, mirroring
the delivery_first precedent.

### Change 5 — docstring + diagnostics

- `_competition_whitespace_score` docstring updated (`expansion_advisor.py:2730-2744`): it previously
  asserted qsr uses the 25 default, which would have been stale; a REF=75 representative-output row
  was added.
- New `scripts/diagnostics/qsr_whitespace_probe.sql`: ~10-line search-scoped post-rollout validation
  query (auto-scopes to the latest qsr `expansion_search`; psql `-f` safe, no `\set`). The original
  city-wide probe was never committed to the repo, so there was nothing in-repo to extend — this
  file is the in-repo home for the search-scoped section.

---

## 4. REF = 75 rationale (verified in code, exact formula)

`score(c) = clamp(max(15, 100·(1 − log1p(c)/log1p(75))))`. Floor onset: c\* = 38.69 → first integer
count that floors is **39**.

| count        | REF=25 (default / cafe) | REF=75 (qsr)        |
|--------------|--------------------------|---------------------|
| 0 (confident)| 100                      | 100                 |
| 1            | 78.7                     | 83.99               |
| 4 (p25)      | 50.0                     | **62.84**           |
| 16 (p50)     | **15.0 (floored)**       | **34.58**           |
| 24 (p75)     | 15.0                     | **25.67**           |
| 32           | 15.0                     | 19.26               |
| 38           | 15.0                     | 15.41 (last off-floor) |
| 39           | 15.0                     | **15.0 (floor onset)** |
| 75           | 15.0                     | 15.0 (structural floor) |
| 145          | 15.0                     | 15.0                |

### ⚠️ Arithmetic mismatches vs the brief (> 0.05 — flagged per instruction)

The brief's test-pin arithmetic was chat-side; tests use the exact recomputed values:

| count | brief said | exact value | delta  |
|-------|-----------|-------------|--------|
| 4     | 62.77     | **62.84**   | 0.07 ⚠️ |
| 16    | 34.39     | **34.58**   | 0.19 ⚠️ |
| 24    | 25.45     | **25.67**   | 0.22 ⚠️ |
| 39    | 15.0      | 15.0 (raw 14.82) | ✓ |
| c\*   | 38.70     | 38.69       | ✓      |

Note the brief's *evidence* section (p25/p50/p75 scores 62.8 / 34.6 / 25.7) matches the exact values
— only the test-pin section drifted.

---

## 5. Validation

### Already run (this patch)

```bash
python -m pytest tests/test_expansion_advisor_service.py -k whitespace -q   # 6 passed
python -m pytest -q                                       # 2303 passed, 24 skipped
```

(HEAD baseline was 2302 passed; the +1 is the new qsr guard test.) flake8 issue count on the two
touched files is identical to HEAD (1433, all pre-existing); black was already failing on both files
at HEAD (pre-existing repo-wide drift) — not introduced or worsened here.

### Post-merge + deploy (Ahmed — this DOES change qsr rankings)

1. Merge → deploy → fresh **city-wide QSR** search (old searches won't backfill).
2. `psql "$DATABASE_URL" -f scripts/diagnostics/qsr_whitespace_probe.sql` (auto-scopes to the latest
   qsr search). Success criteria:
   - `pct_floored` drops from ~67% toward **~5%** (narrow scope);
   - `distinct_whitespace_values` ≥ **6** in a 15-candidate shortlist;
   - `competitor_count_p50` reflects the **1000 m** radius (shortlist p50 roughly **0.6–0.7×** the
     1200 m counts);
   - `count_score_corr` ≤ 0 — monotonic score-vs-count.
3. Spot-check chain_strength inputs on the same search for the expected **mild rise in neutral-50s**
   (radius side effect, see §6).

---

## 6. Risk / tradeoff — blast radius

- **Intended ranking shift, qsr only.** Sites that previously all read 15.0 now spread across the
  p25–p75 band (~26–63 burger scope); the genuinely saturated ≥ 39 tail still floors. Corrected
  behaviour, not a regression.
- **Side effect (expected and accepted):** the competition radius also bounds the chain_strength
  share counts. At 1000 m matched counts shrink and a few more candidates fall under
  `EXPANSION_CHAIN_MIN_MATCHED=3` into the neutral 50 — mirrors the delivery_first precedent, noted
  in-code at the chain-share aggregation.
- **Known limitation:** broad `fast_food`-scope briefs still floor ~32% at REF=75. Acceptable —
  production briefs are narrow-scope; the structural fix is scope-aware REF (future work, explicitly
  out of this PR's scope).
- **Contained.** dine_in (REF 50), delivery_first (REF 50), cafe (REF 25) keep their radii and REFs,
  pinned bit-for-bit by guard tests (their dedicated tests are byte-identical). No change to the
  curve formula, the 15.00 floor, the F4 `count<=0` branches (50/100), `component_weights`, the
  `sum == 100` invariant, gates, or any other model's radii.
- **Curve constant is the review point.** REF=75 lives in the named `_WHITESPACE_LOG_REF` dict so
  Ahmed can tune the knee without touching the formula.

---

## 7. Change inventory (exact anchors)

| Change | Location |
|--------|----------|
| `_CATCHMENT_RADII_M["qsr"]["competition"]` 1200 → 1000 + block comment | `app/services/expansion_advisor.py:830-836` |
| `_WHITESPACE_LOG_REF["qsr"] = 75.0` + comment incl. KNOWN LIMITATION | `app/services/expansion_advisor.py:2704-2715` |
| `_competition_whitespace_score` docstring (REF=75 row; qsr no longer "default") | `app/services/expansion_advisor.py:2730-2744` |
| Chain-share side-effect comment (existing anchor, comment-only) | `app/services/expansion_advisor.py:6935-6941` |
| Guard test: qsr dropped from legacy REF=25 loop | `tests/test_expansion_advisor_service.py:547-575` |
| New `test_competition_whitespace_qsr_reference_varies_off_floor` | `tests/test_expansion_advisor_service.py:622-680` |
| New search-scoped post-rollout probe | `scripts/diagnostics/qsr_whitespace_probe.sql` |

Diff stat: `3 files changed, 128 insertions(+), 11 deletions(-)`.

There was **no pre-existing test pinning the qsr competition radius at 1200** — the only radius pin
(`tests/test_expansion_advisor_demand_generator.py:518`) reads the *demand* radius through the dict
and is unaffected. The REF pin was the qsr entry in the legacy REF=25 guard loop, handled in §3.

---

## 8. Merge recommendation

**Low risk, high signal.** Targeted, well-tested, follows the merged dine_in/delivery_first
precedent exactly; the behaviour-corrupting input is fixed at the source and the only judgement
call — the REF=75 constant — is surfaced in a named dict for review. Recommend merge after Ahmed
signs off on the constant and the 1000 m radius, then run the post-deploy validation in §5.
