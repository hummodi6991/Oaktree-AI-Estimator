# F2 Re-Probe — Fix `[D]` `service_model` Bucketing

**Investigation + probe fix report — Oaktree Atlas / Expansion Advisor**
**Date:** 2026-06-13
**Branch:** `claude/f2-reprobe-section-d-bucketing-msy3u0`
**Mode:** READ-ONLY sizing. No app code, no `_WHITESPACE_LOG_REF` edit, no dedupe.
**Artifact changed:** `scripts/diagnostics/competitor_cross_source_overlap_v2.sql` (`psql -x -f` runnable)
**Re-run owner:** Ahmed (Codespace). This report STOPS at the probe fix.

---

## 1. Executive summary

The re-probe sizes the per-service-model `_WHITESPACE_LOG_REF` re-anchor for the
upcoming F2 dedupe patch. After **F8** (PR #1312, commit `9e5790645`) lit the cafe
POI competitor leg, a re-run showed the **`[D]` qsr anchor jump from deduped p90
50.5 → 134.5**, while only the **cafe category POI keys** changed and the
qsr-category rows in `[A]` did not move. The prompt read this as a `[D]`
bucketing bug — `[D]` inferring `service_model` from category and collapsing
most categories into `qsr`.

**What the code actually shows (grounded in the live tree):**

1. **`[D]` already groups by each candidate's REAL
   `expansion_search.service_model`.** There is **no** category→service_model
   `CASE`/map anywhere in `[D]` (or in the candidate sample) to remove.
2. **The qsr inflation is real data, not a probe artifact.**
   `expansion_search.service_model` **defaults to `"qsr"`**
   (`app/api/expansion_advisor.py:153`; brief-form default
   `frontend/.../ExpansionBriefForm.tsx:34`) and is an **input independent of
   `category`**. Cafe-*category* searches created without explicitly setting the
   service model are stored with `sm='qsr'`. Their (post-F8) dense cafe
   competitor counts therefore land in the qsr bucket **by their TRUE
   service_model**, which is exactly how production keys the REF.
3. Because of #2, `[A]` showing `dominant_service_model = qsr` for the cafe
   category is the **real modal value**, not a mapping bug.

**Per the hard rule** ("group `[D]` by real `expansion_search.service_model`,
full stop; if a candidate has no resolvable service_model, report it rather than
falling back to category inference"), the fix keeps the real-service_model
grouping and adds **visibility** so the re-run makes the bucket composition
unambiguous — instead of silently re-bucketing by category.

---

## 2. Background — what `[D]` sizes

`_WHITESPACE_LOG_REF` (`app/services/expansion_advisor.py:2789`) is the
per-service-model count at which the whitespace-log curve hits its 15.0 floor.
Current values: **qsr=75, dine_in=50, delivery_first=50, default=25.** These were
sized against the un-deduped competitor tail; after cross-source dedupe the
counts shrink, so the re-anchored REF should track the **deduped p90 tail**.
`[D]` prints current vs suggested (= deduped p90, rounded) per service_model.

The observed symptom across two runs that changed **only** the cafe category's
POI keys:

| run | cafe POI keys | `[D]` qsr deduped p90 |
|-----|---------------|------------------------|
| pre-F8 | `{coffee_bakery}` (dark, ~0 matches) | 50.5 |
| post-F8 | `{cafe,coffee,bakery,dessert}` (hand-patched via `sed`) | 134.5 |

For lighting the **cafe** POI leg to move the **qsr** `[D]` row, cafe-category
candidates must sit in the qsr bucket. The question is **why**.

---

## 3. Root-cause analysis

### 3.1 `[D]` already groups by the real `service_model`

Tracing the column end to end:

- **`[1] probe_cand`** builds the candidate sample by joining each candidate to
  its **own producing search**:

  ```sql
  FROM probe_cat pc
  JOIN expansion_search es ON lower(btrim(es.category)) = pc.cat_label
  JOIN expansion_candidate ec ON ec.search_id = es.id
  ...
  es.service_model            -- carried verbatim
  ```

  `ec.search_id = es.id` ties each candidate to the exact search that produced
  it; `service_model` is that search's real `expansion_search.service_model`.
- **`[5] probe_per_cand`** carries `c.service_model` through unchanged.
- **`[D]`** does `GROUP BY pcc.service_model`.

There is **no** `CASE WHEN category ... THEN service_model` and **no** hardcoded
category→service_model map. So the grouping key was already the authoritative
field the prompt asked for. The "offending logic" the prompt expected to quote
**does not exist** in the committed probe.

### 3.2 The actual cause: `service_model` defaults to `qsr`, decoupled from category

`service_model` and `category` are **independent inputs**:

- `app/api/expansion_advisor.py:153`
  `service_model: Literal["qsr","dine_in","delivery_first","cafe"] = "qsr"`
  → request default is **`qsr`**.
- `app/api/expansion_advisor.py:1138` passes `req.service_model` straight to
  `run_expansion_search` (stored as given).
- Frontend brief-form default `ExpansionBriefForm.tsx:34` is also `"qsr"`.
- `frontend/.../studyAdapters.ts:133`:
  `category = (raw.category || "").trim() || service_model;` — only **category**
  falls back to service_model, **never the reverse**.

Consequence: a **cafe-category** search whose `service_model` was left at the
default is stored as **`service_model='qsr'`**. That single fact explains the
entire observed behavior:

- `[A]` cafe row → `dominant_service_model = qsr` (real modal value).
- `[D]` qsr bucket → inflated by cafe-category candidates **whose true
  service_model is `qsr`**, once F8 lit their cafe POI counts (~160).

This is **production's own keying**: in prod those same searches compute
whitespace using the qsr REF=75. So the qsr REF genuinely is being sized partly
against dense cafe-area density — a real observation, not a measurement bug.

### 3.3 Implication for the prompt's expected numbers

The prompt expects post-fix `[D]` to read qsr ~20–35 and a separate cafe row
~150–160. That outcome **only** holds if cafe-category searches actually carry
`sm='cafe'`. If many carry the default `sm='qsr'` (as the API/UI defaults
strongly suggest), grouping by real service_model **cannot** split them out —
and forcing a split by category would violate the hard rule. The fix therefore
**surfaces** the truth via new diagnostic columns and lets the re-run decide,
rather than guessing without DB access (none available in this environment).

---

## 4. The fix (probe only)

All changes are confined to
`scripts/diagnostics/competitor_cross_source_overlap_v2.sql`. Sizing-only.

### 4.1 Bake in the deployed F8 cafe POI keys

`[0] probe_cat` cafe row changed from the dark meta-bucket to the deployed
granular keys, so re-runs are correct by default (no more `sed` hand-patch):

```diff
-  ('cafe',
-     ARRAY['coffee_bakery'],
-     'cafe|coffee|bakery|dessert|pastry|قهوة|مقهى|كافيه|مخبز|حلويات'),
+  ('cafe',
+     -- POST-F8 (PR #1312, commit 9e5790645): deployed granular keys, not the
+     -- dark {coffee_bakery} meta-bucket (which matched ~0 rows).
+     ARRAY['cafe','coffee','bakery','dessert'],
+     'cafe|coffee|bakery|dessert|pastry|قهوة|مقهى|كافيه|مخبز|حلويات'),
```

Header term table, the cafe caveat block, and the `[C]` echo were updated to
match (the caveat now records F8's resolution instead of describing the dark
key as a permanent artifact). `normalize_category()` still never emits the
redundant `fast_food` key, so that one key stays at 0 — noted in place.

### 4.2 `[D]` — keep real grouping, add visibility

`[D]` still `GROUP BY pcc.service_model` (real value, made explicit in the
echoes/comments) and now also reports:

- **`n_candidates`** — sample size per service_model bucket (discount thin ones).
- **`fed_by_categories`** — `array_agg(DISTINCT cat_label)` per bucket: the
  distinct categories feeding each service_model (the requested sanity check —
  is `cafe` sitting in the `qsr` bucket?).
- **`reliability`** — flags `THIN (n<5) — unreliable` and, critically,
  `UNRESOLVED service_model — report, do not anchor` for any NULL service_model.
  NULL is **reported, never inferred from category**.
- Explicit `COALESCE(service_model, '(unresolved/null)')` label so an
  unresolved bucket is shown, not dropped.

### 4.3 New `[D2]` — bucket composition

A diagnostic-only table (does **not** change `[D]`'s sizing grouping) giving the
deduped p50/p90 tail per **`(service_model × category)`**. This quantifies
exactly how much each category feeds each real-service_model bucket, so a
cafe-category cohort leaking into the qsr bucket via the default service_model is
visible and measurable.

### 4.4 Untouched

`[A]` (per-category), `[B]` (pooled), `[C]`/`[C2]` (keys diagnostic) logic is
unchanged. They only see the corrected shared cafe keys — which is the intended
F8 bake-in, not a logic change.

---

## 5. How to read the re-run

Run: `psql -x -f scripts/diagnostics/competitor_cross_source_overlap_v2.sql`

- **`[D].fed_by_categories` for `qsr` = `{burger,chicken,fast food}`** (no
  `cafe`) and a distinct **`cafe`** service_model row appears (~150–160):
  → cafe-category searches carry `sm='cafe'`; the prompt's expected split is
  real; qsr drops to the burger/fast-food tail (~20–35).
- **`[D].fed_by_categories` for `qsr` includes `cafe`** (and `[D2]` shows the
  `qsr × cafe` cell carrying the ~150–160 deduped tail):
  → cafe-category searches carry the **default `sm='qsr'`**; the qsr inflation is
  real production keying, **not** a probe bug. Re-anchoring qsr down to exclude
  that cohort would be a deliberate category-based decision needing sign-off —
  the probe will not make it silently.
- **`delivery_first` / any bucket with `n_candidates < 5`** → `reliability`
  flags it `THIN`; do not anchor on it.
- **Any `(unresolved/null)` bucket** → some sampled candidates have no
  `expansion_search.service_model`; reported, not category-inferred.

### Result tables — TO FILL IN after running

**`[D]` per service_model**

| service_model | current_ref | n | fed_by_categories | union_p90 | deduped_p90 | suggested_ref | reliability |
|---------------|-------------|---|-------------------|-----------|-------------|---------------|-------------|
| qsr            | 75 |  |  |  |  |  |  |
| dine_in        | 50 |  |  |  |  |  |  |
| cafe           | 25 |  |  |  |  |  |  |
| delivery_first | 50 |  |  |  |  |  |  |
| (unresolved)   | 25 |  |  |  |  |  |  |

**`[D2]` per (service_model × category)**

| service_model | category | n | deduped_p50 | deduped_p90 |
|---------------|----------|---|-------------|-------------|
| … | … |  |  |  |

**`[A]` glance (confirm unchanged logic; cafe now lit)**

| Category | n | dom. SM | deduped p50/75/90 |
|----------|---|---------|-------------------|
| burger / cafe / chicken / fast food | … | … | … |

---

## 6. Scope guardrails (what was NOT done)

- No patch to `_bulk_enrich_competitors`, `_WHITESPACE_LOG_REF`, or
  `_CHAIN_NAME_NORM_SQL`.
- No dedupe implementation, no REF edit, no app/frontend change.
- No category-based re-bucketing of `[D]` (hard rule).
- `[A]`/`[B]`/`[C]`/`[C2]` logic untouched.
- Sizing numbers only. **STOP** — Ahmed re-runs the probe and pastes `[D]`,
  `[D2]`, and an `[A]` glance; the REF re-anchor decision (and whether the
  cafe↔qsr service_model default is itself a product follow-up) is made from
  those tables.

---

## 7. Files & commit

- **Changed:** `scripts/diagnostics/competitor_cross_source_overlap_v2.sql`
- **Branch:** `claude/f2-reprobe-section-d-bucketing-msy3u0`
- **Commit:** `diagnostics(EA F2 re-probe): bake in cafe POI keys + make [D]
  bucketing/visibility explicit`
