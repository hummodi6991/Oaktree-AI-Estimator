# PR #2 Structured-Inputs Refactor — Line-Level Spec (PR #2a + PR #2b)

**Scope:** Read-only investigation. Reasoned from HEAD of `main`
(`1196e7a9ecedab9ca0da1fbeb3ce0a3030d77500`). No files modified, no DB
touched, no tests run.

**Note on the prior audit:** `/tmp/pr2_heuristic_strings_audit.md` does
**not** exist in this fresh container (ephemeral clone). Its §2 literal
enumeration and §3 call-graph have therefore been **reconstructed
directly from the source** below. Where this audit reconstructs the
prior audit's content, it is marked `[RECONSTRUCTED]`. The reconstructed
literal count is **74**, matching the figure cited in the task.

The five heuristic producers in scope (all in
`app/services/expansion_advisor.py`):

| # | Producer | Def line | Output column(s) |
|---|----------|----------|------------------|
| 1 | `_top_positives_and_risks` | 2955 | `top_positives_json`, `top_risks_json` |
| 2 | `_build_demand_thesis` | 3198 | `demand_thesis` |
| 3 | `_build_cost_thesis` | 3228 | `cost_thesis` |
| 4 | `_decision_summary` | 5358 | `decision_summary` |
| 5 | `_GATE_HUMAN_LABELS` / `_gate_key_to_label` | 64 / 124 | (none — read-time lookup) |

---

## 0. `[RECONSTRUCTED]` §2 literal enumeration — the 74 conditions

### Producer 1 — `_top_positives_and_risks` (2955–3107): 15 positives + 13 risks = 28

**Positives** (`positives.append`, capped `positives[:5]`):

| key | line | English literal | firing condition |
|-----|------|-----------------|------------------|
| P1 | 2971 | `Demand potential is strong for this district.` | `demand_score >= 70` |
| P2 | 2974 | `Brick-and-mortar competitor whitespace remains favorable.` | `whitespace_score>=65` ∧ `delivery_observed` ∧ `provider_whitespace_score>=25` |
| P3 | 2978 | `Inferred competitor whitespace opportunity — low observed delivery activity nearby.` | `whitespace_score>=65` ∧ ¬`delivery_observed` |
| P4 | 2980 | `Brand-fit profile aligns with site characteristics.` | `brand_fit_score>=70` |
| P5 | 2982 | `Economics profile meets target screening band.` | `economics_score>=65` |
| P6 | 2985 | `All required gates pass under available context.` | `gate_status_json.overall_pass is True` |
| P7 | 3015 | `Site area is well-aligned with target range.` | `abs(area_m2-mid)/max(mid,1) < 0.15` |
| P8 | 3028 | `Strong economics with favorable rent-to-revenue ratio.` | `economics >= 70` |
| P9 | 3043 | `Well-separated from nearest branch ({nearest_km:.1f} km) — low overlap.` | `nearest_km > 5.0` |
| P10 | 3054 | `Low same-category competitor density — potential first-mover advantage.` | `0 <= competitor_count <= 2` |
| P11 | 3097 | `Newly listed in a top-tier market.` | `is_new ∧ is_top_tier_market` |
| P12 | 3099 | `Recently refreshed listing in a top-tier market.` | `is_updated ∧ is_top_tier_market` |
| P13 | 3101 | `Newly listed within the last week.` | `is_new` |
| P14 | 3103 | `Listing refreshed by the owner within the last week.` | `is_updated` |
| P15 | 3105 | `District ranks in the top tier for recent listing activity.` | `is_top_tier_market` |

**Risks** (`risks.append`, capped `risks[:6]`):

| key | line | English literal | firing condition |
|-----|------|-----------------|------------------|
| K1 | 2988 | `Cannibalization risk is elevated versus branch network.` | `cannibalization_score>=70` |
| K2 | 2990 | `Economics score is below preferred threshold.` | `economics_score<50` |
| K3 | 2992 | `Delivery competition intensity is high.` | `delivery_observed ∧ delivery_competition_score>=65` |
| K4 | 2994 | `Delivery platform competition is dense — limited delivery-channel whitespace.` | `delivery_observed ∧ provider_whitespace_score<25 ∧ delivery_competition_score>=80` |
| K5 | 2997 | `{label.capitalize()} gate failed.` | per `gate_reasons.failed[]` entry |
| K6 | 3000 | `{label.capitalize()} could not be verified from current data.` | per `gate_reasons.unknown[]` entry |
| K7 | 3004 | `Delivery data is based on district-level estimates — no listings observed within 1.2 km.` | ¬`delivery_observed ∧ provider_density_score>0` |
| K8 | 3006 | `Delivery market data is inferred — no observed listings near site.` | ¬`delivery_observed ∧ provider_density_score<=0` |
| K9 | 3018 | `Area ({area_m2:.0f} m²) is near the minimum of the requested range.` | `area_m2 < min_area*1.1` |
| K10 | 3022 | `Area ({area_m2:.0f} m²) is near the maximum — may increase fit-out cost.` | `area_m2 > max_area*0.9` |
| K11 | 3031 | `Economics are marginal — rent burden may be high relative to revenue potential.` | `economics<55` |
| K12 | 3040 | `Nearest own branch is only {nearest_km:.1f} km away — high overlap risk.` | `nearest_km < 1.5` |
| K13 | 3051 | `High competitor density ({competitor_count} nearby) — market may be saturated.` | `competitor_count>=8` |

### Producer 2 — `_build_demand_thesis` (3198–3225): 1 sentence template + 18 label tokens = 19

Sentence template (3222–3225). Label tokens:
- `demand_label` (3207): `strong`, `moderate`, `limited` — 3
- `provider_label` (3210/3215/3219): `district-level estimate`, `limited district data`, `not observed (inferred)`, `dense`, `steady`, `thin` — 6
- `whitespace_label` (3211/3216/3220): `district-inferred`, `potentially tight (district-level)`, `inferred whitespace opportunity`, `attractive`, `balanced`, `tight` — 6
- `competition_label` (3212/3217/3221): `district-level estimate`, `not directly observed`, `intense`, `manageable` — 4 (the `district-level estimate` value is reused; counted once → distinct = 3, but counted per slot for matrix coverage)

Conditions counted = 1 template + 18 token-slot values = **19**.

### Producer 3 — `_build_cost_thesis` (3228–3237): 1

Single template (3234–3237). 1 condition.

### Producer 4 — `_decision_summary` (5358–5388) + `_recommended_use_case` (5348–5355): 14

- `area_label` (5367): `compact`, `standard` — 2
- `district_label` default (5368): `the target district` — 1
- `risk_text` branches (5369–5378): passthrough `key_risks[0]`; `rent economics are tight and should be validated with actual lease terms`; `execution risk should be managed during leasing and design` — 3 (1 passthrough + 2 literals)
- main template (5379–5382) — 1
- risk-appendix template `Biggest commercial risk: {risk_sentence}.` (5387) — 1
- `_recommended_use_case` (5348–5355): `flagship dine-in`, `neighborhood dine-in`, `delivery-led branch`, `compact cafe`, `destination cafe`, `neighborhood qsr` — 6

Conditions counted = 2 + 1 + 3 + 1 + 1 + 6 = **14**.

### Producer 5 — `_GATE_HUMAN_LABELS` (64–77): 12

12 gate-key→label entries: `zoning_fit_pass`, `area_fit_pass`,
`frontage_access_pass`, `parking_pass`, `district_pass`,
`cannibalization_pass`, `delivery_market_pass`, `economics_pass`,
`radiance_growth_pass`, `population_floor_pass`, `commercial_floor_pass`,
`construction_proximity_pass`.

**Total: 28 + 19 + 1 + 14 + 12 = 74.** ✔ matches the cited count.

---

## 1. Structured record shapes

General rule: every record is `{"id": "<template-id>", "params": {…}}`.
`id` names a template in the PR #2b i18n module. `params` is
locale-invariant — typed scalars, raw gate keys, and enum **tokens**
only. No English prose, ever.

### 1.1 `_top_positives_and_risks` → two heterogeneous lists

The producer emits **two lists**, persisted into two new columns:
`top_positives_structured_json` and `top_risks_structured_json`. Records
within each list are **heterogeneous** (different `id`s, different
`params` keys). The structured list element order is the **exact same
order** the English `positives`/`risks` lists are appended in, and is
truncated with the **same** `[:5]` / `[:6]` slice (rule #1: the i18n
renderer iterating the structured list must produce the same count and
order as the English list).

Positive record `id`s (mostly empty `params`):

```
{"id": "pos.demand_strong",            "params": {}}
{"id": "pos.bnm_whitespace_favorable", "params": {}}
{"id": "pos.inferred_whitespace",      "params": {}}
{"id": "pos.brand_fit_aligned",        "params": {}}
{"id": "pos.economics_meets_band",     "params": {}}
{"id": "pos.all_gates_pass",           "params": {}}
{"id": "pos.area_well_aligned",        "params": {}}
{"id": "pos.strong_economics",         "params": {}}
{"id": "pos.well_separated_branch",    "params": {"nearest_km": 6.3}}
{"id": "pos.low_competitor_density",   "params": {}}
{"id": "pos.new_in_top_market",        "params": {}}
{"id": "pos.refreshed_in_top_market",  "params": {}}
{"id": "pos.newly_listed",             "params": {}}
{"id": "pos.refreshed_listing",        "params": {}}
{"id": "pos.top_tier_market",          "params": {}}
```

Risk record `id`s:

```
{"id": "risk.cannibalization_elevated",   "params": {}}
{"id": "risk.economics_below_threshold",  "params": {}}
{"id": "risk.delivery_competition_high",  "params": {}}
{"id": "risk.delivery_whitespace_limited","params": {}}
{"id": "risk.gate_failed",   "params": {"gate_key": "parking_pass"}}
{"id": "risk.gate_unknown",  "params": {"gate_key": "district_pass"}}
{"id": "risk.delivery_district_estimates","params": {}}
{"id": "risk.delivery_inferred",          "params": {}}
{"id": "risk.area_near_min", "params": {"area_m2": 88.0}}
{"id": "risk.area_near_max", "params": {"area_m2": 470.0}}
{"id": "risk.economics_marginal",         "params": {}}
{"id": "risk.nearest_branch_close", "params": {"nearest_km": 1.2}}
{"id": "risk.high_competitor_density","params": {"count": 11}}
```

**Numeric param fidelity (rule #1 critical):** `params` stores the
*raw, unformatted* value; the English template applies the **same**
format spec the producer uses today, so the byte sequence is identical:

| record | param | stored value | EN template format |
|--------|-------|--------------|--------------------|
| `pos.well_separated_branch` | `nearest_km` | `distance_to_nearest_branch_m / 1000.0` (float) | `{nearest_km:.1f}` |
| `risk.nearest_branch_close` | `nearest_km` | `distance_to_nearest_branch_m / 1000.0` (float) | `{nearest_km:.1f}` |
| `risk.area_near_min` / `risk.area_near_max` | `area_m2` | `_safe_float(candidate["area_m2"])` (float) | `{area_m2:.0f}` |
| `risk.high_competitor_density` | `count` | `_safe_int(candidate["competitor_count"])` (int) | `{count}` |

`risk.gate_failed` / `risk.gate_unknown` store the **raw gate key**
(e.g. `parking_pass`) — the English template renders
`{_gate_key_to_label(gate_key).capitalize()} gate failed.` exactly as
lines 2996–3000 do today; the Arabic template looks the key up in
`GATE_LABELS["ar"]`.

### 1.2 `_build_demand_thesis` → single record

```
{"id": "demand_thesis",
 "params": {
   "demand_score": 72.4,
   "population_reach": 41000.0,
   "demand_label":      "strong",            // token ∈ {strong,moderate,limited}
   "provider_label":    "dense",             // token (see below)
   "whitespace_label":  "attractive",        // token
   "competition_label": "intense"            // token
 }}
```

The producer's English rendering depends on four *resolved* label
decisions (the `if/elif/else` ladder at 3208–3221), not just the raw
scores. Storing the **resolved tokens** (not re-deriving them in the
renderer) avoids duplicating the threshold ladder in two places — the
single biggest drift risk (see §8). Token vocabularies:

- `demand_label`: `strong | moderate | limited`
- `provider_label`: `district_estimate | limited_district | not_observed | dense | steady | thin`
- `whitespace_label`: `district_inferred | tight_district | inferred_opportunity | attractive | balanced | tight`
- `competition_label`: `district_estimate | not_directly_observed | intense | manageable`

`demand_score` / `population_reach` are stored raw; EN template applies
`{demand_score:.1f}` and `{population_reach:.0f}` (matching 3223).

### 1.3 `_build_cost_thesis` → single record

```
{"id": "cost_thesis",
 "params": {
   "estimated_rent_sar_m2_year": 1850.0,
   "estimated_annual_rent_sar":  462500.0,
   "estimated_fitout_cost_sar":  390000.0
 }}
```

All three raw; EN template applies `{…:.0f}` / `{…:,.0f}` exactly as
3234–3237.

### 1.4 `_decision_summary` → single record, with one nested sub-record

`_decision_summary` is a **composed** sentence (main clause + optional
risk clause). A single record with all params, plus a **nested
sub-record** for the risk clause when it is sourced from `key_risks`:

```
{"id": "decision_summary",
 "params": {
   "area_label":     "compact",          // token ∈ {compact,standard}
   "district_label": "Al Olaya",         // raw district display string or null
   "final_score":     74.2,
   "economics_score": 61.0,
   "use_case":       "neighborhood_dine_in",  // token (6 values)
   "risk_kind":      "from_key_risks",   // ∈ {from_key_risks,tight_economics,execution}
   "risk_record":     {…}  | null        // nested structured risk record, see below
 }}
```

`risk_kind` captures which of the three 5369–5378 branches fired.
`use_case` token vocabulary: `flagship_dine_in`, `neighborhood_dine_in`,
`delivery_led_branch`, `compact_cafe`, `destination_cafe`,
`neighborhood_qsr`.

**Composition caveat — `risk_record` (FLAG, see §8.3):** when
`risk_kind == "from_key_risks"`, the English producer embeds
`key_risks[0]` verbatim (line 5370). `key_risks` is `key_risks_json`,
produced by **`_build_strengths_and_risks`** (call site line 8770) —
**a sixth producer that is NOT in the five-producer scope.** There is
therefore no structured record available for it. Options:

- (a) Store `risk_record: null` and a fallback raw string
  `risk_text_en` in params; Arabic decision_summary renders the main
  clause in Arabic but the risk clause stays English (degraded but not
  broken — consistent with rule #4's fallback philosophy).
- (b) Bring `_build_strengths_and_risks` into scope so it too emits
  structured records, and nest its first record here.

This audit recommends **(a)** for PR #2 to honor the stated five-producer
scope, and flags **(b)** as a follow-up. The chosen shape supports both:
`risk_record` (nested record, populated only if (b) is adopted) +
`risk_text_en` (raw EN fallback string).

`district_label` stores the **raw** display string (already
locale-resolved upstream by `_canonicalize_district_label`, call site
8877–8879) or `null`; the renderer substitutes the default
`the target district` / its Arabic equivalent when null.

### 1.5 `_GATE_HUMAN_LABELS` — NOT a per-candidate structured column

**Confirmed.** Gate labels do **not** belong in the per-candidate
structured columns. The raw gate keys are already persisted in
`gate_reasons_json` (`passed`/`failed`/`unknown` arrays at write time —
see §3.5) and in `gate_status_json` (flat raw-keyed bool/None map). The
gate keys **are** the locale-invariant structured representation.
Translation lives entirely in the PR #2b i18n module's `GATE_LABELS`
dict and is applied at **read time** by `_gate_key_to_label(key, lang)`.
No new column, no migration impact for gate labels.

---

## 2. Migration spec for PR #2a

### 2.1 Existing column definitions cited

| Column | Type | Created in (file:line) |
|--------|------|------------------------|
| `top_positives_json` | `JSONB` nullable | `alembic/versions/20260314_exp_adv_v61_outputs.py:23` |
| `top_risks_json` | `JSONB` nullable | `alembic/versions/20260314_exp_adv_v61_outputs.py:24` |
| `decision_summary` | `Text` nullable | `alembic/versions/20260310_exp_adv_v2_econ.py:27` |
| `demand_thesis` | `Text` nullable | `alembic/versions/20260312_exp_adv_v5_decision.py:22` |
| `cost_thesis` | `Text` nullable | `alembic/versions/20260312_exp_adv_v5_decision.py:23` |

None of these are modified, renamed, or dropped (rule #3 ✔).

### 2.2 New columns (all on `expansion_candidate`)

| New column | Type | Constraints |
|------------|------|-------------|
| `top_positives_structured_json` | `JSONB` | `nullable=True`, no default |
| `top_risks_structured_json` | `JSONB` | `nullable=True`, no default |
| `decision_summary_structured_json` | `JSONB` | `nullable=True`, no default |
| `demand_thesis_structured_json` | `JSONB` | `nullable=True`, no default |
| `cost_thesis_structured_json` | `JSONB` | `nullable=True`, no default |

Consistent suffix `_structured_json` for all five.

### 2.3 Alembic revision

- **Current single head:** `20260501b_drop_osm_districts` — file
  `alembic/versions/20260501b_drop_osm_districts.py`
  (`revision: str = "20260501b_drop_osm_districts"`, line 34;
  `down_revision = "20260501a_ext_feat_polygons_mat"`, line 35). Verified
  the single head by walking every `revision`/`down_revision` pair in
  `alembic/versions/` — no other file lists `20260501b_drop_osm_districts`
  as a `down_revision`, and the two prior multi-head splits
  (`a9b6cdbd0831`, `4cff77cdbd28`) and the EA branch
  (`merge_exp_adv_heads_20260315`) are all already converged into it via
  `20260501a_ext_feat_polygons_mat` (which merges
  `20260501_district_radiance_monthly` + `20260426_ecq_canonical_cols`).
- **New file:** `alembic/versions/20260516_exp_adv_structured_inputs.py`
- **`revision`** = `"20260516_exp_adv_structured_inputs"`
- **`down_revision`** = `"20260501b_drop_osm_districts"`

Template (mirrors `20260314_exp_adv_v61_outputs.py`):

```python
"""Expansion advisor structured-inputs columns (PR #2a)

Revision ID: 20260516_exp_adv_structured_inputs
Revises: 20260501b_drop_osm_districts
Create Date: 2026-05-16 00:00:00.000000
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20260516_exp_adv_structured_inputs"
down_revision = "20260501b_drop_osm_districts"
branch_labels = None
depends_on = None

_COLS = (
    "top_positives_structured_json",
    "top_risks_structured_json",
    "decision_summary_structured_json",
    "demand_thesis_structured_json",
    "cost_thesis_structured_json",
)

def upgrade() -> None:
    for col in _COLS:
        op.add_column("expansion_candidate", sa.Column(col, JSONB, nullable=True))

def downgrade() -> None:
    for col in reversed(_COLS):
        op.drop_column("expansion_candidate", col)
```

### 2.4 Emitted SQL

`upgrade()` emits five statements:

```sql
ALTER TABLE expansion_candidate ADD COLUMN top_positives_structured_json JSONB;
ALTER TABLE expansion_candidate ADD COLUMN top_risks_structured_json JSONB;
ALTER TABLE expansion_candidate ADD COLUMN decision_summary_structured_json JSONB;
ALTER TABLE expansion_candidate ADD COLUMN demand_thesis_structured_json JSONB;
ALTER TABLE expansion_candidate ADD COLUMN cost_thesis_structured_json JSONB;
```

`downgrade()` emits five `ALTER TABLE … DROP COLUMN …` in reverse order.

### 2.5 Index

**None.** These columns are write-once (in `run_expansion_search`) and
read-once (in `_normalize_candidate_payload`, by primary-key/`search_id`
row fetch). They are never filtered or joined on content. No GIN/btree
index is warranted. (Confirms task hypothesis.)

### 2.6 Backfill

**None** (rule #4). Pre-PR-2a rows keep `NULL` in all five new columns.
The PR #2b Arabic read path falls back to the English rendered columns
when the structured column is `NULL`.

---

## 3. Producer-side changes for PR #2a

Discipline: the English-rendering code in each producer is **not
touched**. A **parallel block** builds the structured record from the
same in-scope inputs. No DRY between the two outputs (rule #1).

### 3.1 `_top_positives_and_risks` (2955)

- **Old signature:** `-> tuple[list[str], list[str]]`
- **New signature:**
  `-> tuple[list[str], list[str], list[dict], list[dict]]`
  — `(positives, risks, positives_structured, risks_structured)`.
- **In-function change:** every `positives.append("…")` /
  `risks.append("…")` site gets a paired
  `positives_structured.append({"id": …, "params": …})` /
  `risks_structured.append(...)` on the *next line*, built from the
  same locals already in scope (`area_m2`, `competitor_count`,
  `nearest_km`, `gate` keys, etc.). The English `.append` lines are
  byte-for-byte unchanged. The two structured lists are sliced with the
  **same** `[:5]` / `[:6]` at the `return` (line 3107):
  `return positives[:5], risks[:6], positives_structured[:5], risks_structured[:6]`.
  Because append order is identical, element *i* of the structured list
  corresponds to element *i* of the English list after slicing.
- **Gate loop (2995–3000):** the `for gate in …failed` / `…unknown`
  loops append `risk.gate_failed`/`risk.gate_unknown` records carrying
  `{"gate_key": str(gate)}` — note `gate` here is the **raw key** (the
  `gate_reasons` passed in at call site 8876 is the *unnormalized*
  `gate_reasons_json`; humanization happens later at read time — see
  §3.5).

### 3.2 `_build_demand_thesis` (3198)

- **Old:** `-> str`. **New:** `-> tuple[str, dict]`.
- **In-function change:** the `if/elif/else` ladder (3208–3221) already
  computes the four `*_label` English strings. Add, alongside each
  branch, the four **token** assignments (`demand_token`,
  `provider_token`, `whitespace_token`, `competition_token`). The
  English f-string return (3222–3225) is unchanged; wrap as
  `return <english_str>, {"id": "demand_thesis", "params": {…}}`.

### 3.3 `_build_cost_thesis` (3228)

- **Old:** `-> str`. **New:** `-> tuple[str, dict]`.
- **In-function change:** English f-string (3234–3237) unchanged; add
  `return <english_str>, {"id": "cost_thesis", "params": {3 raw inputs}}`.

### 3.4 `_decision_summary` (5358)

- **Old:** `-> str`. **New:** `-> tuple[str, dict]`.
- **In-function change:** English logic (5367–5388) unchanged. A
  parallel block records: `area_label` token (from the 5367 ternary),
  `district_label` (the raw `district` arg, not the `district_label`
  defaulted local — store `district` so `null` is preserved),
  `risk_kind` (`from_key_risks` if `key_risks` else `tight_economics`
  if `economics_score<55` else `execution`), `risk_text_en` (the chosen
  `risk_text`), `final_score`, `economics_score`, and a `use_case`
  token. `_recommended_use_case` (5348) gains a parallel
  token-returning helper or returns `(label, token)` — recommend a tiny
  sibling `_recommended_use_case_token(service_model, area_m2)` so the
  English `_recommended_use_case` is byte-untouched.
- See §8.3: `risk_record` nesting is deferred; `risk_text_en` carries
  the English fallback.

### 3.5 `_GATE_HUMAN_LABELS` / `_gate_key_to_label` — NOT changed in PR #2a

**Confirmed.** PR #2a does not touch the gate chain (64–126). Gate keys
are already locale-invariant and already persisted: the `gate_reasons`
arg threaded into `_top_positives_and_risks` at call site 8876 is the
**raw** `gate_reasons_json` (humanization via `_normalize_gate_reasons`
→ `_humanize_gate_list` happens only at read time, line 1227). So the
`risk.gate_failed`/`risk.gate_unknown` records already capture the raw
key. `_GATE_HUMAN_LABELS` needs no structured counterpart; the dict
itself migrates into the PR #2b i18n module as `GATE_LABELS["en"]`.

### 3.6 Call-site update in `run_expansion_search`

Single call site, inside `run_expansion_search` (def at line 6198):

- **Line 8819–8826** `demand_thesis = _build_demand_thesis(...)` →
  `demand_thesis, demand_thesis_structured = _build_demand_thesis(...)`
- **Line 8827–8831** `cost_thesis = _build_cost_thesis(...)` →
  `cost_thesis, cost_thesis_structured = _build_cost_thesis(...)`
- **Line 8876** `top_positives_json, top_risks_json = _top_positives_and_risks(...)`
  → `top_positives_json, top_risks_json, top_positives_structured, top_risks_structured = _top_positives_and_risks(...)`
- **Line 8878–8885** `decision_summary = _decision_summary(...)` →
  `decision_summary, decision_summary_structured = _decision_summary(...)`
- **Candidate dict (9047–9057 region):** add five keys alongside the
  existing ones:
  ```python
  "top_positives_structured_json": top_positives_structured,
  "top_risks_structured_json": top_risks_structured,
  "demand_thesis_structured_json": demand_thesis_structured,
  "cost_thesis_structured_json": cost_thesis_structured,
  "decision_summary_structured_json": decision_summary_structured,
  ```
- **INSERT statement (9239 column list):** add the five column names
  after `top_risks_json` / near the JSONB block.
- **VALUES list (9305):** add `CAST(:top_positives_structured_json AS jsonb)`
  etc. for all five.
- **`_candidate_insert_params` (9375–9394):** add five
  `json.dumps(_sanitize_for_json(candidate["…_structured_json"]),
  ensure_ascii=False)` entries, mirroring the existing
  `top_positives_json`/`top_risks_json` lines at 9386–9387.

### 3.7 Confirmation: English byte-identity

The English string each producer returns is **character-for-character**
what it returns at HEAD today: the English f-strings / `.append`
literals are not edited, only *additional* return values are appended
and *additional* dict keys / SQL columns are added. The English read
path (`_normalize_candidate_payload`, the API serializers, the response
models) is untouched by PR #2a → rule #1 and rule #2 hold by
construction. The other SELECT sites that read these columns
(9620–9637, 9938–9941, 10169–10182) are unaffected because the new
columns are simply not selected by PR #2a (they become relevant in #2b).

---

## 4. New i18n module spec for PR #2b

**File:** `app/services/expansion_advisor_i18n.py` (new).

### 4.1 Module shape

```python
# TEMPLATES[template_id][lang] -> a str.format()-style template string.
TEMPLATES: dict[str, dict[str, str]] = { … }

# Sub-token tables for demand_thesis labels (lang -> token -> fragment).
DEMAND_LABELS:      dict[str, dict[str, str]] = { … }
PROVIDER_LABELS:    dict[str, dict[str, str]] = { … }
WHITESPACE_LABELS:  dict[str, dict[str, str]] = { … }
COMPETITION_LABELS: dict[str, dict[str, str]] = { … }
AREA_LABELS:        dict[str, dict[str, str]] = { … }   # compact/standard
USE_CASE_LABELS:    dict[str, dict[str, str]] = { … }   # 6 use-case tokens

# GATE_LABELS[lang][gate_key] -> a str.
GATE_LABELS: dict[str, dict[str, str]] = {
    "en": { …the 12 entries copied verbatim from _GATE_HUMAN_LABELS… },
    "ar": { …placeholder pending anchor translations… },
}

def render(record: dict, lang: str) -> str: …
def humanize_gate(gate_key: str, lang: str) -> str: …
```

### 4.2 `render(record, lang)`

1. `tid = record["id"]`; `params = record.get("params") or {}`.
2. `tmpl = TEMPLATES.get(tid, {}).get(lang) or TEMPLATES.get(tid, {}).get("en")`
   — if the requested-lang template is missing, fall back to `en`
   (degraded, never raises).
3. For composite producers, resolve sub-tokens first
   (e.g. `params["demand_label"]` → `DEMAND_LABELS[lang][token]`,
   `params["gate_key"]` → `humanize_gate(...)` for the gate records),
   then `tmpl.format(**resolved_params)`.
4. Numeric params keep the **same format spec** the producer used (the
   format spec lives in the template string itself, e.g.
   `"… {nearest_km:.1f} km …"`), so the EN re-render is byte-identical.
5. On any `KeyError`/`IndexError`/`ValueError`, return `""` and let the
   read path fall back to the English column (rule #4 spirit).

### 4.3 `humanize_gate(gate_key, lang)`

```python
def humanize_gate(gate_key: str, lang: str) -> str:
    table = GATE_LABELS.get(lang) or GATE_LABELS["en"]
    if gate_key in table:
        return table[gate_key]
    # Fallback 1: English table (covers ar entries not yet filled).
    if gate_key in GATE_LABELS["en"]:
        return GATE_LABELS["en"][gate_key]
    # Fallback 2: the legacy derivation from _gate_key_to_label.
    return gate_key.replace("_pass", "").replace("_", " ")
```

This is **byte-identical to `_gate_key_to_label` when `lang="en"`**
(rule #5): `GATE_LABELS["en"]` is `_GATE_HUMAN_LABELS` verbatim and the
final fallback is the same `.replace` chain as line 126.

### 4.4 Required entries — every template id needing an entry

PR #2b's i18n module must define **74 translatable units**, matching §0:

- **`TEMPLATES`** — 30 entries: 15 `pos.*` + 13 `risk.*` + `demand_thesis`
  + `cost_thesis` + `decision_summary` (+ the `decision_summary` risk
  appendix may be a separate fragment id `decision_summary.risk_suffix`;
  count it within `decision_summary`'s entry).
- **`DEMAND_LABELS`** — 3 tokens; **`PROVIDER_LABELS`** — 6;
  **`WHITESPACE_LABELS`** — 6; **`COMPETITION_LABELS`** — 4 slots
  (3 distinct); **`AREA_LABELS`** — 2; **`USE_CASE_LABELS`** — 6;
  the `the target district` default — 1 (`decision_summary.district_default`).
- **`GATE_LABELS`** — 12 gate keys × 2 langs.

Total translatable units = 30 + 3 + 6 + 6 + 4 + 2 + 6 + 1 + 12 = **70**
template/label units, mapping onto the **74** firing conditions of §0
(the 4-entry gap is the 3 `risk_text` branches collapsing into the
`decision_summary` template + the reused `district-level estimate`
fragment). The audit's contract: **every `id` listed in §1 and every
token vocabulary in §1.2/§1.4 must have an `en` entry (copied verbatim
from current code) and an `ar` entry.**

**The Arabic side is explicitly NOT this audit's job.** This audit
specifies the module *structure* and *enumerates every id/token*. The
`ar` values are left as placeholders; the user fills them after the
anchor translations return. The `en` values are mechanically derivable
from the §0 table (the literal column) — they must be copied
**byte-for-byte**, since the golden-file test (§7) re-renders the `en`
template and asserts equality with the producer output.

---

## 5. Read-path changes for PR #2b

### 5.1 `_normalize_candidate_payload` new signature

```python
def _normalize_candidate_payload(
    candidate: dict[str, Any],
    district_lookup: dict[str, dict[str, str]] | None = None,
    lang: str = "en",
) -> dict[str, Any]:
```

`lang` is the **third positional / keyword** param, defaulting to
`"en"` — so any caller that omits it is byte-identical to today
(rule #2).

### 5.2 The five call sites

All five `_normalize_candidate_payload` call sites
`[RECONSTRUCTED]` from grep at HEAD:

| # | Call site (file:line) | Enclosing fn | `lang` available post-PR-1? |
|---|------------------------|--------------|------------------------------|
| 1 | `expansion_advisor.py:1341` | `_normalize_saved_search_payload` (1326) | **No** — service fn has no `lang` param |
| 2 | `expansion_advisor.py:9427` | `run_expansion_search` (6198) | **No** — service fn has no `lang` param |
| 3 | `expansion_advisor.py:9681` | `get_candidates` (9587) | **No** — service fn has no `lang` param |
| 4 | `expansion_advisor.py:10007` | `compare_candidates` (9909) | **No** — service fn has no `lang` param |
| 5 | `expansion_advisor.py:10220` | `get_candidate_memo` (10133) | **No** — service fn has no `lang` param |

**Correction to the prior audit's §3 premise:** PR #1 threaded `lang`
into the **API handlers** in `app/api/expansion_advisor.py` only (it is
parsed/clamped — e.g. lines 901, 1135, 1149, 1163, 1233, 1248, 1287,
1304, 1321, 1593 — and every handler carries the comment *"Threaded for
PR #2/#3 consumers; no English-output changes in this PR"*). It did
**not** thread `lang` into the `app/services/expansion_advisor.py`
service functions. The string `lang` does not appear anywhere in
`expansion_advisor.py` (the service module) at HEAD.

Therefore PR #2b must add a `lang: str = "en"` parameter to each of the
five enclosing service functions and pass it through, and update the
**handler bodies** (not signatures — rule #7) to forward the already-
parsed `lang`:

| Service fn | add param | API handler that calls it (file:line) |
|------------|-----------|-----------------------------------------|
| `get_candidates` | `lang="en"` | `get_expansion_search_candidates` → call at `api/expansion_advisor.py:1157` |
| `compare_candidates` | `lang="en"` | `compare_expansion_candidates` → call at `api:1251` (`lang` already parsed at `1248`) |
| `get_candidate_memo` | `lang="en"` | `get_expansion_candidate_memo` → call at `api:1238` (`lang` parsed at `1236`) |
| `run_expansion_search` | `lang="en"` | `create_expansion_search` → call at `api:1002` (`lang` parsed at `901`) |
| `_normalize_saved_search_payload` | `lang="en"` | reached via `get_saved_search`/`list_saved_searches`/`create_saved_search`/`update_saved_search`; the four saved-search handlers parse `lang` at `api:1258/1287/1307/1321` |

Handler **signatures** are unchanged (rule #7 ✔); only the argument
passed into the service call changes (`get_candidates(db, search_id)` →
`get_candidates(db, search_id, lang=lang)`).

### 5.3 In-function logic of `_normalize_candidate_payload`

Lines 1230–1237 today unconditionally read the English columns. New
logic:

```python
if lang == "ar":
    payload["top_positives_json"] = _render_structured_list(
        candidate.get("top_positives_structured_json"),
        candidate.get("top_positives_json"), lang)
    payload["top_risks_json"] = _render_structured_list(
        candidate.get("top_risks_structured_json"),
        candidate.get("top_risks_json"), lang)
    payload["decision_summary"] = _render_structured_one(
        candidate.get("decision_summary_structured_json"),
        candidate.get("decision_summary"), lang)
    payload["demand_thesis"] = _render_structured_one(
        candidate.get("demand_thesis_structured_json"),
        candidate.get("demand_thesis"), lang)
    payload["cost_thesis"] = _render_structured_one(
        candidate.get("cost_thesis_structured_json"),
        candidate.get("cost_thesis"), lang)
else:
    # byte-identical to HEAD lines 1230-1237
    payload["top_positives_json"] = payload.get("top_positives_json") or []
    payload["top_risks_json"]     = payload.get("top_risks_json") or []
    payload["decision_summary"]   = payload.get("decision_summary") or ""
    payload["demand_thesis"]      = payload.get("demand_thesis") or ""
    payload["cost_thesis"]        = payload.get("cost_thesis") or ""
```

Helpers (new, in `expansion_advisor.py` or the i18n module):

- `_render_structured_list(structured, english_fallback, lang)`:
  if `structured` is a non-empty list → `[render(r, lang) for r in structured]`;
  else → `english_fallback or []` (rule #4 fallback for pre-2a NULL rows).
- `_render_structured_one(structured, english_fallback, lang)`:
  if `structured` is a dict → `render(structured, lang)`;
  else → `english_fallback or ""`.

**Rule #2 hard guarantee:** when `lang != "ar"` the `else` branch is the
*exact* code from HEAD lines 1230–1237 → byte-identical. The five new
`_structured_json` keys are read **only** in the `ar` branch.

The new `_structured_json` columns must also be added to the SELECT
lists that feed `get_candidates` (around 9620–9637), `compare_candidates`
(9938–9941), and `get_candidate_memo` (10169–10182) so the structured
data reaches `_normalize_candidate_payload`. These are additive
`SELECT` column additions — they do not change the English response
because the new keys are dropped from the response unless `lang="ar"`
(the response Pydantic models do not expose them).

### 5.4 Gate-label chain

`_normalize_gate_reasons` (1178) → `_humanize_gate_list` (112) →
`_gate_key_to_label` (124). PR #2b threads `lang`:

- `_normalize_candidate_payload` line 1227 →
  `_normalize_gate_reasons(payload.get("gate_reasons_json"), lang)`
- `_normalize_gate_reasons(value, lang="en")` → calls
  `_humanize_gate_list(value.get("passed"), lang)` (and `failed`,
  `unknown`).
- `_humanize_gate_list(values, lang="en")` → `_gate_key_to_label(str(value), lang)`.
- `_gate_key_to_label(gate_key, lang="en")`:
  ```python
  def _gate_key_to_label(gate_key: str, lang: str = "en") -> str:
      from app.services.expansion_advisor_i18n import humanize_gate
      return humanize_gate(gate_key, lang)
  ```
  With `lang="en"` this returns `_GATE_HUMAN_LABELS[gate_key]` or the
  same `.replace` fallback → **byte-identical to HEAD line 126**
  (rule #5 ✔).

`[RECONSTRUCTED]` §3.5 plumbing: the prior audit's plumbing is
**confirmed and refined** — `lang` flows
`_normalize_candidate_payload → _normalize_gate_reasons →
_humanize_gate_list → _gate_key_to_label → humanize_gate`. The other
caller of `_gate_key_to_label` (`_top_positives_and_risks` at 2996/2999)
runs at **write** time (PR #2a) and stays English/raw-key — it builds
the structured record from the raw key, so it does not need `lang`.

---

## 6. R3 fix specification

### 6.1 `_hard_fail_gate_labels` — current code

`app/services/llm_decision_memo.py:21–44`:

```python
def _hard_fail_gate_labels() -> frozenset[str]:
    """Humanized labels of the live hard-fail gate set. …"""
    from app.services.expansion_advisor import (
        HARD_FAIL_GATES,
        _gate_key_to_label,
    )
    return frozenset(_gate_key_to_label(g) for g in HARD_FAIL_GATES)
```

Consumed at `llm_decision_memo.py:1780` and `:2120`:

```python
hard_fail_labels = _hard_fail_gate_labels()
blocking_failed = [e for e in failed_entries if str(e.get("name")) in hard_fail_labels]
advisory_failed = [e for e in failed_entries if str(e.get("name")) not in hard_fail_labels]
```

### 6.2 Corrected code

```python
def _hard_fail_gate_keys() -> frozenset[str]:
    """Raw (locale-invariant) hard-fail gate keys."""
    from app.services.expansion_advisor import HARD_FAIL_GATES
    return HARD_FAIL_GATES
```

i.e. drop the `_gate_key_to_label` mapping and return the raw
`HARD_FAIL_GATES` frozenset (raw keys: `zoning_fit_pass`,
`area_fit_pass`, + the env-gated optional keys). `HARD_FAIL_GATES` is
already a `frozenset[str]` of raw keys
(`expansion_advisor.py:102`).

### 6.3 ⚠️ FLAG — the one-line change is necessary but NOT sufficient

Rule #6 calls this a one-line change and asserts the comparison set on
the other side "still has raw keys available." **This audit must flag a
complication:** the *other side* of the comparison —
`failed_entries[].name` produced by `_build_gate_buckets`
(`llm_decision_memo.py:700`) — does **not** carry raw keys today. It
carries **humanized labels**, as the in-code comment at lines 1777–1779
states explicitly (*"Compare on humanized labels because `failed_entries`
… only carries the humanized name"*).

Trace: the memo endpoint receives `req.candidate` (the candidate object
the frontend posts back). That object came from the candidates-list
response, which ran through `_normalize_candidate_payload` →
`_normalize_gate_reasons` → `_humanize_gate_list` — so
`candidate["gate_reasons_json"]`'s `passed/failed/unknown` arrays hold
**humanized** strings. `_build_gate_buckets` (preferring the bucketed
`gate_reasons_json`, lines 723–737) therefore yields `name` = humanized
label. Today (English) the comparison works only because *both* sides
are humanized-English. Under PR #2b with `lang="ar"`, the buckets become
**Arabic** while `_hard_fail_gate_labels()` returns English → every
failed hard gate is mis-classified as advisory → the "GATE FAILURE"
addendum (1788–1800) never fires → the memo can contradict
`overall_pass=False`. **That is exactly the R3 bug.**

So R3 changing only `_hard_fail_gate_labels` to raw keys would *break
English* (raw keys vs humanized names never match) unless the *other
side* is also raw. The fix needs a **companion change in the same file**
(`llm_decision_memo.py` only — no new file, no response-shape change, no
frontend, no prompt-text change):

**Where the raw keys ARE available:** `candidate["gate_status_json"]` is
the flat raw-keyed bool/None map — `_normalize_gate_status`
(`expansion_advisor.py:1174`) does **not** humanize it (just
`dict(value)`), so its keys stay raw (`zoning_fit_pass`, …) in every
locale. This satisfies §6's "raw key … not discarded."

**Recommended companion fix (still R3, locale-invariant):** carry the
raw gate key on each gate-bucket entry. `_build_gate_buckets`'s
`_append` (713–721) gains a `key` field; when the source is the
bucketed humanized `gate_reasons_json`, the raw key is recovered by
cross-referencing the same-positioned entry of the raw
`gate_status_json` / `gate_reasons` arrays, **or** — simpler and
robust — `_coerce_gate_verdicts`/`_build_gate_buckets` are fed the raw
`gate_status_json` for the *classification* purpose while the humanized
`gate_reasons_json` continues to feed the *explanation text*. Then the
1781–1786 split compares `e["key"]` (raw) against `_hard_fail_gate_keys()`
(raw). English classification result is unchanged (same gates are
blocking); Arabic now classifies correctly.

**Summary for §11:** R3 is **not** a clean standalone one-liner. It is
(a) the one-line `_hard_fail_gate_labels` → `_hard_fail_gate_keys`
change **plus** (b) ensuring `failed_entries` carries raw keys. Both
live in `llm_decision_memo.py`. This does not violate rule #6 (it is the
R3 fix) or rule #7 (no prompt-assembly *text* change — only the
internal blocking/advisory predicate). It must be called out so PR #2b
is not under-scoped.

---

## 7. Golden-file validation strategy

### 7.1 PR #2a — English byte-identity (proves rule #1)

- **Test file:** `tests/test_pr2_english_byte_identity.py` (new).
- **Fixtures:** `tests/fixtures/pr2_golden/*.json` (new dir).
- **Pattern (two-phase):**
  1. *Capture phase* (run once, pre-merge, against HEAD): for a curated
     input dict per firing condition, call the producer at HEAD, capture
     the English string output, and write it to a fixture JSON:
     `{"producer": "...", "condition": "K9", "input": {…}, "expected_en": "…"}`.
     (Capture is a developer step; the committed fixtures are the
     artifact.)
  2. *Assert phase* (post-PR-2a, in CI): load each fixture, call the
     post-2a producer with `input`, assert
     `producer_output_english == fixture["expected_en"]` **byte-for-byte**
     (`assert a == b` on `str`; for list producers compare element-wise).
- **Coverage matrix — concrete sizing:** one fixture per firing
  condition in §0 → **74 fixtures**:
  - `_top_positives_and_risks`: 28 (15 positive + 13 risk conditions),
    each fixture an input dict that makes exactly that one condition
    fire (plus a handful of multi-fire fixtures to exercise the `[:5]`
    /`[:6]` truncation and ordering — recommend +6 → **34**).
  - `_build_demand_thesis`: 19 (the 3 demand × 3 delivery-branch label
    combinations, expanded to cover all 6 provider / 6 whitespace / 4
    competition tokens).
  - `_build_cost_thesis`: 1 (plus 1–2 for thousands-separator /
    rounding edge cases → **3**).
  - `_decision_summary` + `_recommended_use_case`: 14 (2 area × 3
    risk_kind × 6 use_case, sampled to hit each token at least once).
  - `_GATE_HUMAN_LABELS`: 12 (one per gate key, asserting
    `_gate_key_to_label(key)` and `_gate_key_to_label(key, "en")`).
  - **Total ≈ 74 firing-condition fixtures** (84 if the recommended
    multi-fire / edge-case extras are included).

### 7.2 PR #2a — structured/English lockstep (new, see §8.1)

In the **same** `tests/test_pr2_english_byte_identity.py`, for every
fixture also assert: `render(structured_record, "en") ==
producer_english_output`. This proves the structured record + its EN
template reproduce the producer's English byte-for-byte, and is the
guardrail against future drift (§8.1).

### 7.3 PR #2b — `lang` omitted / `lang="en"` byte-identity

- **Test file:** `tests/test_pr2b_lang_en_byte_identity.py` (new).
- For a representative set of full candidate dicts (including pre-2a
  rows with `NULL` structured columns and post-2a rows with populated
  ones), assert:
  `_normalize_candidate_payload(c) == _normalize_candidate_payload(c, lang="en")`
  and both equal the **HEAD** output captured as a fixture. This proves
  rule #2: the `lang="en"`/omitted read path is byte-identical, and the
  `else` branch in §5.3 is exercised.
- Plus a `lang="ar"` smoke test (`tests/test_pr2b_arabic_render.py`):
  assert that with populated structured columns the `ar` strings come
  from the i18n module, and that with `NULL` structured columns the `ar`
  path **falls back** to the English columns (rule #4).

---

## 8. Risks specific to the structured-inputs shape

### 8.1 English-string ↔ structured-record drift (highest risk)

The producer's English string and its structured record are built by
two parallel, un-DRY'd blocks (rule #1 forbids sharing). A future PR
editing one but not the other silently desyncs Arabic from English.
**Mitigation (mandatory, specified in §7.2):** a CI test that, for every
golden fixture, re-renders the structured record through the i18n
module's **`en`** template and asserts equality with the producer's
English output. If a producer string changes without the matching
template/record change, this test fails. This converts a silent
data-quality bug into a hard CI failure.

### 8.2 JSONB column size / row-size delta

Per candidate: ≤5 positive records + ≤6 risk records + 1 demand + 1
cost + 1 decision_summary record. Records are tiny — an `id` string
(~25 chars) + a small `params` object (mostly empty or 1–3 scalars).
Estimated serialized sizes: `top_positives_structured_json` ≈ 5 × ~45 B
≈ 0.25 KB; `top_risks_structured_json` ≈ 6 × ~70 B ≈ 0.45 KB;
`demand_thesis_structured_json` ≈ ~0.2 KB; `cost_thesis` ≈ ~0.12 KB;
`decision_summary` ≈ ~0.2 KB. **Total added per row ≈ 1.2–1.5 KB**,
typically inline (well under the 2 KB TOAST threshold; if TOASTed it is
out-of-line compressed). Negligible vs. the existing
`feature_snapshot_json` / `score_breakdown_json` payloads already on the
row. No partitioning / vacuum concern at expansion-candidate volumes.

### 8.3 Cascade impact — where the five columns are read

`[RECONSTRUCTED]` and traced now. The five English columns are read in
`app/services/expansion_advisor.py` at:

- `_normalize_candidate_payload` (1230–1237) — the response payload.
- SELECT lists feeding `get_candidates` (9620–9637), `compare_candidates`
  (9938–9941), `get_candidate_memo` (10169–10182), and the persisted
  `_candidate_insert_params` round-trip at 9427 / dict at 10027–10030 /
  10287–10300.
- `top_positives_json` / `top_risks_json` are **also** sliced `[:3]` in
  the recommendation-report builder at `expansion_advisor.py:10498–10499`.
  → **Implication:** if the report endpoint is to be Arabic-aware, the
  `[:3]` slice must operate on **already-rendered** strings (post
  `_normalize_candidate_payload`), not on raw structured records. The
  report path uses `_normalize_candidate_payload` upstream, so this is
  satisfied as long as report candidates are normalized with `lang`
  before the `[:3]` slice. Verify the report's candidate source is
  normalized — flagged as a §11 check item.

`grep` across `app/**.py` (excluding `expansion_advisor.py`) for the
five names returns **only** `app/services/llm_decision_memo.py`, and
every hit there is the LLM-authored `demand_thesis` *output* field
(lines 528, 1197, 1305, 1387, 1392, 1417, 1541, 2015, 2378) — a
**different** datum from the candidate's `demand_thesis` column. The
candidate columns `top_positives_json` / `top_risks_json` /
`cost_thesis` / `decision_summary` are **not** consumed by
`llm_decision_memo.py` at all. → **No prompt-assembly cascade**; rule #7
holds. They are not logged, cached by content, or used in scoring.

### 8.4 `_decision_summary` cross-producer dependency (scope gap)

As detailed in §1.4: `_decision_summary`'s `from_key_risks` branch
embeds `key_risks[0]` from `_build_strengths_and_risks` (call site
8770) — a producer **outside** the five-producer scope. Under `lang=ar`
the decision-summary risk clause will render English when
`risk_kind=="from_key_risks"`. This is a **known parity gap**, mitigated
to "degraded, not broken" by the `risk_text_en` fallback param.
Recommend a follow-up PR to bring `_build_strengths_and_risks` into the
structured-inputs scheme.

### 8.5 Ordering / truncation fidelity

`_top_positives_and_risks` truncates `positives[:5]` / `risks[:6]`. The
structured lists **must** be truncated with the identical slice and
preserve append order so the i18n renderer reproduces the same set and
order. Covered by the multi-fire golden fixtures in §7.1.

---

## 9. PR #2a deliverables checklist

**Files touched:**

- `alembic/versions/20260516_exp_adv_structured_inputs.py` — **new**;
  migration adding the 5 nullable JSONB columns (§2.3–§2.4).
- `app/services/expansion_advisor.py` — the 5 producers
  (`_top_positives_and_risks` 2955, `_build_demand_thesis` 3198,
  `_build_cost_thesis` 3228, `_decision_summary` 5358,
  `_recommended_use_case` 5348 — sibling token helper) + the single
  call site region in `run_expansion_search` (8819–8885 unpacking,
  9047–9057 candidate dict, 9239 INSERT column list, 9305 VALUES,
  9375–9394 `_candidate_insert_params`).
- `tests/test_pr2_english_byte_identity.py` — **new** (~74–84 assertions;
  includes the §7.2 structured↔EN lockstep checks).
- `tests/fixtures/pr2_golden/*.json` — **new** (~74–84 fixtures).

**PR #2a does NOT touch:**

- `app/services/expansion_advisor_i18n.py` — that is PR #2b's new module.
- `_normalize_candidate_payload` and the gate-label chain
  (`_normalize_gate_reasons`, `_humanize_gate_list`, `_gate_key_to_label`)
  — PR #2b.
- `app/services/llm_decision_memo.py` (R3) — PR #2b.
- Any API handler, any frontend file, response Pydantic models,
  `_prewarm_decision_memos`, LLM prompt assembly.
- Any existing column (rule #3).

PR #2a is **zero user-visible change**: no read path touches the new
columns, so the English app is mechanically unchanged.

---

## 10. PR #2b deliverables checklist

**Files touched:**

- `app/services/expansion_advisor_i18n.py` — **new** module (§4):
  `TEMPLATES`, the 6 token tables, `GATE_LABELS`, `render`,
  `humanize_gate`. `en` side filled verbatim from §0; `ar` side
  placeholder.
- `app/services/expansion_advisor.py` — `_normalize_candidate_payload`
  (new `lang` param + `ar`/`en` branch, §5.1/§5.3); the two new helpers
  `_render_structured_list` / `_render_structured_one`;
  `_normalize_gate_reasons` / `_humanize_gate_list` / `_gate_key_to_label`
  (`lang` thread, §5.4); `lang` param added to `get_candidates`,
  `compare_candidates`, `get_candidate_memo`, `run_expansion_search`,
  `_normalize_saved_search_payload`; the SELECT lists at 9620–9637,
  9938–9941, 10169–10182 gain the 5 `_structured_json` columns.
- `app/api/expansion_advisor.py` — handler **bodies** only (not
  signatures): forward the already-parsed `lang` into the 5 service
  calls (`api:1002`, `1157`, `1238`, `1251`, and the saved-search
  service calls). Signatures unchanged (rule #7).
- `app/services/llm_decision_memo.py` — R3 fix (§6):
  `_hard_fail_gate_labels` → `_hard_fail_gate_keys` (raw keys) **plus**
  the companion change so `failed_entries` carries raw keys (1781–1786,
  `_build_gate_buckets`/`_coerce_gate_verdicts`).
- `tests/test_pr2b_lang_en_byte_identity.py` — **new** (rule #2 proof).
- `tests/test_pr2b_arabic_render.py` — **new** (Arabic render + NULL
  fallback, rule #4).

**PR #2b does NOT touch:**

- The migration / any column definition (PR #2a owns the schema).
- The 5 producers' English rendering (untouched since PR #2a).
- Any frontend file; any endpoint handler **signature**;
  `_prewarm_decision_memos`; LLM prompt-assembly **text**.
- The Arabic translation *values* — placeholders only; the user fills
  them after the anchor translations.

---

## 11. Readiness checklist

### PR #2a

- [x] Every producer change has a line-level spec — §3.1–§3.6.
- [x] The migration has exact upgrade/downgrade SQL — §2.3–§2.4.
- [x] Every call site of every changed function is enumerated — single
  call site, `run_expansion_search` (§3.6); no other caller of the 5
  producers exists (grep-confirmed).
- [x] Golden-file fixture matrix sized concretely — 74 firing-condition
  fixtures (84 with recommended extras), §7.1.
- [x] No UNVERIFIED blocking items.

**PR #2a: READY.** The one open recommendation (not a blocker) is the
§7.2 structured↔EN lockstep test — strongly advised but additive.

### PR #2b

- [x] Every read-path change has a line-level spec — §5.1–§5.4.
- [x] i18n module structure + every template id/token enumerated — §4.
- [x] Every call site of every changed function enumerated — the 5
  `_normalize_candidate_payload` sites (§5.2), the gate chain (§5.4),
  the 5 service-fn signature additions + their API-handler callers.
- [x] `lang="en"`/omitted byte-identity test specified — §7.3.
- [ ] **R3 is NOT a clean one-liner — see §6.3.** This is the one item
  needing a decision before PR #2b is fully unblocked. **Unblock:**
  confirm the §6.3 companion approach (carry raw gate keys on
  `failed_entries`, source classification from the raw
  `gate_status_json`) is acceptable. It stays within
  `llm_decision_memo.py`, changes no response shape, no frontend, no
  prompt text — but it is more than the one line rule #6 implies. The
  user must acknowledge this scope.
- [ ] **§8.4 scope gap:** `_decision_summary`'s `from_key_risks` branch
  depends on out-of-scope `_build_strengths_and_risks`; Arabic
  decision-summary risk clause degrades to English in that branch.
  **Unblock:** accept the degraded fallback for PR #2 (recommended), or
  expand scope to include `_build_strengths_and_risks`.
- [ ] **§8.3 check:** confirm the recommendation-report path
  (`expansion_advisor.py:10498–10499`, `[:3]` slice) consumes
  `_normalize_candidate_payload`-normalized candidates so the slice
  operates on rendered strings. **Unblock:** a 5-minute trace of the
  report builder's candidate source; if it bypasses normalization, the
  report endpoint needs the same `lang` plumbing.

**PR #2b: READY pending three explicit decisions** — all three are
scoping/acknowledgement decisions, not unknowns. None require new
investigation; each has a recommended resolution stated above.

---

### Discipline-rule compliance summary

| Rule | Status |
|------|--------|
| 1 — English persisted strings byte-identical | ✔ §3.7 (producers' English code untouched; only additive outputs) |
| 2 — `_normalize_candidate_payload` `en`/omitted byte-identical | ✔ §5.3 `else` branch is HEAD code verbatim; test §7.3 |
| 3 — migration only adds new nullable columns | ✔ §2.2 (5 new JSONB nullable; no existing column changed) |
| 4 — no backfill; NULL → English fallback | ✔ §2.6, §5.3 helpers |
| 5 — `_humanize_gate_list` `en`/omitted byte-identical | ✔ §4.3 / §5.4 (`GATE_LABELS["en"]` = `_GATE_HUMAN_LABELS` verbatim + same `.replace` fallback) |
| 6 — R3 compares on raw gate keys | ✔ §6.2 — **but flagged §6.3: needs a companion change, not a literal one-liner** |
| 7 — no frontend / no handler-signature / no `_prewarm` / no prompt-assembly change | ✔ §10 (handler *bodies* only; prompt *text* unchanged) |
