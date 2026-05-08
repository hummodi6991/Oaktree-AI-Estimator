# Memo Quality — Diagnostic Findings (2026-05-08)

Read-only investigation of `app/services/llm_decision_memo.py` (and the
upstream `confidence_grade` pipeline in `app/services/expansion_advisor.py`).

Scope:

1. Where the memo prompt or post-format renderer surfaces an **absolute SAR
   delta** vs. `comparable_median_annual_rent_sar` in `headline_recommendation`,
   `ranking_explanation`, `key_evidence`, `financial_framing.summary`,
   `financial_framing.thesis` (and adjacent prose fields).
2. Where `confidence_grade` is computed and whether
   `feature_snapshot.context_sources.road_evidence_band`,
   `parking_evidence_band`, and `comparable_competitors` length feed into it.

No code was patched.

---

## 1. Absolute SAR delta surfacing (vs. `comparable_median_annual_rent_sar`)

### 1.1 Mechanics

The renderer (`render_structured_memo_as_text` →
`_render_advisory_section_lines`) is **a pure pass-through** for prose. It does
not compute a SAR delta itself — it reproduces whatever the LLM emitted in
`headline_recommendation`, `ranking_explanation`, the `key_evidence[*]`
strings, `financial_framing.summary` and `financial_framing.thesis`.

The one piece of post-format computation is the typed bullet
`spread_to_median_sar` (signed delta, in SAR), computed deterministically in
`build_memo_advisory_sections`:

- `app/services/llm_decision_memo.py:1143` — reads
  `comparable_median_annual_rent_sar` from the snapshot.
- `app/services/llm_decision_memo.py:1151–1155` — computes `spread_to_median`
  as `round(annual_rent - comparable_median, 2)` when both are present.
- `app/services/llm_decision_memo.py:1169` — emits
  `comparable_median_annual_rent_sar` into `financial_framing`.
- `app/services/llm_decision_memo.py:1173` — emits `spread_to_median_sar` into
  `financial_framing`.
- `app/services/llm_decision_memo.py:1281` and `:1285` — declares those keys in
  the prompt JSON-schema description.
- `app/services/llm_decision_memo.py:2293, :2297` — `_ADVISORY_SECTION_RENDER`
  whitelist that causes the typed bullets to be rendered as
  `- comparable_median_annual_rent_sar: <n>` and
  `- spread_to_median_sar: <n>` lines under the `## Financial framing`
  section in the legacy text view (`_render_advisory_section_lines`,
  `app/services/llm_decision_memo.py:2321–2359`, looped from
  `render_structured_memo_as_text`, `app/services/llm_decision_memo.py:2362`).

Everything else is LLM prose authored against the prompt and the few-shot
examples. No regex or template post-processor inserts an absolute SAR delta
into any of the five fields named in the task; even the headline safety-net
rewriter (`_rewrite_headline_locally`, `app/services/llm_decision_memo.py:1872`)
just truncates `ranking_explanation` and prepends a verdict prefix — it does
not reach into the financial framing.

### 1.2 Where the prompt instructs / models the absolute SAR delta

The prompt's instructions block does **not** include an explicit directive
that says "cite the absolute SAR delta to the comparable median". The pressure
on the LLM to produce that delta comes from (a) supplying both
`comparable_median_annual_rent_sar` and `spread_to_median_sar` in the typed
payload, and (b) the few-shot examples below, which all do it. The "thin
financial framing" rule (`app/services/llm_decision_memo.py:1389–1395`)
covers only the comparable_n=null case; it does not gate the delta phrasing
when the data is present.

### 1.3 Per-field findings

For each field, file:line annotations of every occurrence in the prompt body
where an **absolute SAR delta against `comparable_median_annual_rent_sar`**
is surfaced (either as guidance text or as a few-shot literal that the LLM
will mimic).

#### `headline_recommendation`

No occurrences. The prompt's headline schema description
(`app/services/llm_decision_memo.py:1257`) and all four worked
example headlines (`:1419`, `:1440`, `:1462`, `:1483`) cite the **percentile**
or a **percent premium** against the median — never an absolute SAR figure.
The "CRITICAL OUTPUT FORMAT RULES" block (`:1564–1631`) likewise does not
introduce an absolute-SAR delta.

#### `ranking_explanation`

- `app/services/llm_decision_memo.py:1420` — Example C:
  `"…SAR 432,000/yr lands at the 28th percentile vs 14 district comparables, a roughly SAR 110,000/yr discount to the median that compounds materially over a five-year lease."`
- `app/services/llm_decision_memo.py:1484` — Example F (same text, repeated):
  `"…a roughly SAR 110,000/yr discount to the median."`

(`:1441` Example D uses a percentage delta — `"34% above the SAR 685,000
median"` — citing the median absolutely but expressing the delta as a
percentage, not a SAR figure; `:1463` Example E is the at-market case and
contains no delta. They are listed here for completeness but do not match the
"absolute SAR delta" pattern in the strict sense.)

#### `key_evidence`

- `app/services/llm_decision_memo.py:1422` — Example C, evidence row "annual
  rent": `"the spread to the district median justifies the entry — roughly SAR 110k/yr saved vs peer listings"`.
- `app/services/llm_decision_memo.py:1486` — Example F, identical row:
  `"…roughly SAR 110k/yr saved vs peer listings"`.

Example D's evidence row at `:1443` cites a percent delta plus the absolute
median (`"asking sits 34% above the comparable median"`) but not an absolute
SAR delta. Example E (`:1465–1469`) has no delta. The `_ADVISORY_SECTION_RENDER`
whitelist drives the bullet-form spread; the LLM-authored evidence rows are
the ones that surface the absolute-SAR phrasing.

#### `financial_framing.summary`

- `app/services/llm_decision_memo.py:1510` — Example F:
  `"SAR 432,000/yr at the 28th percentile vs 14 district comparables — SAR 110k under median."`

#### `financial_framing.thesis`

- `app/services/llm_decision_memo.py:1511` — Example F:
  `"SAR 432,000/yr is decisively below the SAR 542,000 district median across 14 peer listings — a SAR 110,000/yr cushion that compounds across a five-year lease and absorbs the operator's first-year ramp risk."`
- `app/services/llm_decision_memo.py:1556` (inline thin-data note):
  `"…SAR 480,000/yr is a meaningful capital commitment…"` — this is the
  thin-data counter-example. It deliberately does **not** cite a delta.

### 1.4 Adjacent fields (out of scope but worth flagging for reviewers)

- `comparison`: Example C `:1434` and Example F `:1497` —
  `"This site beats Peer Chain A on rent by roughly SAR 90k/yr…"`.
- `competitive_landscape.saturation_thesis`: Example F `:1530` —
  `"…the operator is paying SAR 110k less for a stronger street position."`

These are not in the requested five fields but follow the same pattern:
absolute-SAR deltas are baked into the few-shot examples without an explicit
prose rule.

### 1.5 Net of section 1

- The prompt has **no explicit instruction** to surface an absolute SAR delta
  versus `comparable_median_annual_rent_sar`. Production behaviour is driven
  by (a) the typed payload supplying `spread_to_median_sar`, and (b) the
  Example C / Example F few-shot literals in `ranking_explanation`,
  `key_evidence`, `financial_framing.summary`, and
  `financial_framing.thesis`.
- The renderer surfaces the delta deterministically only as the
  `- spread_to_median_sar: <signed n>` bullet under
  `## Financial framing` — a typed line, not prose.
- Any inconsistency between the LLM's absolute-SAR phrasing and the typed
  payload is **not validated**. `_advisory_section_invalid_reason`
  (`app/services/llm_decision_memo.py:1946`) only checks that prose fields are
  non-empty.

---

## 2. `confidence_grade` — computation and inputs

### 2.1 Definition site

`_confidence_grade(*, confidence_score, district, provider_platform_count,
multi_platform_presence_score, rent_source, road_context_available,
parking_context_available, zoning_available, delivery_observed,
data_completeness_score, is_listing) -> str`
— `app/services/expansion_advisor.py:3115–3179`.

Adjustments before grading:

- `+2.5` if `district` is set (`:3139–3140`).
- `+2.5` if `multi_platform_presence_score > 0` (`:3142–3143`).
- `+3.0` if `rent_source != "conservative_default"` (`:3144–3145`).

Listings branch (`is_listing=True`, `:3147–3156`): grade is mapped purely from
the adjusted score (≥85 → A, ≥70 → B, ≥50 → C, else D). The four
`*_available` flags are deliberately ignored on this path — listings already
encode unit-level ground truth in the score.

Parcels branch (`:3158–3179`): counts `critical_missing` flags
(`zoning_available`, `delivery_observed`, `road_context_available`,
`parking_context_available`), and bands the adjusted score with completeness:

- A: adjusted ≥ 85 AND `critical_missing == 0` AND completeness ≥ 85.
- B: adjusted ≥ 70 AND `critical_missing ≤ 1`.
- C: adjusted ≥ 50.
- D: otherwise.

### 2.2 Call site

`app/services/expansion_advisor.py:8715–8727` (inside the candidate-prep
loop). Inputs at the call site:

- `confidence_score` ← from `_confidence_score`
  (`app/services/expansion_advisor.py:7490–7499`; see §2.3).
- `road_context_available` ← derived at
  `app/services/expansion_advisor.py:8543` from
  `feature_snapshot_json["context_sources"]["road_context_available"]`
  (a **boolean**, not the `road_evidence_band` string).
- `parking_context_available` ← derived at
  `app/services/expansion_advisor.py:8550` from
  `feature_snapshot_json["context_sources"]["parking_context_available"]`
  (a **boolean**, not the `parking_evidence_band` string).
- `zoning_available` ← `bool(landuse_label or landuse_code)`.
- `delivery_observed` ← `provider_listing_count > 0`.
- `data_completeness_score` ← `feature_snapshot_json["data_completeness_score"]`
  (computed in `_assemble_feature_snapshot`,
  `app/services/expansion_advisor.py:2120–2127`).

### 2.3 Does `road_evidence_band` feed in?

**No, not directly.**

- `road_evidence_band` is computed at
  `app/services/expansion_advisor.py:2097–2100` via `_road_evidence_band`
  (defined `:1767`) and stored in
  `feature_snapshot.context_sources.road_evidence_band` for **UI / memo
  rendering only** (the comment at `:2096` is explicit).
- `_confidence_grade` reads `road_context_available` (a boolean), not the
  band string. The band can therefore be `"primary"`, `"secondary"`,
  `"side"`, `"none"`, or `"unknown"` without changing the grade — only
  whether the road join produced any evidence at all matters.
- `_confidence_score` (`:2281–2327`) does not take a road parameter at all.
- The band string does flow into the data-completeness path indirectly: the
  same `road_context_available` boolean is one of the six completeness
  components (`:2125`), and `data_completeness_score` is then passed into the
  grader's parcel-path A-tier gate (`:3173`). So presence/absence of a road
  evidence join indirectly nudges the grade via completeness; the *band
  value itself* does not.

### 2.4 Does `parking_evidence_band` feed in?

**No, not directly.** Same shape as roads:

- Computed at `app/services/expansion_advisor.py:2101–2103` via
  `_parking_evidence_band` (`:1750`) and stored in
  `context_sources.parking_evidence_band` for UI/memo use.
- `_confidence_grade` reads `parking_context_available` (a boolean) only.
- `_confidence_score` does not take a parking parameter.
- Indirect path via `data_completeness_score` (`:2126`) is identical to
  roads: presence/absence affects completeness, the band string itself does
  not.

Note: the memo-side reader `build_memo_advisory_sections`
(`app/services/llm_decision_memo.py:1117–1121`) does consume
`parking_evidence_band` to populate `property_overview.parking_evidence`,
but that is unrelated to confidence grading.

### 2.5 Does `comparable_competitors` length feed in?

**No.**

- `_confidence_grade` takes no `comparable_competitors` parameter
  (`app/services/expansion_advisor.py:3115`).
- `_confidence_score` takes no `comparable_competitors` parameter
  (`app/services/expansion_advisor.py:2281`).
- `comparable_competitors_json` is computed independently at
  `app/services/expansion_advisor.py:8741–8750` (post `_confidence_grade`)
  and is only used downstream for memo rendering / UI surfacing
  (`:10177` and the memo path). Its length never enters either of the
  confidence functions.
- It is also not a component of `data_completeness_score` (only zoning,
  delivery, roads-table, parking-table, road-context, parking-context;
  `:2120–2127`).

### 2.6 Net of section 2

- `confidence_grade` is grounded in `confidence_score`,
  `data_completeness_score`, and four boolean availability flags
  (`zoning_available`, `delivery_observed`, `road_context_available`,
  `parking_context_available`).
- The richer evidence-band strings
  (`context_sources.road_evidence_band`,
  `context_sources.parking_evidence_band`) and the size of
  `comparable_competitors` do **not** participate in either
  `_confidence_score` or `_confidence_grade`. They are surfacing-only
  artefacts.
- Indirect coupling exists for road/parking via the completeness score
  (presence/absence of a successful join, not the band value).

---

## Files referenced

- `app/services/llm_decision_memo.py`
- `app/services/expansion_advisor.py`
