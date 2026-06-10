# Investigation — Demand Strength UI + Decision Memo: rendering the demand-generator index

**Branch:** `claude/investigate-demand-ui-g85ghi` · **Date:** 2026-06-10 · **Read-only investigation; no product code touched.**

---

## One-screen summary

**A2 (the fork): the INPUTS list is FRONTEND-HARDCODED.** The backend sends no
per-component label/value rows. The four stale rows are a static descriptor array
`PER_COMPONENT_INPUTS.demand_potential` in
`frontend/src/features/expansion-advisor/scoreComponentMeta.ts:351-380`, rendered by
`DecisionLogicCard.tsx` (`ContributionsSection`, `frontend/src/features/expansion-advisor/DecisionLogicCard.tsx:345,395-424`),
with labels from i18n keys `expansionAdvisor.scoreComponents.demand_potential.inputs.<key>.label`
(`frontend/src/i18n/en.json:1275-1280`, `ar.json:1255-1260`). Values are resolved
client-side from `feature_snapshot_json`.

**Consequence:** the card fix is **frontend-only**. Everything needed to render the
dg-index inputs is already persisted in `feature_snapshot_json["demand_generator_index"]`
(`app/services/expansion_advisor.py:9737`, dict shape at `:2100-2129`) plus
`feature_snapshot_json["demand_score_source"]` (`:9746`), and both already flow through
the API untouched because `CandidateFeatureSnapshotResponse` is a `FlexibleResponseModel`
(extra keys allowed, `app/api/expansion_advisor.py:179-183`, wired at `:218` for the
list/detail endpoints and `:333` for the memo endpoint). No serializer change is needed.
The only fields the card *cannot* show today without a backend addition are the
delivery-leg score and the blend weights actually used — they are computed inline and
never persisted (see §A3).

**Card description string** is also frontend i18n:
`expansionAdvisor.scoreComponents.demand_potential.definition` (`en.json:1274`,
`ar.json:1254`) — not a backend constant, not memo-generated.

**Memo:** the prompt payload does NOT reliably carry the dg evidence. The memo
feature-snapshot whitelist (`app/services/llm_decision_memo.py:417-451`) excludes
`demand_generator_index`/`demand_score_source`, and the full snapshot is truncated to
that whitelist whenever it serializes >4,000 chars (`:1991-1992`, `:2020-2025`).
The "population reach … supports the dine-in model" framing comes from prompt voice
Example C (`:1538,1544`) and the "Market context" field-usage rules (`:1438-1447`).
Minimal v12 = whitelist additions + an engine-conditional Market-context prompt rule +
AR Rule 7 glossary additions + `MEMO_PROMPT_VERSION` bump (`:53`) + regenerating the
pinned prompt-head fixture (`tests/data/pr4a_structured_memo_system_prompt_en_head.txt`).
The validator does not constrain evidence labels (§B2), so no validator change.

**Patch split confirmed:** PR-D = frontend card (engine-aware INPUTS + definition,
both locales; optional tiny backend snapshot addition for blend/delivery-leg
transparency). PR-E = memo v12. Details and risks in §D.

---

## A. Where the card's INPUTS rows come from

### A1. End-to-end trace

**Backend.** `_score_breakdown` (`app/services/expansion_advisor.py:3457`) returns
`{weights, inputs, weighted_components, display, final_score}` (`:3591-3604`). The
`display` dict carries only `{raw_input_score, weight_percent, weighted_points}` per
component (`:3583-3590`) — component-level numbers, **no input rows, no labels**. The
`inputs` dict is component sub-scores keyed by component name (`:3566-3577`), i.e.
`inputs.demand_potential` is the blended demand score, not its ingredients. The demand
ingredients live in `feature_snapshot_json` (top-level `population_reach`,
`realized_demand_30d` `:9923`, `realized_demand_branches`, `radiance_growth`, plus the
dg block, see A3), built per candidate around `:9705-9746`.

**API.** Both blobs ride the candidate objects unfiltered:
- list/search/compare endpoints: `ExpansionCandidateResponse.feature_snapshot_json`
  and `.score_breakdown_json` (`app/api/expansion_advisor.py:218-219`), both
  `FlexibleResponseModel`s (`:179-183`, `:193-207`) → extra keys pass through.
- memo endpoint: `CandidateMemoCandidateResponse.feature_snapshot` (`:333`) — note the
  key is `feature_snapshot` (no `_json`) on this endpoint.
- Exception: the recommendation-report top-3 projection re-builds a **slim**
  `feature_snapshot_json` that omits all demand fields
  (`app/services/expansion_advisor.py:11948-11958`) — but that surface
  (`ExpansionReportPanel` → `ScoreBreakdownCompact`) renders component bars only, no
  per-input rows, so it is unaffected.

**Frontend.** `DecisionLogicCard` (default export,
`frontend/src/features/expansion-advisor/DecisionLogicCard.tsx:893`) is mounted in one
place: the memo drawer's **Diagnostics** tab (`ExpansionMemoPanel.tsx:328`). Props:
`scoreBreakdown` (the `score_breakdown_json` blob) + `candidate` (loose candidate
accepting either `feature_snapshot` or `feature_snapshot_json`,
`DecisionLogicCard.tsx:21-25,293-297`). `ContributionsSection` iterates
`SCORE_COMPONENT_ORDER` (`:46-57`), and for each component looks up
`PER_COMPONENT_INPUTS[c.key]` (`:345`), resolves each descriptor against
`{candidate, scoreBreakdown, featureSnapshot, contextSources}` (`:346-354`), and renders
the INPUTS heading + label/value/source rows (`:395-424`). Labels come from
`expansionAdvisor.scoreComponents.<comp>.inputs.<key>.label` (`:402-405`); the
description paragraph from `expansionAdvisor.scoreComponents.<comp>.definition`
(`:342-344,376-380`); source chips from `expansionAdvisor.scoreSources.<token>`
(`:267-272`, tokens defined in `scoreComponentMeta.ts:16-32`).

### A2. Data-driven or hardcoded?

**Frontend-hardcoded.** `PER_COMPONENT_INPUTS.demand_potential`
(`scoreComponentMeta.ts:351-380`) statically declares exactly four descriptors —
`population_reach`, `realized_demand_30d`, `realized_demand_branches`,
`radiance_growth_pct` — each with a fixed i18n key and a resolver reading
`feature_snapshot_json`. The list is identical for every service model; nothing in it
consults `demand_score_source`. The backend sends raw data, the frontend decides what
to show. **The card patch is therefore frontend-only** (plus i18n), with one optional
backend addition (A3, last paragraph).

### A3. Which dg fields are already persisted and reachable

When `EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED` is on, `_demand_generator_index`
(`app/services/expansion_advisor.py:2039`) returns the dict stored verbatim at
`feature_snapshot_json["demand_generator_index"]` (`:9736-9737`). Verified key list
from the return statement (`:2100-2129`):

| Wanted field | Exact JSON path in `feature_snapshot_json` | In API response today? |
|---|---|---|
| `demand_score_source` | `demand_score_source` (top level, `"pop_score"` \| `"dg_index"`) — emitted only when the dine-in scoring flag is on (all models get it then) or the QSR flag is on and the candidate is qsr (`:9742-9746`) | Yes (FlexibleResponseModel pass-through) |
| composite | `demand_generator_index.composite_0_100` (`:2101`) | Yes |
| `weights_version` | `demand_generator_index.weights_version` (`"l1_v2_2026-06"` / `"l1_v3_qsr_2026-06"`, `:2102-2106`; constants `:1860,1869`) | Yes |
| `radius_m` | `demand_generator_index.radius_m` (`:2107`); pop sub-radius at `.pop_radius_m` (`:2109`) | Yes |
| `population_local` | `demand_generator_index.population_local_reach` (`:2110`); wide reach mirrored at `.population_reach` (`:2108`) | Yes |
| per-kind OSM counts | `demand_generator_index.osm_generators.{offices, malls_retail, transit, mosques, schools, hospitals, hotels}` (`:2111-2119`) — no `osm_weighted_total` raw; only its normalized sub-score | Yes |
| `building_floors_sum` | `demand_generator_index.building_floors_proxy_sum` (`:2120`) | Yes |
| `fnb_review_weighted` | `demand_generator_index.fnb_review_weighted_density` (`:2121`); venue count at `.fnb_venue_count` (`:2122`) | Yes |
| normalized sub-scores | `demand_generator_index.subscores.{population, osm_generators, building_floors, fnb_review_weighted}` (`:2124-2129`) | Yes |
| delivery-leg score | **Not persisted.** `delivery_score` is computed in the first pass (`:8337-8342`) and reused at `:9599/:9627` but never written to the snapshot or breakdown | Needs backend addition if wanted |
| blend weights used | **Not persisted.** `_demand_blend_weights` (`:2677-2692`: dine_in 0.75/0.25, qsr 0.60/0.40, cafe 0.55/0.45) and the listing/realized split `EXPANSION_REALIZED_DEMAND_BLEND` (`app/core/config.py:107-109`, code default **0.5**) exist only as code/env constants | Needs backend addition if wanted |

The frontend type `CandidateFeatureSnapshot` has an index signature
(`[key: string]: unknown`, `frontend/src/lib/api/expansionAdvisor.ts:146-157`), so no
type change is strictly required either — though adding typed optional fields is cheap
and self-documenting. There are **zero** references to
`demand_generator|dg_index|demand_score_source` anywhere in `frontend/src` today
(repo-wide grep).

### A4. The description string

Frontend i18n, full stop: `expansionAdvisor.scoreComponents.demand_potential.definition`
— `frontend/src/i18n/en.json:1274` ("Combined demand evidence: population reach within
a walking/driving catchment, realized 30-day delivery demand, and the district's
nighttime-light growth trend.") and `frontend/src/i18n/ar.json:1254` (faithful Arabic
translation). Rendered at `DecisionLogicCard.tsx:342-344,376-380`. Not a backend
constant; not memo-generated. The full key family that needs touching:

- `expansionAdvisor.scoreComponents.demand_potential.label` (en:1273 / ar:1253)
- `…demand_potential.definition` (en:1274 / ar:1254)
- `…demand_potential.inputs.{population_reach, realized_demand_30d, realized_demand_branches, radiance_growth_pct}.label` (en:1276-1279 / ar:1256-1259)
- `expansionAdvisor.scoreSources.<token>` if a new source token is added for the
  dg sub-signals (token enum at `scoreComponentMeta.ts:16-32`).

---

## B. Decision memo

### B1. Where demand facts enter the prompt payload

`build_memo_context` (`app/services/llm_decision_memo.py:880`) takes
`candidate["feature_snapshot_json"]` whole (`:896-905`) into `MemoContext.feature_snapshot`,
plus a separate `realized_demand` block via `_extract_realized_demand` (`:846,939`).
Serialization (`_serialize_context_for_user_message`, `:2014`) sends the **full**
snapshot only when it serializes ≤ `_FEATURE_SNAPSHOT_SOFT_LIMIT` = 4,000 chars
(`:1991-1992`); otherwise it is cut to `_MEMO_WHITELIST` (`:441-451`, base list
`:417-439`) — which contains `population_reach`, `realized_demand_30d/branches/
district_median`, `radiance_growth`, `delivery_listing_count`… and **no
`demand_generator_index`, no `demand_score_source`**. A second size guard re-applies
the whitelist at `:2061-2065`. Production snapshots (context_sources, brand_presence,
listing_age, district_momentum, comparables, plus the ~600-char dg dict itself) easily
exceed 4 KB, so in practice **the dg evidence does not reach the LLM**.

The deterministic typed section is the same story: `market_context` is assembled at
`:1258-1266` from exactly `population_reach`, `district_momentum`,
`realized_demand_30d`, `realized_demand_branches`, `delivery_listing_count`
(dataclass `MemoMarketContext` `:522-530`; schema text in the prompt `:1366-1374`).

The "population reach … supports the dine-in model" privileging is prompt-authored:
voice Example C's ranking_explanation and key_evidence row (`:1538,1544`), reinforced
by the Market-context field-usage rules (`:1438-1447`, which also instruct "Lead with
it when present" for realized demand) and the REALIZED DEMAND addendum (`:2088-2095`).

### B2. Minimal v12 shape

1. **Payload:** add `"demand_generator_index"` and `"demand_score_source"` to
   `_MEMO_WHITELIST` only (`:441-451`) — explicitly **not** `_RERANK_WHITELIST`
   (`:417-439`); the comment at `:453-459` documents that boundary as deliberate.
   Optionally extend the typed `market_context` (dataclass `:522-530`, builder
   `:1258-1266`, schema text `:1366-1374`, frontend mirror
   `frontend/src/lib/api/expansionAdvisor.ts:229-237`) — but note the consuming
   component `AdvisorySectionCards` is **not mounted** in production ("cut by
   directive", `ExpansionMemoPanel.test.tsx:197`), so typed-section changes buy
   little and can be deferred.
2. **Prompt rules:** add an engine-conditional rule to the Market-context block
   (`:1438-1447`): when `feature_snapshot.demand_score_source == "dg_index"`, demand
   evidence must cite the demand-generator composite and its strongest sub-signals
   (F&B review mass `fnb_review_weighted_density`, trip generators `osm_generators`,
   built-density `building_floors_proxy_sum`); cite "population reach within walking
   catchment" as the demand anchor only when source is `pop_score` (or the field is
   absent — legacy rows). Voice Example C (`:1538,1544`) needs either a dg-index
   variant or an explicit caveat, since the examples are the dominant key_evidence
   pattern the model imitates (cf. comment `:1792-1795`).
3. **Version:** `MEMO_PROMPT_VERSION = "v11-rent-positioning-deterministic-2026-06"`
   at `:53`; bump to v12. Cache invalidation is lazy-by-version: the column
   `decision_memo_prompt_version` (Alembic `alembic/versions/20260425_memo_prompt_version.py`)
   is compared on read and a mismatch regenerates (persist sites
   `app/services/expansion_advisor.py:11522`, `app/api/expansion_advisor.py:1559`).
4. **Validate-and-retry layer:** `_parse_and_validate_memo_shape` (`:2418`) checks
   only the ten required keys (`_STRUCTURED_REQUIRED_KEYS`, `:2373`) and that
   `key_evidence` is a non-empty list (`:2460`); `_headline_validity_reason` (`:2195`)
   checks headline prefix/consistency only. **No schema constraint on evidence
   labels** — the `{signal, value, implication, polarity}` rows are free-form
   (`:1338-1340`). The one-retry corrective loop (`:2559-2694`, preambles
   `:2507-2535`) keys off shape/headline failures only. So v12 needs **no validator
   change**; the new evidence vocabulary is prompt-side.
5. **Interacting guardrails:** Rule 6 (competitor economics ban, `:1851-1860` AR /
   `:1497-1503` EN) is orthogonal — dg evidence is site-side, not competitor-side;
   leave untouched. The "Thin market context" rule (`:1514-1516`) currently tells the
   model to "lean on population_reach" when realized demand is null — for a dg_index
   candidate that fallback should prefer the composite; worth one clause in v12.

### B3. Arabic memo localization

Evidence labels are **LLM-written in Arabic**, governed by AR Rule 7's fixed glossary
(`:1890-1913`): signal strings must use the listed Arabic terms (e.g. "population
reach" → "عدد السكان القابلين للوصول", realized demand → "التقييمات على الفروع
المجاورة"), and Rule 8 (`:1915-1946`) fixes Arabic unit templates (Latin digits kept,
"<N> ضمن نطاق المشي" etc.), with a fully-Arabic worked example (`:1948-1969`).
**Leak risk:** any new dg evidence terms (demand-generator composite, F&B review mass,
trip generators) introduced in v12 EN rules without parallel Rule 7/8 glossary entries
will surface as English signal labels inside Arabic memos — exactly the BiDi/label-leak
class from the Feasibility PDF work. The legacy-text renderer
(`render_structured_memo_as_text`, `:2856-2878`) and the frontend narrative
(`DecisionMemoNarrative.tsx:104,127`, heading key `expansionAdvisor.keyEvidence`)
print whatever the LLM wrote, so the glossary is the only defense.

Separate latent gap found while checking: `ar.json` `expansionAdvisor.advisorySection.*`
holds untranslated English values — `"populationReach": "Population reach"`,
`"realizedDemandBranches": "Rating-contributing branches"`, `"districtMomentum":
"District momentum"`, etc. (`frontend/src/i18n/ar.json:1370-1388`; and
`expansionAdvisor.realizedDemand30d` `ar.json:1142` *is* translated). Currently
invisible because `AdvisorySectionCards` is unmounted, but any patch that revives that
surface must fix these.

---

## C. Frontend i18n + Arabic parity

### C1. Key families and current Arabic state

- `expansionAdvisor.scoreComponents.<comp>.{label, definition, inputs.<key>.label}` —
  **fully localized today**, including the four demand INPUTS labels
  (`ar.json:1252-1260`). The GateSummary precedent does **not** repeat here; new
  INPUTS labels just need ar.json entries following the existing convention (Latin
  digits, Arabic units — cf. `ar.json:1257` "سرعة تقييمات التوصيل (30 يومًا)").
- `expansionAdvisor.scoreSources.<token>` — source-chip labels for the tokens in
  `scoreComponentMeta.ts:16-32`.
- `expansionAdvisor.scoreLabel.demandStrength` (`en.json:694`, `ar.json:746` "قوة
  الطلب") — component-name map used by `ExpansionMemoPanel.tsx:21`; label stays valid.
- `expansionAdvisor.advisorySection.*` — partially English in ar.json (see B3); latent.

### C2. Every render site of the four stale labels (repo-wide grep)

| Site | What renders | Stale? |
|---|---|---|
| `DecisionLogicCard.tsx` INPUTS rows via `PER_COMPONENT_INPUTS.demand_potential` (`scoreComponentMeta.ts:351-380`), mounted on memo-drawer Diagnostics tab (`ExpansionMemoPanel.tsx:328`) | The four rows, both engines | **Yes — the primary target** |
| i18n values `en.json:1274-1279` / `ar.json:1254-1259` (definition + labels) | Strings for the above | **Yes** |
| Memo KEY EVIDENCE prose (backend LLM output) → `DecisionMemoNarrative.tsx:104-127` (top-4 rows) and legacy text `render_structured_memo_as_text` (`llm_decision_memo.py:2866-2878`) | "population reach … walking catchment" evidence rows | **Yes — PR-E** |
| `AdvisorySectionCards.tsx:146-156` (`advisorySection.populationReach`, `realizedDemand30d` `en.json:1162`, `realizedDemandBranches`) | Memo Market-Context typed card | Stale labels but **unmounted** ("cut by directive", `ExpansionMemoPanel.test.tsx:197`) — skip |
| Gate labels "Population reach floor" (`app/services/expansion_advisor.py:75`, `app/services/expansion_advisor_i18n.py`, pr2_golden fixtures) | Population-floor **gate**, a different feature still genuinely population-based | Not stale — leave |
| `en.json:1046` `pillarGrowthSoftDemote` ("radiance YoY below {{threshold}}%") → `PillarSummaryStrip.tsx:113-123` | Soft-demote diagnostics; the radiance demote leg still exists (`VIABILITY_LEG_ORDER` `scoreComponentMeta.ts:493-500`) | Not stale — leave |
| `ScoreBreakdownCompact` (CandidateDetailPanel, ExpansionReportPanel) | Component bars only, no input rows | Unaffected |
| Memo drawer "Market" inner tab (`ExpansionMemoPanel.tsx:392+`) | Prose summaries (delivery market, competitive context) — no demand-input rows | Unaffected |
| PDF/presentation export | None found for Expansion Advisor (no pdf/print code under `frontend/src/features/expansion-advisor/` beyond CSS); the memo "copy summary" block reuses memo prose | Covered by PR-E |

---

## D. Patch plan

### PR-D — engine-aware Demand Strength card (frontend-only; both locales)

**Scope:**
- `frontend/src/features/expansion-advisor/scoreComponentMeta.ts` — make
  `demand_potential` resolution engine-aware. Smallest shape: keep the existing
  static array as the `pop_score`/legacy set, add a `DEMAND_DG_INPUTS` descriptor set
  reading `feature_snapshot.demand_generator_index.*` (composite_0_100, subscores or
  raw sub-signals, weights_version/radius_m as context rows), and select in
  `DecisionLogicCard.tsx:345` on `featureSnapshot.demand_score_source === "dg_index"`.
  Fallback rule: field absent or `"pop_score"` → today's rows unchanged (covers
  flags-off rows, cafe, delivery_first, and all pre-flag historical candidates).
- `DecisionLogicCard.tsx` — pick definition key by engine
  (`…demand_potential.definition` vs new `…definition_dg_index`).
- `en.json` + `ar.json` — new `inputs.*.label` keys and `definition_dg_index`, Arabic
  with Latin digits / Arabic units per existing convention.
- Source attribution: per-kind OSM counts → existing `osm` token; population_local →
  `population_grid`; `fnb_review_weighted_density` and `building_floors_proxy_sum`
  have no obviously-correct existing token — decide between `oaktree_internal` and a
  new token + `scoreSources` entries (small, but a product call).
- **Optional backend rider (challengeable):** persist the delivery-leg score and the
  blend weights actually used (e.g. `feature_snapshot_json["demand_blend"] =
  {pop_w, del_w, delivery_score, listing_realized_blend}`) next to the
  `demand_score_source` emit at `app/services/expansion_advisor.py:9742-9746`. Without
  it the card can show the dg composite and sub-signals but not the delivery side of
  the blend. Additive, display-only, no scoring change — safe to include in PR-D or
  defer.

**Flag-gating:** none needed. The change is data-driven off `demand_score_source`,
which the backend only emits when the production flags are on — the flags are the gate.

**Risk:** low. Display-only; the API contract already passes the fields; the only
regression class is mis-resolving snapshot paths (descriptor resolvers are defensive,
`scoreComponentMeta.ts:54-78`).

**Tests / validation:**
- `scoreComponentMeta.test.ts` + `DecisionLogicCard.test.tsx`: dg_index dine_in row
  set, dg_index qsr (l1_v3 weights_version), pop_score cafe (unchanged rows),
  missing-field em-dash fallback.
- `Pr4dI18n.test.tsx`-style en/ar key-parity assertion for the new keys.
- `cd frontend && npm run build && npm run test`.
- Screenshots: dg_index dine_in candidate, dg_index qsr candidate, pop_score cafe
  candidate, and the dg_index dine_in card under Arabic locale (memo drawer →
  Diagnostics).

### PR-E — memo v12 (backend-only)

**Scope (per §B2):** `_MEMO_WHITELIST` additions (`llm_decision_memo.py:441-451`);
engine-conditional Market-context rules (`:1438-1447`) + thin-market fallback clause
(`:1514-1516`) + voice-example adjustment (`:1538,1544`); AR Rule 7/8 glossary entries
for the new evidence terms (`:1890-1946`); `MEMO_PROMPT_VERSION` v11 → v12 (`:53`);
regenerate `tests/data/pr4a_structured_memo_system_prompt_en_head.txt` (pinned by
`tests/test_pr4a_arabic_structured_memo.py:48` and
`tests/test_pr4c_arabic_key_evidence.py:32`).

**Flag-gating:** none — follows the established prompt-bump discipline: the version
bump lazily regenerates cached memos on next view (Alembic
`20260425_memo_prompt_version.py` mechanism); the validate-and-retry layer is
untouched (no label constraints to update, §B2.4).

**Risk:** medium-low. LLM-behavioral, mitigated by the deterministic validator +
one-retry loop and by the fact that key_evidence labels are unconstrained. Main
hazards: (a) Arabic label leak if Rule 7/8 entries are skipped; (b) example drift —
if Example C keeps the population-reach demand row un-caveated, dg_index memos will
keep imitating it.

**Tests:** `tests/test_llm_decision_memo.py` (payload contains dg fields when present;
truncation path keeps them), `test_pr4a_arabic_structured_memo.py` /
`test_pr4c_arabic_key_evidence.py` (prompt-head regen + AR glossary),
`test_sample_regression_memos.py` if goldens embed prompt text. Manual: regenerate one
dg_index dine_in memo, one dg_index qsr memo, one pop_score cafe memo, one Arabic
dg_index memo; check KEY EVIDENCE demand rows cite dg evidence vs population reach
appropriately.

**Order:** PR-D and PR-E are independent (different files, different review skills);
PR-D first gives the UI parity that screenshots flagged, PR-E follows.

---

## Discrepancies & framing

1. **Blend weight 0.3/0.7 is environment-set, not in the tree.** The repo default for
   `EXPANSION_REALIZED_DEMAND_BLEND` is **0.5** (`app/core/config.py:107-109`). The
   production 0.3 listing / 0.7 realized split must be a deployment env override —
   nothing in the synced tree records it. The per-model realized anchors 307/402/327
   *are* in code (`app/services/expansion_advisor.py:2748-2751`, re-anchored
   2026-06-10) and cafe deliberately falls back to the 263.0 env default (`:2754-2763`).
   Any UI text describing the blend must read the live setting, not hardcode 0.7.
2. **All three dg flags default OFF in code** (`app/core/config.py:135-138, 163-166,
   180-184`); production-on is env-level. In a default checkout neither
   `demand_generator_index` nor `demand_score_source` is emitted — PR-D's fallback
   path is the default path in dev/CI, so tests must cover both states.
3. **`demand_score_source` emission is asymmetric** (`:9742-9746`): with the dine-in
   scoring flag on, *every* candidate gets the field (cafe/delivery_first get
   `"pop_score"`); with only the QSR flag on, only qsr candidates get it. With both
   production flags on this is moot, but the frontend must treat *absence* as
   pop_score, never as an error.
4. **Delivery-leg score and blend weights are not persisted** (§A3) — the task's A3
   list assumed they might be; they aren't. Showing them requires the optional PR-D
   backend rider.
5. **The memo payload only sometimes contains the dg dict today** — it rides the full
   snapshot under the 4,000-char soft limit (`llm_decision_memo.py:1991-1992,
   2020-2025`) but is dropped by whitelist truncation on realistic snapshots. So the
   stale memo evidence is not just a prompt problem; without the whitelist addition a
   v12 prompt rule would reference fields the model often can't see.
6. **`AdvisorySectionCards` is unmounted** ("cut by directive",
   `ExpansionMemoPanel.test.tsx:197`) and its ar.json keys are partially English
   (`ar.json:1370-1388`). Don't spend PR-D/PR-E budget there, but don't widen the gap
   either if the typed `market_context` section gains dg fields.
7. **Cosmetic prompt drift, pre-existing:** the preamble says "score_breakdown (9
   components…)" (`llm_decision_memo.py:1331`) while `_score_breakdown` emits 10
   (post-Patch-B chain_strength split, `app/services/expansion_advisor.py:3525-3536`).
   Not this task's bug; noting to avoid attributing it to v12.
8. **Framing:** the screenshots describe the cards as "identical for both engines" —
   confirmed and fully explained by A2: the inputs list is static frontend metadata
   that predates dg scoring; the backend has been emitting full dg transparency since
   PR-1 (`:9736-9737`) with zero frontend consumers (grep: no `demand_generator` hits
   in `frontend/src`). The plumbing gap is one-sided, which is what makes PR-D
   frontend-only and low-risk.
