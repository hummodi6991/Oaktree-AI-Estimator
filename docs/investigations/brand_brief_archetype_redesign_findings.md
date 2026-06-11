# Brand-brief redesign — archetype-driven weight profiles: findings report

**Mode:** read-only investigation. No production code changed. Deliverables: this
report + two probe files (`scripts/diagnostics/brief_usage.sql`,
`scripts/diagnostics/archetype_backtest.sql`) for Ahmed to run in Codespace.

All line numbers refer to the checkout at `18355656f` (post weight-stack-v2 +
memo component-lookup fix).

---

## TL;DR

1. The brief is **two surfaces in one**: a weight-domain surface (the inert
   `_brand_weight_multipliers` path) and a **raw-score/gate surface** (brand_fit
   internals, the cannibalization gate, the delivery-market gate, the rent-ceiling
   tier multiplier). An archetype can safely own the weight domain; it must NOT
   silently absorb the knobs that flow into gates and raw scores.
2. `service_model` is **not** a dead parameter generally — it drives 10+ raw-score
   calibrations — it is dead only inside `_brand_weight_multipliers`
   (accepted at `expansion_advisor.py:3451`, never read in the body
   `:3472-3508`). Seeding an archetype from it creates **no circularity**: per-model
   anchors act on raw inputs, archetypes would act on weights — orthogonal domains.
   It does compound channel emphasis (see §1.2), which is the intent; the backtest
   probe checks it stays sane.
3. The effective-weight display gap is **memo-only and smaller than feared**:
   `score_breakdown_json.weights` already persists the post-renormalization
   effective weights per candidate row, and the UI Score-contributions card reads
   exactly that field. Only the LLM memo context overwrites it with static weights
   (`llm_decision_memo.py:963`). The fix is a few lines in `build_memo_context`
   plus a `MEMO_PROMPT_VERSION` bump for cache invalidation.
4. Verified the ±2 pt claim: a single non-neutral knob at gain 0.35 moves its
   target weight by ≈1.8 pts post-renormalization; the strongest legal stack
   (delivery channel + delivery_led goal) reaches ≈+4 on delivery_demand. The
   proposed archetypes move 4–8 pts on at least one component each.
5. Blast radius of a non-neutral default is **controllable** if (and only if)
   `qsr → balanced` in the default mapping: nearly every end-to-end test runs
   `service_model="qsr"`. The tests needing updates are enumerated in §1.4.

---

## 1.1 Brief surface inventory

End-to-end path: `ExpansionBriefForm.tsx` → `ExpansionBrief` type
(`frontend/src/lib/api/expansionAdvisor.ts:78-89`) → POST `/v1/expansion-advisor/searches`
(`ExpansionBrandProfileInput`, `app/api/expansion_advisor.py:105-114`; full request
also archived in `expansion_search.request_json`, api `:946,:992`) →
`persist_brand_profile` → `expansion_brand_profile` table (one row per search,
`search_id` UNIQUE; migration `20260311_exp_adv_brand_v4`) →
`run_expansion_search(brand_profile=...)` defaulted via `_default_brand_profile`
(`expansion_advisor.py:7258`).

**Persistence caveat:** `persist_brand_profile` runs only `if brand_profile_payload`
(api `:1006-1007`) and swallows insert failures (`:6469-6474`). The web UI always
sends an all-neutral profile (`defaultBrief`, `ExpansionBriefForm.tsx:17-34`), so
UI searches always get a row; API/scripted callers may not. Probe E §E.0/E.7
measures this. Memo and saved-search reads go through `get_brand_profile`
(`:6477`), which returns `None` for missing rows.

| Field | UI exposure (EN/AR keys exist for all) | Defaults | Storage | Consumers (file:line) | Archetype can override? |
|---|---|---|---|---|---|
| `service_model` (request-level, not in brand_profile) | Always-visible select, 4 options (`ExpansionBriefForm.tsx:136-144`) | `qsr` | `expansion_search.service_model` | See §1.2 — 10+ raw-score calibrations + dead multiplier param | Seeds default archetype only; never overridden by it |
| `price_tier` | Advanced → Brand basics (`:252-260`), default "—" (null) | `None` | `expansion_brand_profile.price_tier` | `_brand_fit_score` premium penalty `:1603-1606`; `_economics_score` rent-ceiling tier via `_rent_ceiling_tier_multiplier` `:5155`, call sites `:8672,:8686`; revenue index call sites `:9821,:9852` | No — orthogonal economics signal, keep |
| `average_check_sar` | **Not exposed in UI** | `None` | `expansion_brand_profile.average_check_sar` | **None.** Persisted (`:6458`), echoed into memo brief (`:11849-11853`), never read by scoring (revenue uses `_implied_average_check(price_tier, category)` `:4667`) | Dead field — candidate for removal (parked) |
| `primary_channel` | Advanced → Operating strategy (`:268-275`) | `balanced` | `.primary_channel` | Weight domain: `_brand_weight_multipliers:3489-3495`. Raw domain: `_channel_fit_score` inside brand_fit `:1540-1547,:1593`. **Gate domain:** delivery-market gate fires only when `channel == "delivery"` (`_candidate_gate_status:3276-3287`) | Weight role yes; gate + brand_fit roles must stay or move explicitly |
| `parking_sensitivity` | Advanced → Market preferences (`:302-308`) | `medium` | `.parking_sensitivity` | Weight: `:3482-3487` (max of 3 sigs → access_visibility). Raw: brand_fit parking term weight `:1599,:1613` | Weight role yes; raw role is small (±0.06 coefficient) |
| `frontage_sensitivity` | same section (`:310-317`) | `medium` | `.frontage_sensitivity` | Weight: `:3482-3487`. Raw: steers frontage/access blend in `_access_visibility_score:2242-2245`; brand_fit fit term `:1600,:1614` | Weight role yes; blend-steering is a legit raw signal, keep |
| `visibility_sensitivity` | same section (`:319-326`) | `medium` | `.visibility_sensitivity` | Weight: `:3482-3487`. Raw: brand_fit visibility term `:1601,:1615` (score-domain multiplier removed in weight-audit Item 4b, see `:2230-2241`) | Weight role yes |
| `expansion_goal` | Advanced → Operating strategy (`:277-284`) | `balanced` | `.expansion_goal` | Weight: `:3497-3505`. Raw: brand_fit `goal_component` — flagship area-ratio logic, neighborhood spacing, delivery_led provider mix (`:1566-1591`) | **Strongest candidate for replacement by archetype** — enum nearly identical |
| `cannibalization_tolerance_m` | Advanced → Operating strategy (`:287-290`) | `1800.0` | `.cannibalization_tolerance_m` | **Gate:** `cannibalization_min_distance_m` threshold (`:3211,:3274`). Raw: brand_fit `overlap_fit` `:1563-1564` | **No** — hard-constraint semantics, must survive as its own field |
| `preferred_districts` / `excluded_districts` | Hidden (`SHOW_ADVANCED_GEOGRAPHY_SECTION = false`, `:15`) | `[]` | `.preferred/excluded_districts_json` | brand_fit district component `:1554-1561`; excluded → district gate `:3267-3272` | No — orthogonal; already de-exposed in UI |
| `target_customer` | Never exposed | — | DB column only (`20260311_exp_adv_brand_v4:30`) | **None anywhere** — dead column since v4 | Dead — parked |

Other consumers of the whole profile: LLM rerank receives it verbatim in the
prompt payload (`expansion_advisor.py:10704` → `expansion_rerank.py:244-280,:722-777`);
memo brief merges it (`:11818,:11849-11853`); saved-search payloads carry it
(`_normalize_search_payload:1482,:1510-1513`; restore via
`studyAdapters.ts:81-104`, which whitelist-normalizes enums — **a new
`brand_archetype` value must be added to those maps or restores will silently
drop it**).

## 1.2 `service_model` plumbing

Set in the always-visible UI select; stored on `expansion_search.service_model`;
flows into `run_expansion_search(service_model=...)` (`api:1014`) and back out via
`get_search`/memo (`:11061,:11732,:11833,:11852,:12234`).

Raw-score / parameter consumers (all keyed per model `qsr | dine_in |
delivery_first | cafe`):

| Consumer | Line | What it does |
|---|---|---|
| `_catchment_radii` | :852 | demand/competition/provider radii (also bulk enrich `:6932,:7058`, notes `:7776-7778`) |
| `_population_reference` | :865 | population saturation reference |
| `_demand_generator_radius_m` | :873 | L1 DG index radius |
| `_demand_generator_anchors` / index | :1930,:2039 | QSR-specific anchor set + weights version |
| `_parking_score` | :2133 | per-model parking expectation |
| `_demand_blend_weights` | :2677 | population/delivery blend inside demand_potential (e.g. delivery_first 0.40/0.60, dine_in 0.75/0.25) |
| `_WHITESPACE_LOG_REF` | :2695-2725 | whitespace saturation REF (dine_in/df 50, qsr 75) |
| `_realized_demand_reference` | :2754 | realized-demand saturation (df 307 / dine_in 402 / qsr 327) |
| `_channel_fit_score` | :1540 | dine-in signal inside brand_fit |
| `_cannibalization_score` | :4224 | half-life/ceiling decay params |
| `_estimate_fitout_cost_sar` | :4601 | per-model fitout SAR/m² |
| `_recommended_use_case` | :6277 | memo/decision-summary copy |
| zoning veto (industrial × cafe/dine_in) | :8509 | gate-adjacent |
| `_brand_weight_multipliers` | :3451 | **dead parameter — accepted, never read** |

**Circularity check for "service_model seeds the archetype":** none. The
per-model anchors above change RAW component inputs (what a site scores on a
0–100 scale); an archetype changes the WEIGHT each component carries. There is no
feedback path from weights back into raw inputs. What does happen is
**compounding along the same axis** — e.g. `delivery_first` already tilts
demand_potential's internal blend to 0.40/0.60 toward delivery, and a
`delivery_led_qsr` archetype would additionally lift the `delivery_demand`
weight 6→13. That compounding is the product intent (delivery brands should rank
differently), but it is exactly what Probe F's backtest must sanity-check
(delivery archetype on dine_in-heavy validation searches should reorder, not
degenerate). One genuine watch-item: `dine_in`'s 0.75 population blend plus a
flagship archetype's demand_potential lift would make population reach dominant;
the proposed flagship profile therefore lifts demand_potential by only +1.

**Double-application risk with `primary_channel`:** today both `service_model`
(raw domain) and `primary_channel` (weight + gate domain) encode channel. If
`service_model` seeds the archetype AND `primary_channel` keeps its weight
multiplier, a delivery_first + channel=delivery brief stacks three lifts on
delivery_demand. §1.6 recommends retiring `primary_channel`'s weight-multiplier
role for exactly this reason (its gate role stays).

## 1.3 Effective-weight display gap

What `_score_breakdown` persists per candidate: `breakdown["weights"]` is the
**post-multiplier, post-renormalization** dict — the reweight block at
`:3624-3642` replaces `component_weights` in place before it is written at
`:3683`, including the `display` block's `weight_percent` (`:3674-3681`).
`score_breakdown_json` is a JSONB column on `expansion_candidate`
(migration `20260314_exp_adv_v61_outputs.py:22`), written by
`_candidate_insert_params` (`:10878`).

| Surface | What it renders today | Effective or static? |
|---|---|---|
| UI Score-contributions card (`DecisionLogicCard.tsx`, `ContributionsSection`) | `breakdown?.weights` (`:294`) and `weighted_components` per row (`:313-315`) | **Effective** — already correct; no frontend fix needed for the card |
| UI compact breakdown (`ScoreBreakdownCompact.tsx`) | raw sub-scores only, no weights | n/a |
| Memo response → frontend | memo `candidate.score_breakdown_json` passthrough (`expansion_advisor.py:11884`) feeds the same card | **Effective** |
| LLM memo context (`build_memo_context`) | `score_breakdown["weights"] = dict(_active_component_weights())` (`llm_decision_memo.py:963`) **overwrites** the persisted dict; `contributions` recomputed from the same static dict (`:886-895,:964`) | **Static — this is the entire gap.** The LLM cites weights that can disagree with the persisted `weighted_components` it receives in the same payload |
| Memo cache | `expansion_candidate.decision_memo_json` + `decision_memo_prompt_version` (`MEMO_PROMPT_VERSION = "v12.3-component-lookup-2026-06"`, `llm_decision_memo.py:53`) | Cached memo text keeps whatever weights were cited at generation time |

**Conclusion:** the display fix is backend-memo-only: in `build_memo_context`,
prefer `raw_breakdown["weights"]` when present (fall back to
`_active_component_weights()` for legacy rows without persisted weights), derive
`contributions` from the same source, and bump `MEMO_PROMPT_VERSION` so stale
cached memos regenerate. The UI is already displaying effective weights — note
this corrects the task brief's assumption that the UI path also needed fixing.

## 1.4 Reproducibility & caching blast radius (if the default brief stops being neutral)

Ordered by severity:

1. **Within-search consistency: safe by construction.** Weights are computed once
   per candidate from the same `effective_brand_profile` for the whole search;
   `compare_candidates` and the report are within-search. Nothing assumes
   weights are constant ACROSS searches except diagnostics (below).
2. **Cross-search comparability breaks deliberately.** Historical searches were
   scored under static v2; new searches under archetype profiles. Saved-search
   score deltas across re-runs of the "same" brief will include the archetype
   delta. This is the feature, but the memo/UI must show the effective weights
   (already true per §1.3) so it is explainable.
3. **Diagnostics probes are already per-row-weight aware.**
   `weight_discrimination.sql` explodes each row's persisted
   `weighted_components`/`weights`, so mixed-archetype windows aggregate
   correctly but the "nominal_weight_pct" column becomes an average across
   archetypes — interpretation note, not breakage.
4. **Memo caching:** cached `decision_memo_json` predating the change cites old
   weights. A `MEMO_PROMPT_VERSION` bump invalidates them (cache helpers compare
   versions; migration `20260425_memo_prompt_version`).
5. **Memo prewarm path** passes the raw request `brand_profile_payload`
   (api `:1080-1089`), while later memo reads use `get_brand_profile` from DB. If
   archetype is derived (service_model seeding) rather than persisted, the two
   paths could disagree — **persist the resolved archetype** (new column on
   `expansion_brand_profile`) so every consumer reads the same value.
6. **LLM rerank:** receives brand_profile + per-candidate breakdowns; per-search
   constant weights keep it coherent. The shortlist the rerank sees will change
   order under archetypes — expected, not a contract break.

**Tests needing fixture updates** (assuming archetype applied via
`brand_profile`/new param into `_score_breakdown`, and `qsr → balanced` default):

- `tests/test_expansion_advisor_service.py:823` `test_brand_weight_reweight_neutral_profile_is_noop` —
  "neutral" must be redefined as "balanced archetype + neutral knobs".
- `:851` `gain_zero_disables` — gain 0 should disable knob fine-tuning but NOT
  the archetype profile (or the test's contract changes); product decision.
- `:867,:881` knob-lift tests — survive if knobs remain multipliers; baselines
  change if the default archetype for `service_model="qsr"` ≠ balanced.
- `tests/test_expansion_weight_stack.py:61,:133,:165,:201` — pin static v1/v2
  weights via direct `_score_breakdown` calls without a profile; survive as-is
  if archetype enters through `brand_profile`/a new explicit param, break if it
  is read from settings/service_model inside `_score_breakdown`.
- `tests/test_llm_decision_memo.py:455-470` + golden memo regression
  (`scripts/sample_regression_memos.py`, `tests/services/test_llm_decision_memo_grounding.py`)
  — break when `build_memo_context` stops overwriting weights (the §1.3 fix),
  independent of archetypes. Budget the fixture refresh into that PR.
- End-to-end `run_expansion_search` tests with non-qsr models (9 in
  `test_expansion_advisor_service.py`, 5 in `test_expansion_advisor_demand_generator.py`,
  2 in `test_expansion_advisor_regression.py`, 1 each in api/memo tests) — these
  get non-balanced default archetypes under service_model seeding; any exact
  final_score/ordering assertion in them needs re-pinning.
- `tests/fixtures/pr2_golden/*` decision-summary goldens take `final_score` as
  input (no recompute) — safe. `tests/fixtures/pr2b_golden/*` embed weights as
  input payloads — safe unless assertions echo specific weight values.
- Frontend: `ExpansionBriefForm` default brief, `studyAdapters` enum maps,
  `ExpansionAdvisorPage.test.tsx` brief snapshots; i18n EN/AR keys for the new
  field.

## 1.5 Archetype profile sketch

Base = v2 stack: occupancy_economics 20, demand_potential 18,
competition_whitespace 12, access_visibility 11, listing_quality 9, brand_fit 8,
district_momentum 7, delivery_demand 6, landlord_signal 5, chain_strength 4.

Multiplier-scale verification (why archetypes are needed): at gain 0.35 a single
knob, e.g. `primary_channel=delivery` → delivery_demand ×1.35, whitespace ×1.175;
renormalized by 104.2: delivery 6→7.77 (+1.77), whitespace 12→13.53 (+1.53). The
strongest stack (channel=delivery + goal=delivery_led) compounds to ×1.8225 →
delivery 6→9.99 (+3.99). So no single legible user choice today clears a 4-pt
move — the archetype profiles below do.

Pathology guard honored: **no profile raises brand_fit above 8** (demand-inverse
rank dominance, de-dup queued separately). Confidence stays display-only (v2).

### `balanced` — v2 as-is (control; default for `qsr`)

### `delivery_led_qsr` (default for `delivery_first`)

| Component | v2 | Profile | Δ | One-line justification |
|---|---|---|---|---|
| occupancy_economics | 20 | 20 | 0 | rent discipline equally binding |
| demand_potential | 18 | 18 | 0 | per-model blend already tilts its internals to delivery |
| competition_whitespace | 12 | 13 | +1 | overlapping delivery catchments make uncontested supply worth slightly more |
| access_visibility | 11 | 6 | **−5** | customers never see the store; courier access only |
| listing_quality | 9 | 8 | −1 | storefront presentation matters less for a production node |
| brand_fit | 8 | 6 | −2 | street-level fit less relevant; also de-risks the brand_fit pathology |
| district_momentum | 7 | 7 | 0 | neutral |
| delivery_demand | 6 | 13 | **+7** | the channel's primary revenue signal must be top-tier weight |
| landlord_signal | 5 | 5 | 0 | neutral |
| chain_strength | 4 | 4 | 0 | neutral |

Sum 100. Perceptibility: delivery_demand +7, access_visibility −5. ✔

### `flagship_dine_in` (candidate default for `dine_in` — see open decisions)

| Component | v2 | Profile | Δ | Justification |
|---|---|---|---|---|
| occupancy_economics | 20 | 19 | −1 | flagship tolerates a rent premium for the right corner |
| demand_potential | 18 | 19 | +1 | foot traffic is the revenue base (kept to +1: dine_in's 0.75 population blend already compounds, §1.2) |
| competition_whitespace | 12 | 11 | −1 | flagships can trade in contested prime nodes |
| access_visibility | 11 | 17 | **+6** | a flagship is fundamentally a visibility purchase |
| listing_quality | 9 | 8 | −1 | minor trim to fund the visibility lift |
| brand_fit | 8 | 8 | 0 | held flat despite flagship intent — pathology guard |
| district_momentum | 7 | 6 | −1 | flagships pick established nodes, not trajectories |
| delivery_demand | 6 | 2 | **−4** | dine-in destination; delivery marginal |
| landlord_signal | 5 | 5 | 0 | neutral |
| chain_strength | 4 | 5 | +1 | strong-chain co-location validates a destination node |

Sum 100. Perceptibility: access_visibility +6, delivery_demand −4. ✔

### `neighborhood_cafe` (default for `cafe`)

| Component | v2 | Profile | Δ | Justification |
|---|---|---|---|---|
| occupancy_economics | 20 | 22 | +2 | thin café margins; rent discipline is existential |
| demand_potential | 18 | 21 | +3 | walk-in local demand is the core thesis |
| competition_whitespace | 12 | 10 | −2 | cafés cluster successfully; whitespace less decisive |
| access_visibility | 11 | 9 | −2 | corner prominence is nice-to-have, not core |
| listing_quality | 9 | 10 | +1 | unit condition/fit matters for low-capex conversions |
| brand_fit | 8 | 7 | −1 | small trim, pathology-safe direction |
| district_momentum | 7 | 11 | **+4** | cafés ride district trajectory more than any other format |
| delivery_demand | 6 | 4 | −2 | secondary channel for neighborhood cafés |
| landlord_signal | 5 | 4 | −1 | minor funding trim |
| chain_strength | 4 | 2 | −2 | big-chain adjacency doesn't validate an independent café |

Sum 100. Perceptibility: district_momentum +4 (also demand +3, ws −2). ✔

Interaction flags: `neighborhood_cafe`'s district_momentum 11 leans on a
component whose raw input is the district momentum score with a sample floor —
small-sample districts get `sample_floor_applied`; acceptable, but worth a
backtest glance. `delivery_led_qsr` will surface candidates whose
access/visibility is weak by design — gate thresholds (`frontage_access_min` 55)
still apply unchanged, so true street-access failures remain gated, not ranked up.

## 1.6 Knob interaction recommendation

With archetypes carrying the big moves, per knob:

| Knob | Recommendation | Rationale |
|---|---|---|
| `expansion_goal` | **Retire — replaced by `brand_archetype`.** Migrate stored values (`flagship→flagship_dine_in`-ish, `delivery_led→delivery_led_qsr`, `neighborhood→neighborhood_cafe`, `balanced→balanced`); keep the brand_fit `goal_component` reading the archetype instead | The enum is already ~1:1 with the archetype set; keeping both guarantees user confusion and double-application |
| `primary_channel` | **Retire its weight-multiplier role; keep the field** for the delivery-market gate (`:3276-3287`) and `_channel_fit_score` | Channel is now expressed twice (service_model→archetype + knob); the gate role is load-bearing and must not silently vanish |
| `parking/frontage/visibility_sensitivity` | **Keep as fine-tuning multipliers at gain 0.35** on top of the archetype profile | They also steer raw-score blends (`:2242-2245`, brand_fit terms) so they cannot be cleanly retired; ±1.8 pts is the right magnitude for a fine-tune once archetypes do the heavy lifting. Fix the `max()` asymmetry while there: today a single "low" knob is a weight-domain no-op (max of sigs picks medium's 0) — Probe E §E.5 counts how often users hit this |
| `cannibalization_tolerance_m` | **Keep unchanged** | Hard-gate semantics; orthogonal to weights |
| `price_tier` | **Keep unchanged** | Economics raw-domain signal (rent ceiling, premium penalty); orthogonal |
| `average_check_sar`, `target_customer` | Park for removal (separate cleanup) | Dead end-to-end |
| Gain env (`EXPANSION_BRAND_WEIGHT_GAIN`) | Keep at 0.35 | Becomes a true fine-tune dial once archetypes exist; raising it would re-create the double-application problem |

## Part 2 — Probes (author-only; Ahmed runs in Codespace)

- `scripts/diagnostics/brief_usage.sql` (Probe E): coverage of profile rows,
  service_model mix, per-knob non-default rates, value distributions,
  reweight-active share over time, the "single-low-knob no-op" count, and the
  request_json cross-check. Columns verified against migrations
  `20260310_exp_adv_v0` and `20260311_exp_adv_brand_v4`.
- `scripts/diagnostics/archetype_backtest.sql` (Probe F): selects the 10 most
  recent searches with ≥8 v2-scored candidates (v2 marker:
  `inputs ? 'district_momentum'`), recomputes base scores under each archetype
  from persisted `score_breakdown_json->'inputs'`, holds sort-time bonus deltas
  constant via `bonus_detail.base_deterministic`, and reports per-search and
  per-archetype top-5 overlap, max top-10 displacement, rank-1 churn, and rank
  correlation. `balanced` is the built-in harness control (expect ~5/5 overlap,
  ~0 displacement). Note: I found no persisted list of "the 10 v2 validation
  searches" in the repo — the probe uses the same recent-10 selection rule as
  `weight_discrimination.sql`; if Ahmed has explicit search_ids, swap the
  `recent_v2_searches` CTE for a literal ID list.

Acceptance reading for Probe F: archetypes should land in a middle band —
top-5 overlap roughly 2–4 of 5 and rank correlation ~0.6–0.9 (different enough
to be perceptible, correlated enough to be sane). Overlap 5/5 means the profile
is imperceptible; overlap 0–1 or correlation <0.4 means it's rearranging on
noise.

## Part 3 — Open product decisions for Ahmed

1. **Archetype set.** Proposed four: `delivery_led_qsr | flagship_dine_in |
   neighborhood_cafe | balanced`. Alternative naming decoupled from service
   models (`delivery_led | street_flagship | neighborhood_local | balanced`)
   avoids implying "dine_in must be flagship".
2. **Default mapping from service_model.** Proposed: `qsr→balanced`,
   `delivery_first→delivery_led_qsr`, `cafe→neighborhood_cafe`,
   `dine_in→?` — the contentious one. `dine_in→flagship_dine_in` makes every
   default dine-in run a "flagship" search; `dine_in→balanced` keeps defaults
   conservative and makes flagship an explicit choice. Recommend
   **dine_in→balanced** initially (smaller blast radius, flagship stays a
   deliberate selection), revisit after Probe E shows how often dine_in users
   pick a goal today.
3. **Do knobs survive?** Recommended split in §1.6 (retire `expansion_goal`
   into the archetype, demote `primary_channel` to gate-only, keep the three
   sensitivities + cannibalization tolerance + price_tier). Needs sign-off,
   especially the `expansion_goal` migration.
4. **Gain-zero semantics.** Should `EXPANSION_BRAND_WEIGHT_GAIN=0` also disable
   archetype profiles (full kill-switch) or only the fine-tune knobs? Recommend
   a separate `EXPANSION_ARCHETYPE_PROFILES` flag so archetypes get their own
   v1-style inert rollout, mirroring how `EXPANSION_WEIGHT_STACK` shipped.

## Parked items (out of scope, recorded)

- brand_fit demand-inverse rank dominance — de-dup queued separately; archetypes
  deliberately never raise brand_fit until it lands.
- `average_check_sar` and `target_customer` dead-field cleanup.
- `_brand_weight_multipliers` single-"low"-knob no-op (max() asymmetry) — fix
  alongside the knob-demotion work, not before.
- Memo `_build_contributions` static-weight overwrite (`llm_decision_memo.py:886-895,:963`)
  — required pre-work or first commit of the redesign; bump `MEMO_PROMPT_VERSION`.
- `SHOW_ADVANCED_GEOGRAPHY_SECTION` re-exposure decision (preferred/excluded
  districts hidden in UI since the ±0.55-nudge finding).
- Scope-aware whitespace REF for broad `fast_food` briefs (known limitation noted
  at `expansion_advisor.py:2714-2719`).
