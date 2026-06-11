# Investigation — v12.1 dg-evidence enforcement miss on parcel 6706340

**Status:** read-only investigation, no code changed.
**Branch:** `claude/investigate-memo-enforcement-miss-t8ruin`
**Date:** 2026-06-11

---

## TL;DR

1. **Memos do not persist anywhere in production today.** Both persist
   sites write `expansion_candidate` and both are dead in the live flow:
   pre-warm (the only writer that fires unprompted) is explicitly disabled
   in production (`k8s/deployment.yaml:31-39`), and the memo drawer's
   `POST /decision-memo` body never includes `search_id`
   (`frontend/src/lib/api/expansionAdvisor.ts:893-897`), so the keyed-write
   guard fails (`app/api/expansion_advisor.py:1618`) and the persist step is
   skipped (`app/api/expansion_advisor.py:1700-1704`). The NULL
   `decision_memo_prompt_version` / `decision_memo_json` on all three rows is
   expected, not anomalous. Every memo Ahmed saw was a **live, ephemeral
   drawer regeneration**.

2. **The leading hypothesis (slim/truncated snapshot starving the gate) is
   killed.** All three memo entry points hand `generate_structured_memo` the
   full `feature_snapshot_json`; the slim report projection
   (`app/services/expansion_advisor.py:11965-11975`) is never fed into memo
   generation by any code path; the 4,000-char truncation rebinds a local
   copy and cannot strip fields before the gate
   (`app/services/llm_decision_memo.py:2130-2137` vs the gate read at
   `:2933`).

3. **The actual hole is the compliance detector, not the gate.**
   `_dg_evidence_invalid_reason` accepts a bare `"<rounded>/100"` substring
   in *any* key_evidence row's signal/value
   (`app/services/llm_decision_memo.py:2724-2728`). Parcel 6706340's
   composite 59.82 rounds to **60**, so any other 0–100 metric the LLM
   rendered as `60/100` (blended demand score, access score, etc. — the
   demand score is 0.6·composite for dine-in, so it rounds to the same
   integer whenever delivery ≈ composite) silently satisfies Layer 1 on the
   **first** response: no retry, no injection, no log line. The repo's own
   tests assert this false-accept as intended behavior
   (`tests/test_llm_decision_memo.py:2427-2434`: an `80/100` *final-score*
   row satisfies a composite of 80). The compliant candidate's composite 94
   had no colliding score → detector correctly rejected → retry authored the
   row. Both observations come out of one mechanism.

4. A **secondary render-window vector** exists but is distinguishable in
   logs: the detector accepts the composite row at any index
   (`llm_decision_memo.py:2729-2738`) while the frontend renders only the
   top 4 rows (`frontend/src/features/expansion-advisor/DecisionMemoNarrative.tsx:104`),
   so a retry-authored row at index ≥ 4 passes server-side yet is invisible
   in the UI. This vector requires a `dg-evidence rejected` warning in the
   pod logs; the false-accept vector produces none.

---

## psql commands for Ahmed (run first — single-line, iPad-safe)

**A. Confirm nothing persisted on the three rows (adjudicates Q1/Q4 in one
read — `has_memo_text` false on all rows = no persisted memo of any
version, so there is no v12-vs-v12.1 stamp to race-check):**

```
psql "$DATABASE_URL" -P pager=off -c "SELECT id, search_id, computed_at, rank_position, final_rank, decision_memo IS NOT NULL AS has_memo_text, decision_memo_json IS NOT NULL AS has_memo_json, decision_memo_prompt_version, decision_memo_lang FROM expansion_candidate WHERE parcel_id='6706340' ORDER BY computed_at DESC;"
```

**B. Confirm the persist path is dead table-wide (any non-NULL memo rows at
all, by version stamp — rows stamped `v12.1-demand-evidence-enforced-2026-06`
would mean some keyed writer still fires somewhere):**

```
psql "$DATABASE_URL" -P pager=off -c "SELECT decision_memo_prompt_version AS ver, count(*) AS rows, max(computed_at) AS latest FROM expansion_candidate WHERE decision_memo_json IS NOT NULL OR decision_memo IS NOT NULL GROUP BY 1 ORDER BY 2 DESC;"
```

**C. The false-accept adjudicator — does the rounded composite (the
detector's `<N>/100` needle) collide with another 0–100 score on this
candidate:**

```
psql "$DATABASE_URL" -P pager=off -c "SELECT id, computed_at, feature_snapshot_json->>'demand_score_source' AS src, jsonb_typeof(feature_snapshot_json->'demand_generator_index'->'composite_0_100') AS composite_type, feature_snapshot_json->'demand_generator_index'->>'composite_0_100' AS composite, round((feature_snapshot_json->'demand_generator_index'->>'composite_0_100')::numeric)::int AS needle, round(demand_score)::int AS demand_rounded, round(final_score)::int AS final_rounded, round(access_visibility_score)::int AS accvis_rounded, round(economics_score)::int AS econ_rounded FROM expansion_candidate WHERE parcel_id='6706340' ORDER BY computed_at DESC;"
```

If `needle` equals `demand_rounded` (or any other `*_rounded`), the
false-accept is structurally live for this candidate: the LLM citing that
score as `60/100` in any evidence row satisfies the v12.1 detector without
a composite row ever existing.

**D. Same read for the compliant candidate (fill in its parcel id) — expect
`needle`=94 with no colliding rounded score:**

```
psql "$DATABASE_URL" -P pager=off -c "SELECT id, computed_at, feature_snapshot_json->'demand_generator_index'->>'composite_0_100' AS composite, round((feature_snapshot_json->'demand_generator_index'->>'composite_0_100')::numeric)::int AS needle, round(demand_score)::int AS demand_rounded, round(final_score)::int AS final_rounded FROM expansion_candidate WHERE parcel_id='<COMPLIANT_PARCEL_ID>' ORDER BY computed_at DESC LIMIT 3;"
```

**Log greps (not psql — adjudicate false-accept vs render-window):** the
false-accept produces *no* enforcement log lines for the candidate; the
render-window vector requires a rejected-then-retried sequence:

```
kubectl logs deploy/oaktree-estimator --since=72h --all-containers | grep -E "dg-evidence rejected|deterministically injected|dg retry produced"
```

(Warning emitted at `app/services/llm_decision_memo.py:2952-2957` on Layer-1
reject; `:3083-3089` on Layer-2 injection.)

**Live repro (definitive, 1 minute):** re-open the memo drawer for the
candidate with the browser network tab open; inspect
`POST /v1/expansion-advisor/decision-memo` → `memo_json.key_evidence`. The
false-accept predicts a row whose `value` is `60/100` with no
engine-attributed signal ("demand-generator composite" /
"مركب مولدات الطلب") and no `"source": "deterministic_injection"` marker
anywhere in the list.

---

## Q1 — Memo persistence: table, columns, keys

Both persist sites cited by the PR-E investigation write the **same table,
`expansion_candidate`**, same four columns. There is no other memo store.

| Site | Statement | Key | Columns written |
|---|---|---|---|
| `app/api/expansion_advisor.py:1545-1563` (`_decision_memo_cache_write`) | `UPDATE expansion_candidate SET decision_memo, decision_memo_json, decision_memo_prompt_version, decision_memo_lang` | `WHERE search_id = :sid AND parcel_id = :pid` (`:1552`) | all four; `:ver` bound to `MEMO_PROMPT_VERSION` at `:1559` |
| `app/services/expansion_advisor.py:11527-11544` (`_regenerate_candidate_memo_in_locale`) | same four columns | `WHERE id = :cid` (`:11534`) — candidate UUID | all four |

(The task's cited lines land inside these blocks: `api/…:1559` is the
version bind of site 1; `services/…:11522` is the except-arm directly under
the `generate_structured_memo` call at `:11518`, five lines above site 2's
UPDATE.)

`MEMO_PROMPT_VERSION = "v12.1-demand-evidence-enforced-2026-06"`
(`app/services/llm_decision_memo.py:53`). **Every** write through either
site stamps it — there is no code path that writes `decision_memo_json`
while leaving `decision_memo_prompt_version` NULL. Therefore
`decision_memo_prompt_version IS NULL` on all three rows means **no persist
ever executed** for them, not that memos "persist somewhere else."

Why no persist executed:

- **Site 1** is reached from `POST /decision-memo` only when
  `cache_keyed = bool(search_id and parcel_id)` is true
  (`app/api/expansion_advisor.py:1618`, persist call at `:1700-1704`). The
  frontend sends `{candidate, brief, lang}` with **no top-level
  `search_id`/`parcel_id`** (`frontend/src/lib/api/expansionAdvisor.ts:893-897`;
  request model fields exist but default None,
  `app/api/expansion_advisor.py:1367-1376`). `parcel_id` is inferred from
  the candidate dict (`:1614-1616`) but `search_id` stays None →
  `cache_keyed` False → **no write**. (This is the "search_id/parcel_id
  cache-key revert" referenced in the k8s comment below.)
- **Site 1 via pre-warm** (`_prewarm_decision_memos` →
  `_decision_memo_cache_write`, `app/api/expansion_advisor.py:797-799`) is
  keyed correctly, but pre-warm is **disabled in production**:
  `EXPANSION_MEMO_PREWARM_ENABLED: "false"` with an explanatory comment that
  this was Ahmed's product decision (`k8s/deployment.yaml:31-39`), overriding
  the repo default of enabled/top-15 (`app/core/config.py:439-445`).
- **Site 2** fires only from `GET /candidates/{id}/memo` on a locale
  mismatch, and only when the row **already has** a memo
  (`has_memo` guard, `app/services/expansion_advisor.py:11801-11808`).
  Rows never get a first memo (above), so this never fires.

Consequence: the memo Ahmed watched "regenerate" is the live drawer
response. The drawer regenerates on **every** open because the cache lookup
is hard-disabled (`_decision_memo_cache_lookup` returns None
unconditionally, `app/api/expansion_advisor.py:1503-1518`), and the result
lives only in the HTTP response plus a frontend module cache keyed by
candidate-id+lang (`DecisionMemoNarrative.tsx:284-289`). Command A confirms
this (expect `has_memo_text` false too).

## Q2 — Entry points and what snapshot reaches the gate

All call sites of `build_memo_context` / `generate_structured_memo`
(exhaustive — repo-wide grep):

| # | Path | Trigger | Candidate dict handed to `build_memo_context` | Snapshot reaching the gate | Live in prod? |
|---|---|---|---|---|---|
| 1 | Pre-warm: `app/api/expansion_advisor.py:779-786` | background task after `POST /searches` (`:1075-1089`) | in-memory search items via `_build_prewarm_specs` (`:875-894`, `dict(item)` copies of the full service items built at `app/services/expansion_advisor.py:10306-10419`, full `feature_snapshot_json` at `:10349`) | **full** | **No** — disabled (`k8s/deployment.yaml:38-39`) |
| 2 | Memo drawer: `app/api/expansion_advisor.py:1647-1656` (`POST /decision-memo`) | `DecisionMemoNarrative` mount (`DecisionMemoNarrative.tsx:284`) inside `ExpansionMemoPanel` (`ExpansionMemoPanel.tsx:302-306`), fed `candidateRaw = selectedCandidate` (`ExpansionAdvisorPage.tsx:760`) | the frontend's list-item candidate. Chain is lossless: DB → `get_candidates` selects `feature_snapshot_json` whole (`app/services/expansion_advisor.py:10965`) → `_normalize_feature_snapshot` is additive-only (`:1256-1261`) → response model is `extra="allow"` at both nesting levels (`app/api/expansion_advisor.py:87-88, :179-182, :218`) → frontend `normalizeCandidate` spreads the raw object (`expansionAdvisor.ts:633-635`) → POSTed back verbatim (`:896`) → `DecisionMemoRequest.candidate: dict[str, Any]` (`app/api/expansion_advisor.py:1368`) | **full** | **Yes — the only generation path that runs** |
| 3 | Locale-mismatch regen: `app/services/expansion_advisor.py:11513-11518` | `GET /candidates/{id}/memo` with `lang` ≠ stored lang AND an existing memo (`:11801-11808`) | full DB row `dict(row)` (SELECT includes `feature_snapshot_json`, `:11589`) | **full** | dormant (guard never true, Q1) |

**The slim report projection** (`app/services/expansion_advisor.py:11965-11975`,
9 whitelisted snapshot keys, no demand fields) is built inside
`get_recommendation_report` for the report UI payload only. No code path
feeds a report `top_candidates` item into `build_memo_context`: the report
panel renders callouts and `CopySummaryBlock` (display-only,
`ExpansionReportPanel.tsx:251`), and its candidate-select handlers resolve
back into the **full** list via `resolveCandidateById(candidates, …)`
(`ExpansionAdvisorPage.tsx:334-338, :785`). **Hypothesis "slim snapshot ⇒
gate returns None" is killed for every reachable path.**

**Rank 1 vs rank 2:** rank does not select a different path. With pre-warm
off, both candidates' memos came through entry point 2 with full snapshots.
The behavioral difference is explained by the detector (Q3/Q5): composite
94 → needle `94/100` → no collision → correctly rejected → retry authored
the row (Ahmed's "retry-authored at position 4"); composite 59.82 → needle
`60/100` → collides with any other score rendered as `60/100` → false
accept on the first response → no retry, no injection.

## Q3 — Gate ordering, truncation, and the actual silent-skip vector

Call order inside `generate_structured_memo`
(`app/services/llm_decision_memo.py:2856-3099`):

1. `render_structured_memo_prompt(ctx)` at `:2898` serializes the user turn.
   The 4,000-char soft limit (`_FEATURE_SNAPSHOT_SOFT_LIMIT`, `:2104`)
   **rebinds a local** `snap` to a whitelist projection — it never mutates
   `ctx.feature_snapshot` (`:2130-2137`; second-stage 12,000-char fallback
   likewise, `:2169-2177`).
2. The gate reads `ctx.feature_snapshot` at `:2933` — the same dict
   `build_memo_context` stored (`:903-912` source, `:1000` assignment),
   untouched by step 1.
3. Layer 1 validity (`:2934-2938`), corrective retry (`:2958-2978`),
   retry re-check (`:3009-3013`), Layer 2 injection (`:3079-3089`).

So the gate and the whitelist serialization read the same dict, and the
truncation path is **not** a skip vector — additionally moot because the
memo whitelist explicitly carries `demand_generator_index` and
`demand_score_source` (`:441-458`, keys at `:456-457`), so even a truncated
prompt retains them.

**The real silent-skip is in the detector, with the gate engaged.**
`_dg_evidence_invalid_reason` (`:2716-2742`) declares a row compliant when
the lowercased `signal + " " + value` of **any** row contains any of three
needles (`:2724-2728`): the EN phrase, the AR term, or the bare string
`f"{composite_rounded}/100"`. There is no requirement that the `/100` value
co-occur with engine attribution. The test suite pins this as intended:
`tests/test_llm_decision_memo.py:2427-2434` asserts a memo whose only
`…/100` row is an **80/100 final-score row** is *compliant* for
composite 80 (`assert _dg_evidence_invalid_reason(memo, 80) is None`).

For parcel 6706340: `int(round(59.82)) = 60` (`:2713`). The dine-in demand
score is `clamp(composite·w_pop + delivery·w_del)`
(`app/services/expansion_advisor.py:9595-9601`), dominated by the composite,
so `demand_score` rounds to 60 whenever delivery sits near the composite —
and a key_evidence row like `{"signal": "Demand Strength", "value":
"60/100"}` (or any other score rendered `60/100`) makes `dg_reason = None`
on the first response: Layer 1 never retries (`:2944`), Layer 2 never
injects (`:3079` requires `dg_reason is not None`), and **no log line is
emitted** — the only enforcement logs are on reject (`:2952-2957`) and
inject (`:3083-3089`). Command C quantifies the collision; the network-tab
repro confirms it directly.

**Secondary vector (render window):** the detector accepts a matching row
at any index (`:2729-2738`), the retry preamble does not pin a position
(`:2806-2816` — "key_evidence MUST include this row", no placement
constraint), but the frontend renders only `key_evidence.slice(0, 4)`
(`DecisionMemoNarrative.tsx:104`). The deterministic injection deliberately
inserts at index 1 for exactly this reason (`:2787-2798`), but a
*retry-authored* row at index ≥ 4 passes the validator and never renders.
Distinguisher: this vector leaves a `dg-evidence rejected` warning in logs;
the false-accept leaves nothing.

## Q4 — Race check (v12 code serving a post-deploy regen)

Adjudicated as moot by Q1: no memo row was ever written for these
candidates, so there is no stored `decision_memo_prompt_version` to
distinguish a v12-stamped race from a v12.1 code bug (Command A double-
confirms — expect `has_memo_text` false as well). Independent of the race
question, the false-accept is **structural in the v12.1 code currently on
`main`** (detector shipped in PR-E2, merge `e404f0a01`), reproduces on any
re-open of the drawer, and fully explains the observation without invoking
a stale pod. The compliant 94.16 memo on the same deploy proves v12.1 code
was serving and engaging correctly where no needle collision exists.

## Q5 — Fix sketch (PR-E3, single-purpose)

**Cause to fix:** detector false-accept on the bare `"<N>/100"` needle
(`app/services/llm_decision_memo.py:2716-2742`).

**Smallest safe fix** — require engine attribution for a value-only match;
keep the phrase needles as-is:

```python
def _dg_evidence_invalid_reason(parsed, composite_rounded):
    phrase_needles = (_DG_COMPOSITE_SIGNAL_EN.lower(),
                      _DG_COMPOSITE_SIGNAL_AR)
    value_needle = f"{composite_rounded}/100"
    rows = parsed.get("key_evidence")
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            signal = str(row.get("signal") or "").lower()
            value = str(row.get("value") or "").lower()
            haystack = f"{signal} {value}"
            if any(n in haystack for n in phrase_needles):
                return None
            # A bare value match counts only when the signal attributes
            # the number to the generator engine, so a coincidental
            # "<N>/100" on another score can no longer satisfy the
            # mandate (the production miss on composite≈60).
            if value_needle in value and (
                "generator" in signal or "مولدات" in signal
            ):
                return None
    return (
        "demand_score_source is dg_index but key_evidence has no "
        "demand-generator composite row"
    )
```

- One function changed; both layers and the injector inherit the tightened
  semantics automatically (injector idempotency at `:2791` and the retry
  re-check at `:3010` call the same detector).
- False-reject cost is bounded by design: a legitimately-phrased composite
  row that the tightened detector misses just triggers the existing
  one-retry loop, whose preamble instructs "copy signal and value exactly"
  from the canonical row (`:2806-2816`) — and Layer 2 still injects if the
  retry disobeys. Worst case is one extra LLM call, never a lost row.
- Test updates in `tests/test_llm_decision_memo.py`: flip
  `:2434` (`_dg_evidence_invalid_reason(memo, 80)` on a final-score row
  must become **not None**), keep `:2412-2417` green by its
  `signal="demand composite"` … actually update it to a generator-attributed
  signal, and add the regression case: composite 60 + `{"signal": "Demand
  Strength", "value": "60/100"}` row ⇒ invalid.

**Validation:**

```
pytest tests/test_llm_decision_memo.py -k "Dg" -q
make test
```

Plus the live repro: re-open the drawer for a dg_index candidate whose
rounded composite collides with another score; the memo must now carry a
generator-attributed row (retry-authored or injected with
`"source": "deterministic_injection"`).

**Optional riders (separate PRs, not PR-E3):**

- *Render-window pin:* after enforcement resolves, if the compliant row
  sits at index > 3, move it inside the top-4 window so
  `DecisionMemoNarrative.tsx:104` always shows it.
- *Persistence reality:* either pass `search_id`/`parcel_id` from
  `generateDecisionMemo` (`expansionAdvisor.ts:893-897`) so site 1's keyed
  write fires again, or retire the dead persist plumbing + the
  `decision_memo_present` affordance it feeds — currently the columns can
  only ever be NULL for new searches, and any future incident will again
  lack DB evidence (this investigation had to fall back to network-tab
  repros for exactly that reason).

**Merge recommendation:** PR-E3 as sketched is low-risk (single detector
function + tests, no prompt or schema change, no output-shape change) and
high-value — it closes the only confirmed enforcement bypass. Recommend
fast-track after Commands A–D corroborate.

---

## Discrepancies & framing

1. **"Memos persist somewhere else" — false.** They persist nowhere. The
   PR-E persist sites are real code but unreachable in production (Q1). The
   premise that the three NULL rows imply an alternate store inverted the
   actual situation: NULLs are the expected steady state since the cache-key
   revert + pre-warm disable.
2. **The task's leading hypothesis (slim snapshot starves the gate) is
   killed**, with the full chain cited (Q2). The slim report projection at
   `11948-11975` exists but feeds only the report UI; it is never an input
   to memo generation.
3. **The truncation vector is killed** (Q3): truncation rebinds a local
   copy after the prompt is rendered and before nothing — the gate reads
   the unprojected `ctx.feature_snapshot`.
4. **"Layer 2 makes this impossible" was over-trusted.** Layer 2 only fires
   when Layer 1's detector says non-compliant; the detector's `/100`
   needle was specified (and test-pinned, `tests/test_llm_decision_memo.py:2427-2434`)
   in a way that accepts coincidental score collisions. The
   v12.1 enforcement worked exactly as written — the spec of "compliant"
   was too loose at integer-collision boundaries (~1-in-100 per extra
   `…/100` row per candidate, but strongly correlated for dine-in because
   demand_score is composite-dominated, `app/services/expansion_advisor.py:9595-9601`).
5. **Race framing was moot** by the persistence finding: there is no stored
   version stamp to adjudicate, and no need — the miss is structural in
   v12.1 and reproducible (Q4).
6. **Unverifiable from the repo:** which exact `60/100` row the LLM emitted
   (the memo JSON was never persisted). Commands C/D establish the
   collision structurally; the network-tab repro or the log-absence grep
   closes it observationally. If Command C surprisingly shows **no** rounded
   score equal to 60, the fallback explanation is the render-window vector —
   which the same log grep adjudicates (rejected-warning present vs absent).
