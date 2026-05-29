# Decision Memo Latency Investigation — Findings

> Read-only investigation. No code changed, no branch/commit created.
> Date: 2026-05-29. Scope: why decision-memo generation takes ~60s/candidate.

## TL;DR

The ~60s is almost entirely **OpenAI output-token generation on a very large
prompt, sometimes doubled by a corrective retry.** `build_memo_context` does
**zero I/O** — it is pure CPU over data already in the request/row.
`market_research` is **not** a separate web/LLM call (it's `{}`). The two biggest
levers are: (1) the system prompt is **~41 KB (~10–11K input tokens)** and
`max_tokens=2400` output on `gpt-4o-mini`, and (2) a **second full LLM
round-trip** fires whenever the headline fails validation. A third hidden
multiplier is the **OpenAI SDK default `max_retries=2` and `timeout=600s`** — the
client is constructed bare, so transient 429/5xx silently triple the wall time.

---

## 1. End-to-end synchronous paths

### Path A — `POST /expansion-advisor/decision-memo` → `post_decision_memo`
Route: `app/api/expansion_advisor.py:1575`

| # | Step | Type | file:line |
|---|------|------|-----------|
| A1 | Resolve lang / parcel_id / search_id | pure CPU | `expansion_advisor.py:1610-1618` |
| A2 | `_decision_memo_cache_lookup(...)` → **always returns None** (reads disabled) | (DB no-op) | `expansion_advisor.py:1622` / def `:1503-1518` |
| A3 | `build_memo_context(candidate, brief, lang)` | **pure CPU** | `expansion_advisor.py:1647` / def `llm_decision_memo.py:872-1004` |
| A4 | `generate_structured_memo(ctx)` → `render_structured_memo_prompt` (pure CPU) then **`client.chat.completions.create`** | **[LLM call] #1** | `expansion_advisor.py:1656` / call `llm_decision_memo.py:2549` |
| A5 | `_parse_and_validate_memo_shape` + `_headline_validity_reason` | pure CPU | `llm_decision_memo.py:2563-2577` |
| A6 | **If headline invalid → corrective retry** `client.chat.completions.create` | **[LLM call] #2 (conditional)** | `llm_decision_memo.py:2602` |
| A7 | `_record_cost` (in-process dict) | pure CPU | `llm_decision_memo.py:2680` |
| A8 | `render_structured_memo_as_text` | pure CPU | `expansion_advisor.py:1663` |
| A9 | `_structured_to_legacy_shape` | pure CPU | `expansion_advisor.py:1674` |
| A10 | On any structured failure: `generate_decision_memo` legacy | **[LLM call] fallback** | `expansion_advisor.py:1683` / def `:239-378` |
| A11 | `_decision_memo_cache_write` UPDATE + commit (when keyed) | **[DB query]** | `expansion_advisor.py:1701` / def `:1521-1568` |

**Note:** Legacy fallback (A10) only runs when the structured path returns
`None`. It is a *separate* third LLM call, but it's mutually exclusive with
success — it does not normally stack on A4/A6.

### Path B — `GET /expansion-advisor/candidates/{id}/memo` → `get_candidate_memo`
Route: `app/api/expansion_advisor.py:1238` → `get_candidate_memo` at
`expansion_advisor.py:10558`

| # | Step | Type | file:line |
|---|------|------|-----------|
| B1 | `t_start = time.monotonic()` | — | `expansion_advisor.py:10559` |
| B2 | Big `SELECT … FROM expansion_candidate JOIN expansion_search` (single row, PK lookup) | **[DB query]** | `expansion_advisor.py:10560-10648` |
| B3 | `_cached_district_lookup(db)` | [DB query, cached] | `expansion_advisor.py:10652` |
| B4 | `_normalize_candidate_payload(...)` | pure CPU | `expansion_advisor.py:10653` |
| B5 | `get_brand_profile(db, search_id)` | **[DB query]** | `expansion_advisor.py:10654` |
| B6 | Verdict/headline derivation, dict assembly | pure CPU | `expansion_advisor.py:10657-10792` |
| B7 | timing log line emitted | — | `expansion_advisor.py:10676` |
| B8 | **Locale-mismatch regenerate** (only if `has_memo and lang != stored_lang`) → `_regenerate_candidate_memo_in_locale` → `generate_structured_memo` (+retry) + UPDATE/commit | **[LLM call ×1–2] + [DB query]** | `expansion_advisor.py:10809-10816` / def `:10493-10555` |

**Key insight:** The normal English GET path is **DB-read only — it does NOT
call the LLM.** It just returns the persisted memo
(`decision_memo`/`decision_memo_json`). The ~60s on a GET only happens on
**B8**, the locale-mismatch (first Arabic view of an English-prewarmed
candidate) regenerate. Otherwise the slow generation is **Path A** (on-demand
POST) or the **prewarm background task**.

---

## 2. Specific answers

### Q1 — How many LLM round-trips per memo? Retry trigger conditions

**Up to 2 round-trips in `generate_structured_memo`** (3 if you count the legacy
fallback, which is mutually exclusive). Retry logic:
`llm_decision_memo.py:2583-2668`.

The retry fires **iff `_headline_validity_reason(...)` returns non-None**
(`:2583`). That function (`llm_decision_memo.py:2150-2254`) returns a reason —
triggering the retry — when **any** of:

1. **`headline missing or empty`** (`:2178-2179`)
2. **Headline doesn't start with an allowed prefix** — EN:
   `"Recommend with reservations" | "Recommend" | "Decline"`; AR:
   `"نوصي مع تحفظات" | "نوصي" | "نرفض"` (`:2197-2200`). *This is the
   most likely-to-fire condition*: any model drift to "Consider…", "This
   site…", "GO:", a leading quote/whitespace, or a localized verb not in the
   canon trips it.
3. **Rank-1, `final_score ≥ 70`, no blocking gate failures, but headline is
   "Decline"** (`:2212-2222`)
4. **`overall_pass is False` but headline is "Recommend"/"Recommend with
   reservations"** (`:2225-2231`)
5. **`overall_pass is True` and `deterministic_verdict ∈ {go, consider}` but
   headline is "Decline"** (`:2238-2246`)
6. **Confabulation guard:** "Decline" headline containing `"failed "`/`"fails
   on"` (AR: `"فشل"`/`"لم يجتز"`) while `blocking_failed` is empty
   (`:2250-2252`)

**Is it effectively always 2 calls?** Not by construction — at temperature 0.3 a
compliant model usually satisfies condition #2 on the first try, so the *common*
case is **1 call**. **But** the prompt is doing a lot of heavy-handed steering
toward exact prefixes (see the huge `_CRITICAL_BLOCK_EN`), and condition #2 is a
strict `startswith` on the *raw* string. Any stray leading character (a quote, a
markdown bullet, an emoji, a localized synonym) forces a full 2400-token
regeneration. **You cannot determine the real retry rate from code alone** — it
must be measured (see §4 grep). If the retry rate is high, this single factor is
the difference between ~30s and ~60s.

### Q2 — Is `build_memo_context` doing I/O? **No.**

`build_memo_context` (`llm_decision_memo.py:872-1004`) is **pure CPU**. It reads
only keys already present on the `candidate`/`brief` dicts passed in. Enumerated:

- District median rent → **not fetched**; `_format_rent_vs_median` only runs in
  the *legacy* path (`:279`), and even there it just arithmetic on
  `candidate["district_median_rent"]` already in the dict. No query.
- Comparable competitors → `_as_list(candidate.get("comparable_competitors_json"))`
  (`:935`) — reads the dict, **no query**.
- `rent_vs_median`, contributions, gate buckets, realized_demand → all derived
  from in-dict fields (`:907`, `:928`, `:938`) — **pure CPU**.
- `render_structured_memo_prompt` (`:2025-2124`) and
  `_serialize_context_for_user_message` (`:1969+`) → **pure CPU** (string
  composition only; verified no `db/execute/requests/openai`).

So **the only I/O on the generation path is the OpenAI call(s)** plus, on POST,
the final persist UPDATE. The DB reads in Path B happen in `get_candidate_memo`
itself (B2/B3/B5), *not* in `build_memo_context`.

### Q3 — `market_research`: separate web-search/LLM call?

**No.** In `get_candidate_memo`, `"market_research": {}` is **hardcoded empty**
(`expansion_advisor.py:10784`). There is no web search and no secondary LLM call
powering it on the memo critical path. The response model field exists
(`expansion_advisor.py:391`) but is not populated by a live call here. It is
**not** a time sink.

### Q4 — Model / token / API config (structured path)

From `app/core/config.py:122-136` (defaults; real values depend on prod env
vars):

| Setting | Value (default) | Source |
|---|---|---|
| `EXPANSION_MEMO_MODEL` | `"gpt-4o-mini"` | `config.py:126` |
| `EXPANSION_MEMO_MAX_TOKENS` | `2400` | `config.py:127-129` |
| `EXPANSION_MEMO_TEMPERATURE` | `0.3` | `config.py:130-132` |
| `response_format` | `{"type": "json_object"}` | `llm_decision_memo.py:2554, 2607` |
| `EXPANSION_MEMO_STRUCTURED_ENABLED` | `true` | `config.py:133-136` |

- **System prompt size:** `_STRUCTURED_MEMO_PREAMBLE` (~36.6 KB) +
  `_CRITICAL_BLOCK_EN` (~5 KB) ≈ **~41 KB ≈ ~10–11K input tokens**, before the
  user payload (capped at `_MAX_USER_PAYLOAD_CHARS = 12000` chars ≈ ~3K tokens,
  `llm_decision_memo.py:1946`). So **~13–14K input tokens per call**, and up to
  **2400 output tokens**.
- **Streaming:** **Not used anywhere** — both `.create()` calls omit `stream=`
  (`:2549`, `:2602`). The whole 2400-token completion must finish before the
  user sees anything.
- **Client reuse:** **Reused** — lazy module-global singleton `_client` in
  `_get_client()` (`llm_decision_memo.py:95-109`). Not re-instantiated per call.
  ✅ (No per-call connection setup cost.)
- **Client timeout / retries:** **None set.** `OpenAI(api_key=api_key)` is
  constructed bare (`:108`). This means the **SDK defaults apply:
  `timeout=600s` and `max_retries=2`**. So a single logical `.create()` can
  transparently retry up to 2 extra times on 429/5xx/connection errors, each
  waiting on exponential backoff — a real and invisible contributor to
  occasional ~60s+ outliers.

The **legacy** path uses a *different* model constant:
`MODEL_ID = DECISION_MEMO_MODEL` default `"gpt-4o-mini-2024-07-18"`,
`MAX_TOKENS=800`, `TEMPERATURE=0.3` (`llm_decision_memo.py:45-47`) — relevant
only on fallback.

### Q5 — Existing instrumentation (grep targets)

What's already logged on the memo path:

| What | Log string (grep) | file:line |
|---|---|---|
| GET memo total time | `expansion_memo timing: total=%.2fs candidate_id=...` | `expansion_advisor.py:10676` |
| Structured memo success + tokens + cost | `Structured memo generated \| candidate_id=... input_tokens=... output_tokens=... cost=$...` | `llm_decision_memo.py:2682` |
| **Retry fired** (headline rejected) | `Structured memo headline rejected for %s: %s \| headline=...` | `llm_decision_memo.py:2584` |
| Retry still invalid → local rewrite | `Structured memo retry headline still invalid for %s` | `llm_decision_memo.py:2641` |
| Local rewrite applied | `Structured memo headline locally rewritten for %s` | `llm_decision_memo.py:2660` |
| LLM call #1 failed | `Structured memo OpenAI call failed for %s` | `llm_decision_memo.py:2557` |
| Retry call failed | `Structured memo retry call failed for %s` | `llm_decision_memo.py:2610` |
| Legacy memo generated | `Decision memo generated \| aqar_id=...` | `llm_decision_memo.py:372` |
| Prewarm batch timing | `expansion_memo_prewarm done: search_id=... wall_s=... generated=... skipped=... failed=...` | `expansion_advisor.py:866` |

**Gap:** There is **no per-phase timer** wrapping the two `.create()` calls
themselves. You can *infer* call count from token totals and the "headline
rejected" line, but there is no direct `llm_call_ms` log. `build_memo_context`,
render, and persist are also untimed. So today you can attribute "1 vs 2 LLM
calls" (via the rejected-headline log) but **not** "context-build vs LLM vs
render vs persist" without adding timers. The POST `/decision-memo` endpoint has
**no `t_start`** at all — only the GET path does.

### Q6 — Prewarm lever & cache reads

- **`EXPANSION_MEMO_PREWARM_ENABLED`** (default `true`, `config.py:346-348`) is
  indeed the **precompute-vs-on-view lever**. When true,
  `create_expansion_search` schedules `_prewarm_decision_memos` as a
  `BackgroundTask` (`expansion_advisor.py:1085`) that generates structured memos
  for the top `EXPANSION_MEMO_PREWARM_TOP_N` (default 15) candidates with
  concurrency `EXPANSION_MEMO_PREWARM_CONCURRENCY` (default 5) under a
  `EXPANSION_MEMO_PREWARM_BUDGET_S` (default 600s) wall-clock budget
  (`expansion_advisor.py:706-872`).
- **Cache reads are disabled — confirmed.** `_decision_memo_cache_lookup`
  unconditionally `return None` (`expansion_advisor.py:1503-1518`). Writes still
  happen (`_decision_memo_cache_write`, `:1521`) so `decision_memo_present`
  flips true and the GET endpoint can serve the persisted row.
- **Consequence:** Even when prewarm has populated a memo, **POST
  `/decision-memo` always regenerates** (lookup returns None → full LLM path).
  So the ~60s the user feels on a memo *click* is **on-demand generation every
  time**, regardless of prewarm — prewarm only helps the **GET** path (which
  serves the stored row without an LLM call, except on locale mismatch). **If
  the user is hitting the slow path via POST, prewarm does not save them.**

---

## 3. Ranked latency budget

| Rank | Step | Type | file:line | Speed | Reasoning |
|---|---|---|---|---|---|
| **1** | LLM call #1 completion | [LLM] | `llm_decision_memo.py:2549` | **SLOW (~15–40s)** | gpt-4o-mini generating up to **2400 output tokens**; output tokens dominate latency. ~13–14K input tokens add TTFT + prompt-processing. No streaming → full wait. |
| **2** | Corrective retry (LLM call #2) | [LLM] | `llm_decision_memo.py:2602` | **SLOW when it fires (~+15–40s, doubles total)** | Strict `startswith` prefix check (`:2197`) is brittle; a single non-conforming headline forces a second full 2400-token generation. Measure rate before assuming. |
| **3** | SDK transparent retries (max_retries=2, 600s timeout) | [HTTP] | client `:108` (bare) | **SLOW outliers** | Unset → defaults. 429/5xx/transient errors silently retried up to 2× with backoff, each up to 600s. Explains worst-case >60s tails. |
| 4 | Big candidate SELECT + JOIN (GET) | [DB] | `expansion_advisor.py:10560` | medium-fast | Single PK row + EXISTS subquery; fast if `expansion_candidate.id` indexed. Verify in DB. |
| 5 | `get_brand_profile` (GET) | [DB] | `expansion_advisor.py:10654` | fast | Single small lookup. |
| 6 | `_cached_district_lookup` | [DB cached] | `expansion_advisor.py:10652` | fast | Cached. |
| 7 | Persist UPDATE + commit (POST) | [DB] | `expansion_advisor.py:1701` | fast | One UPDATE by (search_id, parcel_id) + commit. |
| 8 | `build_memo_context` / render / serialize | pure CPU | `llm_decision_memo.py:872`, `2025`, `1969` | negligible | No I/O, small dicts/strings. |
| — | `market_research` | n/a | `expansion_advisor.py:10784` | none | Hardcoded `{}`. Not a cost. |

### Top 2–3 suspected time sinks
1. **LLM completion latency on a 2400-max-token output with a ~41 KB system
   prompt** (call #1). This is the floor — likely 60–80% of a single-call memo.
2. **The corrective retry** (`:2583-2608`) — when it fires it roughly
   **doubles** total time. Its real-world firing rate is the single biggest
   unknown and must be measured.
3. **Bare OpenAI client (no timeout/retry tuning)** — SDK default
   `max_retries=2`/`timeout=600s` turns transient API hiccups into multi-minute
   tails.

---

## 4. Commands to confirm empirically (run these before patching)

### Prod pod logs (attribute call count & retry rate)
```bash
# How often the retry fires (each line = a 2nd LLM call for that memo):
grep -c "Structured memo headline rejected" <pod-logs>

# Successful memos with token usage (gauge output-token volume → latency):
grep "Structured memo generated" <pod-logs> | tail -50
# eyeball output_tokens; near 2400 ⇒ hitting the cap ⇒ max latency

# Ratio: retries vs total generations (rough retry rate):
echo "$(grep -c 'Structured memo headline rejected' <pod-logs>) / $(grep -c 'Structured memo generated' <pod-logs>)"

# SDK transparent retries / API errors inflating tails:
grep -E "Structured memo (OpenAI call|retry call) failed" <pod-logs>

# Local-rewrite (both attempts failed = guaranteed 2 calls + degraded memo):
grep "locally rewritten" <pod-logs>

# GET-path total time (only meaningful on locale-mismatch regenerate):
grep "expansion_memo timing: total=" <pod-logs> | sort -t= -k2 -n | tail
```

**Caveat:** there is **no direct LLM-call-duration log** today. To get a true
per-phase breakdown (context-build vs call#1 vs retry vs render vs persist)
you'd need to add `time.monotonic()` brackets around `:2549` and `:2602` — flag
this as a prerequisite if you want hard numbers rather than inference.

### Codespace SQL (no DB access from this investigation — run these yourself)
```sql
-- 1) Confirm a cheap PK lookup for the GET SELECT (index on id):
EXPLAIN ANALYZE
SELECT c.id FROM expansion_candidate c
JOIN expansion_search s ON s.id = c.search_id
WHERE c.id = '<some-candidate-id>';

-- 2) How many candidates actually have a persisted (prewarmed) memo vs not
--    (i.e., how often POST falls to full generation regardless of prewarm):
SELECT
  count(*) FILTER (WHERE decision_memo_json IS NOT NULL) AS has_json,
  count(*) FILTER (WHERE decision_memo IS NOT NULL)      AS has_text,
  count(*)                                               AS total
FROM expansion_candidate;

-- 3) Locale distribution of stored memos (predicts GET-path regenerate cost
--    for Arabic views):
SELECT decision_memo_lang, count(*)
FROM expansion_candidate
GROUP BY decision_memo_lang;
```
> Verify column names against the migration that adds `decision_memo_lang` /
> `decision_memo_json` before running — they're referenced at
> `expansion_advisor.py:10631-10633` and `:1548-1551`, but confirm the actual
> `expansion_candidate` table DDL in `alembic/versions/`.

---

## 5. Candidate remediation directions (NOT implemented)

| Direction | Where | Expected win | Tradeoff / risk |
|---|---|---|---|
| **Stream the completion** (`stream=True` at `:2549`) | service | Cuts *perceived* latency drastically (first tokens in ~1–2s) | Validation (`_parse_and_validate_memo_shape`, headline check) needs the full JSON, so you'd stream to UI but still finish before validating/persisting; more plumbing. Won't reduce total compute. |
| **Reduce `max_tokens`** from 2400 | `config.py:127` | Linear cut in the dominant cost; 1200 ≈ ~½ the generation time | Risk truncating the 10-section memo → shape-validation failure → legacy fallback. Measure typical `output_tokens` first (grep §4). |
| **Suppress / soften the corrective retry** | `:2583-2608` | Removes the up-to-2× doubling | If retry rate is high it's masking a real prompt-conformance problem; better to *fix the prefix brittleness* (normalize/strip headline before `startswith`, or repair locally without a 2nd call) than to drop the safety net. Local rewrite already exists (`_rewrite_headline_locally`) — could skip straight to it instead of a 2nd LLM call. |
| **Shrink the system prompt** (~41 KB) | `_STRUCTURED_MEMO_PREAMBLE` / `_CRITICAL_BLOCK_EN` | Lowers input-token processing + TTFT; also reduces drift that triggers retries | Large effort; risk of regressing memo quality/consistency that the verbose rules enforce. Could move examples to fewer/shorter. |
| **Make POST read the prewarmed memo** (re-enable cache read in `_decision_memo_cache_lookup`) | `:1503` | Turns the user-facing click into a DB read (~ms) when prewarm already ran | This was *intentionally* disabled ("regenerate against live LLM"). Re-enabling means memos can be stale relative to prompt version — but `MEMO_PROMPT_VERSION` already exists to gate that. Strong candidate. |
| **Set explicit client timeout + tune retries** | `_get_client()` `:108` | Caps the 600s tail; fail fast to legacy | Too-low timeout could increase fallback rate to the (cheaper, lower-quality) legacy memo. |
| **Lean harder on prewarm + serve via GET** | UX/flow | If the click went through GET instead of POST, prewarmed candidates would be instant | Requires frontend to call GET `/candidates/{id}/memo` instead of POST `/decision-memo` for prewarmed candidates; and Arabic still pays the one-time regenerate (B8). |

**Recommendation for the eventual patch (for when you decide):** measure retry
rate and typical `output_tokens` first (§4 greps). The highest-leverage,
lowest-risk wins are likely **(a) re-enabling the prewarm cache read on POST
gated by `MEMO_PROMPT_VERSION`** so repeat clicks are instant, and **(b)
streaming** for perceived latency — with **max_tokens reduction** as a follow-up
only after confirming memos don't truncate.

_No code was changed; no branch or commit created during this investigation._
