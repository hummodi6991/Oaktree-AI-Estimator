# PR #4a — READ-ONLY investigation: LLM decision-memo prompt localization

**Repo state:** branch `claude/investigate-memo-localization-EUVyd`, HEAD `9fd9c85a9`
(merge of PR #1238 "Arabic activation"). All citations are live-on-disk.

---

## 0. Headline finding (read this first)

The structured-memo path **already builds an Arabic-aware prompt** — PR #2b/#3
added an Arabic LOCALE addendum at `app/services/llm_decision_memo.py:1801-1810`.
The regression is **not** a missing prompt instruction. It is a **post-processing
validator that is hardcoded English-only** and actively *destroys* the Arabic
output the prompt asks for.

Root cause chain, for `lang=ar`:
1. `render_structured_memo_prompt` appends an addendum telling the model the
   headline must start with the **Arabic** verbs `نوصي / نوصي مع تحفظات / نرفض`
   (`llm_decision_memo.py:1801-1810`).
2. The same system prompt's "CRITICAL OUTPUT FORMAT RULES" block — declared
   *"the most important rules … Other instructions are subordinate"* — still
   mandates the **English** prefixes `Recommend / Recommend with reservations /
   Decline` (`llm_decision_memo.py:1626-1712`, esp. 1630-1638). The two
   instructions contradict each other.
3. After the model returns, `_headline_validity_reason` checks the headline
   against `_ALLOWED_HEADLINE_PREFIXES = ("Recommend with reservations",
   "Recommend", "Decline")` — **English only** (`llm_decision_memo.py:1901-1940`).
4. An Arabic headline (`نوصي …`) fails that prefix check → `generate_structured_memo`
   retries once with an **English-only corrective preamble** that explicitly
   re-orders the model back to `"Recommend"/"Decline"`
   (`llm_decision_memo.py:2263-2280`).
5. The retry's Arabic headline fails again → `_rewrite_headline_locally` prepends
   a **literal English** `"Recommend — "` / `"Decline — "` /
   `"Recommend with reservations — "` and **empties every body field**
   (`ranking_explanation`, `key_evidence`, `risks`, `comparison`, `bottom_line`)
   — `llm_decision_memo.py:1996-2042` + `2331-2357`.

The screenshot — **"Recommend — الإيجار السنوي البالغ 292,000 ريال سعودي يقع في
النسبة المئوية 30% مقارنة بـ 10"** — is the exact signature of step 5:
`_rewrite_headline_locally` does `body = ranking_explanation.strip()[:80]`
(line 2012) then returns `f"Recommend — {body}"` (line 2020). The mid-word
truncation "…مقارنة بـ 10" is the `[:80]` slice. The body fields are then nulled
(2353-2357), which is why the memo card shows only the corrupted headline.

So even though the model *can* and *does* produce Arabic, the English-only
headline gate rejects it on every AR request and the safety-net rewrite stamps
English back on. **PR #4a's fix is in the validator/retry/rewrite layer, not
(only) the prompt text.**

---

## 1. Locate the endpoint

**TL;DR:** `POST /v1/expansion-advisor/decision-memo`, handler
`post_decision_memo` at `app/api/expansion_advisor.py:1560-1561`. It accepts
`lang` in the request body. Call chain: handler → `build_memo_context` →
`generate_structured_memo` → `render_structured_memo_prompt` (structured path),
or → `generate_decision_memo` (legacy fallback).

- Route decorator: `app/api/expansion_advisor.py:1560` — `@router.post("/decision-memo")`.
  The router prefix gives the full path `/v1/expansion-advisor/decision-memo`.
- Handler signature: `app/api/expansion_advisor.py:1561-1564`
  `def post_decision_memo(req: DecisionMemoRequest, db: Session = Depends(get_db))`.
- Request model `DecisionMemoRequest` — `app/api/expansion_advisor.py:1360-1369`:
  ```python
  class DecisionMemoRequest(BaseModel):
      candidate: dict[str, Any]
      brief: dict[str, Any]
      lang: str = "en"
      parcel_id: str | None = None
      search_id: str | None = None
  ```
  `lang` **is** accepted in the body (added pre-PR-#1238). Clamped at
  `expansion_advisor.py:1595`: `lang = req.lang if req.lang in ("en","ar") else "en"`.
- Call chain hops:
  - `expansion_advisor.py:1632-1636` → `build_memo_context(candidate=…, brief=…, lang=lang)`
  - `expansion_advisor.py:1641` → `generate_structured_memo(ctx)`
  - `llm_decision_memo.py:2220` → `render_structured_memo_prompt(ctx)`
  - `expansion_advisor.py:1648` → `render_structured_memo_as_text(memo_json, lang)`
  - Legacy fallback: `expansion_advisor.py:1668-1672` → `generate_decision_memo(candidate=…, brief=…, lang=lang)`

`lang` **does** reach the prompt builder — via `MemoContext.locale`
(`build_memo_context` sets `locale = "ar" if lang == "ar" else "en"` at
`llm_decision_memo.py:984`, stored on the dataclass at `:999`).

---

## 2. The prompt builder

**TL;DR:** Two prompt builders. Structured path: `render_structured_memo_prompt`
(`llm_decision_memo.py:1794-1892`) — builds `[system, user]` messages,
**already has an Arabic addendum** keyed off `ctx.locale == "ar"`. Legacy path:
`generate_decision_memo` picks `_PROMPT_TEMPLATE_AR` vs `_PROMPT_TEMPLATE_EN`
— **fully localized already**. The structured system prompt body
(`STRUCTURED_MEMO_SYSTEM_PROMPT`) is **English-only and never branches on lang**.

### Structured path — `render_structured_memo_prompt` (`llm_decision_memo.py:1794-1892`)

Signature: `def render_structured_memo_prompt(ctx: MemoContext) -> list[dict]`.
Input is a `MemoContext` (no bare `lang` arg — locale rides on `ctx.locale`).
~99 lines; structure breakdown:
- `1800-1810`: builds `addenda` list; **Arabic LOCALE addendum** when `ctx.locale == "ar"`.
- `1811-1818`: realized-demand addendum.
- `1819-1879`: gate-bucket addenda (blocking / advisory / unknown). English text only.
- `1881-1887`: `system_content = STRUCTURED_MEMO_SYSTEM_PROMPT` + `"\n\nSITUATIONAL
  INSTRUCTIONS:\n- " + …addenda`.
- `1889-1892`: returns `[{"role":"system",…}, {"role":"user", "content":
  _serialize_context_for_user_message(ctx)}]`.

Both a **system** prompt (English `STRUCTURED_MEMO_SYSTEM_PROMPT` +
appended situational instructions) and a **user** prompt (JSON-serialized
`MemoContext` via `_serialize_context_for_user_message`,
`llm_decision_memo.py:1738-1791`) are built.

The Arabic addendum verbatim (`llm_decision_memo.py:1801-1810`):
```python
if ctx.locale == "ar":
    addenda.append(
        "LOCALE: Produce every string value in Modern Standard Arabic "
        "(فصحى) — natural, professional Arabic the way a Saudi "
        "real-estate analyst would speak to a restaurant operator. "
        "JSON keys stay in English. Match the directness of the English "
        "voice examples; do not become more formal or hedged just "
        "because you are writing in Arabic. The headline must start "
        "with 'نوصي', 'نوصي مع تحفظات', or 'نرفض'."
    )
```

### Token search across the prompt text

- `Arabic` / `arabic`: `STRUCTURED_MEMO_SYSTEM_PROMPT` line 1393
  (`use display_name_ar in Arabic memos`); addendum lines 1803, 1804, 1808.
- `العربية`: none (the addendum uses `(فصحى)`, not `العربية`).
- `نوصي / نرفض / تحفظات`: addendum line 1809 only.
- `lang` / `language` / `respond in`: no English-vs-Arabic *content* directive
  inside `STRUCTURED_MEMO_SYSTEM_PROMPT` itself — the body is locale-blind. The
  only locale switch is the appended addendum.
- **Conflict:** the body's CRITICAL block (`1626-1712`) hardcodes English
  prefixes and asserts supremacy ("Other instructions are subordinate to
  these", line 1628). The Arabic addendum is appended *after* it under
  "SITUATIONAL INSTRUCTIONS" — i.e. the prompt explicitly tells the model the
  English-prefix rule outranks the Arabic-prefix instruction.

### Legacy path — `generate_decision_memo` (`llm_decision_memo.py:239-`)

`template = _PROMPT_TEMPLATE_AR if lang == "ar" else _PROMPT_TEMPLATE_EN`
(`llm_decision_memo.py:261`). `_PROMPT_TEMPLATE_AR` (`:188-228`) is a full
Arabic template; its headline field is free-form
(`"<≤15 كلمة: حكم انطلق/تأمّل/احذر …>"`, line 216) with **no English-prefix
validation downstream**. The legacy path is already correct for Arabic.

---

## 3. Model call

**TL;DR:** OpenAI-style `client.chat.completions.create`. Model =
`settings.EXPANSION_MEMO_MODEL`, default **`gpt-4o-mini`**. `system` and `user`
are separate message roles. No `tools`. `temperature=0.3`, `max_tokens=2400`,
`response_format={"type":"json_object"}`. No `stop_sequences`.

- Structured call: `llm_decision_memo.py:2222-2229`:
  ```python
  response = client.chat.completions.create(
      model=settings.EXPANSION_MEMO_MODEL,
      messages=messages,
      temperature=settings.EXPANSION_MEMO_TEMPERATURE,
      max_tokens=settings.EXPANSION_MEMO_MAX_TOKENS,
      response_format={"type": "json_object"},
  )
  ```
  Retry call is identical shape — `llm_decision_memo.py:2283-2289`.
- Model id config (`app/core/config.py:126-134`):
  - `EXPANSION_MEMO_MODEL = os.getenv("EXPANSION_MEMO_MODEL", "gpt-4o-mini")`
  - `EXPANSION_MEMO_MAX_TOKENS = 2400`
  - `EXPANSION_MEMO_TEMPERATURE = 0.3`
  - `EXPANSION_MEMO_STRUCTURED_ENABLED` default `true`.
- Legacy path uses a *different* model constant — `MODEL_ID =
  os.environ.get("DECISION_MEMO_MODEL", "gpt-4o-mini-2024-07-18")`
  (`llm_decision_memo.py:45`), called at `:329`.
- `system` vs `user`: separate — `messages[0]` is `{"role":"system",…}`,
  `messages[1]` is `{"role":"user",…}` (`llm_decision_memo.py:1889-1892`).
- `tools`: **none** — no `tools=` parameter on either call. Output is
  constrained only by `response_format` JSON mode.
- `stop_sequences`: none. `temperature=0.3` (low, but not 0 — minor
  determinism variance). No language-affecting params beyond the prompt itself.

The vendor is OpenAI-compatible (`response_format={"type":"json_object"}`,
`response.choices[0].message.content`, `usage.prompt_tokens`). Both OpenAI and
Anthropic accept Arabic in `system`/`user` content, so an Arabic addendum is
not a vendor limitation.

---

## 4. Response post-processing

**TL;DR:** This is the regression locus. After the model returns, the response
goes through `_parse_and_validate_memo_shape` (structure-only, locale-safe) and
then `_headline_validity_reason` / retry / `_rewrite_headline_locally` — the
**last three are English-only** and miscategorize/destroy Arabic headlines.

Flow inside `generate_structured_memo` (`llm_decision_memo.py:2178-2367`):
1. `_parse_and_validate_memo_shape` (`:2237`, defn `:2093-2157`) — JSON parse +
   required-key + advisory-section checks. **Locale-agnostic** — safe. Checks
   only that prose fields are non-empty strings, not their language.
2. `_headline_validity_reason` (`:2243-2250`, defn `:1908-1993`) — **English-only**.
   Line 1937-1940:
   ```python
   if not any(
       lowered.startswith(prefix.lower()) for prefix in _ALLOWED_HEADLINE_PREFIXES
   ):
       return f"headline does not start with an allowed prefix: {stripped[:60]!r}"
   ```
   `_ALLOWED_HEADLINE_PREFIXES = ("Recommend with reservations","Recommend",
   "Decline")` (`:1901-1905`). It also keyword-matches English substrings
   `"failed "`, `"fails on"` (`:1990`) and uses `lowered.startswith("decline")`
   / `"recommend"` (`:1942-1948`) for the consistency guards — all English.
3. Retry (`:2256-2295`) — corrective preamble at `:2266-2277` is **English** and
   re-orders the model to `"Recommend"/"Recommend with reservations"/"Decline"`,
   directly fighting the Arabic addendum.
4. `_rewrite_headline_locally` (`:2331-2347`, defn `:1996-2042`) — prepends
   literal English `"Recommend — "` / `"Decline — "` / `"Recommend with
   reservations — "`. Then `:2353-2357` empties `ranking_explanation`,
   `key_evidence`, `risks`, `comparison`, `bottom_line`.

The text renderer `render_structured_memo_as_text` (`:2490-2530`) **is**
localized — `_TEXT_SECTION_HEADERS_AR` vs `_EN` (`:2373-2396`) — so the
`memo_text` headers come out Arabic when `lang=ar`. The corruption is purely in
the headline validator/retry/rewrite, not the renderer.

Does post-processing assume English? Yes — specifically:
- `_ALLOWED_HEADLINE_PREFIXES` (English literals).
- `_headline_validity_reason` substring checks `"failed "`, `"fails on"`,
  `.startswith("decline"/"recommend")`.
- `_rewrite_headline_locally` returns English-literal prefixes.
- the retry preamble text.

---

## 5. Response shape

**TL;DR:** Shape does **not** change with locale — it is prose substitution
inside a fixed JSON schema. `memo_json` is a structured object; section keys
stay English; only the string *values* localize. Frontend `GeneratedDecisionMemo`
expects exactly `{memo, memo_text, memo_json}`.

Endpoint response (`post_decision_memo`, `expansion_advisor.py:1657-1690`) — no
Pydantic `response_model` declared on the route; it returns a plain dict:
```python
{
  "memo":       <legacy-shape dict>,   # back-compat shim
  "memo_text":  <str | None>,          # rendered plain text
  "memo_json":  <structured dict | None>,
  "cached":     <bool>,
}
```
- `memo` legacy shape (`_structured_to_legacy_shape`, `:1437-1447`): keys
  `headline, fit_summary, top_reasons_to_pursue, top_risks,
  recommended_next_action, rent_context`.
- `memo_json` structured shape — ten keys, validated by `_STRUCTURED_REQUIRED_KEYS`
  (`llm_decision_memo.py:2045-2056`): `headline_recommendation,
  ranking_explanation, key_evidence, risks, comparison, bottom_line,
  property_overview, financial_framing, market_context, competitive_landscape`.
  These **keys stay English in every locale** (the addendum says so:
  "JSON keys stay in English", `:1806`). Only string *values* localize.
- It is **both** a `memo_text` string **and** a structured `memo_json` object —
  not either/or. Each structured section has its own field, but localization is
  value-level prose, not per-section locale tagging.

Frontend consumer type — `GeneratedDecisionMemo`,
`frontend/src/lib/api/expansionAdvisor.ts:876-880`:
```ts
export interface GeneratedDecisionMemo {
  memo: LLMDecisionMemo;
  memo_text: string | null;
  memo_json: StructuredMemo | null;
}
```
`StructuredMemo` keys mirror the backend's English schema keys. No
language-tagged fields. **PR #4a is prose substitution; no response-shape
change, no frontend type change needed.**

---

## 6. Backend caching

**TL;DR:** Read cache is **disabled** (`_decision_memo_cache_lookup` always
returns `None`), so `POST /decision-memo` always regenerates — no stale-English
risk *on that endpoint*. **But** the memo is **persisted** to
`expansion_candidate.decision_memo_json` on every call and **`GET
/candidates/{id}/memo` serves that column verbatim** — so a candidate
pre-warmed in English (or last POSTed in English) returns **stale English** via
the GET endpoint until something re-POSTs it in Arabic.

- `_decision_memo_cache_lookup` (`expansion_advisor.py:1496-1511`) — `del db,
  search_id, parcel_id; return None`. Hard-disabled. So `post_decision_memo`'s
  step-1 cache branch (`:1606-1625`) is dead; every call regenerates.
- `_decision_memo_cache_write` (`expansion_advisor.py:1514-1557`) — runs on
  every keyed call (`:1685-1688`). `UPDATE … SET decision_memo = :txt,
  decision_memo_json = … WHERE search_id=:sid AND parcel_id=:pid` — **overwrites
  unconditionally on every call**, no first-call guard.
- Pre-warm (`_prewarm_decision_memos` / `_process_one`,
  `expansion_advisor.py:699-798`) writes memos for the top-N candidates on
  `POST /searches`, **hardcoded `lang="en"`** (`:776-777` and `:789`
  `render_structured_memo_as_text(memo_json, "en")`). Comment at `:775-776`:
  *"Hardcoded 'en' until heuristic strings have Arabic parity (PR #3)."*
- `GET /candidates/{id}/memo` → `get_candidate_memo`
  (`expansion_advisor.py:10433-10665`) reads `c.decision_memo`,
  `c.decision_memo_json` (`:10506-10507`) and returns them **as-is** at
  `:10663-10664`. It accepts `lang` but only threads it to
  `_normalize_candidate_payload` — **it never re-generates the memo in `lang`.**
- Migrations: `decision_memo` (TEXT), `decision_memo_json` (JSONB) added by
  `alembic/versions/20260414_memo_json.py`; `decision_memo_prompt_version` (TEXT)
  by `20260425_memo_prompt_version.py`. **There is no locale/`decision_memo_lang`
  column.** Nothing records which locale a persisted memo is in.

**Rollout consequence:** post-fix, the FIRST AR `POST /decision-memo` for a
candidate persists Arabic and overwrites the English row. But `GET
/candidates/{id}/memo` on a candidate that was only ever pre-warmed (English,
hardcoded) returns English Arabic-side until a POST happens. The
`decision_memo_prompt_version` column won't help — it tracks `MEMO_PROMPT_VERSION`
(`"v8-rating-velocity-2026-05"`, `:52`), not locale.

---

## 7. Test surface

**TL;DR:** ~40 memo tests in `tests/test_llm_decision_memo.py`, plus
`tests/services/test_llm_decision_memo_grounding.py`. **Every structured-memo
test uses English LLM responses and asserts `headline.lower().startswith(
"recommend"/"decline")`.** Model responses are mocked via `MagicMock`. No test
exercises an Arabic structured headline through the validator/retry/rewrite —
the one Arabic structured test only checks the system prompt *contains* the
addendum.

Test files touching the memo endpoint / generator:
- `tests/test_llm_decision_memo.py` — primary suite (~2170 lines): legacy
  generator, `build_memo_context`, advisory sections, `generate_structured_memo`,
  `render_structured_memo_prompt`, endpoint integration, headline retry/rewrite.
- `tests/services/test_llm_decision_memo_grounding.py` — competitor-economics
  grounding / fabrication checks; also a `test_live_llm_*` (real-API,
  parametrized) and a mocked grounded-response test.
- `tests/test_sample_regression_memos.py` + `scripts/sample_regression_memos.py`
  — regression-sampling harness.
- `tests/test_prewarm_concurrency.py`, `tests/test_expansion_rerank.py`,
  `tests/test_expansion_advisor_phase3_chunk1.py` — touch the pre-warm /
  surrounding paths.
- Frontend: `ExpansionMemoPanel.test.tsx`,
  `DecisionMemoNarrative.structured.test.tsx`,
  `expansionAdvisor.normalizeMemoResponse.test.ts`,
  `expansionAdvisor.generateDecisionMemo.test.ts`, `expansionAdvisor.lang.test.ts`.

English-specific prose assertions that **will need updating** if the prompt /
validator now produces Arabic for AR requests:
- `test_llm_decision_memo.py:2053` — `memo["headline_recommendation"].lower()
  .startswith("recommend")` (TestHeadlineLocalRewriteNullsBody).
- `:2082` — same assert in `test_happy_path_retry_keeps_body`.
- `:2110` — `memo["headline_recommendation"].lower().startswith("decline")`.
- `:2136` — `.startswith("recommend")`.
- TestHeadlineRetry* classes (`:1946`, `:1977`, `:2002`) — all feed English
  headlines (`_memo_with_headline(...)`) and assert English outcomes.
- `:914` — `body["memo_text"].startswith("## Headline Recommendation")` — English
  section header; would be `## التوصية الرئيسية` for an AR call.
- These assertions are all keyed to **`lang="en"` test inputs**, so they stay
  valid for the EN path. New AR-path tests are *additive*; only tests that mix
  `lang="ar"` with English-prefix assertions would need changing — currently
  **none exist**, so the realistic test surface is *new tests*, not edits.

Mocking: yes — `_make_mock_response` (`test_llm_decision_memo.py:80-87`) builds
a `MagicMock` ChatCompletion; `@patch("app.services.llm_decision_memo._get_client")`
injects it. The mock content is whatever the test passes (English dicts today).
**An AR-path test needs an Arabic mock response** — content can be any
JSON-valid dict with Arabic string values; it does **not** need to be
grammatically perfect Arabic, just Arabic-script strings that exercise the
headline-prefix branch (e.g. `headline_recommendation` starting with `نوصي`).
`test_live_llm_*` in the grounding file does hit a real API at runtime (network
gated). The Arabic structured test today — `TestRenderPromptArabicLocale`
(`:785-798`) — only asserts `"Modern Standard Arabic" in system_content`; it
does **not** run `generate_structured_memo`.

CI gate: `make test` runs pytest (per `CLAUDE.md`); `.github/workflows/` runs
the suite on PR. These tests are in-suite, so they gate.

---

## 8. Frontend consumer behavior

**TL;DR:** PR #1238 **did** plumb `lang` into `generateDecisionMemo`. The
"Recommend" header in the screenshot comes from the **backend response**
(`memo.headline_recommendation`), rendered verbatim — it is **not** a frontend
`t()` key. Section labels ("Key Evidence", "Risks to Watch") *are* i18next keys.
So the fix is backend, confirmed.

- `generateDecisionMemo` — `frontend/src/lib/api/expansionAdvisor.ts:882-902`.
  Signature `(candidate, brief, lang = currentLang())`; body
  `JSON.stringify({ candidate, brief, lang })` (`:890`). `lang` **is** sent.
  Body shape `{candidate, brief, lang}` matches `DecisionMemoRequest`
  exactly (see §9). `currentLang()` (`:5-14`) maps `i18n.language` → `"ar"`/`"en"`.
- `DecisionMemoNarrative.tsx` — `StructuredNarrative` renders the headline at
  `frontend/src/features/expansion-advisor/DecisionMemoNarrative.tsx:117`:
  ```tsx
  <h3 className="ea-memo-structured__headline">{memo.headline_recommendation}</h3>
  ```
  **Verbatim from the backend response.** "Recommend" in the screenshot is
  therefore the LLM/backend payload, not a frontend string.
- Section headers ("Key Evidence" / "Risks to Watch" / "How It Compares" /
  "Bottom Line") **are** i18next keys —
  `t("expansionAdvisor.keyEvidence")` etc. (`DecisionMemoNarrative.tsx:127,
  157, 183, 192, 115`). Those localize correctly via `ar.json` and are not in
  scope. Body prose (`ranking_explanation`, `comparison`, `bottom_line`,
  evidence `signal`/`value`/`implication`, `risk`/`mitigation`) is all rendered
  verbatim from the response (`:121, 135, 146, 167, 169, 185, 194`).
- Locale-keyed module cache: `memoModuleCache` keyed by
  `(candidateId, lang)` (`DecisionMemoNarrative.tsx:16-27`) — EN and AR memos
  coexist client-side; a locale toggle won't serve a stale-locale memo from the
  *frontend* cache. (The *backend* DB column staleness in §6 is unrelated.)
- Other derived fields: the numeric/`isNumericValue` styling on evidence values
  (`:137-141`) keys off value format, not language — Arabic-Indic vs ASCII
  digits could affect the "numeric" CSS class, minor cosmetic only. No
  miscategorization that changes meaning.

---

## 9. Cross-reference: PR #1238 actually sends `lang=ar`?

**TL;DR:** Yes. `generateDecisionMemo` sends `lang` in the POST body; the body
shape matches `DecisionMemoRequest`. No frontend drift — the request *does*
carry `lang=ar`. The bug is entirely backend.

`frontend/src/lib/api/expansionAdvisor.ts:887-891`:
```ts
const res = await fetchWithAuth(buildApiUrl("/v1/expansion-advisor/decision-memo"), {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ candidate, brief, lang }),
});
```
- `lang` present in body. ✔
- Body keys `{candidate, brief, lang}` ⊆ `DecisionMemoRequest`
  (`candidate, brief, lang, parcel_id?, search_id?` — `expansion_advisor.py:1360-1369`).
  `parcel_id`/`search_id` optional and omitted — endpoint then runs the
  unkeyed generate-fresh path (no persistence). ✔
- `lang` default is `currentLang()`, derived from `i18n.language` (`:5-14`). When
  the UI is Arabic, `currentLang()` returns `"ar"`. ✔

No drift. The request carries `lang=ar` correctly; the backend prompt path
*receives* it (`ctx.locale == "ar"`) and the addendum *fires*. The failure is
the English-only validator downstream, not request plumbing.

---

## 10. Other LLM endpoints (queued for follow-up, NOT PR #4a)

**TL;DR:** Three other LLM surfaces in/around Expansion Advisor lack `lang`
plumbing. None is in PR #4a scope; logged here for the queue.

1. **Legacy memo fallback** — `generate_decision_memo`
   (`llm_decision_memo.py:239`), called from `post_decision_memo`
   (`expansion_advisor.py:1668`). Already localized (`_PROMPT_TEMPLATE_AR`),
   **but** it embeds `{llm_reasoning}` (`_PROMPT_TEMPLATE_AR:212`) which is the
   English `llm_suitability` output (see #3 below) — so even Arabic legacy
   memos carry an English clause. Minor; same path, worth noting.
2. **LLM rerank** — `app/services/expansion_rerank.py:781` builds
   `RERANK_SYSTEM_PROMPT` (`:775`) and produces `rerank_reason` prose persisted
   on `expansion_candidate.rerank_reason`. **No `lang` plumbed** → English
   reason text. Surfaces in `DecisionLogicCard`-style UI. Gated off by default
   (`EXPANSION_LLM_RERANK_ENABLED=False`), so low urgency. Route: indirect —
   runs inside `create_expansion_search`. Prompt builder:
   `_serialize_shortlist_for_prompt` + `RERANK_SYSTEM_PROMPT`.
3. **LLM listing suitability** — `app/services/llm_suitability.py:207,244`
   (`_classify_text_only` / `_classify_with_photos`), `_SYSTEM_PROMPT` is
   English; `classify_listing` (`:265`) runs at **ingestion time**, no `lang`
   concept. Output `llm_suitability_verdict` + reasoning feeds the memo prompt's
   `{llm_reasoning}`. Localizing this is an ingestion-pipeline change — large
   follow-up.

`compare_candidates` (`expansion_advisor.py:10209`) is **deterministic**, not
LLM-backed — no action. No separate "memo retry endpoint", "comparison
narrative endpoint", or `verify_memo` endpoint exists — the only memo
"verifier" is the in-process `_headline_validity_reason` (§4).

---

## 11. The seven-rule discipline for PR #4a

1. **English byte-identical** — *constraint holds, with care.* For
   `lang=en|omitted`, `ctx.locale == "en"`, the Arabic addendum at `:1801` does
   not fire, and `_ALLOWED_HEADLINE_PREFIXES` etc. are unchanged for EN. **Any
   PR #4a change must branch the validator on `ctx.locale`** and leave the
   `locale=="en"` path executing the exact current code, so EN memo prose is
   byte-identical. The risk: if PR #4a edits `_headline_validity_reason` /
   `_rewrite_headline_locally` signatures, it must thread locale without
   perturbing EN behavior.
2. **`_normalize_candidate_payload(lang=…)` byte-identical** — **N/A.** The memo
   path does not pass memo prose through `_normalize_candidate_payload`.
   (`get_candidate_memo` calls it for candidate fields, not memo text.)
3. **Migrations** — *possibly needed.* See §6 / §12: a `decision_memo_lang`
   column would let `GET /candidates/{id}/memo` know the persisted memo's locale
   (and would let pre-warm / cache logic avoid serving a stale-locale memo).
   Without it, PR #4a accepts that pre-fix English persisted memos stay English
   on the GET path until re-POSTed. Additive column → low-risk migration if Ahmed
   wants it.
4. **No backfill** — *holds.* Even if a `decision_memo_lang` column is added,
   existing rows can default `NULL`/`'en'`; no backfill of memo *content* is
   needed (regeneration is on-demand).
5. **`_humanize_gate_list` en/omitted byte-identical** — **N/A** to the memo
   prompt path. (Gate humanization for the memo runs via `_build_gate_buckets`
   with `lang`, already PR #2b territory; PR #4a should not touch it.)
6. **Gate split** — **N/A.** The blocking/advisory gate split
   (`_hard_fail_gate_keys`, `_blocking_failed_from_buckets`) is locale-invariant
   by design (keyed on raw gate keys) and PR #4a need not touch it.
7. **No frontend / no handler-signature / no `_prewarm` / no LLM prompt-text
   changes** — **this is the rule PR #4a relaxes, by design.** PR #4a *is* an
   LLM-prompt-text + post-processor change for the AR path. Discipline:
   - EN path stays byte-identical (rule #1).
   - Handler signature `post_decision_memo(req, db)` need not change — `lang` is
     already in `DecisionMemoRequest`.
   - Frontend need not change — `generateDecisionMemo` already sends `lang`
     (§9), `headline_recommendation` already rendered verbatim (§8).
   - `_prewarm` is hardcoded `lang="en"` (`:776-777`); PR #4a can **leave it**
     (English pre-warm is acceptable; AR users re-POST) — but be aware it means
     the GET endpoint serves English until a POST. Touching `_prewarm` to warm
     AR too is optional scope.

**Hidden conflict to flag:** the structured system prompt's CRITICAL block
(`:1626-1712`) asserts *"Other instructions are subordinate to these"* and
hardcodes English prefixes. The Arabic addendum is appended *after* it as a
"SITUATIONAL INSTRUCTION" — i.e. the prompt currently tells the model the
English-prefix rule **outranks** the Arabic-prefix instruction. A prompt-only
fix that just strengthens the addendum will still be undercut both by this
precedence statement *and* by the English-only validator. PR #4a must address
the validator (`_ALLOWED_HEADLINE_PREFIXES` / `_headline_validity_reason` /
`_rewrite_headline_locally` / retry preamble) **and** reconcile the CRITICAL
block's English-prefix mandate with the AR locale — they cannot be fixed
independently.

---

## 12. Risks and open questions (decide before drafting the patch prompt)

- **Q-cache / migration:** Do you add `decision_memo_lang` to
  `expansion_candidate`? Without it, `GET /candidates/{id}/memo` cannot tell an
  English persisted memo from an Arabic one and will serve whatever was last
  written (pre-warm writes English). Options: (a) add the column + have GET
  regenerate when `lang` ≠ stored locale; (b) accept stale-English on GET until
  a POST overwrites; (c) make pre-warm locale-aware. PR #4a-minimal = (b),
  documented as a rollout caveat.
- **Headline-prefix canon:** Should the AR headline canon be the Arabic verbs
  `نوصي / نوصي مع تحفظات / نرفض` (addendum already says so), and the validator
  gain an `_ALLOWED_HEADLINE_PREFIXES_AR`? Or keep `headline_recommendation`
  English-prefixed and localize only the prose tail? The frontend renders the
  field verbatim (§8), so an English prefix on an Arabic memo is exactly the
  screenshot bug — the canon must be Arabic for AR. This forces parallel
  changes in `_headline_validity_reason`, `_rewrite_headline_locally`, the
  retry preamble, and the `.startswith("decline"/"recommend")` consistency
  guards (`:1942-1948, 1990`).
- **Test mocks:** AR-path tests need an Arabic mock LLM response. Mock content
  lives inline in `tests/test_llm_decision_memo.py` (`VALID_STRUCTURED_RESPONSE`
  ~`:268-355`, `_memo_with_headline` helper). New Arabic fixtures need
  Arabic-script string values; they need **not** be grammatically perfect —
  just enough to exercise the prefix branch (`headline_recommendation`
  starting with `نوصي`/`نرفض`).
- **Vendor:** Both OpenAI (`gpt-4o-mini`, the live model) and Anthropic accept
  Arabic in `system` and `user` content — no vendor blocker to an Arabic
  system prompt or addendum. Flagged for completeness only.
- **Structured section keys:** keep JSON keys English (addendum `:1806` already
  mandates this; `StructuredMemo` TS type and `_STRUCTURED_REQUIRED_KEYS` both
  assume English keys). Only *values* localize. No schema change.
- **Mixed AR+EN prose / BiDi:** the model may emit Arabic with embedded English
  tokens (brand names "Burger King", "SAR", `/100`). Frontend renders headline
  in `<h3>` with `dir={dir}` set from `lang` on the wrapper
  (`DecisionMemoNarrative.tsx:112`); embedded LTR runs inside an RTL block are
  generally safe but long mixed runs weren't stress-tested by PR #1238's
  manual checklist. Worth a manual smoke item, not a code change.
- **Content-safety / hallucination post-validator:** the only memo
  post-validator is `_headline_validity_reason` + `_advisory_section_invalid_reason`
  + the grounding assertions in `test_llm_decision_memo_grounding.py`. The
  grounding *tests* match English fabrication phrases ("typical AOV", "Dunkin
  rent") — they run against `lang="en"` fixtures only, so they won't false-flag
  Arabic. But `_advisory_section_invalid_reason` (`:2070-2090`) only checks
  *non-empty string* — locale-safe. No English-heuristic validator would
  false-reject Arabic **except** the headline-prefix gate (the bug).

---

## 13. The two-path question — (a) ask-in-Arabic vs (b) generate-EN-then-translate

**TL;DR:** Approach **(a) single Arabic prompt** is the clear structural fit and
is *already* the implemented design — the addendum at `:1801` is approach (a).
Approach (b) would require an extra model call and would not survive the
existing English-only validator any better. Evidence below; final call Ahmed's,
but (a) has a strong lead.

Evidence for (a):
- The prompt is **retrieval-augmented**: the user message is a JSON
  `MemoContext` (`_serialize_context_for_user_message`, `:1738-1791`) — score
  breakdowns, gate buckets, typed `advisory_sections`, competitors. All grounding
  data is structured input, *not* English prose to translate. There is no
  English narrative artifact to feed a translation pass.
- The model already produces Arabic when asked (the addendum fires; the
  screenshot's Arabic `ranking_explanation` proves the model *did* emit Arabic
  — it was the validator that stamped English back).
- Few-shot examples: the system prompt has extensive **English** voice examples
  (Examples C/D/E/F, `:1467-1599`). These anchor *tone*, not output language;
  the addendum already says "Match the directness of the English voice
  examples … do not become more formal … because you are writing in Arabic"
  (`:1804-1808`). Approach (a) keeps the English few-shots as tone anchors and
  asks for Arabic output — already the design.
- Cost: (a) is one call (plus the existing retry); (b) doubles calls and the
  $/day ceiling (`_check_daily_ceiling`, `:74`, $5/day cap) covers both — (b)
  roughly doubles AR memo cost.

Why (b) does not help: the regression is the **validator**, not output quality.
A translate-pass would still hand an Arabic headline to the same English-only
`_headline_validity_reason`. (b) solves nothing PR #4a needs and adds cost.

The real PR #4a work, under either approach, is making the
validator/retry/rewrite locale-aware.

---

## Recommended PR shape

**Single most likely root cause:** the structured-memo headline
validator/retry/rewrite layer is English-only and incompatible with the Arabic
LOCALE addendum. For `lang=ar`, an Arabic headline (`نوصي …`) fails
`_headline_validity_reason`'s `_ALLOWED_HEADLINE_PREFIXES` check
(`llm_decision_memo.py:1901-1940`), triggers an English-only retry
(`:2263-2280`), fails again, and `_rewrite_headline_locally` (`:1996-2042`,
applied `:2331-2357`) stamps a literal English `"Recommend — "` onto the
truncated Arabic `ranking_explanation` and **nulls every body field** — exactly
the screenshot. Secondarily, the system prompt's CRITICAL block (`:1626-1712`)
asserts the English-prefix rule outranks the addendum, so a prompt-only fix is
insufficient.

**Minimum file count / line count to fix:** essentially **one file** —
`app/services/llm_decision_memo.py`. Changes cluster in:
- the headline-prefix canon — add an Arabic prefix set / make
  `_ALLOWED_HEADLINE_PREFIXES` + `_headline_validity_reason` locale-aware
  (it needs `ctx.locale`/`lang` passed in; currently it gets only the raw
  headline string);
- the consistency guards in `_headline_validity_reason` that `.startswith
  ("decline"/"recommend")` and substring-match `"fails on"` (`:1942-1991`);
- `_rewrite_headline_locally` — emit Arabic prefixes for AR;
- the retry corrective preamble (`:2266-2277`) — Arabic for AR;
- reconcile the CRITICAL block (`:1626-1712`) with the AR locale (either make
  the block locale-aware or have the addendum explicitly override the
  English-prefix mandate for AR).
Realistic size: ~60-120 lines, one file. No endpoint, no frontend, no
`build_memo_context` change. EN path must stay byte-identical (branch on
`ctx.locale == "ar"`).

**Migration needed?** Not strictly for PR #4a-minimal. A `decision_memo_lang`
column on `expansion_candidate` is *optional* and only matters for `GET
/candidates/{id}/memo` stale-locale correctness (§6). Recommend deciding
explicitly: ship PR #4a without it and document that GET serves the
last-persisted locale until a re-POST, OR add the additive column. Ahmed's call.

**(a)/(b) choice:** (a) — single Arabic prompt — has a clear lead and is the
already-implemented design; (b) adds a model call and cost without solving the
validator bug. Recommend (a); flag for Ahmed only as a sign-off, not an open
design question.

**Estimated test surface:** mostly **new tests**, not edits. Existing
structured-memo tests all use `lang="en"` inputs and stay valid. Add: an AR
`generate_structured_memo` happy-path test (Arabic mock response, headline
`نوصي …`, asserts it survives validation un-rewritten), an AR retry/rewrite
test, and an AR endpoint test asserting `memo_json.headline_recommendation`
stays Arabic. Arabic mock fixtures go inline in `tests/test_llm_decision_memo.py`
alongside `VALID_STRUCTURED_RESPONSE`; they need Arabic-script values, not
perfect grammar. ~4-8 new test cases. The existing
`TestRenderPromptArabicLocale` (`:785-798`) could be extended but currently only
checks the addendum is present.

**Other LLM endpoints to queue (NOT PR #4a):** (1) LLM rerank
`rerank_reason` prose — `expansion_rerank.py`, no `lang`, gated off by default;
(2) `llm_suitability.classify_listing` — `llm_suitability.py`, ingestion-time
English, its output leaks into the legacy memo via `{llm_reasoning}`;
(3) localizing pre-warm so `GET /candidates/{id}/memo` returns Arabic for
pre-warmed candidates (`_prewarm_decision_memos` hardcodes `lang="en"`).
