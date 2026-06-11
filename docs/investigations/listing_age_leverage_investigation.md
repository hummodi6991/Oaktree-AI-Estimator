# Investigation — Listing-age negotiating-leverage line in the decision memo

**Scope:** READ-ONLY mapping. No prompt copy authored here. Goal: determine
what the memo needs to add a HEDGED, rent-zone-aware tenant-side
negotiating-leverage point to RISKS-TO-WATCH when a listing has been vacant
**long relative to peers**, reframing (not replacing) the current
listing-age staleness line.

---

## ONE-SCREEN SUMMARY (lead = answer to B)

**Does a relative-age signal exist? → NO. It must be computed.** This forks
the patch into **prompt + backend rider**, not prompt-only.

- The memo payload carries only **absolute** listing age:
  `feature_snapshot.listing_age = {effective_age_days, source, created_days,
  updated_days}` (built `app/services/expansion_advisor.py:9789-9794`),
  surfaced to the LLM as the scalar `property_overview.listing_age_days`
  (`app/services/llm_decision_memo.py:1263`). There is **no** district/peer
  age distribution, no median/percentile of listing age, and no "stale vs
  peers" flag anywhere in the snapshot.
- The current "Listing has been live for N days…" risk line is **not
  templated** — it is LLM-authored, driven by the data-dictionary rule at
  `llm_decision_memo.py:1536`, the risks rule at `:1569`, and the few-shot
  examples C/D/F (`:1629`, `:1663`, `:1704`). Those examples already
  editorialize relativity ("longer than is typical for prime corner units in
  this district") **with no data backing it** — a hedge the model invents.
- **Recommended path = Option (a):** compute a listing-age percentile at
  snapshot-build time from the **same comparable set** the rent percentile
  already uses (`_percentile_rent_burden`,
  `app/services/expansion_advisor.py:4808`). That comparable query runs over
  `commercial_unit`, which already has **indexed** `aqar_created_at` /
  `aqar_updated_at` columns (`app/models/tables.py:443-444, 472-478`). Add one
  `SUM(CASE WHEN …)` to the existing aggregate, persist **one scalar**, and
  surface it the same way `comparable_*` already flows
  (`expansion_advisor.py:10257-10270`). This mirrors exactly how rent
  percentile solved the identical "relative to district comparables" problem.
- **Rent-zone awareness (C) is available in-scope.** The
  `_rent_positioning` zone (low/mid/high) is already computed into
  `financial_framing.rent_positioning` (`llm_decision_memo.py:1300`, function
  at `:1083`) — the same `advisory_sections` payload the LLM reads when it
  writes `risks`. A rule can condition leverage wording on the zone.
- **Version/fixtures (D):** prompt-rule change ⇒ bump `MEMO_PROMPT_VERSION`
  (`llm_decision_memo.py:53`), regenerate the byte-identity fixture
  `tests/data/pr4a_structured_memo_system_prompt_en_head.txt`, and update the
  version-pin test (`tests/test_llm_decision_memo.py:2769`). Option (a) adds
  one snapshot scalar — **backend rider, flagged below**.

### Discrepancy flag (read before planning)
The task brief locates the rent zone "in PR-E." It is **not** in
`expansion_advisor.py` — `_rent_positioning` and the zone object live in the
**memo builder** `app/services/llm_decision_memo.py:1083` and are assembled
into the payload at `:1300`. Any rule conditioning on rent zone references
the memo-side object, not a PR-E backend field.

---

## psql commands for Ahmed (run BEFORE committing to Option a)

**Q1 — Age-column coverage (the key feasibility gate for Option a).**
`aqar_created_at` / `aqar_updated_at` are detail-scrape fields, "nullable
because existing rows and newly discovered list-page rows predate the
detail-scrape step" (`app/models/tables.py:441-442`). If sparse, the
age-percentile comparable set is too thin to be trustworthy.

```sql
SELECT count(*) AS total, count(*) FILTER (WHERE status='active' AND restaurant_suitable) AS active_suitable, count(aqar_updated_at) FILTER (WHERE status='active' AND restaurant_suitable) AS active_suitable_has_updated, count(aqar_created_at) FILTER (WHERE status='active' AND restaurant_suitable) AS active_suitable_has_created, count(first_seen_at) FILTER (WHERE status='active' AND restaurant_suitable) AS active_suitable_has_first_seen FROM commercial_unit;
```

**Q2 — Citywide listing-age distribution (anchors Option c fallback ONLY if
Option a is ruled out by Q1).** Uses the same COALESCE precedence as
`_effective_listing_age_days` (`expansion_advisor.py:2890-2918`).

```sql
SELECT percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(DAY FROM now() - COALESCE(aqar_updated_at, aqar_created_at, first_seen_at))) AS p50_days, percentile_cont(0.75) WITHIN GROUP (ORDER BY EXTRACT(DAY FROM now() - COALESCE(aqar_updated_at, aqar_created_at, first_seen_at))) AS p75_days, percentile_cont(0.90) WITHIN GROUP (ORDER BY EXTRACT(DAY FROM now() - COALESCE(aqar_updated_at, aqar_created_at, first_seen_at))) AS p90_days FROM commercial_unit WHERE restaurant_suitable = true AND status = 'active' AND COALESCE(aqar_updated_at, aqar_created_at, first_seen_at) IS NOT NULL;
```

---

## A. Listing-age signal

### A1 — Where the current risk line comes from

**Payload field & build site.** The snapshot field is
`feature_snapshot_json["listing_age"]`, assembled in the candidate loop:

- `app/services/expansion_advisor.py:9789-9794`
  ```python
  feature_snapshot_json["listing_age"] = {
      "effective_age_days": effective_age_days,
      "source": effective_age_source,
      "created_days": _created_days,
      "updated_days": _updated_days,
  }
  ```
  `created_days` / `updated_days` are computed by `_raw_age_days`
  (`:9773-9787`) from `unit_aqar_created_at` / `unit_aqar_updated_at`;
  `effective_age_days` is the GREATEST-of-three from
  `_effective_listing_age_days` (`:2885-2922`).

**Typed field the memo sees.** `_build_advisory_sections` reads the dict and
exposes one scalar:

- `app/services/llm_decision_memo.py:1244` — `listing_age = snapshot.get("listing_age")…`
- `app/services/llm_decision_memo.py:1263` — `"listing_age_days": _safe_int(listing_age.get("created_days"))`
  (i.e. the memo's `property_overview.listing_age_days` is **created_days**,
  not effective/updated).

**Prompt rule(s) that emit the risk line.** There is **no hard template** —
the line is LLM-generated, steered by three things:

1. Data-dictionary rule, `app/services/llm_decision_memo.py:1536`:
   `- listing_age.created_days / updated_days: flag stale listings (>90 days).`
2. Risks composition rule, `:1569`:
   `risks must be 2–4 distinct items… Draw from gates.failed, gates.unknown,
   listing staleness, parking unknowns, frontage signals, cannibalization,
   brand saturation.`
3. Few-shot examples that model the exact wording:
   - `:1629` (Ex C, 102 days), `:1704` (Ex F, 64 days):
     `"Listing has been live for N days, longer than is typical for prime
     corner units in this district." … "Open negotiation 8–12% below asking…"`
   - `:1663` (Ex D, 147 days):
     `"Listing has been on market for 147 days — pricing has not cleared…"`
   - `:1657` (Ex D key_evidence): `{"signal": "listing age (created)",
     "value": "147 days", "implication": "stale listing suggests the market
     has already declined this rent"…}`

   Note: Example F flags a **64-day** listing as "longer than is typical" —
   the model is already inventing the peer-relative hedge with no data behind
   it. This is precisely the gap Option (a) closes.

**Whitelist membership & truncation survival.** Confirmed present:

- `_MEMO_WHITELIST` includes `"listing_age"` —
  `app/services/llm_decision_memo.py:442` (added in the Phase-4 split,
  `:410-416`). The dict therefore survives snapshot truncation.
- The typed scalar `listing_age_days` is in the property_overview render set
  — `:3149` (`_ADVISORY_SECTION_RENDER["property_overview"]`,
  `:3141-3152`) — so it survives into the rendered memo bullets.
- Field typed on the dataclass at `MemoPropertyOverview.listing_age_days`
  (`:512`).

### A2 — Is there ALREADY a relative-age signal? → NO

Searched the snapshot assembly and comparables block:

- `listing_age` block (`expansion_advisor.py:9789`) — absolute days only.
- `district_momentum` block (`expansion_advisor.py:9799-9809`) — this is
  district **activity_30d** percentile (`:9806-9809`,
  `percentile_raw/absolute/composite`), i.e. how active the *district* is, not
  how old *this listing* is relative to peers. Not usable as a stale-vs-peers
  signal.
- The rent-percentile comparable set (`_percentile_rent_burden`,
  `:4808-4990`) computes a rent percentile (`n_below / n`, `:4970`) but
  selects **no age column** and applies **no age filter** — see the aggregate
  at `:4944-4960`.

**Conclusion (central finding):** no per-search or per-district listing-age
distribution is persisted. There is no JSON path to a comparable-age
distribution. A relative signal must be computed — see B.

---

## B. Relative-threshold feasibility

### Recommendation: Option (a) — compute a listing-age percentile in `_percentile_rent_burden`

**Why it's the cleanest mirror of the rent-percentile machinery.** The rent
percentile already solves the identical "relative to district comparables"
problem in `_percentile_rent_burden`
(`app/services/expansion_advisor.py:4808-4990`):

- It builds a tiered comparable set over `commercial_unit` with a
  narrowest→broadest fallback chain (`:4886-4939`:
  `district_band_type → district_type → district → city_band_type → city`)
  and min-N gates (8/12/20).
- The aggregate (`:4944-4960`) already does
  `SUM(CASE WHEN (rate) <= :listing_rate THEN 1 ELSE 0 END) AS n_below`, then
  `percentile = n_below / n` (`:4970`).

`commercial_unit` carries the age columns needed, already indexed:

- `app/models/tables.py:443-444` — `aqar_created_at`, `aqar_updated_at`
  (`DateTime(timezone=True)`).
- `app/models/tables.py:472-478` — `idx_commercial_unit_aqar_created_at`,
  `idx_commercial_unit_aqar_updated_at` (descending).
- `first_seen_at` (`:438`) is always populated (`server_default now()`), a
  safe COALESCE floor.

**The change (one scalar):** add to the same aggregate a count of comparables
**at least as old as** the candidate, e.g.
`SUM(CASE WHEN EXTRACT(DAY FROM now() - COALESCE(aqar_updated_at,
aqar_created_at, first_seen_at)) >= :listing_age_days THEN 1 ELSE 0 END) AS
n_older`, then `age_percentile = n_older / n` (share of peers as old or
older). The candidate's own age is already in hand from
`_effective_listing_age_days(row)` (`:2885`, called at `:9704`). Persist the
one scalar into `rent_burden_meta` (`:5059`) and surface it where
`comparable_*` already lands (`:10257-10270`) — or attach it to the
`listing_age` dict at `:9789` (which is whitelisted, so no whitelist edit
needed).

**Cost:** one extra `SUM(CASE …)` on a query that already runs for every
listing candidate; no new query, no new table scan. The age columns are
indexed but the aggregate is a full scan of the (already-filtered) comparable
cell regardless, so no added round-trips.

**Caveat to resolve with psql Q1:** `aqar_created_at`/`aqar_updated_at` are
detail-scrape fields and may be sparse (`tables.py:441-442`). Mitigation
inside Option (a): compute age over
`COALESCE(aqar_updated_at, aqar_created_at, first_seen_at)` so the comparable
age set is never thinner than the rent set — but note the semantic caveat that
`first_seen_at` is *crawl-discovery* age, not true listing age, so a
first_seen-dominated cell measures "how long we've tracked it," which is a
weaker (but directionally valid) staleness proxy. Surface the dominant source
alongside the scalar so the prompt can hedge harder when it's first_seen-based.

### Option (b) — approximate at memo-build time: NOT VIABLE
The memo payload contains only the candidate's **own** age
(`listing_age.created_days/updated_days`, `expansion_advisor.py:9789`) and no
peer ages. `_build_advisory_sections` (`llm_decision_memo.py` around
`:1239-1300`) has no comparable-age data to compute a percentile from. There
is nothing to approximate against. Rejected.

### Option (c) — absolute threshold fallback: ONLY if Q1 rules out (a)
If age columns are too sparse for a trustworthy percentile, fall back to an
absolute cut anchored on the citywide distribution from **psql Q2** (p75/p90),
NOT a guessed day count. A p75/p90 anchor keeps "stale" meaning "old relative
to the city," preserving the relative spirit. This is strictly the fallback —
prefer (a).

---

## C. Cross-signal interaction (avoid contradiction)

### C1 — Rent zone is available in the same prompt scope → YES
- `_rent_positioning(percentile, scope)` returns `{zone, pct_value, scope,
  phrase_en, phrase_ar}` with `zone ∈ {low, mid, high}` —
  `app/services/llm_decision_memo.py:1083-1139` (zones: `low` = below most
  comparables/cheap `:1124`, `mid` = at-market `:1116`, `high` = above
  most/expensive `:1133`).
- It is assembled into the payload at `:1300`
  (`"rent_positioning": _rent_positioning(rent_percentile, comparable_scope)`)
  inside `financial_framing`, which is part of the `advisory_sections` object
  the LLM receives. The `risks` field is authored in the **same** completion,
  so a risks rule can read `financial_framing.rent_positioning.zone`.

**Hedged wording differentiation by zone (recommendation only — no final copy):**
- **LOW** (rent already below median): long vacancy on an already-cheap unit
  reads as a possible problem with the *space* (access, condition, layout),
  not clean leverage. Wording should lean toward a diligence caveat ("vacant
  long despite below-market asking — pressure-test why it hasn't cleared")
  and only soft leverage. Avoid implying easy discount on an already-cheap
  rent (would contradict the at-/below-market rent framing).
- **MID** (at-market): cleanest leverage case — "vacant noticeably longer than
  peers at an at-market ask" supports a tenant-side negotiation point without
  contradicting the rent reading.
- **HIGH** (above median): leverage compounds with the existing
  over-priced-vs-peers finding ("priced above most comparables AND vacant
  longer than them — the market has not cleared this rent"). Must stay
  consistent with, not duplicate, the high-zone rent risk the memo already
  raises.
- When `rent_positioning` is null/absent (`:1108`, non-listing or no
  comparable set), omit the zone-conditioned leverage framing and fall back to
  a plain staleness caveat.

### C2 — Does the existing risk line already mention negotiation? → YES, but only in few-shot examples (LLM-authored, not templated)
- "Open negotiation 8–12% below asking…" appears **only** in the few-shot
  example mitigations: `app/services/llm_decision_memo.py:1629`, `:1704`
  (and grounding test `tests/services/test_llm_decision_memo_grounding.py:353`).
  These are illustrative examples the model mimics — there is **no template
  string** that emits a negotiation sentence.
- Implication for the patch: the new leverage point must **reframe the same
  staleness risk item** so it carries both the staleness caveat and the
  possible leverage — it must not add a second, separate negotiation risk
  (would violate the "2–4 distinct items" rule at `:1569` and read as
  duplication). The existing example mitigations already pair staleness with
  "negotiate below asking," so the reframed rule should subsume/standardize
  that pairing and make it conditional on **(i)** old-relative-to-peers being
  true and **(ii)** the rent zone, rather than the model volunteering it
  unconditionally.

---

## D. Arabic + version

### D1 — AR glossary/unit support needed (Rules 7 & 8)
- Rule 7 (signal-term canon) — `app/services/llm_decision_memo.py:1981-2012`.
  Already has `"listing age (created)" → "عمر الإعلان (تاريخ النشر)"`
  (`:2001-2002`).
- Rule 8 (unit-token policy) — `:2014-2058`. Already has
  `"<N> days" → "<N> يوماً"` (`:2045-2046`).

Terms that have **no fixed Arabic yet** and would leak English into AR memos
if the reframed line is added without glossary support:
- **"negotiating leverage" / "tenant-side leverage"** — needs a canonical AR
  term (e.g. ورقة تفاوضية / قوة تفاوضية for the tenant). Add to Rule 7.
- **"vacant/live longer than peers" / "longer than typical for comparable
  listings"** — the *relative* phrasing. Needs fixed AR (e.g. "أطول من
  المعتاد مقارنةً بالإعلانات المماثلة") so the model doesn't free-translate
  and so it stays consistent with the AR rent-comparable scope tokens already
  defined at `:2033-2035` (المقارنات في الحي / على مستوى المدينة).
- If Option (a) surfaces an age **percentile** value, decide whether it is
  ever spoken in prose; if so it needs the same "never render the raw rank as
  N%" discipline the rent percentile carries (Rule 8, `:2019-2032`).
  Recommendation: keep the age percentile internal (drives the
  old-relative-to-peers boolean), not a spoken number, to avoid a second
  percentile-inversion footgun.

### D2 — Version bump, fixture regen, and the backend rider
This is a prompt-rule change, therefore:
- **Bump** `MEMO_PROMPT_VERSION` — `app/services/llm_decision_memo.py:53`
  (currently `"v12.1-demand-evidence-enforced-2026-06"`). The cache key uses
  it (`app/api/expansion_advisor.py:1559`; Alembic compare-guard
  `alembic/versions/20260425_memo_prompt_version.py:7`), so cached memos
  regenerate on next view.
- **Update** the version-pin test —
  `tests/test_llm_decision_memo.py:2769` (`assert MEMO_PROMPT_VERSION == …`).
- **Regenerate** the byte-identity prompt fixture —
  `tests/data/pr4a_structured_memo_system_prompt_en_head.txt`, asserted equal
  to `_compose_structured_system_prompt("en")` at
  `tests/test_pr4a_arabic_structured_memo.py:59-63` (also consumed by
  `tests/test_pr4c_arabic_key_evidence.py:32`). The reframed risk rule, any
  new few-shot example edits, and the new Rule 7/8 AR terms all change these
  bytes.
- **Backend rider (flag):** if Option (a) is chosen, the snapshot gains one
  scalar (age percentile / old-relative-to-peers). If attached to the existing
  `listing_age` dict (`expansion_advisor.py:9789`), **no whitelist edit is
  needed** — `"listing_age"` is already in `_MEMO_WHITELIST`
  (`llm_decision_memo.py:442`). If added as a new **top-level** snapshot key
  instead, it must be appended to `_MEMO_WHITELIST` or truncation will drop
  it. Prefer nesting under `listing_age` to avoid the whitelist edit.

---

## Discrepancies & framing

1. **"Rent zone located in PR-E" is imprecise.** The zone object
   (`_rent_positioning`) lives in the memo builder
   `app/services/llm_decision_memo.py:1083` and is assembled into the payload
   at `:1300`, not in a PR-E backend module. A zone-conditioned risk rule
   reads the memo-side `financial_framing.rent_positioning.zone`. (Section C1.)
2. **The existing staleness line is not templated.** It is entirely
   LLM-authored from the data-dictionary rule (`:1536`), the risks rule
   (`:1569`), and few-shot examples (`:1629/1663/1704`). There is no string to
   "reframe" mechanically — the reframe is a prompt-rule + example edit, and
   the model already invents an *unbacked* peer-relative hedge today (Ex F flags
   64 days as "longer than typical"). Option (a) is what makes that hedge
   truthful. (Sections A1, B.)
3. **`property_overview.listing_age_days` is `created_days`, not effective
   or updated age** (`:1263`). If the new relative signal should compare on
   the same basis the memo already shows, decide deliberately whether the age
   percentile is computed on `created_days`, `effective_age_days`, or the
   COALESCE used elsewhere — they can differ materially given the scraper's
   daily-cadence note (`expansion_advisor.py:9764-9769`).
4. **Age-column sparsity is the one real risk to Option (a)** and is not
   knowable from code (detail-scrape fields, `tables.py:441-442`). psql Q1
   gates the decision; if coverage is poor, the COALESCE-to-first_seen
   mitigation or Option (c) applies. (Section B.)
5. **Keep the age percentile internal.** Surfacing a second spoken percentile
   re-opens the rent-percentile inversion class of bug (the entire reason
   `_rent_positioning` pre-renders phrases, `:1095-1099`). Recommend the age
   percentile drives a boolean/zone-conditioned hedge, not a number in prose.
