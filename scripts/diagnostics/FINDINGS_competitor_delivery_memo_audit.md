# Findings — Decision Memo: inflated competitor count, hidden delivery leg, wrong service-model copy

**Mode:** READ-ONLY investigation. No `app/` edits. Snapshot line numbers from the
brief had drifted heavily (the files are ~12.6k / ~3.4k lines, not ~60k); every
reference below was re-confirmed against the live tree on branch
`claude/competitor-delivery-memo-audit-hzpw4r`.

Case under audit: QSR candidate `parcel_id 6706340`, Al Olaya, lat 24.6543170
lon 46.7107299, score 79.8, GO, Data A. Memo "HOW IT COMPARES" claimed
"Domino's Pizza has **6 branches within 138 m**".

Files of record:
- `app/services/expansion_advisor.py` (scoring + snapshot assembly)
- `app/services/llm_decision_memo.py` (memo context, prompt, advisory sections)
- `app/ingest/expansion_advisor_competitors.py` (chain-name normalization, denylist)

---

## Q1 — Competitor comparison lineage & the inflated "6"

### Q1.1 — The two competitor queries and their emitted fields

There are **two independent competitor queries**, by design, with different
radii and different dedup behavior. The architecture comment at
`expansion_advisor.py:9835-9840` states this explicitly ("Distinct from the
proximity-competitor query above (5 closest unique chains within 1500m for the
Market tab). The two run in parallel.").

**(A) Score-card / Breakdown-tab count — `_bulk_brand_presence`**
`expansion_advisor.py:9852-9955`.
- Table: `_EA_COMPETITOR_TABLE` = `settings.EXPANSION_COMPETITOR_TABLE`
  (default `expansion_competitor_quality`) — `expansion_advisor.py:54`.
- Radius: **500 m** (`ST_DWithin(..., 500)` — `:9879`, `:9904`). Confirmed.
- GROUP BY: a UNION ALL of two legs —
  - canonical leg: `GROUP BY ecq.canonical_brand_id` (`:9881`), filtered to
    `canonical_brand_id IS NOT NULL` (`:9875`);
  - name leg: `GROUP BY candidate_pid, norm_name_key` (`:9913`), filtered to
    `canonical_brand_id IS NULL` (`:9900`), with `norm_name_key` =
    `_CHAIN_NAME_NORM_SQL` over `ecq.brand_name` (`:9854`).
- Emitted fields: `branch_count = COUNT(*)` (`:9868`, `:9888`) and
  `nearest_distance_m = MIN(ST_Distance(...))` (`:9869-9872`, `:9889`). These
  are stored as **separate** keys on each top_chains entry
  (`:9939-9940`) and surface into
  `feature_snapshot_json["brand_presence"]["top_chains"]`
  (`:10378-10390`), each entry carrying `branch_count` AND `nearest_distance_m`
  side by side.

**Conclusion (Q1.1):** the data carries `branch_count` (count within 500 m) and
`nearest_distance_m` (distance of the single closest row) as two distinct
numbers. The memo prompt even documents this correctly —
`llm_decision_memo.py:1634-1636`: "named chains within 500m with branch_count
and nearest_distance_m." **The "6 within 138 m" conflation is produced by the
LLM prose, not by the data.** branch_count=6 is the 500 m count; 138 m is the
nearest-row distance; the model glued them into a false "6 within 138 m" radius.
This is a **prompt-rule** issue, not a data issue.

**(B) Comparison-narrative competitors — `_comparable_competitors` /
`_bulk_competitors`**
`expansion_advisor.py:4254-4324` (per-candidate) and `:9730-9828` (bulk; the
path actually used). The bulk path is preferred at `:10664-10665`; the
per-candidate function is the fallback at `:10667-10673`.
- Same table (`expansion_competitor_quality`).
- Radius: **1500 m** (`ST_DWithin(..., 1500)` — `:9775`, `:4295`).
- Category-filtered: `lower(COALESCE(ecq.category,'')) = lower(:category)`
  (`:9773`, `:4294`).
- Dedup: `DISTINCT ON (dedup_key)` keeping nearest, where
  `dedup_key = COALESCE(ecq.canonical_brand_id, 'poi:'||restaurant_poi_id)`
  (`:9769-9770`, `:4290`). Returns the 5 nearest distinct brands.
- This is the field the memo "comparison" anchors on — prompt
  `llm_decision_memo.py:1504` and `:1693`: "Comparison MUST reference at least
  one named competitor from **comparable_competitors**." It is injected into the
  user payload as `comparable_competitors` (`:2300`) and surfaced from
  `comparable_competitors_json` (`expansion_advisor.py:12246`).

### Q1.2 — Do "Domino's" and "Domino's Pizza" collapse, and via what key?

`_CHAIN_NAME_NORM_SQL` (`app/ingest/expansion_advisor_competitors.py:54-66`)
is **case-fold + Arabic Alef/Ya-Maksura/tatweel collapse + non-alphanumeric →
space + whitespace squeeze**. Its own docstring (`:46-48`) says it **does NOT
merge bilingual variants**. By hand:
- `"Domino's"` → `domino s` (apostrophe → space)
- `"Domino's Pizza"` → `domino s pizza`

These are **different** `norm_name_key`s. So the **name leg would NOT collapse**
the EN variants together, and would never merge an Arabic "دومينوز" spelling with
either.

Therefore the only way all six rows surface as a single `branch_count=6` entry
is the **canonical leg**: they share one `canonical_brand_id`, and
`GROUP BY canonical_brand_id` (`:9881`) folds every EN/AR spelling into one
group with `COUNT(*)=6`. **Probe section C confirms whether the six rows in fact
share a single `canonical_brand_id`.** (Expected: yes — that is what produces the
6.) Classification: the grouping that yields "6" is **brand canonicalization**,
working as intended at the brand level; the defect is purely the lack of
spatial dedup (Q1.3), not name normalization.

### Q1.3 — No spatial dedup

`branch_count` is a raw `COUNT(*)` over `expansion_competitor_quality` rows
(`:9868`, `:9888`). There is **no** `DISTINCT` on coordinates, no `ST_SnapToGrid`,
no cluster collapse anywhere in `_bulk_brand_presence`. **Two ecq rows at
byte-identical lat/lon both count toward `branch_count`.** The reported case
(two rows at 24.65562/46.70820, plus a 4-row ~15×27 m cluster alternating
EN/AR "Domino's"/"Domino's Pizza") therefore reports `branch_count=6` for what
is physically ≈2 stores. Probe `competitor_dup_audit.sql` sections A/B quantify
the raw-vs-distinct-vs-25m-cluster gap; `competitor_dup_systemic.sql` measures
whether the inflation is systemic across the search. Classification:
**data/dedup**.

### Q1.4 — Two-source hypothesis (KFC 401 m vs 388 m)

Confirmed: the score-card count and the comparison-narrative distances come from
**different queries** (Q1.1 A vs B). The memo's "KFC operates a branch 401 m
away" originates from **`comparable_competitors`** (1500 m, category-filtered,
`DISTINCT ON (dedup_key)` keeping nearest) — `expansion_advisor.py:9730-9828`,
surfaced as `comparable_competitors` at `llm_decision_memo.py:2300`. The
"388 m nearest KFC" your Codespace probe saw came from an unfiltered ecq probe.

The most likely cause of the 13 m disagreement: the **category filter**. The
388 m KFC row carries a `category` that is not byte-equal to the search category
(`qsr`), so it is excluded from `comparable_competitors` (which filters on
category) while still visible to `brand_presence` (which does **not** filter on
category). The next KFC that *does* match the category sits at 401 m. A secondary
possibility is the `DISTINCT ON (canonical_brand_id)` dedup picking a canonical
KFC row over a nearer `canonical_brand_id IS NULL` KFC row keyed as `poi:<id>`.
**Probe `competitor_dup_audit.sql` section D reproduces both KFC distances
side-by-side and dumps the per-row categories**, so the exact cause can be read
off directly. Classification: **expected behavior of two intentionally different
queries** — the fix (if any) is to make the memo prose state the radius/source
honestly, i.e. a **prompt-rule** clarification, not a data fix.

---

## Q2 — Demand evidence hides the delivery leg

Observed: demand sub-score 68.5 = dg-composite 59.82 × 0.6 + delivery leg
81.62 × 0.4. The qsr blend weights (0.60 / 0.40) are confirmed at
`expansion_advisor.py:2778-2786` (`_demand_blend_weights("qsr") = (0.60, 0.40)`),
and the blend is applied at `:8855-8856` / `:8976`. The prose never mentions
delivery — under-selling the site (delivery leg 81.62 > foot-traffic composite
59.82).

### Q2.1 — Is `demand_blend` passed to the memo?

`demand_blend` **is written** to the snapshot at
`expansion_advisor.py:10276-10285`, with exactly the fields the brief names:
`pop_or_index_weight`, `delivery_weight`, `delivery_score`,
`listing_realized_split`. But it is **filtered out before the LLM sees it**:

- The memo whitelist `_MEMO_WHITELIST` (`llm_decision_memo.py:476-493`) lists
  `demand_generator_index` and `demand_score_source` but **not** `demand_blend`
  and **not** `delivery_score`.
- `_serialize_context_for_user_message` keeps the full snapshot only while it
  is under `_FEATURE_SNAPSHOT_SOFT_LIMIT = 4000` chars; once over, it truncates
  to the whitelist (`:2271-2276`), and the last-resort path truncates to the
  whitelist again (`:2312-2316`). A real candidate's snapshot is far over 4000
  chars, so `demand_blend` is dropped in the normal case.
- The deterministic `advisory_sections.market_context`
  (`llm_decision_memo.py:1416-1424`) carries `population_reach`,
  `district_momentum`, `realized_demand_30d`, `realized_demand_branches`,
  `delivery_listing_count` — but **not** `demand_blend` / `delivery_score`.
  (Note: `delivery_listing_count` is the area's listing depth, a *different*
  quantity from the `delivery_score` leg that actually entered the demand
  sub-score.)

So the delivery leg is **absent from every channel the LLM reads**.

### Q2.2 — Why the prose omits it

It is **both (a) absent from context AND (b) the prompt anchors elsewhere** —
not (c). The dg_index demand rule at `llm_decision_memo.py:1616-1631` instructs:
when `demand_score_source == "dg_index"`, the demand evidence MUST cite the
dg composite and its sub-signals (F&B review mass, trip generators, built
density, local population reach); "Population reach within walking catchment" is
demand anchor only for `pop_score`. **The delivery leg is never mentioned in the
rule set.** Worse, the few-shot key_evidence example at `:1745` actively trains
the model *away* from delivery: `"dine-in mix is supportable without leaning on
delivery to fill seats"`. So even if `demand_blend` were plumbed in, the prompt
would still steer the prose to population/dg-composite. (Option (c) — surfacing
only when `realized_demand_30d` is non-null — does not apply; `realized_demand`
is a separate signal that *is* whitelisted at `:467-469`, but here it is null.)

### Q2.3 — Minimal change surface (do not implement)

Two coordinated touch-points, both in the memo layer:
1. **Context-plumbing:** add `demand_blend` (or at least `delivery_score` +
   `delivery_weight`) to `_MEMO_WHITELIST` (`llm_decision_memo.py:476-493`) so it
   survives the 4000-char truncation; optionally also surface it in
   `advisory_sections.market_context` (`:1416-1424`) so it is truncation-proof.
2. **Prompt-rule:** extend the dg_index demand rule (`:1616-1631`) so that when
   the delivery leg is material (e.g. `delivery_score` meaningfully exceeds the
   population/dg composite) the demand evidence MUST name it, and amend/relax the
   anti-delivery few-shot at `:1745`.

The scoring math itself is correct and needs no change. Classification:
**context-plumbing + prompt-rule** (a deterministic `phrase_en` is a viable
alternative for the delivery sentence if you prefer not to trust the LLM with
the number).

---

## Q3 — "dine-in model" copy on a QSR candidate

### Q3.1 — Origin of the phrase

The phrase is a **hardcoded few-shot prompt example the LLM is copying** —
option (b). It is **not** a deterministic `phrase_en` template and **not** free
generation. Occurrences, all inside the static "VOICE EXAMPLES" block of the
system prompt in `llm_decision_memo.py`:
- `:1739` — Example C `ranking_explanation`: "…population reach of 41,000 inside
  the walking catchment **supports the dine-in model**."
- `:1745` — Example C `key_evidence`: "**dine-in mix** is supportable without
  leaning on delivery to fill seats" (also the Q2 anti-delivery trainer).
- `:1800` — example `key_evidence`: "demand base **supports the dine-in mix**".
- `:1815` — example `ranking_explanation`: "**supports the dine-in model**".

These examples are **static** — they do not vary by `service_model`. There is
**no** deterministic dine-in phrase generator in `expansion_advisor.py` (the
`dine_in` hits there at `:810-881`, `:1563` are scoring radii / blend weights,
not prose). So a qsr candidate gets "dine-in" prose purely because the model is
parroting the few-shot examples.

### Q3.2 — Is `service_model` in scope?

Yes. `service_model` is rendered into the system prompt header at
`llm_decision_memo.py:152` ("- Service model: {service_model}"), filled from the
brief at `:294`, and is part of the candidate field whitelist at `:1059`. So a
service-model-aware fix is feasible — either make the few-shot examples
conditional on / neutral to service model, or add a prompt rule forbidding
"dine-in" framing when `service_model in {qsr, delivery_first}`. The offending
string originates entirely from the hardcoded examples; no code computes it.
Classification: **copy/i18n (prompt examples)** + a small **prompt-rule** guard.

---

## Cross-cutting

### Fix classification per finding

| # | Finding | Class | Touches Arabic? |
|---|---------|-------|-----------------|
| Q1.1/Q1.4 | "6 within 138 m" conflation; 401 vs 388 two-source distances | **prompt-rule** (state count vs nearest, and radius/source honestly) | No (EN prose; AR mirror at `:1828` if templated) |
| Q1.3 | raw `COUNT(*)` with no spatial dedup inflates `branch_count` | **data/dedup** | No (numeric) |
| Q2 | delivery leg invisible to memo | **context-plumbing + prompt-rule** (or deterministic `phrase_en`) | Mirror AR rule at `:2844-2968` if delivery rule is added |
| Q3 | "dine-in model" on qsr | **copy/i18n + prompt-rule** | **Yes** — AR few-shots/rules at `:1828`, `:2147`, `:2966-2968` need the same guard; full codepoint validation later |

### Roadmap linkage

- **Competitor POI spatial dedup** (Q1.3) is the cleanest standalone fix and the
  one that changes a *number* the user sees. It belongs with the existing
  competitor-dedup roadmap item. A dedup could be applied at **ingest**
  (`app/ingest/expansion_advisor_competitors.py`, the `chain_counts` CTE) so the
  whole app benefits, or **query-side** in `_bulk_brand_presence` via
  `ST_SnapToGrid`/cluster collapse before `COUNT(*)`. Ingest-side is preferable
  (single source of truth; benefits `chain_strength` too) but is the larger
  change.
- **`chain_name` backfill / brand canonicalization** is what *correctly* groups
  the 6 Domino rows into one brand (Q1.2); it is working. Do not "fix" the
  grouping — only the spatial count.
- **Deterministic `phrase_en`/`phrase_ar` pattern** (the rent `phrase_en` at
  `:1190-1233`, copied verbatim by the LLM) is the proven template for Q2's
  delivery sentence and Q3's service-model-correct demand sentence, if you want
  to remove LLM discretion entirely.

### Recommended single-purpose PR sequence

1. **PR 1 — Q3 service-model copy guard (smallest, safest, no data).** Neutralize
   the "dine-in" few-shot examples and add a prompt rule keyed on
   `service_model`. Pure prompt/i18n; touches AR mirrors; zero scoring or schema
   risk. Lands first because it is a visible correctness bug with a one-file
   blast radius and no DB dependency.
2. **PR 2 — Q1 comparison-prose honesty (prompt-rule).** Add a rule that
   `branch_count` is a 500 m **count** and `nearest_distance_m` is the **single
   nearest** — they must not be fused into "N within <nearest> m" — and that the
   comparison radius is up to 1500 m. Removes the false "6 within 138 m" reading
   without waiting on the dedup data work. Prompt-only.
3. **PR 3 — Q2 delivery-leg plumbing + rule.** Whitelist `demand_blend`, surface
   it in `advisory_sections`, and add the "name the delivery leg when material"
   rule (+ relax the anti-delivery few-shot). Backend context + prompt; AR
   mirror. Independent of PR 1/2.
4. **PR 4 — Q1.3 competitor spatial dedup (data/dedup, largest).** Land last
   because it is the heaviest change (ingest or query-side, with regression
   exposure on `branch_count` / `unique_brands` / `chain_strength`), and the
   prose fix in PR 2 already prevents the *misleading* reading in the interim.

Rationale for ordering: prose/prompt fixes (PR 1-3) are low-risk and
independently shippable; the data fix (PR 4) is the one that needs the probe
results and careful regression validation, so it lands once the cheaper
corrections are already protecting the output.

---

## Demo-safety line

**Is any number currently shown in the memo wrong beyond the comparison block?**
From the code path, **no** — the headline, key_evidence, demand/economics
sub-scores, gate verdicts, and risks are computed deterministically and are
internally consistent:

- The demand sub-score **68.5 is correct** — it genuinely includes the delivery
  leg (`0.6·59.82 + 0.4·81.62`, `expansion_advisor.py:8855-8856`); Q2 is an
  *under-disclosure* in prose, not a wrong number.
- `brand_presence.branch_count` (the "6") is an honest `COUNT(*)` of ecq rows
  within 500 m — it is **inflated by duplicate POIs**, so it is the **one number
  that is materially misleading**. It is confined to the competitor / "HOW IT
  COMPARES" surface (score card + comparison block). It does not feed the
  headline or the demand/economics sub-scores.
  - Caveat to verify: `brand_presence.unique_brands` / `total_branches`
    (`expansion_advisor.py:10384-10387`) also derive from these un-deduped rows,
    and `unique_brands` is read by `commercial_floor_pass` (gate logic). If the
    same duplicate POIs ever split across **distinct** `canonical_brand_id`s or
    name keys they would also inflate `unique_brands` and could perturb that
    gate — **probe `competitor_dup_audit.sql` section A's `distinct_coords` vs
    `cluster_count` per brand_group answers whether any brand is split**. For the
    Domino case (single canonical_brand_id) `unique_brands` is unaffected; the
    inflation is contained to `branch_count` / `total_branches`.

Net: the only demo-visible **wrong** number is the duplicated competitor
**branch count**; everything in the headline / key-evidence / risks path is
correct.

---

## Probe files delivered

- `scripts/diagnostics/competitor_dup_audit.sql` — single-candidate audit
  (defaults to parcel 6706340 / qsr): per-brand raw vs distinct-coord vs
  25 m-cluster counts + inflation ratio (A); Domino row-level dump showing the
  duplicate coords (B); canonical_brand_id grouping check (C); the
  500 m-score-card vs 1500 m-comparison KFC distance contrast with per-row
  categories (D); brand_alias coverage (E).
- `scripts/diagnostics/competitor_dup_systemic.sql` — same audit across every
  candidate of the most recent qsr search, with a search-wide aggregate
  (multi-row brands, inflated brands, average and global inflation ratios) to
  decide whether the dup problem is systemic or a single Al Olaya cluster.

Both are `psql -x -f` friendly, READ-ONLY, parameterized via `\set`, join the
candidate point with `ST_MakePoint(lon, lat)`, and read ecq coordinates from
`geom` (ecq has no lat/lon columns). Column names verified against alembic
`20260310_exp_adv_v0`, `d4e5f6a1b2c3`, and `20260426_ecq_canonical_cols`.
