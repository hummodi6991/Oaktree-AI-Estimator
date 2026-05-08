# Decision Memo / Diagnostics defect investigation — 2026-05-08

Scope: read-only investigation of two defects in the Decision Memo →
Diagnostics view of the Expansion Advisor frontend, observed on
http://8.213.84.191/ for `Burger / 100–500 m² / 200 m² target`, rank-1
candidate (`parcel_id 6449914`, العقيق).

The Diagnostics view tab strip (Economics, Market, Site, Risks & Validation,
Breakdown) is rendered by a single component:

- `frontend/src/features/expansion-advisor/ExpansionMemoPanel.tsx`
- The five-tab nav is at `ExpansionMemoPanel.tsx:296-308`
- Tab labels resolve via i18n keys `expansionAdvisor.memoTab_economics …
  memoTab_breakdown` (`frontend/src/i18n/en.json:1005-1009`)

The drawer also has a top-level `Memo | Diagnostics` strip
(`ExpansionMemoPanel.tsx:182-202`); the five-tab strip lives inside the
Diagnostics drawer tab. The Memo drawer tab renders
`DecisionMemoNarrative` instead, which is a separate read path discussed
below for cross-reference.

---

## Defect A — Market tab renders blank

### 1. Component file:line where the tab is rendered

`frontend/src/features/expansion-advisor/ExpansionMemoPanel.tsx:342-371`

```tsx
{/* Market tab */}
{activeTab === "market" && (
  <div className="ea-memo-tab-panel">
    {comps.length > 0 && (
      <>
        <h5>…</h5>
        <table className="ea-comp-table">…{comps.map(…)}…</table>
      </>
    )}
  </div>
)}
```

There is **no `else` branch and no fallback message**. When `comps` is
empty, the panel renders an empty `<div className="ea-memo-tab-panel">`,
which is exactly the symptom Ahmed reports (heading + underline render —
because the nav button is unconditional — but the content area is
empty).

### 2. Backend field path the component reads

`comps` is the only data the Market tab consumes:

- Defined at `ExpansionMemoPanel.tsx:167`:
  `const comps = (cand.comparable_competitors || []) as Array<…>`
- `cand` is `memo.candidate` — i.e.
  `CandidateMemoResponse.candidate.comparable_competitors`
  (typed at `frontend/src/lib/api/expansionAdvisor.ts:441-466`, field
  declared at line 453).
- Backend wire path: `GET /v1/expansion-advisor/candidate-memo/{id}` →
  `candidate.comparable_competitors`.

No other backend field feeds the Market tab. Notably, the candidate-memo
response also exposes `market_research.delivery_market_summary`,
`market_research.competitive_context`, `market_research.district_fit_summary`
(`expansionAdvisor.ts:468-472`), and the candidate's `feature_snapshot`
carries the rich market-signals payload (provider density, whitespace,
multi-platform, delivery competition, cannibalization, district
momentum, brand presence, etc.) — **none of those are read inside the
`activeTab === "market"` branch**. They are read by the Breakdown tab
instead (`ExpansionMemoPanel.tsx:547-587` plus surrounding sections).

### 3. Whether that field is populated on a representative response

For the rank-1 candidate in question, `comparable_competitors` is
empty/absent (consistent with the visible blank panel and with the
"Brick-and-mortar competitor whitespace remains favorable" / "no direct
competitors within immediate proximity" narrative on the Memo tab — the
candidate's nearby same-category competitor pool is sparse).

A representative API sample to confirm presence/absence is requested as
an open question below; this finding does not depend on the value, only
on the contract: the tab will go blank for **any** candidate whose
`comparable_competitors` is empty.

### 4. Root cause classification

**Wired-but-render-conditional bug** — the panel's only content branch
is `{comps.length > 0 && …}` with no fallback. Effectively the tab is
*single-purpose* (a comparable-competitors table) and falls back to a
literal empty `<div>` when that single source has no rows.

There is also a latent product question (see open questions): the tab is
labelled "Market" but its only content is comparable competitors — none
of the broader market-signal data already on the Breakdown tab, and
none of the `market_research` summaries from the memo response, are
surfaced here.

### 5. Smallest patch surface that would fix it (described in prose)

Two viable shapes, in increasing scope:

(a) **Minimum**: inside the `activeTab === "market"` block, add a
fallback paragraph rendered when `comps.length === 0`. New i18n keys
`expansionAdvisor.marketTabEmpty` (en + ar). Diff is roughly four lines.
This stops the blank state but does not enrich the tab.

(b) **Light wiring**: in the same block, additionally surface the
already-fetched market context from `memo.market_research`
(`delivery_market_summary`, `competitive_context`, `district_fit_summary`)
when present, plus the comparable-competitors table when present, plus a
single fallback line if all of those are empty. Still confined to the
`activeTab === "market"` JSX block in `ExpansionMemoPanel.tsx`; new i18n
keys for section headings and the empty-state line; no backend changes.

Either option keeps the patch local to the Market tab branch in
`ExpansionMemoPanel.tsx`. Option (b) is a closer match for what users
expect a tab labelled "Market" to contain on a candidate where the
comparable-competitor table is empty.

> The comparable-competitors data path itself is not the bug surface —
> for some candidates that array is genuinely empty, and that is a valid
> data state. The bug is the absence of a fallback in the renderer.

---

## Defect B — TOP RISKS shows "—" on Risks & Validation tab

### 1. Component file:line where the tab is rendered

`frontend/src/features/expansion-advisor/ExpansionMemoPanel.tsx:394-428`,
specifically the right column at lines 421-424:

```tsx
<div>
  <span className="ea-memo-callout__label" …>{t("expansionAdvisor.topRisks")}</span>
  {risks.length > 0
    ? <ul className="ea-memo-list">{risks.map((s, i) => <li key={i}>{s}</li>)}</ul>
    : <p className="ea-detail__text">—</p>}
</div>
```

The em-dash is the explicit empty-array fallback — so the renderer is
correct; the source array is empty.

### 2. Backend field path the component reads

`risks` is defined at `ExpansionMemoPanel.tsx:169`:

```ts
const risks = toList(cand.top_risks_json).slice(0, 3);
```

Wire path: `CandidateMemoResponse.candidate.top_risks_json`
(`expansionAdvisor.ts:449`), populated by
`GET /v1/expansion-advisor/candidate-memo/{id}` and ultimately produced
by the heuristic `_top_positives_and_risks(...)` in
`app/services/expansion_advisor.py:2960-3110+`. That heuristic emits
risks only when specific thresholds fire (e.g. economics_score < 50,
high cannibalization, gate failures, delivery-data inferred, area
near-min/near-max, nearest own branch < 1.5 km, competitor count ≥ 8,
etc.). For a high-quality rank-1 candidate the rule set legitimately
fires zero risks, leaving `top_risks_json = []`.

### 3. Whether that field is populated on a representative response

For this candidate: per Ahmed's prior session note, `top_risks_json`
returned `[]` in the search response we reviewed earlier. That is
consistent with what the heuristic produces for a rank-1 candidate that
clears all gates and has economics ≥ 65, no high cannibalization, no
high delivery competition, etc.

`top_positives_json` for the same candidate did populate (three items
visible in the Risks & Validation tab: "Demand potential is strong…",
"Brand-fit profile aligns…", "Economics profile meets…"), which exactly
matches the corresponding three positive triggers in
`_top_positives_and_risks` at `expansion_advisor.py:2975-2987`.

### 4. Cross-reference: TOP RISKS vs RISKS TO WATCH on the Memo tab

The Memo tab's "RISKS TO WATCH" section is rendered by
`DecisionMemoNarrative.tsx`:

- `frontend/src/features/expansion-advisor/DecisionMemoNarrative.tsx:99`
  → `const risks = (Array.isArray(memo.risks) ? memo.risks : []).slice(0, 3);`
- Rendered at `DecisionMemoNarrative.tsx:148-167`, label
  `expansionAdvisor.risksToWatch`.
- `memo` here is a `StructuredMemo` returned by
  `generateDecisionMemo(candidate, brief, lang)`
  → `POST /v1/expansion-advisor/decision-memo`
  (`expansionAdvisor.ts:838-864`).
- `StructuredMemo.risks` is typed `StructuredMemoRisk[]` where each item
  is `{ risk: string; mitigation?: string; … }`
  (`expansionAdvisor.ts:235`, validated by `isValidStructuredMemo` at
  `DecisionMemoNarrative.tsx:34-48`).

So the two are **unrelated fields produced by unrelated pipelines**:

| Surface | Field | Source | Type |
|---|---|---|---|
| Diagnostics → Risks & Validation → TOP RISKS | `cand.top_risks_json` | `_top_positives_and_risks` heuristic in `app/services/expansion_advisor.py` | `string[]` |
| Memo → RISKS TO WATCH | `memo.risks` (i.e. `StructuredMemo.risks`) | LLM-generated structured memo from `POST /v1/expansion-advisor/decision-memo` | `{ risk, mitigation }[]` |

Different endpoints, different generators, different data shapes. The
two arrays only coincidentally agree when both pipelines flag the same
issue. For this candidate the LLM produced two qualitative risks
("No direct competitors within immediate proximity…", "The listing has
been active for 187 days…") that the heuristic does not have rules
for — neither competitor-paucity (the heuristic's nearest-branch /
cannibalization rules look at *own branches*, not competitor
sparseness) nor stale-listing duration are wired into
`_top_positives_and_risks`. Hence the asymmetry the user observed.

### 5. Root cause classification

**Field-mismatch + wired-but-empty-source.** The renderer is fine; the
source field is intrinsically sparse for high-quality candidates. The
problem is product-level: TOP RISKS reads a heuristic-only field while
the user-visible counterpart on the Memo tab reads a richer LLM-derived
field, so the diagnostics view's TOP RISKS will look empty whenever the
narrow heuristic doesn't fire — which is precisely the case for a
clean rank-1.

### 6. Smallest patch surface that would fix it (described in prose)

Three options, smallest first:

(a) **Fallback to memo risks when heuristic is empty.** In
`ExpansionMemoPanel.tsx`, after computing `risks` at line 169, also
read `memo.candidate.decision_memo_json?.risks` (already typed at
`expansionAdvisor.ts:465` as `StructuredMemo | null` and surfaced on
the candidate payload). If `top_risks_json` is empty and
`decision_memo_json.risks` has items, display the first three
`{ r.risk }` strings. This keeps the heuristic field as the primary
source where it does fire, and only borrows from the structured memo
when the heuristic is silent. Local edit, no backend change. Worth
verifying: whether `decision_memo_json` is reliably populated on the
candidate-memo response, or only after the user has visited the Memo
tab (it is persisted per `expansion_advisor.py:9532` / `9850` writes,
so it should be available for cached candidates — needs a quick
production sample to confirm).

(b) **Use memo risks as primary, heuristic as fallback.** Inverse
priority of (a). Argument: the LLM risks are the same ones the user
already sees on the Memo tab labelled "RISKS TO WATCH", so showing
them on the Diagnostics tab keeps the two surfaces consistent. The
heuristic remains a backstop for candidates where the structured memo
hasn't been generated.

(c) **Augment the heuristic.** Add new triggers in
`_top_positives_and_risks` for at least the two cases the LLM caught
(competitor-sparseness ⇒ brand-recognition risk; stale listing
duration ⇒ pricing/demand risk). Larger blast radius — touches scoring
code paths, affects every candidate, needs new ingestion guarantees
(`feature_snapshot.listing_age.created_days` is already read elsewhere
so listing-age risks are tractable). Not recommended for a single-
display-symptom fix; appropriate only if Ahmed wants the heuristic
itself to be richer.

Recommended shape: **(a)** or **(b)**, both confined to
`ExpansionMemoPanel.tsx`. The choice between them is a product call
captured below.

---

## Open questions (require a product decision before patch design)

1. **Market tab content scope.** Is the Market tab supposed to render
   data that's already on the Breakdown tab's "Market signals"
   sub-section (provider density / whitespace / multi-platform /
   delivery competition / cannibalization / district momentum) — i.e. a
   *promotion* of those bars to a top-level tab — or is it meant to
   render a different cut (`memo.market_research` summaries +
   `comparable_competitors` table + brand presence)? Today it is
   neither; it is just the comparable-competitors table.

2. **Market tab empty state vs richer wiring.** If the answer to (1) is
   "richer cut", do we want option (b) above (light wiring) or just
   option (a) (fallback message only)? The minimum patch to make the
   blank state go away is option (a); option (b) is the better product
   answer but is broader.

3. **TOP RISKS source of truth.** Should the Diagnostics view's TOP
   RISKS read the same field as the Memo tab's RISKS TO WATCH
   (`decision_memo_json.risks`), keeping the surfaces consistent, or
   should it remain a heuristic-only diagnostic that is *expected* to
   be empty when the rule-based scorer finds no concerns? Today the
   field is `top_risks_json` (heuristic) but the label "TOP RISKS"
   reads, to a user, like a summary of the same risks shown elsewhere.

4. **Availability of `decision_memo_json` on the candidate-memo
   response.** Patch (a) and (b) for Defect B both rely on
   `memo.candidate.decision_memo_json` being present at the point the
   Diagnostics tab is rendered. The field is persisted (per writes in
   `app/services/expansion_advisor.py:9532` and `9850`) and exposed in
   the typed response, but a production sample of the candidate-memo
   response for parcel `6449914` would confirm the field is non-null at
   the time the Diagnostics view is opened (vs. lazily generated only
   on Memo tab visits). Ahmed: please paste a current
   `GET /v1/expansion-advisor/candidate-memo/{id}` body for this
   candidate so this can be verified before any patch is written.

5. **i18n parity.** Any new fallback strings (Market empty state, any
   new section headings) must be added to both `en.json` and `ar.json`.
   Calling this out so it isn't dropped during patch sizing.

---

## Summary table

| Defect | Component | Field read | Source state | Class | Fix surface |
|---|---|---|---|---|---|
| A — Market tab blank | `ExpansionMemoPanel.tsx:342-371` | `cand.comparable_competitors` (only) | empty for this candidate | wired-but-render-conditional bug (no fallback); also content scope is narrow | local JSX in the `activeTab === "market"` block; new i18n keys; optionally also wire `memo.market_research.*` |
| B — TOP RISKS "—" | `ExpansionMemoPanel.tsx:421-424`, source at `:169` | `cand.top_risks_json` | empty `[]` for this candidate (heuristic didn't fire) | field-mismatch / wired-but-empty-source vs the Memo tab's `memo.risks` LLM field | local JSX in same file: fall back to `cand.decision_memo_json?.risks[*].risk` when heuristic array is empty (or invert priority) |

No code edits made. No commits. No branches created. No PRs opened.
