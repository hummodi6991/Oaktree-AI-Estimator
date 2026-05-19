# PR #4b — Arabic Gap Trace (read-only investigation)

**HEAD SHA (`main`):** `9fd9c85a9147202412aacb0a850c53f1b41bc725`
(investigation branch `claude/trace-arabic-gaps-g5L5K` HEAD: `2ec02ade995e7d96405910dcbf3a52da0fe2d5bc`)

Read-only trace. No patches proposed. No edits made.

---

## 1. Bucket map

### §1 — key_evidence row strings (the four visible English `signal` strings)

| String | source | surface |
|---|---|---|
| `annual rent` (signal) | `app/services/llm_decision_memo.py:1472` (prompt example) + schema `:1306` | **llm-output** — model copies the English example `signal` verbatim into the AR JSON |
| `realized demand 30d` (signal) | `app/services/llm_decision_memo.py:1937-1940` (realized-demand addendum, English) | **llm-output** — model writes the signal label; addendum that steers it is English-only |
| `rent percentile vs comparables` (signal) | `app/services/llm_decision_memo.py:1473` (prompt example) | **llm-output** — copied from English prompt example |
| `access/visibility score` (signal) | `app/services/llm_decision_memo.py:1475` (prompt example) | **llm-output** — copied from English prompt example |

`value` strings (`SAR 292,000/yr`, `83/100`, `ratings/30d 310`, `30th percentile (vs 10 district comparables)`): same surface — **llm-output**, format dictated by schema `:1306` and the English examples `:1472-1541`.

Frontend renders `signal`/`value` **verbatim** — no label table involved (see §1.1).

### §2 — header badges

| String | source | surface |
|---|---|---|
| `Data:` (confidence-grade prefix) | `frontend/src/features/expansion-advisor/ConfidenceBadge.tsx:14` | **frontend-i18n** — hardcoded string literal, no i18n key |
| `GO` (deterministic verdict) | backend literal `app/services/expansion_advisor.py:10602-10606`; rendered `frontend/src/features/expansion-advisor/ExpansionMemoPanel.tsx:246`; uppercased by CSS `expansion-advisor.css:3548` | **backend-prompt / llm-output** — value is a backend-derived English token (`"go"`/`"consider"`/`"caution"`) emitted in the `recommendation` object; frontend renders it raw |

---

## §1 — Key-evidence row labels

### §1.1 — Is the frontend rendering `signal` verbatim, or mapping through a label table?

**Verbatim. No label table.** The component is
`frontend/src/features/expansion-advisor/DecisionMemoNarrative.tsx`, function
`StructuredNarrative` (`:99`). The evidence rows are emitted at `:130-149`:

```jsx
{evidenceItems.map((item: StructuredMemoEvidence, i: number) => (
  <li key={i} className="ea-memo-structured__evidence-item">
    <PolarityMarker polarity={item.polarity} />
    <div className="ea-memo-structured__evidence-body">
      <div className="ea-memo-structured__evidence-head">
        <span className="ea-memo-structured__evidence-signal">{item.signal}</span>
        <span
          className={
            typeof item.value === "number"
              ? "ea-memo-structured__evidence-value ea-memo-structured__evidence-value--numeric"
              : "ea-memo-structured__evidence-value"
          }
        >
          {String(item.value)}
        </span>
      </div>
      <div className="ea-memo-structured__evidence-implication">{item.implication}</div>
    </div>
  </li>
))}
```

- `:135` — `{item.signal}` is the API response field rendered directly.
- `:143` — `{String(item.value)}` is the API response field rendered directly.
- `:146` — `{item.implication}` rendered directly.

There is **no `t(...)` call, no i18n lookup, no constants/label table** anywhere
in the evidence-row path. `evidenceItems` (`:104`) is `memo.key_evidence` sliced
to the top 4. `memo` is `result.memo_json` (`:326`), i.e. the
`decision_memo_json` blob straight from the API. Confirmed by a repo-wide grep:
the only `key_evidence`-bearing frontend files are this component, its tests,
and `lib/api/expansionAdvisor.ts` (the typed API client) — no label/constants
file maps `signal`.

**Conclusion: the bug is NOT in the frontend.** Whatever the LLM writes into
`signal`/`value` is what the user sees. The English strings in the screenshot
mean the model wrote English into an `lang=ar` memo.

### §1.2 — Real AR memo `key_evidence` dump

**Could not run.** No PostgreSQL server is reachable in this environment:

```
$ psql -c "SELECT decision_memo_json->'key_evidence' FROM expansion_candidate
           WHERE decision_memo_lang='ar' AND decision_memo_json IS NOT NULL
           ORDER BY computed_at DESC LIMIT 3;"
psql: error: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432"
failed: No such file or directory
    Is the server running locally and accepting connections on that socket?
```

This does not change the §1.1 conclusion: since the frontend renders `signal`
and `value` verbatim with no localization layer, the literal English
`annual rent` / `SAR 292,000/yr` in the screenshot can only have come from the
`decision_memo_json` itself. The query should still be run against a live DB to
confirm whether the model is emitting English `signal`/`value` while writing the
`implication` in Arabic (which is exactly what the screenshot shows — English
row head, Arabic implication line below).

### §1.3 — If frontend maps via a label table

N/A — confirmed in §1.1 there is no label table. The structured-memo *section
titles* are localized (`expansionAdvisor.keyEvidence` etc.), but the per-row
`signal`/`value` content is not:

- `frontend/src/i18n/en.json:1257` — `"keyEvidence": "Key evidence"`
- `frontend/src/i18n/ar.json:1237` — `"keyEvidence": "الأدلة الرئيسية"`

These cover the **`Key evidence` heading only** (`DecisionMemoNarrative.tsx:127`),
not the row labels. The AR heading is correctly Arabic; the rows underneath are
not.

### §1.4 — Canonical key_evidence examples in the structured memo prompt

File: `app/services/llm_decision_memo.py`.

The prompt is composed by `_compose_structured_system_prompt(locale)` (`:1819`):
it is `_STRUCTURED_MEMO_PREAMBLE` (`:1294-1624`) + a CRITICAL block
(`_CRITICAL_BLOCK_EN` `:1629` **or** `_CRITICAL_BLOCK_AR` `:1723`). The Arabic
swap **only replaces the CRITICAL tail** — the entire preamble, including the
schema and every `key_evidence` example, is **locale-invariant** and shown to
the AR model unchanged.

**Schema line** (`:1305-1306`, inside the locale-invariant preamble):

```
  "key_evidence": [
    {"signal": "string", "value": "string — MUST include a unit (SAR/yr, SAR/m²/yr, ratings/30d, /100, m, count, %, etc.); never a bare number", "implication": "string — one clause naming the investment consequence (not a description of what the number is)", "polarity": "positive | negative | neutral"}
  ],
```

The schema gives no localization instruction for `signal`/`value`, and the
worked unit tokens (`SAR/yr`, `ratings/30d`, `/100`) are English.

**Canonical examples** — `VOICE EXAMPLES` block, also in the locale-invariant
preamble. All English. Quoting three:

`:1472-1477` (Example C):
```
    {"signal": "annual rent", "value": "SAR 432,000/yr", "implication": "asking sits roughly 20% below the district median — the entry basis is genuinely below peer listings, not just below list", "polarity": "positive"},
    {"signal": "rent percentile vs comparables", "value": "28th percentile (vs 14 district comparables)", "implication": "deal pricing is genuinely below market, not just below list", "polarity": "positive"},
    {"signal": "frontage", "value": "24 m corner", "implication": "signage works in both traffic directions on a primary artery", "polarity": "positive"},
    {"signal": "access/visibility score", "value": "82/100", "implication": "site quality reinforces the rent advantage rather than offsetting it", "polarity": "positive"},
    {"signal": "population reach", "value": "41,000 within walking catchment", "implication": "dine-in mix is supportable without leaning on delivery to fill seats", "polarity": "positive"},
    {"signal": "named chains within 500 m", "value": "3 count", "implication": "the catchment validates the category but raises the bar on differentiation", "polarity": "negative"}
```

`:1493-1498` (Example D):
```
    {"signal": "annual rent", "value": "SAR 920,000/yr", "implication": "asking sits 34% above the comparable median — the deal is mispriced for the catchment", "polarity": "negative"},
    {"signal": "rent percentile vs comparables", "value": "88th percentile (vs 11 citywide comparables in the same band/type)", "implication": "no peer-listing evidence that this rent is achievable for this format", "polarity": "negative"},
    {"signal": "economics gate", "value": "failed", "implication": "deterministic threshold breached; the deal cannot be defended on rent burden", "polarity": "negative"},
    ...
```

`:1515-1519` (Example E):
```
    {"signal": "annual rent", "value": "SAR 540,000/yr", "implication": "at-market pricing offers no entry advantage; margin must come from operations", "polarity": "neutral"},
    {"signal": "rent percentile vs comparables", "value": "51st percentile (vs 22 district comparables)", "implication": "deal pricing is market-clearing, neither premium nor discount", "polarity": "neutral"},
    {"signal": "access/visibility score", "value": "88/100", "implication": "site quality is the primary thesis here; signage and approach support brand visibility", "polarity": "positive"},
    ...
```

(Example F at `:1536-1541` repeats the same English signal labels.)

**None of these examples are localized.** Every example `signal`
(`annual rent`, `rent percentile vs comparables`, `access/visibility score`,
`frontage`, `population reach`, `named chains within 500 m`, `economics gate`)
and every example `value` (`SAR 432,000/yr`, `82/100`, `28th percentile (vs 14
district comparables)`) is English.

The **only** Arabic guidance the AR model receives is the LOCALE addendum,
appended to the *user-side* messages at `:1924-1933`:

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

This says "every string value in Arabic" generically, but the model is
simultaneously shown ~20 English `signal`/`value` exemplars as the canonical
format and given explicit instruction to "match the … voice examples." The
realized-demand addendum (`:1937-1940`) likewise instructs the model in English
to "Lead the key_evidence with the delivery rating velocity figure
(ratings/30d)" — `ratings/30d` is itself English.

**Fix surface for §1:** backend prompt (`app/services/llm_decision_memo.py`).
The English `key_evidence` examples in `_STRUCTURED_MEMO_PREAMBLE` and the
schema line `:1306` are the canonical format the AR model imitates; the LOCALE
addendum at `:1924-1933` is the only place an Arabic-specific instruction for
row content could be reinforced. (No proposed patch text per the task rules.)

---

## §2 — Header badges (`Data: A`, `GO`)

### §2.1 — `Data: A` — the confidence-grade pill

Component: `frontend/src/features/expansion-advisor/ConfidenceBadge.tsx` (full file):

```jsx
import { confidenceColor } from "./formatHelpers";

type ConfidenceBadgeProps = {
  grade: string | null | undefined;
  /** When true, just show the letter without "Data:" prefix. */
  compact?: boolean;
};

export default function ConfidenceBadge({ grade, compact }: ConfidenceBadgeProps) {
  const color = confidenceColor(grade);
  const label = grade || "—";
  return (
    <span className={`ea-badge ea-badge--${color}`} title="Data confidence grade">
      {compact ? label : `Data: ${label}`}
    </span>
  );
}
```

- `:14` — `` `Data: ${label}` `` — the literal `Data:` prefix is a **hardcoded
  string**, not an i18n key. `useTranslation` is not even imported in this file.
- `:13` — `title="Data confidence grade"` — the tooltip is also a **hardcoded
  English string** (see §3).
- `label` is `grade` itself (`A`/`B`/`C`/`D`), a single letter — locale-neutral,
  no action needed.

Render site in the memo header: `ExpansionMemoPanel.tsx:249` — `<ConfidenceBadge
grade={cand.confidence_grade as string | undefined} />` inside the
`ea-memo-verdict-row` (`:240-253`). Also rendered at `ExpansionMemoPanel.tsx:441`
and `CandidateDetailPanel.tsx:65`.

**Fix surface:** frontend-i18n — `ConfidenceBadge.tsx` needs an i18n key for the
`Data:` prefix (and the `title`). No such key exists in `en.json`/`ar.json`
today.

### §2.2 — `GO` — the deterministic verdict pill

**Render** — `ExpansionMemoPanel.tsx:204` and `:239-253`:

```jsx
// :204
const verdictColor = rec.verdict?.toLowerCase() === "go" ? "green" : rec.verdict?.toLowerCase() === "consider" ? "amber" : "red";
...
// :239-253
{(rec.verdict || cand.confidence_grade) && (
  <div
    ref={initialSection ? verdictRowRef : undefined}
    className={`ea-memo-verdict-row${anchorCls}`}
  >
    {rec.verdict && (
      <span className={`ea-memo-verdict-badge ea-badge ea-badge--${verdictColor}`}>
        {rec.verdict}
      </span>
    )}
    <ConfidenceBadge grade={cand.confidence_grade as string | undefined} />
    {displayScore != null && (
      <span className="ea-memo-verdict-score">{fmtScore(displayScore, 1)}</span>
    )}
  </div>
)}
```

`:246` renders `{rec.verdict}` **verbatim** — no `t(...)`. `rec` is
`memo.recommendation` (`ExpansionMemoPanel.tsx:162`: `const rec =
memo.recommendation || {};`).

**Origin of the token** — `app/services/expansion_advisor.py:10602-10606`:

```python
if final_score >= 78 and economics_score >= 70 and cannibalization_score <= 55:
    verdict = "go"
elif final_score >= 58 and economics_score >= 45 and cannibalization_score <= 75:
    verdict = "consider"
else:
    verdict = "caution"
```

and is placed into the response at `:10716-10722`:

```python
"recommendation": {
    "headline": headline,
    "verdict": verdict,
    "best_use_case": best_use_case,
    "main_watchout": main_watchout,
    "gate_verdict": _gate_verdict_label((candidate.get("gate_status_json") or {}).get("overall_pass")),
},
```

So the verdict is a **backend-derived English literal**: lowercase
`"go"` / `"consider"` / `"caution"`. The screenshot shows `GO` (uppercase) — this
is purely CSS: `expansion-advisor.css:3544-3548`:

```css
.ea-memo-verdict-badge {
  ...
  text-transform: uppercase;
}
```

The verdict computation does **not** take a `lang` parameter into account — it
is identical for EN and AR memos. It is **hardcoded English on the backend**,
not an i18n key and not LLM output.

### §2.3 — i18n / hardcoded state

- `Data:` — **hardcoded** (`ConfidenceBadge.tsx:14`), not i18n. No AR key exists.
- `GO`/`CONSIDER`/`CAUTION` — **hardcoded backend literal**
  (`expansion_advisor.py:10602-10606`), rendered raw by the frontend. Not i18n,
  not localized anywhere.

The verdict triad set is exactly `{go, consider, caution}` (three values,
`expansion_advisor.py:10602-10606`).

### §2.4 — Other call sites for the verdict token

`rec.verdict` (the GO/CONSIDER/CAUTION token) has **only one render call site**:
`ExpansionMemoPanel.tsx:246`. It is also read at `:204` solely for color
selection (`verdictColor`) and at `:239` for a presence check — neither displays
text. No filter chip, label, or tooltip elsewhere uses this token.

Note — do **not** conflate with `gate_verdict` (a *different* field: the
gate pass/fail status). `gate_verdict` is already properly localized via i18n
keys at `ExpansionReportPanel.tsx:175-177`
(`t("expansionAdvisor.gatePass")` / `gateFail` / `gateNeedsValidation`) and used
at `ExpansionComparePanel.tsx:191-192`. The GO/CONSIDER/CAUTION
`recommendation.verdict` token is the one with no localization.

**Fix surface for §2:** `Data:` → frontend-i18n (`ConfidenceBadge.tsx`). `GO`
verdict → either a frontend-i18n display map keyed off the raw token at
`ExpansionMemoPanel.tsx:246`, or a backend localized field — the raw token
`"go"`/`"consider"`/`"caution"` itself must stay stable for the `verdictColor`
logic at `:204`. (No proposed patch text per the task rules.)

---

## §3 — Anything else flagged

### §3.1 — Property overview / memo facts strip (`m²`, `/سنة`, `يوماً … شاغر`)

Component: `frontend/src/features/expansion-advisor/MemoPropertyFactsRow.tsx`.

```jsx
// :29-31
if (area != null && Number.isFinite(area)) {
  segments.push(`${Math.round(area)} m²`);
}
// :32-37
if (streetWidth != null && Number.isFinite(streetWidth)) {
  segments.push(t("expansionAdvisor.memoFacts.frontage", { width: streetWidth }));
}
if (rent != null && Number.isFinite(rent)) {
  segments.push(t("expansionAdvisor.memoFacts.rentPerYear", { rent: fmtSARCompact(rent) }));
}
// :38-44
if (isVacant) {
  if (vacantDays != null && Number.isFinite(vacantDays)) {
    segments.push(t("expansionAdvisor.memoFacts.vacantDays", { days: vacantDays }));
  } else {
    segments.push(t("expansionAdvisor.memoFacts.currentlyVacant"));
  }
}
```

i18n keys (both locales present and translated):

| key | en.json | ar.json |
|---|---|---|
| `memoFacts.frontage` | `:1021` `"{{width}} m frontage"` | `:1001` `"{{width}} م واجهة"` |
| `memoFacts.rentPerYear` | `:1022` `"{{rent}}/yr"` | `:1002` `"{{rent}}/سنة"` |
| `memoFacts.vacantDays` | `:1023` `"vacant {{days}} days"` | `:1003` `"شاغر منذ {{days}} يومًا"` |
| `memoFacts.currentlyVacant` | `:1024` `"currently vacant"` | `:1004` `"شاغر حاليًا"` |

Findings:

1. **`m²` is NOT routed through i18n.** `MemoPropertyFactsRow.tsx:30` builds the
   area segment as a raw template literal `` `${Math.round(area)} m²` ``. It is
   the *only* segment in this row not built via `t(...)`. `m²` is an SI unit
   symbol and is identical in Arabic, so it renders acceptably either way — but
   it is inconsistent with the other three segments and has no key. Flag:
   **frontend**, `MemoPropertyFactsRow.tsx:30`. (Minor — cosmetic/consistency,
   not a visible-English defect since `m²` is locale-neutral.)

2. **`SAR` / `K` / `M` tokens inside `rentPerYear` are hardcoded Latin.** The
   `{{rent}}` interpolation is `fmtSARCompact(rent)` from
   `frontend/src/features/expansion-advisor/formatHelpers.ts:21-33`:

   ```ts
   export function fmtSARCompact(value: NumericLike): string {
     ...
     return `SAR ${m % 1 === 0 ? m.toFixed(0) : m.toFixed(1)}M`;   // :27
     ...
     return `SAR ${k % 1 === 0 ? k.toFixed(0) : k.toFixed(0)}K`;   // :31
     return `SAR ${Math.round(n)}`;                                 // :33
   }
   ```

   So an AR memo facts row renders e.g. `SAR 292K/سنة` — Latin `SAR` and `K`
   spliced into an Arabic string. The `vacantDays` / `frontage` keys are clean
   Arabic; only the `rentPerYear` value carries Latin tokens. This is a
   **deliberate-looking** house style (`SAR` as a currency code is widely used
   untranslated, and compact `K`/`M` are common), but per the task it is called
   out for a decision: the `SAR` / `K` / `M` tokens are **hardcoded** in
   `formatHelpers.ts:21-33` and never localized. Flag: **frontend**,
   `formatHelpers.ts:27,31,33`.

3. `frontage`, `rentPerYear`, `vacantDays`, `currentlyVacant` — all four come
   through i18n cleanly with correct AR translations. No defect.

### §3.2 — Hardcoded `aria-label` / `title` / `alt` in the memo card

A grep for `aria-label="…"`, `title="…"`, `alt="…"` (and template-literal
variants) across `DecisionMemoNarrative.tsx`, `ExpansionMemoPanel.tsx`,
`MemoPropertyFactsRow.tsx`, `ConfidenceBadge.tsx`, `CandidateDetailPanel.tsx`
returned exactly **one** hardcoded English attribute:

- `frontend/src/features/expansion-advisor/ConfidenceBadge.tsx:13` —
  `title="Data confidence grade"`. Hardcoded English tooltip, no i18n key.
  Flag: **frontend** (same file/component as the §2 `Data:` literal — they would
  be fixed together).

All other `aria-label` attributes in `ExpansionMemoPanel.tsx` /
`DecisionMemoNarrative.tsx` already use `t(...)` (e.g.
`DecisionMemoNarrative.tsx:115` `aria-label={t("expansionAdvisor.theRecommendation")}`,
`:192` `aria-label={t("expansionAdvisor.bottomLine")}`;
`ExpansionMemoPanel.tsx:215` `aria-label={t("expansionAdvisor.decisionMemo")}`).

### §3.3 — Additional English-still surface noticed (not on the deferred list)

- **`SCORE_LABEL_MAP`** — `frontend/src/features/expansion-advisor/ExpansionMemoPanel.tsx:18-35`
  is a hardcoded English label table for the score-breakdown component
  (`Competitor Openness`, `Demand Strength`, `Economics`, `Delivery Market`,
  `Access & Visibility`, `Brand Fit`, `Data Quality`, `Provider Density`,
  `Market Gap`, `Multi-platform`, `Delivery Competition`, `Zoning Fit`,
  `Frontage`, `Parking`, `Cannibalization`). `humanizeScoreLabel` (`:37-40`)
  reads it with no `t(...)` call and falls back to a raw key humanizer. This is
  the **score-breakdown table**, a different surface from the key_evidence
  rows, but it is English-still and visible inside the AR memo drawer. Flag:
  **frontend**, `ExpansionMemoPanel.tsx:18-35`. Surfacing here only — it was not
  on the deferred list and was not in the §1/§2 scope.

---

## Notes / limitations

- The `psql` dump in §1.2 could not run — no DB server in this environment. The
  §1 conclusion (bug is in LLM output, driven by an English-only prompt) holds
  regardless, because the frontend was confirmed to render `signal`/`value`
  verbatim with zero localization layer. The query should still be run against a
  live DB to confirm the exact EN/AR split inside `decision_memo_json`.
- No patches proposed and no edits made, per task rules. This document is a
  pure trace.
