# Finding 5 — Rent-percentile "cheaper than X%" inversion · READ-ONLY Investigation Report

**Mode:** READ-ONLY. No edits to product code, no commits, no PR. (This report file is the only artifact.)

**Scope checked:** branch `claude/rent-percentile-inversion-T1rCk`, which has **zero diff vs `origin/main`** (`git diff origin/main --stat` empty) — so this report is against live `main`. Riyadh-only.

**Headline conclusion (and where it diverges from the brief's hypothesis):** Every *deterministic* surface in current `main` already uses the correct `1 − percentile` direction. **No code path on `main` emits raw `percentile` inside a "cheaper than" phrase.** The "cheaper than ~38%" text in the deployed memo is **LLM-authored prose** (`headline_recommendation` + `key_evidence`), and the LLM is handed only the *raw fraction* `0.375` and asked to perform the `1 − fraction` inversion itself. The 38% is the model failing that inversion (anchoring on `0.375 → 38`), not a flipped formula in code.

---

## Item 1 — Every place `rent_burden.percentile` becomes human text

| # | Location | Direction used | Status |
|---|----------|----------------|--------|
| 1a | `app/services/llm_decision_memo.py:1370-1379` (EN prompt rules) | LOW zone `N = round((1 − fraction) × 100)`; HIGH zone `N = round(fraction × 100)` | ✅ Correct |
| 1b | `app/services/llm_decision_memo.py:1884-1895` (AR Rule 8) | `منطقة منخفضة … حيث N = (100 − المئوية)` (LOW = 100−pct) | ✅ Correct |
| 1c | `frontend/src/features/expansion-advisor/AdvisorySectionCards.tsx:28-39` (`pctFromFraction`) | `if (pct < 40) … value: 100 - pct` (LOW); `else value: pct` (HIGH) | ✅ Correct |
| 1d | `frontend/src/i18n/en.json:1315-1317` / `ar.json:1314-1316` (`rentPositioningLow/Mid/High`) | `"cheaper than ~{{value}}%"` fed `100-pct` | ✅ Correct |
| 1e | `app/services/expansion_advisor.py:_build_strengths_and_risks` (5641-5677) | Does **not** reference percentile at all | ✅ N/A |
| 1f | `render_structured_memo_as_text` → `_render_advisory_section_lines` (`llm_decision_memo.py:2747-2810`) | Renders the **raw fraction** as a bare bullet (`- rent_percentile_vs_comparables: 0.375`); no "cheaper than" verbalization | ✅ (raw number only) |

**Minimal quotes:**
- `llm_decision_memo.py:1372`: `where N = round((1 − fraction) × 100). Example: 0.28 → "cheaper than about 72%…"`
- `AdvisorySectionCards.tsx:35-36`: `if (pct < 40) { return t("…rentPositioningLow", { value: 100 - pct }); }`

**Finding:** The four deterministic verbalizers (1a–1d) are all correct. There is **no deterministic "cheaper than N%" producer that uses raw percentile.** The KEY EVIDENCE row and headline are produced by the LLM, not by `_build_strengths_and_risks` or any backend evidence builder.

---

## Item 2 — Single source or several? What the LLM receives

**Single upstream value, LLM-computed phrase on both surfaces.**

- `llm_decision_memo.py:1192`: `rent_percentile = _safe_float(rent_burden.get("percentile"))` → reads `0.375`.
- `llm_decision_memo.py:1216`: `"rent_percentile_vs_comparables": rent_percentile` — the payload carries the **raw `0.375` fraction only**.
- **No pre-formatted phrase is ever added to the payload** (grep for `positioning/phrase/cheaper` in the payload builder returns only prompt-instruction text and worked examples; line 99 of `AdvisorySectionCards.tsx` confirms only the frontend formats it).

So:
- The **typed field** `financial_framing.rent_percentile_vs_comparables = 0.375` → frontend `pctFromFraction` renders it correctly ("cheaper than ~62%").
- The **headline + `key_evidence` prose** are LLM free text. The LLM gets `0.375` plus the prompt's "compute `N = round((1 − fraction) × 100)`" instruction and must do the arithmetic. Both prose surfaces draw from the *same single raw value*, but the model recomputes the phrase independently each time.

**What the LLM literally receives for rent percentile:** the number `0.375` (no string). Everything visible as "cheaper than ~38%" is the model's own rendering of that number.

---

## Item 3 — Correct direction, pinned from code

`app/services/expansion_advisor.py:4536`:
```
percentile = max(0.0, min(1.0, n_below / n))
```
`n_below` = count of comparables with monthly rent/m² **≤ the listing's rate** (`:listing_rate`, line 4515). So **lower `percentile` ⇒ fewer peers at/below it ⇒ listing is cheaper.**

Burden-score map (4540-4547), evaluated at the sample:
```
0.10 < 0.375 ≤ 0.50 → burden = 60.0 + (0.50 − 0.375)/0.40 × 32.0 = 60 + 0.3125×32 = 70.0 ✓
```
Matches the stored `burden_score 70.0`. **Definitive mapping:** `percentile ↑ ⇒ burden_score ↓` (cheaper = low percentile = high burden_score). Therefore the "cheaper than" share = **`(1 − percentile) × 100`**.

**Invariant cross-check:** listing `141.47 < median 164.72` ⇒ cheaper than **> 50%**. `1 − 0.375 = 0.625 → ~63%` ✅ satisfies it. The deployed `38%` **violates** it. Confirmed: correct output is `~63%`, internally consistent with the memo's own "14% below median" line.

---

## Item 4 — Regression bisect

`git log -- app/services/llm_decision_memo.py` + `git show 7f5e8a81d^`:

- **Before PR #4f** (parent of `7f5e8a81d`): prompt said `Multiply by 100 to phrase ("at the 69th percentile vs comparables")` and examples used the **ordinal "Nth percentile"** form (`0.28 → "28th percentile"`). No "cheaper than N%" template existed.
- **PR #4f — `7f5e8a81d` (2026-05-20, "v9-lay-friendly-percentile")**: introduced the `"cheaper than about N%"` template **with the correct `N = round((1 − fraction) × 100)`** *and* fixed the frontend deterministically (`pctFromFraction`, line 36). This is the commit that **created the failure mode**: a "cheaper than" phrase whose number depends on an LLM-performed inversion of the fraction — even though its written formula is correct.
- **Finding 4 — `acc055a28` (2026-06-03)**: `git show acc055a28 -- …memo.py` touches only `MEMO_PROMPT_VERSION` (v9→v10) and the competitor-economics ban. **Did NOT touch percentile phrasing.** (F1/F2 live in `expansion_advisor.py` rent-ceiling/economics, not the phrasing — confirmed no overlap.)

**Before/after of the relevant line:**
- `7f5e8a81d^`: `Multiply by 100 to phrase ("at the 69th percentile vs comparables").`
- `7f5e8a81d`: `… "cheaper than about N% …" where N = round((1 − fraction) × 100).`

**Conclusion:** No commit in git history emits raw percentile inside a "cheaper than" phrase. The deployed `38%` is **not reproducible from any code path on `main`** — it is an LLM transform error under the #4f "cheaper than" template (the model emitted the new template wording with the *old* raw number `0.375→38`). The "earlier ~63%" render was the same prompt computed correctly; the flip to 38% is **LLM non-determinism**, with PR #4f as the change that made the failure mode possible. The brief's "code emits `percentile` directly" hypothesis is **not borne out** — the exposure is the delegated arithmetic, not an inverted constant. F1/F2/F4 did not touch the verbalization line.

---

## Item 5 — Two-endpoint / deployment check (context only)

- `8.213.84.191` appears twice: `.github/workflows/smoke-pr2b.yml:23` (`default: "http://8.213.84.191"` — production smoke host) and `scripts/diagnostics/frontend_decision_memo_2026-05-08/findings.md` (a pre-#4f diagnostic, dated 2026-05-08, i.e. before the lay-friendly phrasing existed).
- `8.213.28.129` appears **nowhere** in the repo (workflows, Terraform/IaC, manifests, docs, config — all empty).

**Finding:** The repo contains **no evidence of two distinct builds / LBs / services**. There is no infra artifact tying the two IPs to different deployments. Whether `8.213.28.129` and `8.213.84.191` run different builds is **not answerable from the repo** — flagging for Ahmed as an infra/runtime question. Note the only repo-visible "old" rendering (`38th percentile`-style) predates #4f and used `8.213.84.191`.

---

## Item 6 — Blast radius of the eventual fix

| Consumer | File:line | Impact of a direction/phrasing fix |
|----------|-----------|-----------------------------------|
| EN prompt rules + worked examples | `llm_decision_memo.py:1364-1388, 1469-1611` | Already correct; examples use `0.28→72%`. If we add a deterministic phrase, prompt text changes. |
| AR Rule 8 + AR worked example | `llm_decision_memo.py:1884-1895, 1916-1932` | Already correct (100−pct). |
| Frontend `pctFromFraction` + i18n | `AdvisorySectionCards.tsx:28-39`, `en.json/ar.json:1314-1317` | Already correct; **no change needed.** |
| API raw exposure | `financial_framing.rent_percentile_vs_comparables` (raw `0.375`) | Stays raw; frontend re-renders. No change. |
| **EN byte-identity snapshot** | `tests/data/pr4a_structured_memo_system_prompt_en_head.txt` | **Will need regeneration** if any prompt bytes change. |
| Grounding/golden tests | `tests/services/test_llm_decision_memo_grounding.py:326-382`, `tests/test_llm_decision_memo.py:300-349,543` | Use **mock** LLM outputs already at correct values (`0.28→72%`, `0.22→78%`) and assert raw-fraction pass-through (`== 0.28`). They will **not "move"** on a direction fix; only payload-shape assertions need touching if a phrase field is added. |
| Cache | `MEMO_PROMPT_VERSION = "v10-…"` (`llm_decision_memo.py:52`) | **Must bump** so cached memos (including listing 6545795) regenerate. |

There are **no golden memos that are themselves regenerated by the LLM** — the "cheaper than X%" strings in fixtures are hand-written mocks, so a real fix won't invalidate them automatically. The one true regeneration target is the **EN prompt snapshot** + a **version bump**.

---

## Proposed minimal fix (described, not applied)

Because every deterministic surface is already correct and the only leaky surface is **LLM-performed arithmetic**, the robust minimal fix is to **stop asking the LLM to compute the percentage**:

1. In `_build_structured_user_payload` (right where `rent_percentile` is read, `llm_decision_memo.py:1192/1216`), compute the verbalized phrase **once, deterministically**, mirroring `pctFromFraction` exactly:
   - `pct = round(percentile * 100)`
   - LOW (`pct < 40`): `"cheaper than about {100 - pct}% of {scope} comparables"`
   - MID (`40 ≤ pct ≤ 60`): `"around the district median rent"` (no number)
   - HIGH (`pct > 60`): `"more expensive than about {pct}% of {scope} comparables"`
   - `scope` from `comparable_source_label` (district / citywide-band-type / citywide).
2. Add it to the payload as a ready-made string (e.g. `financial_framing.rent_positioning_phrase`) and change the prompt rule from *"compute N = round((1 − fraction) × 100)"* to *"copy `rent_positioning_phrase` verbatim; do not recompute."* This single source covers **both** `headline_recommendation` and `key_evidence` because they read the same context.
3. Bump `MEMO_PROMPT_VERSION` (v10 → v11) and regenerate `tests/data/pr4a_structured_memo_system_prompt_en_head.txt`.

(A smaller, less robust alternative — keep delegating to the LLM but add an explicit anti-inversion worked example near `0.375` — leaves the arithmetic with the model and does not close the non-determinism gap. Not recommended.)

**Introducing commit:** `7f5e8a81d` (PR #4f, 2026-05-20) — created the "cheaper than N%" template that depends on an LLM-side inversion. Formula text is correct; the exposure is the delegation.

## Validation plan

1. Bump version, regenerate memo for **listing 6545795** → expect `"cheaper than ~63%"` (or ~62%), and confirm internal consistency with its own **"14% below the SAR 339,982 median"** line and `burden_score 70`.
2. Assert the invariant programmatically: for any percentile-mode listing, `listing_monthly_rent_per_m2 < median_monthly_rent_per_m2` ⟺ rendered "cheaper than > 50%".
3. Spot-check 2–3 more percentile-mode listings across zones: one LOW (`<0.40` → "cheaper than ~N%", N>50), one MID (`0.40–0.60` → "around the district median rent", no number), one HIGH (`≥0.61` → "more expensive than ~N%"), each cross-checked against its median relationship.
4. Re-run `tests/services/test_llm_decision_memo_grounding.py`, `tests/test_llm_decision_memo.py`, frontend `AdvisorySectionCards.test.tsx`, and the EN prompt-snapshot test.

**Merge recommendation (for the follow-up implementation PR):** Low risk — additive payload field + prompt copy-instruction + version bump; deterministic surfaces unchanged; closes an LLM-arithmetic correctness gap that currently can flip a memo's central economic claim.
