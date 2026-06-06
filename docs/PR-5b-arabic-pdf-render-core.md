# PR-5b — Arabic Feasibility PDF: Font + Shaping + RTL (Render Core)

**Branch:** `claude/arabic-pdf-font-shaping-rtl-OsZhe`
**Base:** PR-5a (`PR-5a: Arabic PDF scaffolding (backend, EN-safe, AR latent)`, commit `ad6d300a0`, open PR #1278)
**Commit:** `67cecfffc`
**Status:** Pushed to origin. No PR opened (per instructions).
**Date:** 2026-06-06

---

## 1. Context & What Was Wrong

The feasibility (Estimator) PDF is rendered server-side in `app/services/pdf.py`. PR-5a
threaded a `lang` parameter through `build_memo_pdf`, routed every hardcoded English label
through a new EN/AR table (`app/services/estimator_i18n.py`), and selected the persisted
`*_ar` narratives — **but deliberately left the ASCII gates in place**, so the Arabic path
was *latent*: any Arabic text was stripped to blank/garbled, there was no Arabic font, no
contextual shaping (joining), and no right-to-left layout.

PR-5b is the **render core**: it makes the AR path actually produce a correct Arabic PDF
(font embedding + shaping + BiDi + RTL layout), while keeping the **EN PDF byte-identical**.

### Repo-state note (important)

The task was written assuming "current HEAD (post-5a)". In reality PR-5a (#1278) was **not
merged to `main`**, and this branch was cut from `main`. I fast-forwarded the 5a commit onto
this branch first, then implemented 5b on top. Consequences:

- This branch's history contains the 5a commit `ad6d300a0` followed by the 5b commit.
- When #1278 merges to `main`, the delta of this branch becomes exactly the single 5b commit.
- The "5a EN-lock + byte-identity tests" referenced in the task are the 5a artifacts
  (`tests/test_estimator_i18n.py` with `EXPECTED_EN`) — they are present and stay green.

---

## 2. Scope Delivered

### 2.1 Vendor the font (committed)

Added under `app/services/assets/fonts/`:

| File | Size | Notes |
|---|---|---|
| `NotoNaskhArabic-Regular.ttf` | 200,416 B | static instance, wght=400 |
| `NotoNaskhArabic-Bold.ttf` | 200,556 B | static instance, wght=700 |
| `OFL.txt` | 4,382 B | SIL Open Font License v1.1 |

**Why these specific files (a real gotcha):** the font the project already references for map
glyphs — `googlefonts/noto-fonts/.../hinted/ttf/NotoNaskhArabic-Regular.ttf` — is an
**Arabic-only subset**. Its cmap is **missing** `(` `)`, all Latin letters (A–Z/a–z), `%`,
`-`, `+`, `/`, `×`, `÷`, `−`. These mixed AR/Latin labels need all of those (e.g.
`ROI غير المموّل`, `NLA × معدل الإيجار`, `NOI ÷ إجمالي…`, `(غير محسوب في معامل الكثافة)`,
`SAR/m2`, percentages). Using that subset would silently drop those glyphs.

**Resolution:** I took the **full** Noto Naskh Arabic from `google/fonts` (which has full
Latin + punctuation coverage) but it is a *variable* font (no real static Bold). I used
`fontTools.varLib.instancer` to produce static **Regular (wght=400)** and **Bold (wght=700)**
instances — full glyph coverage **and** a genuine bold weight for headers/titles.

The TTFs are committed because the build-time `curl` used for map glyphs is **not available to
the server at render time** (per the task and `scripts/build-glyphs.sh`).

**Known coverage gap (deferred to PR-5c):** `²` (U+00B2) is not in Noto Naskh Arabic, so the
`م²` unit (from `explanations_ar` and the price header) currently drops its superscript-two.
This is consistent with the task's note that units stay "visibly inconsistent for now"; the
digit/unit policy is PR-5c.

### 2.2 Dependencies

`requirements.txt`:

```
arabic-reshaper>=3.0.0   # Arabic contextual shaping for the AR feasibility PDF (PR-5b)
python-bidi>=0.4.2       # BiDi reordering for the AR feasibility PDF (PR-5b) — NOTE: LGPL-licensed
```

> ⚠️ **Deps-review flag:** `python-bidi` is **LGPL-licensed**. Raised here for your license review.

(Floors use `>=` to match the existing repo convention — every other line in
`requirements.txt` is `>=`. Validated against `arabic-reshaper 3.0.0`, `python-bidi 0.6.10`.)

### 2.3 Register + select font by lang (`pdf.py`)

- A per-document family resolver stamps the language and font family onto the PDF object:
  ```python
  pdf._oak_lang = lang
  pdf._oak_font_family = AR_FONT_FAMILY if lang == "ar" else FONT_FAMILY
  if lang == "ar":
      _register_ar_fonts(pdf)   # add_font(..., uni=True) for "" and "B"
  ```
- Every `set_font(FONT_FAMILY, …)` call site now reads `_doc_family(pdf)` →
  **`Helvetica` for EN (unchanged), `NotoNaskh` for AR**. EN never registers/loads the AR font.

### 2.4 Shaping + BiDi helper

```python
def _shape_ar(text: str) -> str:
    # reshape = contextual joining; get_display = BiDi reordering to visual order
    return _bidi_get_display(arabic_reshaper.reshape(text))
```

- Applied to **all** AR text **immediately before drawing** — after composition and after
  ellipsizing — because BiDi reordering depends on the final string.
- EN text is **never** shaped.
- Imports are guarded (`try/except ModuleNotFoundError`) and `bidi.algorithm.get_display`
  falls back to the top-level `bidi.get_display` (python-bidi ≥ 0.5). If the libs are absent,
  `_shape_ar` returns the text unchanged — the EN path never needs them.

### 2.5 Lifted the ASCII gates (AR path only)

Made the four gates lang-aware. For `lang=="ar"` they pass Arabic through (shaping happens at
draw time); for EN they keep the **exact** prior ASCII/latin-1 behavior byte-for-byte:

| Function | EN behavior (unchanged) | AR behavior (new) |
|---|---|---|
| `_pdf_safe_text(v, lang)` | strip non-ASCII → latin-1 | `_shape_ar(str(v))` (draw-time shaper) |
| `_short_note(n, …, lang)` | strip non-ASCII, truncate | keep Arabic, truncate (shaped later) |
| `_resolve_ascii(v, lang)` | `""` if non-ASCII | pass through (shaped later) |
| `_strip_non_ascii` | unchanged (EN-only callers) | — |

`lang` is threaded into every call site (labels **and** data: `summary_ar`,
`explanations_ar`, comps city/district, assumption keys/units/sources).

### 2.6 RTL layout (AR path only) — minimum-viable

- **`_draw_table`**: when `lang=="ar"`, mirror column **order + widths**, swap **L↔R**
  alignment (`_flip_align`), and reverse each row's cells. Per-cell BiDi handles intra-cell
  ordering. **Crucial ordering detail:** AR cells are ellipsized on the *raw* (logical) string
  and shaped *after* truncation, so joining/BiDi stay correct.
- **Totals metric strip**: drawn right-to-left (cells are center-aligned, so only order is
  mirrored).
- **Parking block**: value cell on the left, label cell on the right (mirror of EN).
- **Section titles & document title**: right-aligned for AR.
- **Executive summary narrative**: a new `_draw_rtl_paragraph` greedily wraps the *logical*
  text by rendered width, shapes each line independently, and draws it right-aligned. This is
  necessary because fpdf's `multi_cell` wraps in logical order and cannot BiDi-reorder a
  paragraph. (No full right-origin rework — minimum-viable per the task.)

### 2.7 AR-table corrections in `estimator_i18n.py` (AR-only; EN-lock stays green)

| Token | Before (AR) | After (AR) |
|---|---|---|
| `upper_annex_non_far_bua` | `…غير محسوب في FAR، +0.5 طابق…` | `…غير محسوب في معامل الكثافة، +0.5 طابق…` |
| `upper_annex_non_far_cost` | `…غير محسوب في FAR…` | `…غير محسوب في معامل الكثافة…` |
| `far_model_prior` | `FAR (الأولوية النموذجية)` | `معامل الكثافة (القيمة المسبقة للنموذج)` |
| `unlevered_roi` | `العائد غير الممول` | `ROI غير المموّل` |

- FAR → `معامل الكثافة` matches the v2 standardization (no Latin `FAR` token left in any AR
  value). `far_model_prior` also fixes the prior mistranslation (`الأولوية النموذجية` =
  "model priority", wrong for statistical "prior").
- `unlevered_roi` avoids colliding with Yield (on-screen `العائد`); keeps the `ROI` token as v2 does.
- `construction_direct` **kept** as `الإنشاء (مباشر)`. **Flag (not changed here):** the
  on-screen financial-detail tab uses `التنفيذ المباشر` for the same concept — an app-wide
  terminology reconciliation, deferred.
- These are AR-value-only edits: `EXPECTED_EN` and the token set are untouched, so the 5a
  EN-lock (`test_en_values_match_pre_pr5a_literals`, `test_no_unexpected_tokens_without_en_lock`)
  stay green.

---

## 3. Files Changed

```
 app/services/assets/fonts/NotoNaskhArabic-Bold.ttf    | (new, binary)
 app/services/assets/fonts/NotoNaskhArabic-Regular.ttf | (new, binary)
 app/services/assets/fonts/OFL.txt                     | (new)
 app/services/pdf.py                                   | +193 / −53
 app/services/estimator_i18n.py                        |   4 AR values
 requirements.txt                                      |  +2
 tests/test_export_pdf.py                              |  +2  (5a-regression repair)
 tests/test_pr5b_arabic_pdf_render.py                  | (new)
```

---

## 4. Validation

### 4.1 EN byte-identity (the central safety property)

Generated the EN PDF for a comprehensive fixture (every section/branch: cost + revenue
tables, parking, executive summary, assumptions incl. the `far` special-case, calc-trace
appendix, top comps) with the **post-5b** code, then with the **5a baseline `pdf.py`** swapped
in, normalized volatile metadata (`/CreationDate`, `/ID`), and compared:

```
EN bytes (5b): 15269
EN bytes (5a): 15269
EN byte-identity (metadata-normalized): IDENTICAL ✓
```

Confirmed EN **never** embeds NotoNaskh, **never** shapes, **never** flips layout
(asserted in tests).

### 4.2 Test suite

```
212 passed   (test_pr5b_arabic_pdf_render.py, test_estimator_i18n.py,
              test_pdf_labels.py, test_export_pdf.py, test_pdf_excel_mode.py)
```

New `tests/test_pr5b_arabic_pdf_render.py` guards: EN never embeds Naskh / never shapes;
EN omitted-lang == explicit-en; AR embeds **both** faces (2× `FontFile2`); `_shape_ar`
reshapes to Arabic Presentation Forms (U+FB50–U+FEFF) and reorders; lang-aware gates
strip on EN / pass on AR; `_flip_align` mirroring.

**Pre-existing 5a regression repaired:** `test_export_pdf.py`'s `build_memo_pdf` test double
did not accept the `lang` kwarg that PR-5a added to the real `export_pdf` call. It was failing
on the 5a baseline (verified by reverting source). Fixed by aligning the double's signature
(`lang="en"`); also captures `lang` for assertion. No business-logic change.

### 4.3 AR render (the point of this PR) — validated from the rendered PDF

Generated a real AR PDF for the fixture (carrying `summary_ar` **and** `explanations_ar`) and
**rasterized** both pages at 150 dpi (delivered as `ar_p1.png`, `ar_p2.png`). Confirmed by eye:

- Arabic glyphs are correctly **joined and shaped** (presentation forms, not isolated letters).
- Text flows **right-to-left**; mixed Latin tokens (`ROI`, `NLA`, `OPEX`, IDs, dates, numbers)
  sit correctly within the BiDi order.
- Table **columns are mirrored** (`البند` on the right, `طريقة الحساب` on the left); totals
  strip and parking block mirrored; titles right-aligned.
- The AR **executive-summary narrative** wraps over multiple lines and renders right-aligned.
- The corrected terms render: `معامل الكثافة (القيمة المسب…)` for the FAR prior, `ROI غير المموّل`.

Font embedding confirmed: `b"NotoNaskhArabic" in out`, **2× `/FontFile2`** streams (Regular +
Bold subsets), AR PDF materially larger than EN (font embedded). OFL license committed.

### 4.4 Arabic byte hygiene

```
estimator_i18n.py — U+06CC (Farsi yeh): 0   U+06BE (heh-doachashmee): 0
pdf.py            — U+06CC: 0   U+06BE: 0
AR values still containing Latin "FAR": []
```

All four corrected AR strings verified byte-for-byte.

### 4.5 Lint / formatting

`flake8` (default) reports `E501` on the new lines — but the 5a baseline `pdf.py` already has
44 such violations and there is **no `.flake8`/`setup.cfg`/`pyproject` config**, and CI does
not run flake8. `black` would reformat, but the **5a baseline is itself not black-clean**, so
running it would churn unrelated pre-existing lines. Per "smallest patch / match surrounding
code", I matched the file's established long-line style rather than introduce a large
formatting diff.

---

## 5. Out of Scope (→ PR-5c)

- Number/unit digit policy: Arabic-Indic vs Latin digits; reconciling `ر.س`/`م²` vs `SAR`/`m²`
  across labels + formatters. **The `م²` `²` glyph currently drops** (font has no U+00B2) — a
  unit-glyph issue to settle in 5c.
- Frontend `memoPdfUrl` `?lang` exposure (AR stays **unexposed** — validated only by generating
  a PDF directly, never via the UI).
- On-screen formatting pass; the `construction_direct` ↔ `التنفيذ المباشر` terminology
  reconciliation.

---

## 6. Risk Assessment

**Low.** EN output is provably byte-identical to the 5a baseline; every behavior change is
gated on `lang == "ar"`, which remains unexposed in the product. The only new runtime
dependencies (`arabic-reshaper`, `python-bidi`) are import-guarded and required solely on the
AR path. Reviewer attention items: (1) the **LGPL** of `python-bidi`; (2) the deferred `م²`
`²` glyph and unit inconsistency (expected, 5c); (3) this branch carries the unmerged PR-5a
commit as its base.

## 7. Merge Recommendation

Mergeable after PR-5a (#1278) lands. The EN lock + parity tests are green, the AR render is
verified from the rasterized output, and the diff is additive and AR-gated.

---

## 8. Addendum — Review Round 1 (git state, rebase plan, red-test, 5c note)

### 8.1 Git state — verified against `origin/main`

`origin/main` is at `ac5a8b435`. **PR-5a (#1278) is NOT merged to main**: `estimator_i18n.py`
does not exist on main and `pdf.py` has no `lang` parameter / no `_shape_ar`. Verified:

```
git ls-tree origin/main -- app/services/estimator_i18n.py      # (empty → file absent)
git grep -c 'lang: str = "en"' origin/main -- app/services/pdf.py   # no lang param on main
git merge-base --is-ancestor ad6d300a0 origin/main             # NO — 5a commit not on main
```

Clarification of "merged 5a": last round I `git merge --ff-only`'d the **5a branch into this
working branch**, i.e. carried the 5a commit `ad6d300a0` onto the stack — **not** a merge of
5a into main. The branch is therefore a correct stack:

```
ac5a8b435  (origin/main HEAD)
   └─ ad6d300a0  PR-5a: Arabic PDF scaffolding (carried; == #1278 head)
        └─ 67cecfffc  PR-5b: font + shaping + RTL
             └─ a75e247ec  docs: PR-5b report
```

**5b-only delta is clean** — `git diff --stat ad6d300a0 HEAD` shows only 5b files, and
`estimator_i18n.py` appears as a **4-value modification**, not a re-introduction of the file
(proof it sits on top of 5a, not a re-application of it):

```
 app/services/assets/fonts/NotoNaskhArabic-Bold.ttf    | Bin
 app/services/assets/fonts/NotoNaskhArabic-Regular.ttf | Bin
 app/services/assets/fonts/OFL.txt                     |  93 ++
 app/services/estimator_i18n.py                        |   8 +-
 app/services/pdf.py                                   | 246 ++++--
 docs/PR-5b-arabic-pdf-render-core.md                  | 272 ++++++
 requirements.txt                                      |   2 +
 tests/test_export_pdf.py                              |   2 +
 tests/test_pr5b_arabic_pdf_render.py                  | 139 +++
```

I deliberately did **not** rebase onto current main: with 5a still open, a rebase would *drop*
`ad6d300a0`, leaving 5b's `pdf.py` importing a non-existent `estimator_i18n.py` — broken.

### 8.2 Rebase plan (run when #1278 lands)

If #1278 is **squash-merged**, main gets a new SHA ≠ `ad6d300a0`, so merging 5b as-is could
re-apply / conflict on 5a's content. Correct procedure:

```bash
git fetch origin main
# Replant ONLY the 5b commits; 5a's content now arrives via main's squash:
git rebase --onto origin/main ad6d300a0 claude/arabic-pdf-font-shaping-rtl-OsZhe
git diff origin/main...HEAD --stat   # confirm delta == 5b files only (no 5a re-application)
make test                            # full suite in Codespace (see 8.3)
git push --force-with-lease
```

(If #1278 is merge-committed rather than squashed, `ad6d300a0` becomes an ancestor of main and
a plain `git merge origin/main` / fast-forward is enough; the `--onto` rebase is still safe.)

### 8.3 "Red test on main" nuance

`test_export_pdf.py::test_export_pdf_includes_excel_breakdown_from_wrapped_notes` only goes red
**once 5a is on main** — 5a added `lang=lang` to the real `export_pdf` call but did not update
the test's `build_memo_pdf` double. Today main is green there (5a unmerged); 5b carries the
fix. Note this test runs **without a real DB** (it overrides `get_db` with a `DummySession`),
so it *was* executable in this environment and passes — the full PDF/i18n slice is **212
passed**. Action: run full `make test` in Codespace immediately after the rebase to confirm the
entire suite (not just the PDF slice).

### 8.4 `م²` / U+00B2 — confirmed font-coverage, queued for 5c

This is **font coverage, not AR-vs-Latin**: Noto Naskh has no U+00B2, so even a Latin `m²`
would drop its superscript in the AR document. The 5c fix is to **avoid U+00B2 entirely** and
render the unit with a **baseline 2** — ASCII `2` or Arabic-Indic `٢` per the digit policy,
both present in the vendored font. Folded into the 5c scope alongside the digit/unit policy.

### 8.5 Still-open inputs for 5c (the final PR)

1. **Digit/unit policy** — Arabic-Indic vs Latin digits; reconcile `ر.س`/`م²` vs `SAR`/`m²`
   across labels + formatters; now also absorbing the U+00B2 → baseline-2 fix.
2. **The `_ar [DB]` check** — confirm the AR narrative source columns.
3. After both: the frontend `memoPdfUrl` `?lang` exposure that flips AR live (kept unexposed
   through 5b).

`python-bidi` (LGPL) remains flagged for the deps-review decision.
