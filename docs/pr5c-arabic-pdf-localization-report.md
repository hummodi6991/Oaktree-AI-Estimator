# PR-5c — Arabic PDF number/unit policy + leak fixes + go-live

**Branch:** `claude/arabic-pdf-localization-eYjM2`
**Base:** HEAD = `55ec4efe5` (5a + 5b on `main`)
**Commit:** `5339a2db9` — *PR-5c: Arabic PDF number/unit policy, leak fixes, frontend go-live*

This is the PR that **exposes Arabic** end-to-end (frontend `?lang`). EN output
stays **byte-identical**: every number/unit/label change is gated on
`lang == "ar"`.

---

## 1. What was wrong

The 5b render core gave us correct font embedding + shaping + RTL, but turning
on the AR path exposed four content problems that a rendered/rasterized AR PDF
made obvious:

1. **Numbers were Latin.** `1,000,000` / `3.250` / `9.2%` rendered with ASCII
   digits even on the AR page.
2. **Units were Latin / broken.** `SAR`, `m2`, `SAR/m2` stayed English, and the
   squared-metre superscript (`م²`, U+00B2) **silently dropped its 2** because
   the embedded Naskh face has no U+00B2 glyph — so areas read `م` with no `2`.
3. **Two English leaks.**
   - Income-component rows (revenue table + calc-trace appendix) rendered
     `residential rent` / `retail rent` / `الدخل: residential rent`.
   - Key-assumption keys rendered raw English (`land_price`, `rent_rate`, …);
     only `far` was special-cased.
4. **AR labels truncated mid-word.** Arabic labels are longer than the EN labels
   the column widths were tuned for, so the standard label set cut mid-word
   (`…الفعّال (فو…`, `…(القيمة المسب…`).

And the frontend never sent a locale, so even a correct AR PDF was unreachable.

## 2. Why it happened

- The formatters (`_fmt_money`, `_fmt_percent`, …) and the inline unit tokens
  were lang-agnostic — they always produced ASCII digits and Latin units.
- The income-component label was `str(key).replace("_", " ")` (English) and the
  assumption key was the raw key string (English), with no i18n routing.
- `_draw_table` used a single set of EN-tuned column widths/char-budgets/font
  size for both languages.
- `memoPdfUrl()` built a URL with no `?lang`, unlike `exportCsvUrl()`.

## 3. The fix (smallest safe diff)

### 3.1 Number + unit formatting — `app/services/pdf.py` (AR path only)

New module-level primitives:

```python
# Eastern Arabic numerals (٠١٢٣٤٥٦٧٨٩). Grouping (",") and decimal (".")
# separators are kept for legibility.
_AR_DIGITS = str.maketrans("0123456789", "٠١٢٣٤٥٦٧٨٩")

# AR unit tokens. The squared-metre superscript renders as a baseline ٢
# (never U+00B2 — the embedded Naskh face lacks the superscript glyph).
_AR_UNIT_MAP = {
    "SAR": "ر.س",
    "m2": "م٢", "m²": "م٢",
    "SAR/m2": "ر.س/م٢", "SAR/m²": "ر.س/م٢",
    "SAR/m2/yr": "ر.س/م٢/سنة",
    "SAR/m2/mo": "ر.س/م٢/شهر",
    "%": "٪",
}

def _ar_digits(text: str) -> str:
    return text.translate(_AR_DIGITS)

def _localize_unit(unit, lang="en") -> str:
    if unit is None:
        return ""
    text = str(unit)
    if lang != "ar":
        return text                      # EN unchanged
    if text in _AR_UNIT_MAP:
        return _AR_UNIT_MAP[text]
    # general fallback: rewrite tokens, kill any stray U+00B2, Arabize digits
    text = text.replace("SAR", "ر.س").replace("m²", "م٢").replace("m2", "م٢")
    text = text.replace("²", "٢").replace("/yr", "/سنة").replace("/mo", "/شهر")
    return _ar_digits(text)
```

Every formatter became lang-aware and branches only when `lang == "ar"`:

```python
def _fmt_money(x, lang="en"):
    if x is None: return _label("na", lang)          # EN: "N/A" (identity)
    out = f"{float(x):,.0f}"
    return _ar_digits(out) if lang == "ar" else out

def _fmt_percent(x, digits=1, lang="en"):
    if x is None: return _label("na", lang)
    out = f"{float(x) * 100:.{digits}f}%"
    return _ar_digits(out).replace("%", "٪") if lang == "ar" else out

def _format_amount(value, unit="SAR", lang="en"):
    if unit == "SAR":
        return _fmt_money(value, lang)
    return f"{_fmt_number(value, lang)} {_localize_unit(unit, lang)}"
```

(`_fmt_number` and `_fmt_decimal` follow the same shape.) `lang` is threaded
through every call site (cost/revenue/assumption/comps rows, the totals metric
strip, the parking summary). The comps row's hardcoded `SAR/m2` now routes
through `_localize_unit('SAR/m2', lang)` — EN returns `SAR/m2` unchanged.

> **EN identity note:** `_label("na", "en") == "N/A"`, so the `None` path is
> byte-identical to the old `return "N/A"`.

### 3.2 Label/unit reconciliation — `app/services/estimator_i18n.py`

Fixed the one AR label that embedded the broken superscript:

```diff
- "header_price_sar_m2": {"en": "Price (SAR/m2)", "ar": "السعر (ر.س/م²)"},
+ "header_price_sar_m2": {"en": "Price (SAR/m2)", "ar": "السعر (ر.س/م٢)"},
```

`totals_section` AR (`الإجماليات (ر.س)`) already agreed with the amounts.

### 3.3 Income-component leak — `estimator_i18n.py` + `pdf.py`

New tokens (`income_component.*`) + helper. EN is byte-identical to the prior
`str(key).replace("_", " ")` for **every** key; AR reuses the asset-class
terminology and passes unknown keys through humanized:

```python
def income_component_label(key, lang="en"):
    if lang != "ar":
        return str(key).replace("_", " ")     # EN identity
    canon = str(key).strip().lower()
    if canon.endswith("_rent"):
        canon = canon[: -len("_rent")]         # residential_rent → residential
    entry = LABELS.get(f"income_component.{canon}")
    return entry["ar"] if entry and entry.get("ar") else str(key).replace("_", " ")
```

| key (canonical) | EN (unchanged) | AR |
|---|---|---|
| `residential` / `residential_rent` | `residential` / `residential rent` | سكني |
| `retail` / `retail_rent` | `retail` / `retail rent` | تجاري |
| `office` / `office_rent` | `office` / `office rent` | مكتبي |
| `commercial` | `commercial` | تجاري |
| `parking_income` | `parking income` | دخل المواقف |
| *(unknown)* | humanized passthrough | humanized passthrough |

Wired into both `_build_revenue_breakdown_rows` (revenue table) and
`_build_appendix_rows` (calc-trace), replacing `str(key).replace("_", " ")`.

### 3.4 Assumption-key leak — `estimator_i18n.py` + `pdf.py`

New tokens (`assumption.*`) + helper. EN returns the raw key (current behavior);
AR maps the enumerated real key set; unknown keys pass through unchanged:

```python
def assumption_key_label(key, lang="en"):
    if not key:
        return key or ""
    if lang != "ar":
        return key                              # EN identity = raw key
    entry = LABELS.get(f"assumption.{key}")
    return entry["ar"] if entry and entry.get("ar") else key
```

In `_build_assumption_rows`, `far` keeps its existing special-case
(`far_model_prior`); all other keys route through `assumption_key_label`, and AR
units localize via `_localize_unit` (EN keeps the exact ASCII-strip behavior, so
`m²` still drops in EN as before):

```python
if key.lower() == "far":
    key = _label("far_model_prior", lang)
else:
    key = assumption_key_label(key, lang)
...
unit_text = _localize_unit(unit, lang) if lang == "ar" else _resolve_ascii(unit, lang)
```

Key set enumerated from the real payloads (`app/api/estimates.py`,
`app/services/revenue.py`):

| key | AR |
|---|---|
| `land_price` | سعر الأرض |
| `rent_rate` | معدل الإيجار |
| `excel_method` | طريقة إكسل |
| `site_area_m2` | مساحة الموقع (م٢) |
| `ppm2` | سعر المتر المربع |
| `real_estate_price_index_scalar` | معامل مؤشر أسعار العقار |
| `parking_required_spaces` | المواقف المطلوبة |
| `parking_provided_spaces` | المواقف المتوفرة |
| `parking_supply_gross_m2_per_space` | إجمالي مساحة الموقف (م٢/موقف) |
| `parking_supply_layout_efficiency` | كفاءة تخطيط المواقف |
| `avg_unit_size_residential_m2` | متوسط مساحة الوحدة السكنية (م٢) |
| `avg_unit_size_retail_m2` | متوسط مساحة الوحدة التجارية (م٢) |
| `avg_unit_size_office_m2` | متوسط مساحة الوحدة المكتبية (م٢) |
| `sale_price_per_m2` | سعر البيع لكل م٢ |
| `rent_per_m2` | الإيجار لكل م٢ |
| `avg_unit_m2` | متوسط مساحة الوحدة (م٢) |
| `occ` | نسبة الإشغال |
| `op_ex_ratio` | نسبة المصاريف التشغيلية |
| `cap_rate` | معدل الرسملة |
| `parking_extra_spaces_monetized` | المواقف الإضافية المستثمَرة |
| `parking_monthly_rate_sar_per_space` | الإيجار الشهري للموقف (ر.س/موقف) |
| `parking_occupancy` | إشغال المواقف |
| `parking_public_access` | وصول عام للمواقف |
| `parking_income_y1` | دخل المواقف (السنة الأولى) |
| *(unknown, e.g. `zoning`)* | passthrough unchanged |

### 3.5 AR label truncation — `_draw_table` in `pdf.py`

Done on the logical-order lists **before** the RTL mirroring, AR only:

```python
table_font_size = 9
if lang == "ar":
    table_font_size = 8                      # one step down
    if len(col_widths) >= 2:
        label_min_w = 80.0
        if col_widths[0] < label_min_w:
            donor = max(range(1, len(col_widths)), key=lambda i: col_widths[i])
            extra = label_min_w - col_widths[0]
            if col_widths[donor] - extra >= 20.0:
                col_widths[0] = label_min_w   # widen label col from donor
                col_widths[donor] -= extra
        if max_chars_list:
            max_chars_list[0] = max(max_chars_list[0], 50)   # higher char budget
    headers_list.reverse(); col_widths.reverse(); ...        # existing mirroring
```

EN is untouched (`table_font_size` stays 9, no width/char changes).

### 3.6 Frontend go-live — `frontend/src/api.ts`

```ts
import i18n from "./i18n";

function normalizeLang(language?: string): "en" | "ar" {
  return (language || "").toLowerCase().startsWith("ar") ? "ar" : "en";
}

export function memoPdfUrl(estimateId: string) {
  const encodedId = encodeURIComponent(estimateId);
  const lang = normalizeLang(i18n.language);
  return withBase(`/v1/estimates/${encodedId}/memo.pdf?lang=${lang}`);
}
```

`downloadMemoPdf` calls `memoPdfUrl`, so it inherits `?lang`. The caller at
`App.tsx:444` is unchanged. EN locale → `?lang=en` (unchanged PDF); AR locale →
`?lang=ar` (the Arabic PDF). The backend endpoint already accepts
`lang: Literal["en","ad"]` (`app/api/estimates.py`).

## 4. Files touched

```
 app/services/estimator_i18n.py | 122 +++++++++++++++++++++-   tokens + 2 helpers + U+00B2 label fix
 app/services/pdf.py            | 185 +++++++++++++++++--------   formatters + units + truncation + threading
 frontend/src/api.ts            |  10 ++-                         ?lang on memoPdfUrl
 tests/test_estimator_i18n.py   |  31 +++++                       EXPECTED_EN extended (EN-lock contract)
 4 files changed, 289 insertions(+), 59 deletions(-)
```

## 5. Validation

### 5.1 EN byte-identity (the hard gate)

Built the EN PDF for a comprehensive fixture against the working tree vs a
`HEAD` (5a+5b) git worktree, metadata-normalized (`/CreationDate`, `/ID`):

```
13381 /tmp/en_new.pdf
13381 /tmp/en_head.pdf
cmp → BYTE-IDENTICAL ✓
```

### 5.2 AR (rasterize + eyeball, not a diff)

Generated a real AR PDF for the comprehensive fixture, rasterized at 150 dpi
(PyMuPDF). Confirmed on the rendered pages:

- All numbers are Eastern-Arabic numerals (`٣.٢٥٠`, `٧١,٣٥٠,٠٠٠`, `٩.٢٪`).
- Units render `ر.س` / `م٢` / `٪` / `ر.س/م٢` / `ر.س/م٢/سنة` with the **2 visible**
  (baseline `٢`, no dropped superscript).
- Income-component rows: سكني / تجاري / مكتبي / دخل المواقف (no `residential rent`).
- Calc-trace appendix: الدخل: سكني / تجاري / مكتبي / دخل المواقف.
- Assumption keys: سعر الأرض / معدل الإيجار / مساحة الموقع (م٢) / سعر المتر المربع /
  طريقة إكسل / معدل الرسملة; unknown `zoning` passes through.
- No mid-word truncation: `معامل الكثافة (القيمة المسبقة للنموذج)` and
  `معامل الكثافة الفعّال (فوق الأرض)` now fit fully.
- Shaping / RTL still correct (5b regression check).

Pre-shape trace (logical strings handed to the renderer):

```
pre-shape contains م٢:        True
pre-shape contains ر.س/م٢:    True
pre-shape contains ر.س:       True
pre-shape contains U+00B2:    False
سكني → ['مساحة البناء السكنية', 'سكني']        تجاري → [..., 'تجاري']
مكتبي → [..., 'مكتبي']        دخل المواقف → ['دخل المواقف', 'الدخل: دخل المواقف']
سعر الأرض → ['سعر الأرض']     معدل الإيجار → ['NLA × معدل الإيجار']
م٢/موقف → ['إجمالي مساحة الموقف (م٢/موقف)']    مساحة الموقع (م٢) → ['مساحة الموقع (م٢)']
```

Drawn-string trace (post-shape, what actually hits the page):

```
strings containing U+00B2: []      ← zero U+00B2 in rendered output
has Eastern-Arabic digits: True
has ٪: True
```

### 5.3 AR byte hygiene

```
estimator_i18n.py: U+06CC 0  U+06BE 0
pdf.py:            U+06CC 0  U+06BE 0
LABELS ar values containing U+00B2: []
```

The only U+00B2 left in source are **match-targets** (`_AR_UNIT_MAP` keys and the
`.replace("m²", …)` / `.replace("²", …)` in `_localize_unit`) and docstrings —
i.e. the code that *consumes* U+00B2 and emits baseline `٢`. None reach output.

### 5.4 Tests

```
tests/test_estimator_i18n.py
tests/test_pdf_labels.py
tests/test_pdf_excel_mode.py
tests/test_pr5b_arabic_pdf_render.py
tests/test_export_pdf.py
→ 299 passed
```

`EXPECTED_EN` in `test_estimator_i18n.py` was extended for the new tokens
(income-component EN = humanized key; assumption EN = raw key) so the EN
byte-identity lock and the "no unexpected tokens" parity test stay explicit and
green. CI runs `pytest` only — there is no black/flake8 gate, and the committed
HEAD files are not black-formatted, so the patch deliberately matches the
surrounding style instead of reformatting (smallest reviewable diff).

### 5.5 Frontend

`memoPdfUrl` / `downloadMemoPdf` now append `?lang=<en|ar>`; no other frontend
code or test references them. (Local `tsc` not run — `node_modules` absent in the
sandbox — but the import resolves to the existing default export in
`src/i18n/index.ts`.)

## 6. Risk / tradeoffs

- **Risk: low.** Every AR change is behind `lang == "ar"`; EN is proven
  byte-identical. The widest surface is the formatter `lang` threading, which is
  mechanical and covered by the EN identity check.
- **Out of scope (intentional):** non-policy assumption units such as `ratio` /
  `fraction` / `spaces` / `2014=1.0` and the Latin word `space` in `m²/space`
  pass through (their U+00B2, if any, is still stripped). The decided policy
  covers `SAR` / `m2` / `SAR/m2` / `%` / `SAR/m2/yr` only.
- **Document title** localization (`doc_title_prefix` → `تقدير`) was already
  wired in `app/api/estimates.py` and is untouched.

## 7. Merge recommendation

**Recommend merge — low risk.** Solves all four reported AR-render problems,
keeps EN byte-identical, flips the frontend go-live switch, and is validated
against both a fixture and a rasterized AR page. Frontend/backend/test contracts
stay aligned.
