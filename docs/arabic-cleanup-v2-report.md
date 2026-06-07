# On-screen Arabic cleanup (v2) — implementation report

**Branch:** `claude/arabic-cleanup-v2-DXgiV`
**Scope:** v2 surfaces only (`ExcelForm.tsx`, `main.tsx` / `ParcelInfoBar.tsx`, `i18n/*.json`). Dead `App.tsx` and its `ui.*` keys untouched. EN wording unchanged. Build clean.

This PR deliberately **excludes** the digit/unit formatting pass (separate investigation): no number formatters or unit glyphs were changed here.

---

## Summary of changes

| File | Change |
|------|--------|
| `frontend/src/i18n/en.json` | Additive keys only: `excel.componentBasement`, `app.landuseMethod.*` |
| `frontend/src/i18n/ar.json` | Same additive keys (AR) + `FAR → معامل الكثافة` in 4 v2 values |
| `frontend/src/components/ExcelForm.tsx` | `componentLabel()` helper + both render loops; FAR rounding (3 seeds); construction term + inline FAR strings |
| `frontend/src/main.tsx` | Parcel banner: drop stray land-use code, localize land-use method |

Diff stat: `4 files changed, 44 insertions(+), 24 deletions(-)`.

---

## Part 1 — Asset-class / component labels rendered English

**What was wrong:** Both v2 loops rendered the raw data key.

- Revenue **"إيرادات الإيجار حسب فئة الأصول"** — `ExcelForm.tsx:3478` rendered `prettifyRevenueKey(item?.label || key)`, where `item.label = key.replace(/_/g," ")` (i.e. the raw English key) → showed `residential` / `retail` / `office` / `basement`.
- Parking **"المطلوب حسب المكوّن"** — `ExcelForm.tsx:3908` rendered `{key}` directly from `Object.entries(parking.requiredByComponent)`.

**Why it happened:** Neither loop routed the component key through the translation layer; they printed the data key verbatim.

**Fix:** Reused the existing component vocabulary already used by the Summary unit-mix cards / construction panel (`excel.componentResidential/Retail/Office`), added the one missing key `excel.componentBasement` (EN `Basement`, AR `قبو`), and added a small `componentLabel()` helper (`ExcelForm.tsx:1864`) that maps known keys and **passthrough-falls back** to the prettified key so an unknown component never blanks. Parking iterates the data type's keys (not a hardcoded four); the revenue fixed array now also routes through `componentLabel`.

```ts
const componentLabelKeys: Record<string, string> = {
  residential: "excel.componentResidential",
  retail: "excel.componentRetail",
  office: "excel.componentOffice",
  basement: "excel.componentBasement",
};
const componentLabel = (key: string) => {
  const translationKey = componentLabelKeys[key];
  return translationKey ? t(translationKey) : prettifyRevenueKey(key);
};
```

Render sites updated: revenue label `:3479`, revenue infotip label `:3483`, parking label `:3909`.

> **EN note:** EN rows now read `Residential / Retail / Office / Basement` (matching the summary cards) instead of the old lowercase raw key — a consistency alignment, not a wording change.

---

## Part 2 — `FAR` leak in the construction-cost panel

**What was wrong:** The unit-cost panel showed `تكلفة وحدة الملحق العلوي (غير محسوب في FAR)` — Latin `FAR`, missed in the v2 `FAR → معامل الكثافة` standardization.

**Fix:** Fixed the named key plus every other Latin `FAR` in **v2** AR values:

| Location | Before → After |
|----------|----------------|
| `excel.unitCostUpperAnnexNonFar` (reported) | `…في FAR)` → `…في معامل الكثافة)` |
| `excel.upperAnnexNonFarBua` | `…في FAR، +0.5 طابق)` → `…معامل الكثافة…` |
| `excel.upperAnnexNonFarCost` | `…في FAR)` → `…في معامل الكثافة)` |
| `excelNotes.coverageMassing` | `تُستخدم مع FAR…` → `تُستخدم مع معامل الكثافة…` |
| inline `ExcelForm.tsx:2736` | `…غير محتسب في FAR)` → `…غير محتسب في معامل الكثافة)` |

Grep confirms **0× Latin `FAR` left in v2 AR strings**.

**Left untouched (correctly):** the 5 remaining `FAR` occurrences in `ar.json` —
`ui.projectInputs.farLabel`, `ui.projectInputs.useAutoFar`, `ui.builtForm.subtitle`, `ui.builtForm.suggestedFar`, `ui.builtForm.potentialBua` — are referenced **only by dead `App.tsx`**, outside the v2-only scope.

---

## Part 3 — Construction term fork (decided)

**What was wrong:** The financial-detail tab used `التنفيذ المباشر` while the calc panel + PDF use `الإنشاء (مباشر)` for the same "Construction (direct)" concept.

**Fix (standardize on `الإنشاء (مباشر)`, matching the shipped PDF):**

- `ExcelForm.tsx:2618` — `directConstructionLabel` AR fallback `التنفيذ المباشر` → `الإنشاء (مباشر)`.
- `ExcelForm.tsx:2739` — sub-note `…مضمنة ضمن التنفيذ المباشر` → `…مضمنة ضمن الإنشاء (مباشر)`, so the same tab isn't internally forked.

EN fallback `Direct construction` unchanged.

---

## Part 4 — Grep-verify two AR strings (trust bytes, not the screenshot)

Both confirmed **correct at the byte level — no change made.**

- **Revenue upper-annex allocated-to note** (`excel.revenueAllocatedTo`): committed bytes are `مخصّص إلى` =
  م (U+0645) خ (U+062E) ص (U+0635) **ّ (U+0651 shadda)** ص (U+0635) + `إلى`.
  → "allocated" — **not** `مختص` (which would carry a teh ت). ✅ left as-is.
- **Effective-FAR label** (`excel.effectiveFar`): committed bytes are `الفعّال` =
  …ف (U+0641) **ع (U+0639 AIN)** **ّ (U+0651 shadda)** ا ل.
  → correct — **not** `الفقال` (no QAF). ✅ left as-is.
- Whole-file hygiene: **0× U+06CC** (Farsi yeh), **0× U+06BE** (heh doachashmee). Yeh = U+064A, heh = U+0647 throughout.

---

## Part 5 — Effective-FAR value renders as a malformed float

**Render site:** the editable field is bound to `farDraft` (input at `ExcelForm.tsx:2801`, label `excel.effectiveFarAboveGround`).

**Why it happened:** `farDraft` was seeded with raw `String(displayedFar)` in three places, so an unrounded float like `1.5000000003` surfaced in the field:
- `useEffect` at `:1324`
- `resetFarDraft()` at `:1780`
- `startFarEdit()` at `:1825`

**Fix:** All three seeds now use `String(roundTo(Number(displayedFar), 2))` — **display-only**, locale-agnostic. The underlying computed `displayedFar` / area ratios are unchanged. (The read-only table cell at `:3180` was already rounded via `formatNumberValue(..., 3)`.)

---

## Part 6 — Parcel banner: stray `m` + English method

**Render site:** `ui-v2/ParcelInfoBar.tsx` (estimator banner), fed `landUseLabel={codeLabel}` and `methodLabel` from `main.tsx:378`.

- **Stray `m`:** `codeLabel` built the string via `app.landUseCodeLabel` = `"{{code}} — {{label}}"`, prefixing the bare land-use code (`m`/`s`) before the localized use → `m — مختلط/تجاري`. Fixed `codeLabel` to return just the localized label (`app.landUse.mixed` / `app.landUse.residential`), dropping the bare code on both banner sites and both locales.
- **English method:** `formatLanduseMethod` returned hardcoded English (`ArcGIS parcel label`, `Suhail zoning`, `OSM overlay`). Localized via new `app.landuseMethod.*` keys:
  - AR: `وسم قطعة ArcGIS`, `تقسيم مناطق Suhail`, `طبقة OSM` (brand names ArcGIS/Suhail/OSM kept Latin, matching existing AR vocabulary).
  - EN values identical to the prior strings (no wording change).

**Shared with Expansion Advisor?** **No.** `ParcelInfoBar` is imported only by `main.tsx`; `codeLabel` / `formatLanduseMethod` are local to the estimator `App`. EA pages (`ExpansionAdvisorPage`, `CompareOutcomeBanner`) don't use them — EA is not touched or regressed.

---

## Validation

- `cd frontend && npm ci && npm run build` → **clean** (`tsc && vite build` ✓, 578 modules transformed).
- No `*.test.ts(x)` references the changed surfaces (banner / labels / keys).
- Grep: **0× Latin `FAR`** in v2 AR strings; AR byte hygiene **0× U+06CC / 0× U+06BE**.
- EN locale strings unchanged (only additive keys: `componentBasement`, `landuseMethod.*`).

**Risk: low.** Targeted, display-layer changes; no business-logic or computed-value changes.

### AR walk checklist

- [x] Revenue + Parking component rows now Arabic (سكني/تجاري/مكتبي/قبو)
- [x] Construction panel + financial-detail FAR = `معامل الكثافة`
- [x] Construction term consistent (`الإنشاء (مباشر)`)
- [x] Effective-FAR shows a clean number
- [x] Banner has no stray `m` and no raw English method
- [x] EN locale unchanged; 0× Latin `FAR` left in v2 AR i18n values
