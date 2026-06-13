# Expansion Advisor — Ground-Truth Inventory for CEO Test/Assessment

**Verified against the live tree** at `claude/expansion-advisor-inventory-ec1dg6` (HEAD `07cafa3df`). Read-only; no edits. Verbatim UI copy in code spans with `file:line`. Locale files: `frontend/src/i18n/en.json` (EN), `frontend/src/i18n/ar.json` (AR).

---

## 1. Search / Entry Flow

**Where:** `ExpansionBriefForm.tsx`, mounted by `ExpansionAdvisorPage.tsx:608` inside a card titled `Brand Brief` / `ملخص العلامة التجارية` (`en.json:862` / `ar.json:913`), subtitle `Find the best location for your next branch in Riyadh` / `اعثر على أفضل موقع لفرعك القادم في الرياض` (`en.json:674` / `ar.json:726`).

### Always-visible inputs
| Field | EN | AR | file:line |
|---|---|---|---|
| Brand name (text) | `Brand Name` | `اسم العلامة` | `ExpansionBriefForm.tsx:193` / `en.json:537` |
| Category (searchable combobox) | `Category` | `الفئة` | `:204` / `en.json:538` |
| Service model (select) | `Service model` | `نموذج الخدمة` | `:213` / `en.json:832` |
| Expansion archetype (select) | `Expansion archetype` | `نموذج التوسع` | `:222` / `en.json:608` |
| Describe your brand (textarea) | `Describe your brand (optional)` | `صف علامتك التجارية (اختياري)` | `:241` / `en.json:614` |

- **Service model options:** `Quick Service`/`خدمة سريعة`, `Dine-in`/`داخل المطعم`, `Delivery`/`توصيل`, `Café`/`مقهى` (`en.json:838-841`).
- **Archetype options:** `Auto (from service model)`, `Balanced`, `Delivery-led`, `Street flagship`, `Neighborhood local` (`en.json:609-613`).
- **Brand-name placeholder:** `e.g. Al Baik, Kudu` / `مثال: البيك، كودو` (`en.json:546`).
- **Category placeholder:** `Select a restaurant category` / `اختر فئة المطعم` (`en.json:545`); helper `Choose the closest match for better search quality` / `اختر أقرب تطابق للحصول على نتائج بحث أفضل` (`en.json:547`).

### Area row (`:298-339`)
`Minimum Area (m²)` / `أقل مساحة (م²)`, `Maximum Area (m²)` / `أكبر مساحة (م²)`, `Target Area (m²)` / `المساحة المستهدفة (م²)` (`en.json:539-541`). Placeholders `80 / 500 / 200`; defaults min 100 / max 500 / target 200.

### Districts & branches
- **Target Districts (comma-separated)** / `الأحياء المستهدفة (مفصولة بفواصل)` (`en.json:542`) — `DistrictMultiSelect`; placeholder is a hardcoded Arabic example `e.g. العليا، الملقا، النخيل`.
- **Existing branches** collapsed counter: `{{count}} branches added` / `No branches added yet.` (`en.json:1150` / `:859`), expanding to `BranchLocationPicker` (name/district/lat/lon, add/remove).

### Advanced Options (collapsed; toggle `Advanced Options` / `خيارات متقدمة`, `en.json:842`)
Three sub-sections: **Brand basics** (price tier `Value`/`Mid`/`Premium`), **Operating strategy** (primary channel, cannibalization tolerance `(m)`, search limit, default 15), **Market preferences** (parking / frontage / visibility sensitivity, each `Low`/`Medium`/`High`). The **Geography** sub-section (preferred/excluded districts) is hidden behind `SHOW_ADVANCED_GEOGRAPHY_SECTION = false` (`:29`) and only appears if a brief extraction populated it.

### Submit & states
- **Submit button:** `Find Branch Candidates` / `البحث عن مواقع مرشحة` (`en.json:784`), disabled while loading / when brand empty. Busy label `Analyzing Riyadh…` / `جاري تحليل الرياض…` (`en.json:785`).
- **Triggers:** `handleSubmit` → `onSubmitBrief` (`ExpansionAdvisorPage.tsx:349`) → `createExpansionSearch` → **`POST /v1/expansion-advisor/searches`** (`lib/api/expansionAdvisor.ts:803`). Results-mode re-run is `Run Again` / `إعادة التشغيل` (`en.json:865`).
- **Loading:** results area shows the busy label + a live `{seconds}s` timer and a 5-row skeleton (`CandidateListSkeleton`); footnote `Completed in {{seconds}}s` (`en.json:665`). Error: `Unable to run search.` / `تعذر تنفيذ البحث.` (`en.json:666`).
- **Empty:** there is **no dedicated zero-candidate empty state** — a completed search returning 0 candidates falls back to the first-run hero (`Branch Expansion Advisor` / `مستشار توسع الفروع`). The only "nothing here" copy is the per-district banner `No matching listings found in {{districts}} for your area and category criteria.` / `لا توجد عقارات مطابقة في {{districts}}…` (`en.json:1076`).

### Brief-extraction ("paste a brief") — **LIVE in production**
The "Describe your brand" textarea is gated on `VITE_EXPANSION_BRIEF_EXTRACTION_ENABLED`, which is set **`=true` in the production deploy** (`.github/workflows/deploy-sccc.yml:147`), so the CEO will see it. User types 2–4 sentences (`2–4 sentences, Arabic or English`, `en.json:615`; 600-char cap), clicks `Build profile from my description` / `اقترح الإعدادات من وصفي` (`en.json:616`) → **`POST /v1/expansion-advisor/brief-extraction`** → a `Reading your brief as:` / `فهمنا وصفك كالتالي:` (`en.json:618`) panel of per-field chips with confidence badges (`High`/`Medium`/`Low confidence`, `en.json:626-628`), each removable, then `Apply`/`Dismiss` (`en.json:619-620`). Correct behavior: applying writes values into the visible form controls (auto-expanding Advanced).

---

## 2. Results / Candidate Ranking

**Presentation:** a single **flat, ranked vertical list of cards** (not a table; the map is a synced companion view, not the ranking surface). `ExpansionResultsPanel.tsx:26-44` renders one `ExpansionCandidateCard` per item, **in backend rank order, with no row cap** (see §7). The map is reached via each card's `Show on Map` / `عرض على الخريطة` (`en.json:1022`).

### What each card shows (`ExpansionCandidateCard.tsx`)
- **Lead tag:** `Lead Site` / `الموقع الرئيسي` (`en.json:910`) when it's the lead and all gates pass; otherwise `Top exploratory candidate` / `أفضل مرشح استكشافي` (`en.json:953`).
- **Rank:** literal `#{rank_position}` (backend-authoritative).
- **District** label, then a **numeric score pill** (no text label; color ≥70 green / ≥60 amber / else red).
- **Tier chip:** `Premier` / `متميّز` (`en.json:1101`, tooltip "High confidence, clears all gates, and scores in the top tier") or `Exploratory` / `استكشافي` (`en.json:1103`).
- **Value-band chip:** `Best value` / `أفضل قيمة` (`en.json:1042`) or `Above market` / `أعلى من السوق` (`en.json:1045`), with low-confidence variants `Above market (citywide est.)` (`en.json:1047`).
- **Nearest-branch distance** pill (when <5 km).
- **Freshness chips:** `New` / `جديد` (`en.json:1094`) and `Updated` / `محدّث` (`en.json:1098`) — these fire only when the listing was created/updated within **7 days**.
- **Momentum chip:** `Top-tier market` / `حي ضمن الفئة الأعلى` (`en.json:1090`).
- **TierBadge:** `Available Unit` / `وحدة متاحة`, `Proven Location` / `موقع مُثبت`, `High Potential` / `إمكانات عالية` (`en.json:1137-1139`), plus `✓ Actual rent` / `إيجار فعلي` (`en.json:1140`).
- **Why-#N chip:** `Why #{{rank}}` / `لماذا #{{rank}}` (`en.json:1220`) — opens the memo scrolled to the ranking logic.
- **Actions:** `Decision Memo` / `مذكرة القرار` (`en.json:879`), `Show on Map`, `Add to Compare` / `إضافة للمقارنة` (`en.json:881`).
- **Note:** the **confidence grade is not shown on the card** — only in the detail panel, finalists workspace, and memo.

### Sorting / filtering (`SortFilterBar.tsx`)
- **Filter** / `تصفية` (`en.json:887`): `All candidates`, `Pass only`, `Best value only`, `Strongest economics`, `Strongest brand fit`, `Lowest cannibalization`, `Strongest delivery signal` (`en.json:889-895`).
- **Sort** / `ترتيب` (`en.json:888`): `Rank (default)`, `Best value`, `Best economics`, `Best brand fit`, `Lowest cannibalization`, `Strongest delivery`, `District (A-Z)` (`en.json:896-902`).
- **District** dropdown appears only when >1 district; default `All districts` (`en.json:903`).
- Re-sorting shows `Showing {{shown}} of {{total}}` (`en.json:904`) + a `Local sort` / `ترتيب محلي` (`en.json:905`) badge, while preserving the original backend `#rank`.

### Which sources currently produce candidates
Candidates are retrieved from `candidate_location` joined to `commercial_unit` where `listing_type IN ('store','showroom')` (`app/services/expansion_advisor.py:7021-7023`). Two live listing platforms feed this:
- **Aqar** — both `store` and `showroom`; scraper `aqar-scraper.yml`.
- **Bayut** — **`Showroom` only** (v1 restricts intake to showrooms; `app/ingest/bayut/detail_scraper.py:69`); scraper `bayut-scraper.yml` (daily cron). Bayut IDs carry a `bayut:` storage prefix that is stripped before display.

Both flow into the nightly `candidate-locations-refresh.yml`. **Source labeling in UI is thin** — the platform name appears only via the TierBadge listing link `View on {{platform}}` / `عرض على {{platform}}` (`en.json:1144`; resolves to `Aqar` / `Bayut`) and inside the New/Updated tooltips (`Listing newly created on {{platform}} within the last 7 days`, `en.json:1095`). There is **no "Source: Aqar" attribution line**.

---

## 3. Client-Visible Cards

### Ranking Logic card (`DecisionLogicCard.tsx`) — title `Ranking logic` / `منطق الترتيب` (`en.json` `decisionLogicTitle`)
Three sub-sections: **Gates** / `البوابات` (passed/failed/unverified buckets), **Score contributions** / `مساهمات الدرجات`, **Ranking decision** / `قرار الترتيب`. **Should communicate:** the full audit of how the final score and rank were produced. *Right* when gate buckets match the candidate, component points roughly sum to the final score, and the ranking note matches reality.
- ⚠ **Gate rows inside this card render English even under AR** — `displayGateName()` calls `humanGateLabel(raw)` **without** the `t` function (`DecisionLogicCard.tsx:144`; confirmed against `formatHelpers.ts:263`). So e.g. `Zoning fit`, `Parking` stay English in Arabic.

### Score Contributions — confirmed current row labels
Rendered via i18n `expansionAdvisor.scoreComponents.<key>.label`; order from `DecisionLogicCard.tsx:52-64`. The 11 rows:

| EN | AR |
|---|---|
| `Economics` | `الجدوى الاقتصادية` |
| `Listing Quality` | `جودة القائمة` |
| `District Momentum` | `زخم الحي` |
| `Brand Fit` | `ملاءمة العلامة` |
| `Landlord Signal` | `إشارة المؤجر` |
| `Competitor Openness` | `انفتاح المنافسة` |
| `Demand Strength` | `قوة الطلب` |
| `Access & Visibility` | `الوصول والوضوح` |
| `Delivery Market` | `سوق التوصيل` |
| `Data Quality` | `جودة البيانات` |
| `Chain Strength` | `قوة السلسلة` |

**Should communicate:** how much each factor added to the score. *Right* when the visible weighted points stack to the final score. Note `Demand Strength` keeps its label whether the legacy or DG-index demand engine is used; only the underlying definition/inputs swap.

### Gate Summary strip (`GateSummary.tsx`) — **renders Arabic correctly under AR**
Pass/fail/unknown chips (✓ / ✗ / ?) per hard gate; `overall_pass` is hidden. **Should communicate:** at-a-glance which gates a site clears. The standalone strip calls `humanGateLabel(key, t)` **with** the translator (`GateSummary.tsx:36,38`) → localized gate names from `expansionAdvisor.gateLabel.*` (e.g. `مواقف السيارات`, `ملاءمة التنطيق`), verified by `GateLabelI18n.test.tsx`. This was added recently (commit `bb8287aba`, "F5 Stage 1 — Arabic gate labels").

> **Important nuance for the tester:** the **standalone Gate Summary strip = Arabic ✅**, but the **gate rows inside the Ranking Logic card = English ❌** under Arabic. Same gates, two label behaviors.

### Decision Snapshot (`DecisionSnapshotCard.tsx`)
Score pill + gate verdict (`Pass` / `Fail` / `Needs validation`, `en.json` `gatePass/gateFail/gateNeedsValidation`) + confidence badge `Data: {{grade}}` (`en.json` `confidenceBadge.prefix`) + best format + lead-site line `#rank district`. **Should communicate:** one-glance verdict for the top candidate. ⚠ The site label `Lead Site` / `Top ranked candidate` is a **hardcoded English string** (`studyAdapters.ts:1018`) — English under AR.

### Other client-visible cards
- **Validation Checklist** / `قائمة التحقق` (`DecisionChecklist.tsx`) — grouped strong/caution/risk/verify signals (Market Demand, Site Fit, Cannibalization, Delivery Market, Economics, Unknowns to Verify). *Right* when icons match the underlying gates/scores.
- **Pillar Summary Strip** (`PillarSummaryStrip.tsx`) — `Well-populated areas` / `Strong sales potential` / `Business growth`; renders **only** when demote-leg diagnostics exist, otherwise hidden.

---

## 4. Decision Memo (drawer)

**Opens** as a right-side **drawer** (`ea-drawer--wide`, `ExpansionMemoPanel.tsx:142`) from each card's `Decision Memo` button (`ExpansionCandidateCard.tsx:369`) or the `Why #N` chip (opens pre-scrolled to diagnostics). Header title `Decision Memo` / `مذكرة القرار`.

**Two top tabs:** `Memo` / `المذكرة` (default) and `Diagnostics` / `التشخيص` (`en.json:1159-1160`).

- **Memo tab** (reader-facing), in order: verdict + confidence + score row → one-line property facts (`MemoPropertyFactsRow`) → **LLM Decision Narrative** with headings `The recommendation` / `التوصية`, `Key evidence` / `الأدلة الرئيسية`, `Risks to watch` / `المخاطر التي يجب مراقبتها`, `How it compares` / `كيف يُقارَن`, `Bottom line` / `الخلاصة` (`en.json:1423-1427`) → copy-ready summary (lead candidates only).
- **Diagnostics tab:** the Ranking Logic card, then a 5-sub-tab strip `Economics` / `Market` / `Site` / `Risks & Validation` / `Breakdown` (`en.json:1153-1157`), then collapsed `Score breakdown` and `Technical details`.

**EN/AR end-to-end:** every memo heading, tab, and narrative section label is translated in **both** locales (confirmed key-by-key). **Two caveats:**
1. The narrative *body prose* is LLM-generated per locale (`generateDecisionMemo(candidate, brief, lang)`), so its Arabic-ness is the model's responsibility, not a static-string guarantee — the chrome is fully translated.
2. The sibling **Compare panel** (`ExpansionComparePanel.tsx`) table body is **hardcoded English** (`Rank`, `Final score`, `Best Overall`, etc., `:27-97`) — known i18n debt, English under AR.

**Render-window behavior — current state:** **No render-window cap exists in the current code.** `ExpansionResultsPanel.tsx:28` maps *all* items (no `.slice`, no `maxVisible`, no pagination), and the memo-open resolver `resolveCandidateById` is a `.find` over the full candidate array with no index ceiling (`ExpansionAdvisorPage.tsx:66`). **A candidate at index ≥4 is both rendered and has an openable memo.** The "eligible memo row at index ≥4 hidden" symptom **does not reproduce in this tree** — the only nearby slices are unrelated (saved-studies preview `slice(0,3)`, compare set `slice(0,6)`, exec-report `slice(0,3)`). *(Current state only, as requested.)*

---

## 5. Localization

- **Switch mechanism:** `LanguageSwitcher.tsx` — a select with `english`/`arabic`; choosing AR calls `restartInLocale("ar")`, which flips `document.documentElement.dir` to `rtl`. No per-component toggle.
- **Client-visible surfaces still English-only under AR (current state):**
  1. **Gate rows inside the Ranking Logic card** (`DecisionLogicCard.tsx:144`).
  2. **Decision Snapshot site label** `Lead Site` / `Top ranked candidate` (`studyAdapters.ts:1018`).
  3. **Compare panel table body** (`ExpansionComparePanel.tsx`).
  4. A few **brief/branch helper strings** with no AR key (inline English fallbacks): branch search placeholder `Search branch name, address, or district…`, `Enter coordinates manually`, unnamed-branch `Branch N`, and the no-results rows `No matching categories` / `No matching districts`.
- Everything else in the `expansionAdvisor.*` namespace has a complete AR counterpart (0 missing keys), and the standalone Gate Summary strip + score-component labels are correctly Arabic.

---

## 6. Data Provenance Shown in UI

- **Platform/source:** only via TierBadge `View on {{platform}}` / `عرض على {{platform}}` (resolves to `Aqar` / `Bayut`) and inside the New/Updated freshness tooltips. No standalone "Source:" line.
- **Freshness/recency:** the binary `New` (≤7 days created) and `Updated` (≤7 days owner-refreshed) chips are the only on-card recency cues — they show *state*, not a date or age-in-days.
- **Listing age in days** surfaces in the memo **only** as `vacant {{days}} days` / `شاغر منذ {{days}} يومًا` (`MemoPropertyFactsRow.tsx:40`, `en.json:1165`) and **only when the unit is flagged vacant**. There is **no generic "as of"/listing-date string anywhere**.
- The detail panel's only data-origin field is `Data completeness`; the memo's `Context sources:` line lists internal feature sources (debug), not listing platforms.
- (Note: `AdvisorySectionCards.tsx` defines a `Listing age` / `Vacancy` field block, but that component is **not wired into the live memo** — imported only by tests — so it does not surface to users.)

---

## 7. Known Quirks (honest framing for the tester)

| # | Quirk (current state) | Where | Tester impact |
|---|---|---|---|
| 1 | **Gate labels are inconsistent by surface under Arabic**: the standalone **Gate Summary strip is Arabic ✅**, but **gate rows inside the Ranking Logic card are English ❌**. | `GateSummary.tsx:36` vs `DecisionLogicCard.tsx:144` | In AR, the CEO will see Arabic gate names in one place and English ones in another — *expected*, not breakage. |
| 2 | **Decision Snapshot site label is English under AR** (`Lead Site`/`Top ranked candidate`, hardcoded). | `studyAdapters.ts:1018` | One English phrase in an otherwise-Arabic card. |
| 3 | **Compare panel table body is English-only** (tracked i18n debt). | `ExpansionComparePanel.tsx:8-9` | Compare view looks un-localized in AR. |
| 4 | **Memo narrative body is LLM-generated**, not static-translated; its Arabic quality depends on the model, and it's cached per `candidateId:lang`. | `DecisionMemoNarrative.tsx:18,284` | Prose tone/length may vary run-to-run; chrome stays correct. |
| 5 | **No dedicated zero-result empty state** — a 0-candidate search falls back to the first-run hero; only the per-district "No matching listings" banner explains it. | `ExpansionAdvisorPage.tsx:519,656` | A genuinely empty result may look like the app "reset" rather than "found nothing." |
| 6 | Minor **English inline fallbacks** in branch picker / category & district no-results rows. | `BranchLocationPicker.tsx`, `CategorySelect.tsx:213` | Small AR gaps in secondary controls. |
| 7 | **Memo render-window: NOT reproducible in this tree.** No row cap; index-≥4 candidates render and their memos open. | `ExpansionResultsPanel.tsx:28`, `ExpansionAdvisorPage.tsx:66` | If the brief lists this as in-flight, current code already shows all rows — *don't flag as broken*. |

---

**Scope note:** Per instructions, internal scoring weights, env-var inventories, gate hard-fail thresholds, and percentile mechanics are excluded. The biggest "looks wrong in Arabic" risks for the CEO test are quirks **#1–#3**; the memo render-window concern (#7) does not reproduce in the current code.
