# Arabic translation glosses — `expansionAdvisor.*`

Review aid for the 52 `expansionAdvisor.*` keys added to `ar.json` in the
Frontend Arabic Activation PR (PR-FE-AR). JSON has no comment syntax, so the
English-gloss notes for Ahmed's review live here (mirrors the PR #3 pattern).

Conventions (per PR #2b): Latin digits, inline English units in parentheses
(`SAR`, `m²`, `km`), parenthetical-English for jargon (`whitespace`,
`cannibalization`), em-dashes preserved, "gate" → "معيار", Arabic-yeh (ي).

## Validation Plan

- `validationPlan` — "Validation Plan" → `خطة التحقق`
- `vpMustVerify` — "Must verify before commitment" → `يجب التحقق قبل الالتزام`
- `vpNiceToConfirm` — "Nice to confirm" → `يُستحسن تأكيده`
- `vpAlreadyStrong` — "Already strong" → `قوي بالفعل`

## Assumptions & Confidence

- `assumptionsConfidence` — "Assumptions & Confidence" → `الافتراضات ومستوى الثقة`
- `acStrong` — "Strong / Observed" → `قوي / مُلاحَظ`
- `acEstimated` — "Estimated / Modeled" → `مُقدّر / نموذجي`
- `acMissing` — "Missing / Unknown" → `غير متوفر / غير معروف`
- `acStrongShort` — "observed" (chip) → `مُلاحَظ`
- `acEstimatedShort` — "estimated" (chip) → `مُقدّر`
- `acMissingShort` — "missing" (chip) → `غير متوفر`

## Decision snapshot

- `topStrength` — "Top Strength" → `أبرز نقطة قوة`
- `gatesNeedVerification` — "Needs verification" → `يحتاج إلى تحقق`
- `decisionSnapshot` — "Decision Snapshot" → `لمحة القرار`
- `dsWhyItWins` — "Why it wins" → `لماذا يتفوق`

## Compare outcome banner

- `coWinsOverall` — "Wins overall:" → `الأفضل إجمالاً:`
- `coRunnerUpStronger` — "Runner-up stronger on:" → `المرشح الثاني أقوى في:`
- `coWhatWouldChange` — "What would change:" → `ما الذي قد يغيّر النتيجة:`
- `coLeadMismatch` — "Compare winner differs from lead" → `الفائز في المقارنة يختلف عن المرشح الرئيسي`

## Shortlist meta

- `smCompared` — "{{count}} compared" → `{{count}} قيد المقارنة`

## Copy block

- `copySiteVisitBriefing` — "Copy Site Visit Briefing" → `نسخ موجز زيارة الموقع`

## Compare table — column labels

- `rentPerM2Year` — "Rent (SAR/m²/yr)" → `الإيجار (ر.س/م²/سنة)`
- `annualRentLabel` — "Annual Rent (SAR)" → `الإيجار السنوي (ر.س)`
- `fitoutCostLabel` — "Est. Fit-out" → `تكلفة التجهيز التقديرية`
- `revenueIndexLabel` — "Revenue Index" → `مؤشر الإيرادات`
- `cannibalizationLabel` — "Cannibalization" → `تداخل الفروع (cannibalization)`
- `nearestBranchLabel` — "Nearest Branch" → `أقرب فرع`
- `demandScoreLabel` — "Demand" → `الطلب`
- `fitScoreLabel` — "Fit" → `الملاءمة`
- `rentBurdenLabel` — "Rent Burden" → `عبء الإيجار`

## Compare winner badges

- `compareWinnerBestOverall` — "Best Overall" → `الأفضل إجمالاً`
- `compareWinnerHighestDemand` — "Highest Demand" → `أعلى طلب`
- `compareWinnerBestEconomics` — "Best Economics" → `أفضل جدوى اقتصادية`
- `compareWinnerBestBrandFit` — "Best Brand Fit" → `أفضل ملاءمة للعلامة`
- `compareWinnerStrongestWhitespace` — "Strongest Whitespace" → `أكبر فجوة سوقية (whitespace)`
- `compareWinnerMostConfident` — "Most Confident" → `الأعلى ثقة`
- `compareWinnerBestGatePass` — "Best Gate Pass" → `أفضل اجتياز للمعايير`

## Report sections

- `reportSummaryLabel` — "Executive Summary" → `الملخص التنفيذي`
- `reportDetailedSummary` — "Detailed Summary" → `الملخص التفصيلي`
- `reportDimensionWinners` — "Dimension Winners" → `المتفوقون حسب كل بُعد`

## Memo sections

- `memoFeatureSnapshot` — "Data Quality & Sources" → `جودة البيانات ومصادرها`
- `memoScoreBreakdown` — "Detailed Score Breakdown" → `تفصيل النتيجة`
- `memoGateChecklist` — "Gate Checklist" → `قائمة فحص المعايير`

## Gate groups

- `gatePassedGroup` — "Passed" → `مُجتاز`
- `gateFailedGroup` — "Failed" → `غير مُجتاز`
- `gateUnknownGroup` — "Needs Verification" → `يحتاج إلى تحقق`

## Form validation messages

- `validationRequired` — "Brand name is required" → `اسم العلامة مطلوب`
- `validationAreaRange` — "Min area must be less than max area" → `يجب أن تكون المساحة الدنيا أقل من المساحة القصوى`
- `validationLatRange` — "Latitude must be between -90 and 90" → `يجب أن يكون خط العرض بين -90 و90`
- `validationLonRange` — "Longitude must be between -180 and 180" → `يجب أن يكون خط الطول بين -180 و180`

## Compare warnings

- `compareLimitWarning` — "You can compare up to 6 candidates" → `يمكنك مقارنة حتى 6 مرشحين`
- `compareMinWarning` — "Select at least 2 candidates to compare" → `اختر مرشحَين على الأقل للمقارنة`
