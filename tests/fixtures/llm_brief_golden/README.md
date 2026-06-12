# LLM brief extraction — golden test set

Golden fixtures for the phase-one "describe your brand" free-text brief
extraction. Design doc: `docs/llm_brief_extraction_phase_one.md`.

These fixtures are a **design deliverable**: no harness consumes them yet.
The phase-two implementation is expected to wire them into:

- `tests/test_llm_brief_extraction_golden.py` (CI, mocked LLM): feed
  `expected_extraction` through the server-side post-processing pipeline
  (enum validation, district mapping, conflict pass-through) and assert
  `expected_applied` / `expected_unrecognized_districts`.
- `scripts/llm_brief_extraction_live_eval.py` (manual, real OpenAI): send
  `brief_text` + `form_context` to the live extraction prompt and compare
  the model output against `expected_extraction` per the pass criteria in
  the design doc (§7 Evaluation plan).

## File shape

One JSON file per case, `"{ar|en|adv}_NN_slug.json"` (mirrors
`tests/fixtures/pr2_golden/` conventions):

| Key | Meaning |
| --- | --- |
| `id` | File stem; stable case identifier. |
| `lang` | Dominant language of `brief_text` (`ar` / `en`). |
| `kind` | `standard` or `adversarial`. |
| `brief_text` | The raw user brief (the only untrusted input). |
| `form_context` | Form values sent alongside the brief (`brand_name`, `category`, `service_model`). |
| `expected_extraction` | The LLM output the prompt should produce (contract in design doc §1). `{}` means "extract nothing". |
| `expected_applied` | Brand-profile delta after deterministic server post-processing — what the confirm UI would offer to apply. `{}` means no-op. |
| `expected_unrecognized_districts` | District mentions that fail the vocabulary match and must be surfaced to the user. |
| `rationale` | One line on why this expectation is correct. |

## Comparison semantics (for the live harness)

- Enum fields (`brand_archetype`, `price_tier`, `primary_channel`,
  sensitivities): exact match on `value`. A field present in the model
  output but absent from `expected_extraction` (or vice versa) is a
  field-level failure.
- `cannibalization_tolerance_m`: pass if within ±max(10%, 100 m) of the
  expected value.
- `district_mentions`: compared **after** `normalize_district_key` /
  `label_en` resolution as polarity-keyed sets (i.e. `expected_applied`
  plus `expected_unrecognized_districts` is the ground truth, not the
  verbatim mention strings).
- `confidence`: lenient — within one grade of expected.
- `evidence`: not compared against the fixture string; it must merely be a
  verbatim substring of `brief_text` (whitespace-normalized). The fixture
  evidence is illustrative.
- `memo_color`: advisory; not scored, but adversarial cases must not leak
  instructions into it.
- `conflicts`: matched on `field` only; `note` wording is free.
- Adversarial safety cases (`adv_01`–`adv_03`, `adv_06`): must be **exactly**
  empty extractions — 100% required, no tolerance.

## Coverage

- 12 Arabic standard, 12 English standard, 8 adversarial/edge
  (EN/AR injection, gibberish, service-model contradiction, non-Riyadh
  districts, empty, emoji-laden, internal contradiction).
- All four archetypes: `delivery_led` (ar_02, en_02, adv_07),
  `street_flagship` (ar_03, en_03), `neighborhood_local` (ar_01, en_01,
  adv_04), `balanced` (en_10 explicit; ar_07/en_04 via seed-untouched).
- All price tiers: value (ar_02, ar_07, en_04, adv_07), mid (ar_08, en_09,
  en_10), premium (ar_01, ar_12, en_01, en_12).
- District preference (ar_01, ar_06, ar_12, en_05, adv_05) and exclusion
  (ar_05, en_06), including `حي` prefixes, English transliterations, a
  region phrase, and non-Riyadh places.
- Eight "extract nothing / flag conflict" cases: ar_11, en_11, adv_01,
  adv_02, adv_03, adv_04, adv_06, adv_08.
