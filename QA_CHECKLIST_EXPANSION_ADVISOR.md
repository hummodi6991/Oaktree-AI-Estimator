# Expansion Advisor — Manual QA Checklist

Run through these steps after deploying the regression test patch.

## Setup
- [ ] Run expansion advisor with **target 200 m²**, area range **100–500 m²**
- [ ] Use at least 2 target districts (e.g. Al Olaya, Al Malqa)

## Ranking & Area Preference
- [ ] Confirm top candidates are **not all just max-size parcels** (500 m²) by default
- [ ] Verify candidates closer to 200 m² target rank higher than 500 m² parcels (all else equal)

## Zero-Pass Scenario
- [ ] Trigger a search where no candidate passes all gates (e.g. restrictive excluded districts + delivery channel)
- [ ] Confirm report says **"No candidate currently passes all required gates"** (not "Lead Site")
- [ ] Confirm `best_pass_candidate_id` is `null` in the API response
- [ ] Confirm the UI shows **"Top ranked candidate"** not "Lead Site"

## Gate Verdicts
- [ ] Confirm memo gate checklist shows **? (unknown)** for missing parking/road context — not ✗ (fail)
- [ ] Confirm gate verdict values in API are `"pass"`, `"fail"`, or `"unknown"` (not booleans)
- [ ] Verify raw keys like `zoning_fit_pass` never appear in any user-visible text

## Score Breakdown
- [ ] Confirm score breakdown shows **points + weight %** (e.g. "20.6 pts", "25% weight")
- [ ] Confirm no component ever displays 2000%, 2500%, or any value > 100% as a weight
- [ ] Verify `weighted_points` ≤ `raw_input_score` for every component

## Verdict & Confidence Badges
- [ ] Confirm "Fail" verdict and confidence grade (e.g. "Data: B") are **separate UI elements**
- [ ] Confirm they never appear mashed together as "Fail B" or "FailB"

## Provider Scores
- [ ] Confirm provider scores can still be **0** without breaking layout
- [ ] Verify a candidate with `provider_density_score: 0` still renders correctly

## API Smoke
- [ ] `GET /v1/expansion-advisor/searches/{id}/report` returns valid JSON with `best_pass_candidate_id`
- [ ] `GET /v1/expansion-advisor/candidates/{id}/memo` returns `gate_status` with tri-state values
- [ ] `POST /v1/expansion-advisor/candidates/compare` returns `best_gate_pass_candidate_id`
