# Weight audit — findings log

> Note: the original audit write-up (Items 1–6) was produced outside the repo;
> this file starts the in-repo log with the PR-C entry. Earlier items are
> referenced by number only.

## Item 2 / PR-C — realized-demand re-anchor: two-step deploy (2026-06-10)

PR-C re-anchors the realized-demand reference per service model
(`_REALIZED_DEMAND_REFERENCE` in `app/services/expansion_advisor.py`:
delivery_first 307 / dine_in 402 / qsr 327, p75 anchors from
`scripts/diagnostics/delivery_demand_legs_probe.sql`; cafe falls back to the
`EXPANSION_REALIZED_DEMAND_REFERENCE` env default of 263) and makes the bulk
delivery-leg count radius read `EXPANSION_REALIZED_DEMAND_RADIUS_M`
explicitly (no value change — still 1200 m).

Deploy in two steps:

1. **Merge + deploy this PR.** The re-anchor takes effect immediately:
   realized-demand leg scores come off the 100 ceiling (pre-fix probe:
   realized_p50 score 100 for qsr, 98.5 for dine_in; 62.5% of qsr candidates
   sat at/over the stale global anchor of 263). Validate with section B of
   `scripts/diagnostics/delivery_demand_legs_probe.sql` on fresh searches —
   realized_p50 should land in the ~75–90 band.
2. **Then** shift the blend toward the demand-native leg (env-only, no code
   change — `_delivery_score` already reads the setting):

   ```bash
   kubectl set env deployment/oaktree-estimator -n default \
     EXPANSION_REALIZED_DEMAND_BLEND=0.7
   ```

   This moves the listing(supply)/realized blend from 0.5/0.5 to 0.3/0.7.
   Rationale: the listing leg is competitor supply (its correlation with
   provider_whitespace runs −0.94…−0.96 across service models), while
   realized rating-velocity is the demand-native signal. After step 2,
   re-run the probe and confirm the delivery-leg spread widens and
   corr(demand_score, competitor_count) does not increase.

Do step 2 only after step 1 is verified — raising the realized weight while
the realized leg is still saturated at 100 would compress, not widen, the
delivery-leg spread.
