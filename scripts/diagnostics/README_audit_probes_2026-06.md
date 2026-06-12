# Scoring/ranking audit probes — 2026-06

Companion probes for `docs/investigations/scoring_ranking_audit_2026-06.md`.
All four are **read-only** and written for Ahmed to run against the
production replica via Codespace `psql`. Do **not** run them from CI, app
pods, or this investigation branch's tooling.

| Probe | Finding | What it fingerprints |
| --- | --- | --- |
| `balancing_order_probe.sql` | 1 | Under-filled multi-district result sets caused by hard floors running after the balancing truncation (failures are dropped, never persisted, so missing slots are the only visible trace). |
| `value_band_tier_bias_probe.sql` | 2 | value_band distribution and `estimated_revenue_index >= 99.5` saturation by brand price tier — premium briefs should show pinned revenue indexes and a best_value skew if the ticket-multiplier leak is biasing the band. |
| `viability_stack_depth_probe.sql` | 4 | How often viability legs co-fire (−10 each, stacking) — sizes the real-world gap between the documented "single demote" and the implemented stacking before the product decision. |
| `repost_duplicate_probe.sql` | 7 | Re-posted listings (same unit, new aqar_id) surviving both dedupe passes: pairs within ~30 m, area within 5%, different parcel_id. |

Each file carries the relevant `app/services/expansion_advisor.py` line
anchors in its header comment. Time windows: 30 days (90 days for the
value-band probe, for sample size). Adjust the `interval` literals as needed.
