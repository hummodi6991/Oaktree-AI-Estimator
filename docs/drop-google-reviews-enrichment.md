# Drop paid Google reviews enrichment — investigation & change report

**Decision:** Stop all paid Google Places **reviews** enrichment spend. Keep every existing
`restaurant_poi` row, keep the `google_*` columns and their current data, keep the enrichment code
**dormant** (re-enableable). Cost decision: ratings/`review_count` have near-zero scoring impact in
the Expansion Advisor, and the match rate (~58%) + cost (~$650 backfill + ~$1,500/yr) don't justify
it. Accepted trade-off: we lose fresh `business_status` (closure) tracking from the cutoff onward.

**Status:** Branch `chore/drop-google-enrichment` pushed, **not merged**. Disable-only — no dropped
columns, no removed functions, no deleted data.

**Base note:** This branch is based on commit `43b6ee528` (the production state that actually carries
the semiannual schedule cron). `main` does **not** yet contain that schedule, so basing off `main`
would have had nothing to disable.

---

## Phase 1 — Read-first findings

### Q1.1 — `enrich-google-reviews.yml` (the paid reviews job)
- **Triggers (confirmed, exactly two):** `workflow_dispatch` (inputs `limit`, `force`,
  `only_missing`, `reset_cursor`) and `schedule` cron **`0 4 1 1,7 *`** (semiannual: Jan 1 & Jul 1 @
  04:00 UTC). Lines 3–25.
- Paid step: **"Enrich restaurant POIs with Google Reviews"** → `python -m
  app.ingest.google_reviews_enrich` (lines 89–118), gated by `GOOGLE_PLACES_API_KEY`.
- **How to disable spend but keep manual runs:** remove/comment **only** the `schedule:` block
  (lines 22–25). Keeping `workflow_dispatch` means it never auto-runs but can be launched by hand.
  This is the only Phase 2 edit.

### Q1.2 — `expansion-advisor-data-competitors.yml` (weekly)
- `refresh_google_reviews` **defaults `"false"`** (string, lines 8–11). The enrichment step (lines
  52–58) is gated `if: github.event.inputs.refresh_google_reviews == 'true'`.
- **On the weekly `schedule` cron (`0 7 * * 5`), `github.event.inputs.*` are empty → never `'true'`
  → the Google step is skipped.** The weekly job triggers **no Google calls**. The competitor-quality
  ingest itself (`expansion_advisor_competitors`) only reads `restaurant_poi`; it makes no Places
  calls.
- **Only way it can trigger enrichment:** a **manual** `workflow_dispatch` with
  `refresh_google_reviews=true`. (`GOOGLE_PLACES_API_KEY` is in `env` but unused unless the flag is
  set.) No change needed.

### Q1.3 — `google-places-grid-search.yml` (discovery)
- Separate **discovery** job: runs `python scripts/google_places_grid_search.py` (line 83),
  discovering new `restaurant_poi` rows. Distinct script/module from `app.ingest.google_reviews_enrich`.
- Has its **own** `schedule` cron **`0 4 1 1,4,7,10 *`** (quarterly) + `workflow_dispatch`, and its
  own `GOOGLE_PLACES_API_KEY`. Independent of reviews enrichment. **We are not disabling discovery.**

### Q4 — `business_status` reads in scoring (2 found repo-wide; left as-is per decision)
1. **Site B density read** — `app/services/expansion_advisor.py:6436-6437` (in `_bulk_enrich_competitors`):
   ```sql
   WHERE (rp.business_status IS NULL
          OR rp.business_status = 'OPERATIONAL')
     AND ST_DWithin(rp.geom::geography, ...)
   ```
2. **ECQ chain-size build** — `app/ingest/expansion_advisor_competitors.py:211-212` (in `chain_counts`,
   feeds `chain_strength` / `expansion_competitor_quality`, which the comparables read prefers):
   ```sql
   -- Closed venues do not count toward chain size.
   AND (business_status IS NULL
        OR business_status = 'OPERATIONAL')
   ```
- **Both already treat `NULL` as "keep."** Freezing `business_status` (existing values stay, new
  closures stop arriving) is a **no-op** for both predicates: pre-cutoff closures stay excluded;
  nothing newly drops out.
- **Decision: (a) leave as-is.** Option (b) neutralizing would *re-include* venues already marked
  CLOSED — a change in the opposite direction of accuracy. No code change made.

### Q5 — UI / ratings surfaces (where Google data goes stale-but-visible)
- **Visible:** `restaurant_poi.rating` in the **Restaurant Location** feature (not Expansion Advisor):
  `frontend/src/components/RestaurantLocationPanel.tsx:466` renders a "Competitor Rating" column from
  `nearby_competitors[].rating`, sourced from `restaurant_poi` (`app/services/restaurant_location.py:114,157,963`).
  After cutoff these ratings simply stop updating — visible and slowly staling. No "as of" date shown.
- **Not visible:** Expansion Advisor comparables show only name/category/district/distance
  (`CandidateDetailPanel.tsx:204-210`; `AdvisorySectionCards` shows only `peer.name`). `rating`/
  `review_count` exist in the API payload/type (`expansionAdvisor.ts:174-175`, `comparable_competitors_json`)
  but are **not rendered**. No stale-rating exposure in Expansion Advisor.
- **LLM decision memo** "ratings" wording = **delivery rating velocity (ratings/30d)** from delivery
  data, **not** Google `restaurant_poi.rating`. Unaffected.

### Reality differs from the brief — flagged
1. **A third paid Google-Places workflow exists beyond the brief:**
   `expansion-advisor-data-parking-google.yml` — semiannual cron `0 4 1 1,7 *`, runs
   `python -m app.ingest.expansion_advisor_parking_google` with `GOOGLE_PLACES_API_KEY` (parking-asset
   discovery). **Decision: scope = reviews enrichment only — left running.** If "stop all Google
   Places spend" should include it, the same one-line schedule-disable applies.
2. **"Near-zero scoring impact" holds for Expansion Advisor but NOT the Restaurant Location feature.**
   `app/services/restaurant_location.py` uses Google rating/review fields as scoring signals:
   `competitor_rating` factor weight **0.08** (line 71, `competitor_rating_score`); feature mappings
   `google_rating→competitor_rating`, `google_review_count→delivery_demand`, `log_review_count`,
   `google_price_level→income_proxy` (lines 574-586); `has_google`/`google_confidence` model features
   (lines 1052-1053). These **freeze** (not break) on cutoff.

---

## Phase 2 — Change applied

Single-file edit, `.github/workflows/enrich-google-reviews.yml` (14 insertions, 4 deletions):

```diff
-  schedule:
-    - cron: "0 4 1 1,7 *"   # semiannual: Jan 1 & Jul 1 @ 04:00 UTC (matches the parking-google
-                            # biannual cadence). Paired with STALE_DAYS=150 + limit=0, this
-                            # refreshes the whole warmed table 2×/year (≤6-month staleness).
+  # ── Scheduled Google reviews enrichment DISABLED (cost decision) ──
+  # Paid Google Places reviews enrichment was stopped: ratings/review_count
+  # have near-zero scoring impact, and the match rate (~58%) + cost (~$650
+  # backfill + ~$1,500/yr) don't justify it. Existing restaurant_poi rows and
+  # google_* columns/data are kept; the enrichment code stays dormant. Accepted
+  # trade-off: no fresh business_status (closure) tracking from this point on.
+  #
+  # To RE-ENABLE: un-comment the schedule block below. workflow_dispatch above
+  # is intentionally left active, so the job can still be run manually at any
+  # time without re-enabling the cron.
+  # schedule:
+  #   - cron: "0 4 1 1,7 *"   # semiannual: Jan 1 & Jul 1 @ 04:00 UTC (matches the parking-google
+  #                           # biannual cadence). Paired with STALE_DAYS=150 + limit=0, this
+  #                           # refreshes the whole warmed table 2×/year (≤6-month staleness).
```

**Validation:** YAML parses; active triggers are now `['workflow_dispatch']` only. No columns, data,
or enrichment functions touched. Discovery (`google-places-grid-search.yml`), competitors
(`expansion-advisor-data-competitors.yml`), and parking (`expansion-advisor-data-parking-google.yml`)
are unchanged.

---

## How to re-enable Google reviews enrichment later (one line)

Un-comment the `schedule:` / `cron:` block in `.github/workflows/enrich-google-reviews.yml` (and
merge) — the semiannual run resumes. `workflow_dispatch` was never removed, so manual runs keep
working in the meantime.
