# Runbook: one-time Google reviews cold-start backfill

**Scope:** Riyadh `restaurant_poi` (≈49,412 rows; ≈21,468 never enriched as of this writing).
**Goal:** warm the table once, cheaply, so the semiannual steady-state cron only has to keep it fresh.
**Audience:** operator running the `Enrich Google Reviews` GitHub Action manually.
**This is documentation only — it changes no runtime behavior.**

> ℹ️ There is **no `--text-search-only` flag**, and you don't need one. The never-enriched
> path is already **Text-Search-first**: for a row with no `google_place_id`, the job runs
> Google **Text Search** (which already returns `rating`, `user_ratings_total`,
> `price_level`, `business_status`, and `formatted_address`) and only falls back to a
> **Place Details** call per-row when Text Search did not return both `rating` **and**
> `user_ratings_total` (`app/ingest/google_reviews_enrich.py:226-234`). So selecting
> "only missing" rows *is* the Text-Search-first backfill.

---

## 1. Pre-flight: size the never-enriched set

Run in Codespace (no app DB access from CI):

```bash
psql -c "SELECT count(*) AS never_enriched FROM restaurant_poi WHERE review_count IS NULL OR google_place_id IS NULL;"
```

This is the set the backfill will touch (the job's `--only-missing` filter is
`review_count IS NULL OR google_place_id IS NULL`, plus the Riyadh bbox).

## 2. Cost math (Text-Search-first)

- **Floor estimate:** ≈21,468 rows × ~1 Text Search call × ~$0.003 ≈ **~$64**.
- **Why it's a floor, not the expected bill:** `api_calls` counts **every** HTTP request,
  including each name-variant × type attempt and every retry. A **matched** row stops at
  the first hit, but a **`no_match`** row exhausts up to three passes (multiple Text Search
  calls). Effective cost = `api_calls × per-call price`, and `api_calls / rows` rises with
  the no-match rate. Track it from the run's printed stats: `api_calls`, `no_match`,
  `updated`, and `avg_attempts`.
- **Credit ceiling:** stay under the ~$200/mo Google credit. At the floor, a single backfill
  fits comfortably; if `avg_attempts` runs high (poor match rate), split it (below).

## 3. Trigger the backfill — single window (if comfortably under credit)

GitHub → Actions → **Enrich Google Reviews** → **Run workflow** with:

| input | value | why |
|---|---|---|
| `limit` | `0` | unlimited — walk the entire eligible (missing) set in one pass |
| `force` | `false` | don't re-touch already-enriched rows |
| `only_missing` | `true` | Text-Search-first path; only rows lacking Google data |
| `reset_cursor` | `true` | start from the beginning of the table |

This runs `--resume --batch-size 200 --reset` (no `--limit`, `only_missing` stays true).

## 4. Trigger the backfill — split across two windows (to stay under credit)

Use `limit` as a per-run cap; the resume cursor persists between runs, so a second run
continues where the first stopped. **Do not reset the cursor on the second window.**

**Window 1** (start clean, first ~half):

| input | value |
|---|---|
| `limit` | `11000` |
| `force` | `false` |
| `only_missing` | `true` |
| `reset_cursor` | `true` |

**Window 2** (resume, remainder) — run later, e.g. next billing window:

| input | value |
|---|---|
| `limit` | `11000` |
| `force` | `false` |
| `only_missing` | `true` |
| `reset_cursor` | `false`  ← resumes from Window 1's cursor |

Repeat Window 2 (cursor keeps advancing) until the post-run coverage query (below) shows
`never_enriched` ≈ 0.

## 5. Verify coverage after each run

```bash
psql -c "SELECT count(*) AS total, count(*) FILTER (WHERE google_place_id IS NOT NULL) AS enriched, count(*) FILTER (WHERE google_place_id IS NULL) AS never_enriched, count(*) FILTER (WHERE google_fetched_at >= now() - interval '150 days') AS fresh FROM restaurant_poi;"
```

Also read the workflow run's **stats** (printed at the end and in the job log):
`processed`, `updated`, `no_match`, `api_calls`, `avg_attempts` — `api_calls` is the
real cost driver.

## 6. Hand-off to steady state

Once `never_enriched` ≈ 0, the table is warmed and the **semiannual** cron
(`enrich-google-reviews.yml`, `0 4 1 1,7 *`) takes over. Its scheduled run resolves
`limit=0` + `--no-only-missing` + `--reset`, so it walks the whole table each half-year and
re-fetches every row older than `STALE_DAYS=150` — a ≤6-month freshness window. No further
manual action is required unless coverage drops (re-run §3).

> Note: the steady-state refresh path is **Details-only** by design (it re-fetches each
> warmed row by its stored `google_place_id`, not via Text Search). That is the accepted
> behavior; this runbook does not change it.
