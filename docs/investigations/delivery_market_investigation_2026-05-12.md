# Delivery Market sub-score & dead scrapers — investigation

**Date:** 2026-05-12
**Branch:** `claude/investigate-delivery-market-scrapers-TrK2q`
**Mode:** Read-only. No source files were edited. New files written only under
`scripts/diagnostics/` and this report under `/tmp/`.

> **Critical caveat:** the local repository is a **shallow clone**
> (`git rev-parse --is-shallow-repository` → `true`; 108 commits total; oldest
> commit `2026-04-29 10:23:06 +0000`). The alleged lockstep death window of
> 2026-03-23/24 is **before** the available history. Git archaeology against
> that window is not possible from this checkout. See §3 for what this means.

---

## §1 — Briefing verification table

All file references are against `app/services/expansion_advisor.py` unless
noted. The file is 10 537 lines long; many briefing line numbers had drifted by
a handful of lines.

| # | Claim | Verified? | Current line(s) | One-line excerpt | Notes |
|---|-------|-----------|-----------------|-------------------|-------|
| 1.1 | provider_intelligence_composite weights 0.28/0.30/0.22/0.20 over (density, whitespace, multi_platform, 100-competition) | **Yes** | 7601-7606 | `provider_density_score * 0.28 + provider_whitespace_score * 0.30 + multi_platform_presence_score * 0.22 + (100.0 - delivery_competition_score) * 0.20` | Exact match. Briefing line range correct. |
| 1.2 | delivery_market_pass = 0.6/0.4 mix of density & multi_platform, threshold 45.0, hard-fail only when primary_channel == "delivery" | **Yes** | 2597-2607 | `if primary_channel == "delivery": _delivery_composite = provider_density_score * 0.6 + multi_platform_presence_score * 0.4; delivery_market_pass = _delivery_composite >= thresholds["delivery_provider_density_min"]` | Threshold 45.0 set at 2530 (`"delivery_provider_density_min": 45.0`). Else-branch at 2606-2607 returns `True`. Exact match. |
| 1.3 | Bulk enrichment block ~6929-6952 is gated by `_cached_ea_table_has_rows(db, 'expansion_delivery_market')` and does NOT fall through to a LATERAL multi-platform path | **Yes** | gate at 6694 + 6899; enrichment block 6896-6967 | `ea_delivery_populated = _cached_ea_table_has_rows(db, _EA_DELIVERY_TABLE)` (6694); `if ea_delivery_populated:` (6899) wraps the entire SQL block | `_EA_DELIVERY_TABLE = settings.EXPANSION_DELIVERY_TABLE` (51); production default is `expansion_delivery_market` per CLAUDE.md and migration `d4e5f6a1b2c3`. The `except Exception` at 6965-6966 only logs `"bulk delivery enrichment failed, using legacy counts"` — there is no LATERAL multi-platform fallback. |
| 1.4 | Line ~6942 hardcodes **1200** instead of `provider_radius_m` from `_catchment_radii(service_model)` | **Yes** | **6942** | `ST_SetSRID(ST_MakePoint(...),4326)::geography, 1200` | Confirmed: literal `1200` (meters). **All other call sites of `_catchment_radii`** (5 in total): 5888 (`demand_radius_m`), 6003 (`competition_radius_m`), 6679 (`demand_radius_m`), 6680 (`competition_radius_m`), 6681 (`provider_radius_m`). The bulk delivery enrichment at 6896-6967 is the **only** code path that bypasses `_catchment_radii`. The persisted `provider_radius_m` (6681) is therefore *displayed* in evidence but **not actually used** for the spatial join — a contract-vs-implementation drift the briefing did not call out. |
| 1.5 | SCCC daily cron defaults `platforms` to `'hungerstation'` only | **Yes** | `.github/workflows/expansion-advisor-data-delivery-sccc.yml:11` and `:78` | input default `default: "hungerstation"` (line 11) and `PLATFORMS="${{ github.event.inputs.platforms \|\| 'hungerstation' }}"` (line 78) | Flow: `PLATFORMS` → `python -m app.ingest.expansion_advisor_delivery --platforms "hungerstation"` (line 122). `resolve_platforms` (app/ingest/expansion_advisor_delivery.py:45-79) treats anything that is not the literal `"all"` or a preset key as a comma-separated literal list and validates each name against `SCRAPER_REGISTRY`. So the scheduled SCCC run invokes a single scraper: `hungerstation`. Note: the **non-SCCC** workflow `expansion-advisor-data-delivery.yml` has the same default (line 12 and 59) — also HungerStation-only by default. |
| 1.6 | Weekly POI cron calls `run_all_platforms()` invoking all 14 scrapers (Monday 03:00 UTC) | **Yes** with naming caveat | `.github/workflows/ingest-restaurant-pois.yml:27` | `- cron: "0 3 * * 1"` | The function actually called is `app.delivery.pipeline.run_all_platforms` (app/delivery/pipeline.py:379), reached via `ingest_delivery_platforms` (app/ingest/restaurant_pois.py:146-174). On scheduled runs, `delivery_platforms=` is empty (workflow line 79), so `sources or list(SCRAPER_REGISTRY.keys())` (restaurant_pois.py:171) expands to **all 14 registered scrapers**. Briefing said the function lives in `app/connectors/delivery_platforms.py` — it does not; the connectors module exposes `scrape_all_platforms` (delivery_platforms.py:1359), a different, unused-by-the-workflow function. **The 14 (briefing said 14, not 13 + HungerStation = 14) registered scrapers, in registration order, are:** `hungerstation`, `talabat`, `mrsool`, `jahez`, `toyou`, `keeta`, `thechefz`, `lugmety`, `shgardi`, `ninja`, `nana`, `dailymealz`, `careemfood`, `deliveroo`. The module docstring at delivery_platforms.py:7-10 claims "16 total" but only 14 are decorated with `@_register`. |
| 1.7a | `_channel_fit_score` consumes multi_platform_presence_score | **Yes** | 1374-1381 | `if channel == "delivery": return _clamp(provider_density_score * 0.7 + multi_platform_presence_score * 0.3)` (1377); balanced branch also uses it: `return _clamp(55.0 + (multi_platform_presence_score - 50.0) * 0.2)` (1381) | Two uses, not one — both the `delivery` and `balanced` branches consume it. |
| 1.7b | `_confidence_grade` adds +2.5 if `multi_platform_presence_score > 0` | **Yes** | 3076-3077 | `if float(multi_platform_presence_score or 0.0) > 0: adjusted += 2.5` | Briefing line ~3076 confirmed. |
| 1.7c | `_top_positives_and_risks` uses `>0` as observed-delivery flag | **Yes** | 2900-2905 | `delivery_observed = (_safe_float(candidate.get("provider_density_score")) > 0 or _safe_float(candidate.get("multi_platform_presence_score")) > 0 or _safe_float(candidate.get("delivery_competition_score")) > 0)` | Used to qualify positives, risks (2910-2931) and the `"Delivery market data is inferred — no observed listings near site."` risk (2943). |
| 1.7d | `strongest_delivery_market` selector in compare summary ~10004 | **Yes** | 10004 | `strongest_delivery_market = max(items, key=lambda item: _safe_float(item.get("provider_density_score")) + _safe_float(item.get("multi_platform_presence_score")))["candidate_id"]` | Surfaced under key `"strongest_delivery_market_candidate_id"` at 10037 in the compare panel. |
| extra | Weight constant for `delivery_demand` component is **4.3820** | **Yes** | 2826 (definition), 2864 (application) | `"delivery_demand": 4.3820` (2826); `"delivery_demand": round(_safe_float(provider_intelligence_composite) * 0.043820, 2)` (2864) | The two values must stay in lock-step. Sum-to-100 invariant asserted at 2833. |
| extra | Frontend resolver for `delivery_demand` | **Yes** | `frontend/src/features/expansion-advisor/scoreComponentMeta.ts:426-454` | `delivery_demand: [ { key: "provider_listing_count", … }, { key: "provider_platform_count", … }, { key: "multi_platform_presence_score", … } ]` | Briefing said 426-448; actual end is closer to 454. Three inputs surfaced; `delivery_source` is overridden to `"expansion_delivery_market"`. |
| extra | i18n `delivery_demand.definition` | **Yes** | `frontend/src/i18n/en.json:1189-1197` (and ar.json:1117) | `"label": "Delivery Market", "definition": "Multi-platform delivery presence in the area — how many providers are active, how many platforms list food in this district, and how strong multi-platform presence is."` | UI promises multi-platform information that the production data layer (HungerStation-only) cannot deliver. Latent product-claim risk. |

**Additional downstream consumers of `multi_platform_presence_score` the
briefing did not list:**

- Persisted as a top-level numeric column on `expansion_candidate` (mirrored at
  ~8960, INSERT bind at ~9187, 9253, 9550, 9868; SELECT at 10068).
- Mirrored in `feature_snapshot_json` at 7661 (so the frontend resolver hits it
  via `featureSnapshot`).
- Returned in the per-candidate API row (9957, 10186).
- Listed as a known field name in the explain/score serializer (10335, 10532).

Result: **every claim in §1 of the briefing is verified.** A handful of line
numbers have shifted by ≤6 lines; the constructs are intact.

---

## §2 — Scraper architecture summary

The 14 scrapers all live in `app/connectors/delivery_platforms.py` and share a
single thin spine:

- **HTTP client:** `httpx.get(url, timeout=45, headers={"User-Agent": …},
  follow_redirects=True)` (lines 35, 105-149). No proxy support, no cookie
  jar, no session, no anti-bot helper (no `cloudscraper`,
  `undetected-chromedriver`, etc.). Two User-Agent strings (`_UA` and
  `_BROWSER_UA`) — only Jahez (1136) and Keeta (1180) pass `_BROWSER_UA`; the
  other 12 use the oaktree default UA.
- **Robots.txt:** every fetch goes through `_safe_get` (345-360) which calls
  `_robots_allows_cached`. If robots.txt disallows, the URL is silently
  skipped (`return None`).
- **JS-rendered pages:** `_safe_get` calls `_requires_js` (152-162) and
  **silently returns `None`** when the body is short and contains `<script` or
  contains "you need to enable javascript" / "please enable javascript". A
  Cloudflare interstitial that lands on a JS challenge will match this
  heuristic and the scraper will skip every URL.
- **Retry policy:** `_fetch_with_retries` (105-149) retries on 429/503 and
  transient timeouts (with backoff 2/4/8/16 s); **403 is treated as a hard
  failure and returns `None` with only `logger.debug(...)`** — so a Cloudflare
  WAF block produces a silent miss, no warning, no metric.
- **Generic crawler:** `_generic_sitemap_scrape` (752-869) is used by 12 of
  the 14 platforms (HungerStation has its own bespoke flow at 876-1066;
  Talabat reuses the generic but with `multi_strategy=True`). It pulls a
  sitemap, optionally expands a sitemap index, filters by URL pattern, fetches
  each page, and yields a dict built by `_extract_page_data` (609-695). The
  per-page extractor tries JSON-LD → `__NEXT_DATA__` / `window.__data__` /
  `window.__INITIAL_STATE__` → OpenGraph → `<title>`.
- **Per-platform isolation:** `run_all_platforms` (app/delivery/pipeline.py:379)
  opens a fresh `SessionLocal()` per platform and runs `run_platform_scrape`
  (163-333). One platform's failure cannot poison another.
- **Audit log:** `DeliveryIngestRun` (app/delivery/models.py:118-142) records
  one row per platform invocation with `started_at`, `finished_at`, `status`
  ∈ {`running`,`completed`,`completed_with_errors`,`failed`}, plus row
  counters and a JSONB `error_summary`. Status is set to `failed` only when
  the scraper itself raised an uncaught exception (pipeline.py:117-118 and
  216-311). **A scraper that fetches 5 000 URLs and parses none of them
  successfully completes with `status='completed'` and `rows_inserted=0`.**

**Plausible simultaneous-failure surfaces** (single line of code whose
behavior change breaks ≥13 platforms at once):

1. `_safe_get` (345-360) — the silent-on-JS, silent-on-403, silent-on-empty
   path. Any target switching to a JS-rendered SPA or applying a Cloudflare
   WAF rule will route through this funnel.
2. `_fetch_with_retries` (105-149) — silent on 403, retry on 429/503 only.
   A KSA-wide WAF tier that returns 403 to cloud-IP egress flips all
   platforms in one moment.
3. `_extract_page_data` (609-695) — if the targets stop emitting JSON-LD /
   `__NEXT_DATA__` blobs (e.g., a CDN re-skinning), parsing falls back to
   `<title>` and yields garbage like `"Just A Moment..."` (the Cloudflare
   challenge title is exactly that and is > 2 chars, so it passes the
   `len(name.strip()) >= 2` gate at hungerstation:1026).
4. `httpx` version bump — a transport-level regression (e.g., HTTP/2 default,
   default UA, default redirects) is one shared dependency away from
   identical, simultaneous failure.

---

## §3 — Lockstep-death timeline

**Available git history in this checkout** (output of `git log --all
--pretty=format:%ai | sort | head/tail`):

```
oldest commit: 2026-04-29 10:23:06 +0000
newest commit: 2026-05-11 20:19:48 +0300
total commits: 108
shallow clone: true
```

The alleged lockstep death window (2026-03-23/24) is **roughly five weeks
before the start of this checkout's history**. Running
`git log --since="2026-03-15" --until="2026-04-01"` returns **0 rows**. The
ground truth for that window is not present locally — it lives only on the
remote.

**Blame cluster for the registry decorator lines** (`git blame` of the 14
`@_register` lines in `app/connectors/delivery_platforms.py`):

```
all 14 lines:  ^dd75482ed  hummodi6991  2026-04-29 12:07:46 +0300
               "Merge pull request #1172 from
                hummodi6991/claude/parallelize-prewarm-threadpool-H5qkH"
```

The leading `^` is `git blame`'s "this is the oldest commit in this shallow
clone, the true origin is unknown." Every scraper line in the current file
collapses onto the shallow-clone boundary, so the local blame **cannot
distinguish** which scrapers were touched together. To do the real
archaeology, the operator needs to either:

- `git fetch --unshallow` on a separate non-investigation branch (note:
  this is a state-changing operation — I did not run it because the brief is
  read-only), or
- Inspect history on GitHub directly for `app/connectors/delivery_platforms.py`
  and adjacent files (`app/delivery/pipeline.py`, `requirements*.txt`,
  `pyproject.toml`).

This is the single most important finding for §3: **the briefing's claim about
the March 23-24 lockstep date is unfalsifiable from this checkout.** Treat it
as still-unverified until run against the unshallowed remote or against the
`delivery_ingest_run` audit table (see §5).

---

## §4 — Three most plausible root-cause hypotheses, ranked

### H1 (most likely) — Bot-protection / Cloudflare WAF flipped the targets

**Evidence for**
- 13 of 14 scrapers share `_safe_get → _fetch_with_retries → httpx.get` with
  default UA. 403 is treated as a silent terminal state (no retry, no warn,
  `return None`).
- KSA delivery platforms widely use Cloudflare; a single Cloudflare account
  policy change ("block requests from AWS/GHA ASNs") affects every protected
  site in the region in the same hour.
- HungerStation is the only platform whose scraper *isn't* an instance of
  `_generic_sitemap_scrape` (it has its own bespoke flow at 876-1066) — if
  HungerStation's sitemap structure is the only one not hidden behind the
  new bot rule, you get a clean 1-of-14 survivor pattern. The 13-of-14
  symmetry argues for an environment-level cause, not 13 independent code
  bugs.
- `_requires_js` returns `True` for short HTML bodies with `<script>` —
  this matches a typical Cloudflare interstitial page byte-for-byte.

**Evidence against**
- Local code shows no recent (2026-04-29 → today) change to either
  `_safe_get` or `_fetch_with_retries`; the alleged failure predates the
  shallow-clone boundary. So the "trigger" sits on the server side, not in
  this repo.
- No proxy infrastructure exists. If H1 is true, scrapers cannot recover
  without either (a) a new code path with residential proxies / scrape-as-
  a-service / Playwright, or (b) a network change at the SCCC egress.

**Read-only discriminator I would run next**
- Pull `delivery_ingest_run.error_summary` for runs since 2026-03-15 grouped
  by `platform`. A high concentration of HTTP-status-403 or
  "page requires JS rendering" entries across the 13 dead platforms (and
  *not* in HungerStation) would confirm H1 without leaving the read path.

### H2 — Shared parser regression in `_extract_page_data`

**Evidence for**
- `_extract_page_data` is shared by all 14 platforms. If JSON-LD extraction
  silently breaks (e.g. a `re.DOTALL` removal, a new content-type quirk),
  every platform that used to lean on it falls through to `<title>`-only
  records, which the persistence layer then accepts.
- The pipeline's "scraper succeeded with zero rows" / `runs_ok > 0,
  rows_inserted = 0` state is exactly what a parser-only regression
  produces: HTTP fetches succeed, but every record has no name and is
  dropped at lines 1026-1030 (`if not name or len(name.strip()) < 2:
  no_parse += 1`).

**Evidence against**
- A parser regression that takes out 13 of 14 in lockstep on a specific
  calendar day requires a code change on that day. The available local
  history starts five weeks later, but the file boundary at 2026-04-29
  shows the parser as it stands today — and `_extract_page_data` is still
  shaped exactly as expected. A parser regression in the dead window
  would have to have been **reverted** before the shallow-clone window,
  which is unlikely.
- HungerStation uses the same `_extract_page_data` and the same JSON-LD
  ladder. If the parser were the problem, HungerStation would also break.

**Read-only discriminator I would run next**
- Compare `rows_scraped` vs `rows_inserted` per platform per run. A pure
  parser break shows `rows_scraped > 0, rows_parsed > 0` (it doesn't —
  `rows_parsed` is incremented after `parse_legacy_record`, not after
  `_extract_page_data`) but **most rows_skipped under
  `rejection_reasons.parse_error` would be absent** since the parse failure
  is swallowed inside `_extract_page_data` (no exception, just a sparse
  dict). The signal is therefore: `rows_scraped > 0` but
  `rows_with_name = 0` and `rows_with_coords = 0`. A query on those two
  audit columns discriminates H1 from H2 cleanly.

### H3 — Infrastructure egress / runner-image change on the dead date

**Evidence for**
- The SCCC daily cron submits a K8s Job on Alibaba Cloud (ACK), inheriting
  the cluster's egress IP. A NAT change on 2026-03-23 (e.g., a new node
  pool with a different SNAT IP added to most cluster nodes) would flip
  every scraper that crawls a WAF-protected target simultaneously.
- The deployed image is reused from the running app
  (`kubectl get deployment ...` → image tag) — if the image's `httpx` or
  `urllib3` version was bumped in a deploy that landed on 23 March, every
  scraper inherits the same TLS fingerprint / default header change.
- Cloudflare/Akamai are known to silently classify and block
  cloud-provider ASNs in waves; the symmetric impact across 13/14 is a
  textbook signature.

**Evidence against**
- Cannot be confirmed from the repo alone — needs cluster / deploy /
  base-image history. The image bump is not under git control.

**Read-only discriminator I would run next**
- Cross-check the dates of the last green run vs the first all-empty run in
  `delivery_ingest_run` against (a) deploy history of `oaktree-estimator`
  on ACK and (b) `requirements.txt` / lockfile history on the **remote**
  (visible via GitHub blame without unshallowing). If the two changed on
  2026-03-23/24 we have a smoking gun for H3. If the last app deploy
  predates 2026-03-23 by weeks, H3 is much weaker than H1.

**Combined likelihood:** H1 ≳ H3 >> H2. H1 and H3 are not mutually exclusive
— a Cloudflare rule rollout combined with a Saudi-region egress change would
plausibly produce *exactly* this fingerprint. H2 stays in the bag because the
audit-table check is cheap and would dispose of it in one query.

---

## §5 — Diagnostic SQL files

Four files written under `scripts/diagnostics/`. All use `psql -f`-compatible
syntax (no heredocs, no `:var` substitutions that would require `-v` flags).

| File | What it shows |
|------|----------------|
| `scripts/diagnostics/delivery_scraper_health.sql` | Per-platform run counts, last successful insert, rows inserted in 30 / 90 / 365 days from `delivery_ingest_run`. The primary "is the scraper dead?" check. |
| `scripts/diagnostics/delivery_platform_distribution.sql` | Distinct platforms appearing in `expansion_delivery_market` and `delivery_source_record`, with row counts and date ranges per platform. |
| `scripts/diagnostics/provider_platform_count_distribution.sql` | Distribution of `provider_platform_count` over the last 30 days of `expansion_candidate` (reads from `feature_snapshot_json`); quantifies the "1 everywhere" claim. |
| `scripts/diagnostics/multi_platform_presence_score_distribution.sql` | Score distribution + density-vs-presence cross-tab (column is persisted on `expansion_candidate` — confirmed in code, around expansion_advisor.py:8960 / 9187). |

Run each with `psql "$DATABASE_URL" -f scripts/diagnostics/<name>.sql`.

---

## §6 — Surprises / latent issues the briefing did not flag

Items found in passing. **Not pursued — flagged only.** No fixes proposed.

1. **`provider_radius_m` is persisted in evidence but ignored in spatial join.**
   The candidate row at 6679-6681 captures `provider_radius_m` from
   `_catchment_radii(service_model)`, surfaced to the UI as evidence of the
   service-model-aware catchment used to compute the score. The bulk SQL at
   6942 ignores it and uses literal `1200`. The UI claim and the
   implementation drift apart silently.

2. **`_active_platform_count` denominator silently fabricates a fallback of 5.**
   At expansion_advisor.py:6697 (`_active_platform_count = 5  # fallback`),
   when `ea_delivery_populated` is false (i.e., no rows at all) the
   multi-platform presence denominator is **5**, not the registry size (14)
   and not 1. Because the score then defaults to `50.0  # unknown, not zero,
   not 100` (7425, 7458), the fallback never actually flows through; but if
   the table has any rows at all and `COUNT(DISTINCT platform)` succeeds, the
   denominator becomes that count — meaning if **only HungerStation** has
   rows, the denominator collapses to 1 and the score saturates bimodally
   (0 if the catchment has no HungerStation listing, 100 if it has any).
   That's the smoking-gun shape `multi_platform_presence_score_distribution.sql`
   is built to surface.

3. **No alerting / circuit-breaker / dead-scraper detection.** Searches
   across `app/delivery/`, `app/ingest/`, and the workflows return nothing
   alerting-shaped. The only zero-row guard is the one in
   `expansion-advisor-data-delivery.yml:64-78` ("Fail on zero useful
   ingest"), and it only sees the platforms that the run was invoked with.
   The SCCC daily cron's default of `'hungerstation'` therefore means the
   guard cannot ever detect "the other 13 are dead" — it can only detect
   "HungerStation is dead too."

4. **The weekly POI workflow runs on GHA-hosted runners**
   (`runs-on: ubuntu-latest`) with a 180-minute timeout
   (`ingest-restaurant-pois.yml:39`). Sequential per-platform iteration via
   `run_all_platforms` × 14 platforms × ≤5 000 pages × 2-3 s crawl-delay
   each → easily multi-hour. If most scrapers silently return 0 records
   quickly (H1 / H2), the workflow may currently fit inside 180 minutes
   *only because* the scrapers are dead. A fix that restores the dead
   scrapers will blow this timeout unless the workflow is re-scoped.

5. **The module docstring at `delivery_platforms.py:7-10` advertises "16
   total" platforms** while only 14 are decorated with `@_register`. The
   stale claim isn't load-bearing, but it conflicts with the i18n string
   "Multi-platform delivery presence in the area" (en.json:1191) — both
   the code-internal count and the UI promise are detached from the
   single-platform reality of `expansion_delivery_market`.

6. **`<title>` fallback can yield `"Just A Moment..."`-shaped records.**
   `_extract_page_data` (683-693) accepts any title > 2 chars after
   stripping ` | `, ` - `, ` – `, ` — ` suffixes. The Cloudflare challenge
   page title `"Just a moment..."` survives this stripping and would
   produce a "valid" record named `"Just A Moment..."` if the page got
   past `_requires_js` first — which is plausible when the challenge body
   is long enough or doesn't contain the literal phrase
   "you need to enable javascript". Worth grepping
   `delivery_source_record.restaurant_name_normalized` for `moment` or
   `attention required` to confirm if any such names slipped in.

7. **HungerStation scraper has thousand-line-scale bespoke logic** (876-1066)
   that no other platform shares; the other 13 share the generic
   `_generic_sitemap_scrape` path. This is *exactly* the asymmetry that
   would produce a "13 dead, 1 alive" pattern when something changes that
   affects the generic path but not the bespoke one (e.g., the JS-detection
   heuristic at 152-162, the silent 403 path at 128-135, or the sitemap
   discovery strategies at 254-316).

---

**Report path:** `/tmp/delivery_market_investigation_2026-05-12.md`
