#!/usr/bin/env python3
"""besttime_coverage_probe.py — go/no-go coverage probe for Layer 2 (BestTime.app).

READ-ONLY DIAGNOSTIC. This script is NOT part of the app. It exists to answer
ONE question BEFORE we commit ~$500-900/yr of spend:

    "For Riyadh F&B venues, how many does BestTime actually return, and how many
     have a usable busyness forecast (vs. 'forecast unavailable')?"

It samples ~10 district centroids spread across Riyadh, calls the BestTime
**New Venue Search** (radius) endpoint for restaurant venues, and reports, per
district: venues returned, venues WITH a usable forecast, venues with NONE, and
a running credit tally. It HARD-CAPS the run so it can never burn more than the
free 100-credit allotment.

------------------------------------------------------------------------------
HOW I RUN IT (Codespace — the author has NO key and the besttime.app domain is
outside the agent network allowlist, so this was never executed here):

    export BESTTIME_API_KEY="pri_xxxxxxxxxxxxxxxxxxxx"   # PRIVATE key (pri_...)
    python3 scripts/diagnostics/besttime_coverage_probe.py

Optional knobs (env):
    BESTTIME_MAX_VENUES_PER_DISTRICT   default 8   (forecasts requested/district)
    BESTTIME_CREDIT_CAP                default 80  (global hard ceiling < 100)
    BESTTIME_RADIUS_M                  default 2000
    BESTTIME_DRY_RUN                   set to 1 to print the plan and exit w/o
                                       any network call (cost = 0 credits)

VERIFY-FIRST NOTE: BestTime's request/response field names and credit-per-call
accounting can change. The author could not hit the live API. Before trusting
the totals, sanity-check one district against the BestTime dashboard credit
meter, and confirm the response keys used below (`venues`, `venue_foot_traffic`,
`_links`) against current BestTime docs. The CREDIT_CAP is a belt-and-braces
guard, but the dashboard meter is the source of truth.
------------------------------------------------------------------------------
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

API_BASE = "https://besttime.app/api/v1"
SEARCH_URL = f"{API_BASE}/venues/search"
PROGRESS_URL = f"{API_BASE}/venues/progress"

# ~10 district centroids spread across Riyadh (lat, lng). Chosen to span
# north (Nakheel/Yasmin/Sahafa), centre (Olaya/Malaz/Murabba), west (Wurud/
# Irqah), east (Qurtubah/Rawdah) and south (Aziziyah) so coverage isn't a
# single-neighbourhood artifact.
RIYADH_DISTRICTS: list[dict] = [
    {"name": "Al Olaya",        "lat": 24.6904, "lng": 46.6850},
    {"name": "Al Malaz",        "lat": 24.6628, "lng": 46.7392},
    {"name": "Al Nakheel",      "lat": 24.7505, "lng": 46.6386},
    {"name": "Al Yasmin",       "lat": 24.8290, "lng": 46.6360},
    {"name": "Al Sahafa",       "lat": 24.8060, "lng": 46.6420},
    {"name": "King Fahd Dist.", "lat": 24.7560, "lng": 46.6700},
    {"name": "Al Murabba",      "lat": 24.6480, "lng": 46.7130},
    {"name": "Al Wurud",        "lat": 24.7080, "lng": 46.6660},
    {"name": "Qurtubah",        "lat": 24.8120, "lng": 46.7560},
    {"name": "Al Aziziyah",     "lat": 24.5560, "lng": 46.7330},
]

# F&B venue query. BestTime matches venues by free-text type; "restaurants"
# is the broad bucket. Keep it broad so we measure the UPPER bound of coverage.
SEARCH_QUERY = "restaurants"


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "").strip() or default)
    except ValueError:
        return default


MAX_VENUES_PER_DISTRICT = _env_int("BESTTIME_MAX_VENUES_PER_DISTRICT", 8)
CREDIT_CAP = _env_int("BESTTIME_CREDIT_CAP", 80)
RADIUS_M = _env_int("BESTTIME_RADIUS_M", 2000)
DRY_RUN = os.environ.get("BESTTIME_DRY_RUN", "").strip() in ("1", "true", "yes")

# Assume worst case 1 credit per forecasted venue (BestTime bills per venue
# forecast). This is the conservative accounting used to enforce CREDIT_CAP.
CREDITS_PER_VENUE = 1


def _http_post(url: str, params: dict) -> tuple[int, dict]:
    """POST form-encoded params; return (status_code, parsed_json_or_error)."""
    data = urllib.parse.urlencode(params).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, json.loads(body or "{}")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        try:
            parsed = json.loads(body or "{}")
        except json.JSONDecodeError:
            parsed = {"error": body[:300]}
        return exc.code, parsed
    except (urllib.error.URLError, TimeoutError) as exc:
        return 0, {"error": f"network: {exc}"}
    except json.JSONDecodeError as exc:
        return -1, {"error": f"bad json: {exc}"}


def _http_get(url: str, params: dict) -> tuple[int, dict]:
    full = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(full, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as exc:
        return exc.code, {"error": exc.read().decode("utf-8", "replace")[:300]}
    except (urllib.error.URLError, TimeoutError) as exc:
        return 0, {"error": f"network: {exc}"}
    except json.JSONDecodeError as exc:
        return -1, {"error": f"bad json: {exc}"}


def _venue_has_forecast(venue: dict) -> bool:
    """A venue is 'usable' when BestTime returned an actual busyness forecast.

    BestTime flags forecastable venues with `forecast: true` and embeds the
    week busyness under `venue_foot_traffic_forecast` / `day_raw`. We treat any
    of those signals as a usable forecast and an explicit `forecast: false`
    (or a 'forecast_unavailable' status) as none.
    """
    if venue.get("forecast") is False:
        return False
    if venue.get("forecast") is True:
        return True
    # Fallbacks if the boolean key is absent on this API version:
    if venue.get("venue_foot_traffic_forecast"):
        return True
    analysis = venue.get("analysis") or venue.get("venue_foot_traffic")
    if isinstance(analysis, list) and analysis:
        return True
    status = str(venue.get("status") or venue.get("forecast_status") or "").lower()
    if "unavailable" in status or "not_enough" in status:
        return False
    return False


def _search_district(api_key: str, district: dict, budget_venues: int) -> dict:
    """Run one radius search. Returns a per-district result dict."""
    params = {
        "api_key_private": api_key,
        "q": SEARCH_QUERY,
        "lat": district["lat"],
        "lng": district["lng"],
        "radius": RADIUS_M,
        "num": budget_venues,          # cap venues forecasted this call
        "fast": "true",                # cheaper/faster forecast variant
        "opening_day": 1,              # any non-trivial window; tune as needed
    }
    status, payload = _http_post(SEARCH_URL, params)

    if status == 429:
        return {"error": "rate_limited (429) — back off and retry later",
                "venues": 0, "with_forecast": 0, "without_forecast": 0,
                "credits_spent": 0}
    if status in (401, 403):
        return {"error": f"auth error ({status}) — check BESTTIME_API_KEY is the PRIVATE key",
                "venues": 0, "with_forecast": 0, "without_forecast": 0,
                "credits_spent": 0}
    if status != 200 or not isinstance(payload, dict):
        return {"error": f"http {status}: {str(payload)[:160]}",
                "venues": 0, "with_forecast": 0, "without_forecast": 0,
                "credits_spent": 0}

    venues = payload.get("venues")

    # Async job path: poll the progress endpoint a bounded number of times.
    if venues is None:
        job_id = payload.get("job_id") or (payload.get("_links") or {}).get("job_id")
        if job_id:
            for _ in range(8):
                time.sleep(3)
                pstatus, ppayload = _http_get(PROGRESS_URL, {
                    "api_key_private": api_key, "job_id": job_id,
                })
                if pstatus == 200 and isinstance(ppayload, dict):
                    if ppayload.get("venues") is not None:
                        payload = ppayload
                        venues = ppayload.get("venues")
                        break
                    if str(ppayload.get("job_status") or "").lower() in ("completed", "ok"):
                        venues = ppayload.get("venues") or []
                        break
    if venues is None:
        return {"error": "no venues field in response (API shape changed?)",
                "venues": 0, "with_forecast": 0, "without_forecast": 0,
                "credits_spent": 0}

    n = len(venues)
    with_fc = sum(1 for v in venues if _venue_has_forecast(v))
    # Conservative billing: BestTime bills for venues it forecasts.
    credits_spent = with_fc * CREDITS_PER_VENUE
    return {
        "error": None,
        "venues": n,
        "with_forecast": with_fc,
        "without_forecast": n - with_fc,
        "credits_spent": credits_spent,
    }


def main() -> int:
    api_key = os.environ.get("BESTTIME_API_KEY", "").strip()

    print("=" * 78)
    print("BestTime.app Riyadh F&B coverage probe (Layer 2 go/no-go)")
    print(f"  districts={len(RIYADH_DISTRICTS)}  query='{SEARCH_QUERY}'  "
          f"radius={RADIUS_M}m")
    print(f"  per-district forecast cap={MAX_VENUES_PER_DISTRICT}  "
          f"GLOBAL credit cap={CREDIT_CAP} (free tier = 100)")
    print("=" * 78)

    if DRY_RUN:
        planned = min(CREDIT_CAP,
                      len(RIYADH_DISTRICTS) * MAX_VENUES_PER_DISTRICT * CREDITS_PER_VENUE)
        print("DRY RUN — no network calls made. Planned worst-case spend "
              f"<= {planned} credits.")
        for d in RIYADH_DISTRICTS:
            print(f"  would search: {d['name']:<16} ({d['lat']}, {d['lng']})")
        return 0

    if not api_key:
        print("ERROR: BESTTIME_API_KEY not set. Export your PRIVATE key (pri_...).",
              file=sys.stderr)
        return 2

    total_venues = 0
    total_with_fc = 0
    total_credits = 0
    rows: list[tuple[str, dict]] = []

    for d in RIYADH_DISTRICTS:
        remaining = CREDIT_CAP - total_credits
        if remaining <= 0:
            print(f"[SKIP] {d['name']:<16} credit cap reached "
                  f"({total_credits}/{CREDIT_CAP}).")
            rows.append((d["name"], {"error": "skipped (credit cap)",
                                     "venues": 0, "with_forecast": 0,
                                     "without_forecast": 0, "credits_spent": 0}))
            continue

        budget = max(1, min(MAX_VENUES_PER_DISTRICT, remaining // CREDITS_PER_VENUE))
        res = _search_district(api_key, d, budget)
        rows.append((d["name"], res))

        total_venues += res["venues"]
        total_with_fc += res["with_forecast"]
        total_credits += res["credits_spent"]

        if res["error"]:
            print(f"[WARN] {d['name']:<16} {res['error']}")
        else:
            cov = (100.0 * res["with_forecast"] / res["venues"]) if res["venues"] else 0.0
            print(f"[OK]   {d['name']:<16} venues={res['venues']:>3}  "
                  f"forecast={res['with_forecast']:>3}  none={res['without_forecast']:>3}  "
                  f"coverage={cov:5.1f}%  credits+={res['credits_spent']}  "
                  f"(running {total_credits}/{CREDIT_CAP})")

        time.sleep(1.5)  # gentle rate-limit courtesy

    print("-" * 78)
    overall_cov = (100.0 * total_with_fc / total_venues) if total_venues else 0.0
    districts_with_any = sum(1 for _, r in rows if r["venues"] > 0)
    print("VERDICT")
    print(f"  districts returning >=1 venue : {districts_with_any}/{len(RIYADH_DISTRICTS)}")
    print(f"  total F&B venues seen         : {total_venues}")
    print(f"  total with usable forecast    : {total_with_fc}")
    print(f"  overall forecast coverage     : {overall_cov:.1f}%")
    print(f"  credits spent (est.)          : {total_credits}/{CREDIT_CAP} "
          f"(free allotment 100)")
    print("-" * 78)
    print("READ: high coverage (e.g. >40% of venues forecastable across most")
    print("districts) => L2 is worth the paid tier. Low coverage / many empty")
    print("districts => BestTime does not cover Riyadh F&B well enough; hold spend.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
