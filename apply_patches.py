#!/usr/bin/env python3
"""
Apply all 6 Expansion Advisor patches to /app/services/expansion_advisor.py
Usage:
    python apply_patches.py                    # dry-run (default)
    python apply_patches.py --apply            # apply patches
    python apply_patches.py --apply --backup   # apply + create .bak
Each patch uses exact string matching. If a patch fails to match, it is
skipped with a warning (the code may have already been patched).
"""
import argparse
import sys
import shutil
from pathlib import Path
TARGET = Path("/app/services/expansion_advisor.py")
# ── Patch definitions ──────────────────────────────────────────────────
# Each tuple: (name, old_text, new_text)
PATCHES = []
# ── Patch 1: Cannibalization — continuous decay ──────────────────────
PATCHES.append(("1-cannibalization-continuous-decay",
# OLD
'''def _cannibalization_score(distance_m: float | None, service_model: str) -> float:
    if distance_m is None:
        return 25.0
    if distance_m < 1000:
        base = 85.0
    elif distance_m <= 2500:
        base = 55.0
    else:
        base = 25.0
    if service_model in {"qsr", "cafe"}:
        base -= 8.0
    elif service_model == "dine_in":
        base += 10.0
    elif service_model == "delivery_first":
        base -= 3.0
    if service_model == "delivery_first" and distance_m is not None and distance_m < 500:
        base += 7.0
    return _clamp(base)''',
# NEW
'''def _cannibalization_score(distance_m: float | None, service_model: str) -> float:
    """Continuous exponential-decay cannibalization risk.
    Returns 0-100 where higher = more cannibalization risk.
    Uses a smooth curve so that every candidate gets a distinct score,
    enabling meaningful ranking differentiation.
    """
    if distance_m is None:
        # No existing branches — low but non-zero baseline risk.
        return 15.0
    # Service-model-specific parameters:
    #   half_life_m  — distance at which risk drops to 50% of maximum
    #   ceiling      — maximum risk score at distance=0
    params = {
        "qsr":            {"half_life_m": 1200.0, "ceiling": 82.0},
        "cafe":           {"half_life_m": 1000.0, "ceiling": 80.0},
        "delivery_first": {"half_life_m":  800.0, "ceiling": 78.0},
        "dine_in":        {"half_life_m": 1800.0, "ceiling": 92.0},
    }
    p = params.get(service_model, {"half_life_m": 1400.0, "ceiling": 85.0})
    half_life = p["half_life_m"]
    ceiling = p["ceiling"]
    # Exponential decay: risk = ceiling * 2^(-distance / half_life)
    # At distance=0 → ceiling, at distance=half_life → ceiling/2,
    # at distance=2*half_life → ceiling/4, etc.
    decay = math.pow(2.0, -distance_m / half_life)
    base = ceiling * decay
    # Extra overlap penalty for delivery-first when extremely close
    if service_model == "delivery_first" and distance_m < 400:
        base += 7.0 * (1.0 - distance_m / 400.0)
    return _clamp(base)'''))
# ── Patch 2: Provider whitespace guard for zero delivery data ────────
PATCHES.append(("2-whitespace-zero-guard",
# OLD
'''        provider_density_score = _clamp((provider_listing_count / 45.0) * 100.0)
        provider_whitespace_score = _clamp(100.0 - max(0.0, (delivery_competition_count - 6) * 6.0) - min(35.0, provider_density_score * 0.2))
        multi_platform_presence_score = _clamp((provider_platform_count / 5.0) * 100.0)
        delivery_competition_score = _clamp((delivery_competition_count / 35.0) * 100.0)''',
# NEW
'''        # Guard: when no delivery data is observed, scores must reflect
        # *uncertainty* (neutral 50), not opportunity (100).  Without this,
        # the whitespace formula yields 100 for zero-data candidates.
        _delivery_observed = (provider_listing_count > 0 or provider_platform_count > 0 or delivery_competition_count > 0)
        if _delivery_observed:
            provider_density_score = _clamp((provider_listing_count / 45.0) * 100.0)
            provider_whitespace_score = _clamp(100.0 - max(0.0, (delivery_competition_count - 6) * 6.0) - min(35.0, provider_density_score * 0.2))
            multi_platform_presence_score = _clamp((provider_platform_count / 5.0) * 100.0)
            delivery_competition_score = _clamp((delivery_competition_count / 35.0) * 100.0)
        else:
            provider_density_score = 0.0
            provider_whitespace_score = 50.0   # unknown ≠ excellent
            multi_platform_presence_score = 0.0
            delivery_competition_score = 0.0'''))
# ── Patch 3: N+1 delivery enrichment → bulk query ────────────────────
PATCHES.append(("3-bulk-delivery-enrichment",
# OLD
'''    for row in rows:
      try:
        area_m2 = _safe_float(row.get("area_m2"))
        population_reach = _safe_float(row.get("population_reach"))
        competitor_count = _safe_int(row.get("competitor_count"))
        delivery_listing_count = _safe_int(row.get("delivery_listing_count"))
        provider_listing_count = _safe_int(row.get("provider_listing_count"))
        provider_platform_count = _safe_int(row.get("provider_platform_count"))
        delivery_competition_count = _safe_int(row.get("delivery_competition_count"))
        landuse_label = row.get("landuse_label")
        landuse_code = row.get("landuse_code")
        district = row.get("district")
        # ── Enrich delivery scores from normalized table when available ──
        if ea_delivery_populated:
            try:
                with db.begin_nested():
                    ea_del = db.execute(
                        text(f"""
                            SELECT
                                COUNT(*) AS listing_count,
                                COUNT(DISTINCT platform) AS platform_count,
                                COUNT(*) FILTER (WHERE lower(COALESCE(category, '')) LIKE :cat_like) AS cat_count
                            FROM {_EA_DELIVERY_TABLE}
                            WHERE geom IS NOT NULL
                              AND ST_DWithin(
                                  geom::geography,
                                  ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                                  1200
                              )
                        """),
                        {
                            "lat": _safe_float(row.get("lat")),
                            "lon": _safe_float(row.get("lon")),
                            "cat_like": f"%{category.lower()}%",
                        },
                    ).mappings().first()
                    if ea_del:
                        provider_listing_count = _safe_int(ea_del.get("listing_count"))
                        provider_platform_count = _safe_int(ea_del.get("platform_count"))
                        delivery_listing_count = _safe_int(ea_del.get("cat_count"))
                        delivery_competition_count = delivery_listing_count
            except Exception:
                logger.debug("expansion_delivery_market enrichment failed, using legacy", exc_info=True)''',
# NEW
'''    # ── Bulk delivery enrichment (replaces per-candidate N+1 pattern) ──
    _bulk_delivery: dict[str, dict[str, int]] = {}
    if ea_delivery_populated:
        try:
            # Build a VALUES list of (parcel_id, lon, lat) for all candidates
            _del_values_parts: list[str] = []
            _del_params: dict[str, Any] = {"cat_like": f"%{category.lower()}%"}
            for _idx, _r in enumerate(rows):
                _pid = str(_r.get("parcel_id") or "")
                _lon = _safe_float(_r.get("lon"))
                _lat = _safe_float(_r.get("lat"))
                if _pid and _lon != 0.0 and _lat != 0.0:
                    _del_values_parts.append(f"(:dp_{_idx}, :dx_{_idx}, :dy_{_idx})")
                    _del_params[f"dp_{_idx}"] = _pid
                    _del_params[f"dx_{_idx}"] = _lon
                    _del_params[f"dy_{_idx}"] = _lat
            if _del_values_parts:
                _del_values_sql = ", ".join(_del_values_parts)
                with db.begin_nested():
                    _del_rows = db.execute(
                        text(f"""
                            WITH candidates(parcel_id, lon, lat) AS (
                                VALUES {_del_values_sql}
                            )
                            SELECT
                                c.parcel_id,
                                COUNT(d.*) AS listing_count,
                                COUNT(DISTINCT d.platform) AS platform_count,
                                COUNT(d.*) FILTER (
                                    WHERE lower(COALESCE(d.category, '')) LIKE :cat_like
                                ) AS cat_count
                            FROM candidates c
                            LEFT JOIN {_EA_DELIVERY_TABLE} d
                              ON d.geom IS NOT NULL
                             AND ST_DWithin(
                                 d.geom::geography,
                                 ST_SetSRID(ST_MakePoint(c.lon::double precision, c.lat::double precision), 4326)::geography,
                                 1200
                             )
                            GROUP BY c.parcel_id
                        """),
                        _del_params,
                    ).mappings().all()
                for _dr in _del_rows:
                    _bulk_delivery[str(_dr["parcel_id"])] = {
                        "listing_count": _safe_int(_dr.get("listing_count")),
                        "platform_count": _safe_int(_dr.get("platform_count")),
                        "cat_count": _safe_int(_dr.get("cat_count")),
                    }
                logger.info(
                    "expansion_search bulk delivery enrichment: search_id=%s enriched=%d/%d",
                    search_id, len(_bulk_delivery), len(rows),
                )
        except Exception:
            logger.warning("expansion_search bulk delivery enrichment failed, using legacy counts", exc_info=True)
    t_delivery_enrich_done = time.monotonic()
    logger.info(
        "expansion_search timing: delivery_enrichment=%.2fs search_id=%s",
        t_delivery_enrich_done - t_query_done, search_id,
    )
    for row in rows:
      try:
        area_m2 = _safe_float(row.get("area_m2"))
        population_reach = _safe_float(row.get("population_reach"))
        competitor_count = _safe_int(row.get("competitor_count"))
        delivery_listing_count = _safe_int(row.get("delivery_listing_count"))
        provider_listing_count = _safe_int(row.get("provider_listing_count"))
        provider_platform_count = _safe_int(row.get("provider_platform_count"))
        delivery_competition_count = _safe_int(row.get("delivery_competition_count"))
        landuse_label = row.get("landuse_label")
        landuse_code = row.get("landuse_code")
        district = row.get("district")
        # ── Apply bulk delivery enrichment results ──
        _pid_key = str(row.get("parcel_id") or "")
        if _pid_key and _pid_key in _bulk_delivery:
            _del_stats = _bulk_delivery[_pid_key]
            provider_listing_count = _del_stats["listing_count"]
            provider_platform_count = _del_stats["platform_count"]
            delivery_listing_count = _del_stats["cat_count"]
            delivery_competition_count = delivery_listing_count'''))
# ── Patch 4: Logging bug — type(Exception) → type(exc) ──────────────
PATCHES.append(("4-logging-exc-class",
# OLD
'''        except Exception:
            logger.warning(
                "Expansion search district-filtered query failed, retrying without SQL district filter: "
                "search_id=%s category=%s area=[%s-%s] target_districts=%s "
                "district_sql_enabled=True exc_class=%s",
                search_id, category, min_area_m2, max_area_m2, target_districts,
                type(Exception).__name__,
                exc_info=True,
            )
            district_sql_used = False
            rows = None''',
# NEW
'''        except Exception as exc:
            logger.warning(
                "Expansion search district-filtered query failed, retrying without SQL district filter: "
                "search_id=%s category=%s area=[%s-%s] target_districts=%s "
                "district_sql_enabled=True exc_class=%s exc_msg=%s",
                search_id, category, min_area_m2, max_area_m2, target_districts,
                type(exc).__name__,
                str(exc)[:300],
                exc_info=True,
            )
            district_sql_used = False
            rows = None'''))
# ── Patch 5: review_score / 25.0 → / 20.0 ───────────────────────────
PATCHES.append(("5-review-score-scale",
# OLD
'                            ecq.review_score / 25.0 AS rating,',
# NEW
'                            ecq.review_score / 20.0 AS rating,'))
# ── Patch 6: Rent rounding to whole SAR ──────────────────────────────
PATCHES.append(("6-rent-rounding",
# OLD
'''        estimated_annual_rent_sar = area_m2 * estimated_rent_sar_m2_year
        estimated_fitout_cost_sar = _estimate_fitout_cost_sar(area_m2, service_model)''',
# NEW
'''        estimated_annual_rent_sar = round(area_m2 * estimated_rent_sar_m2_year)
        estimated_fitout_cost_sar = round(_estimate_fitout_cost_sar(area_m2, service_model))'''))
# ── Apply logic ──────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Apply Expansion Advisor patches")
    parser.add_argument("--apply", action="store_true", help="Actually apply patches (default: dry-run)")
    parser.add_argument("--backup", action="store_true", help="Create .bak before modifying")
    parser.add_argument("--target", default=str(TARGET), help=f"Path to expansion_advisor.py (default: {TARGET})")
    args = parser.parse_args()
    target = Path(args.target)
    if not target.exists():
        print(f"ERROR: {target} not found")
        sys.exit(1)
    content = target.read_text(encoding="utf-8")
    applied = 0
    skipped = 0
    for name, old, new in PATCHES:
        count = content.count(old)
        if count == 0:
            print(f"  SKIP  {name}  (pattern not found — already patched?)")
            skipped += 1
        elif count > 1:
            print(f"  WARN  {name}  (pattern found {count} times — ambiguous, skipping)")
            skipped += 1
        else:
            if args.apply:
                content = content.replace(old, new, 1)
                print(f"  OK    {name}")
            else:
                print(f"  READY {name}  (dry-run, use --apply to write)")
            applied += 1
    if args.apply and applied > 0:
        if args.backup:
            shutil.copy2(target, target.with_suffix(".py.bak"))
            print(f"\n  Backup: {target.with_suffix('.py.bak')}")
        target.write_text(content, encoding="utf-8")
        print(f"\n  Written: {target}")
    print(f"\n  Total: {applied} applied, {skipped} skipped")
if __name__ == "__main__":
    main()
