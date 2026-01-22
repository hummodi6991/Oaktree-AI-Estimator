from datetime import date, datetime, time, timezone
import math

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import case, func, or_
from sqlalchemy.orm import Session

from app.db.deps import get_db
from app.models.tables import UsageEvent
from app.security.auth_context import require_admin_context

router = APIRouter(
    prefix="/admin/usage",
    tags=["admin"],
    dependencies=[Depends(require_admin_context)],
)


def _parse_since(since: str | None) -> datetime | None:
    if not since:
        return None
    try:
        parsed = date.fromisoformat(since)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid since date") from exc
    return datetime.combine(parsed, time.min, tzinfo=timezone.utc)


def _error_rate(errors: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return errors / total


def _conversion_rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _percentile(values: list[int], pct: float) -> float:
    if not values:
        return 0.0
    values_sorted = sorted(values)
    index = max(0, math.ceil((pct / 100) * len(values_sorted)) - 1)
    return float(values_sorted[index])


def _percentile_float(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    values_sorted = sorted(values)
    index = max(0, math.ceil((pct / 100) * len(values_sorted)) - 1)
    return float(values_sorted[index])


def _feedback_rollups(
    db: Session, filters: list
) -> tuple[dict[str, int | float], dict[str, int], dict[str, dict[str, int]]]:
    rows = (
        db.query(UsageEvent.user_id, UsageEvent.meta)
        .filter(*filters, UsageEvent.event_name == "feedback_vote")
        .all()
    )
    count_up = 0
    count_down = 0
    down_reasons: dict[str, int] = {}
    by_user: dict[str, dict[str, int]] = {}
    by_landuse_method: dict[str, dict[str, int]] = {}
    by_provider: dict[str, dict[str, int]] = {}

    for row in rows:
        meta = row.meta if isinstance(row.meta, dict) else {}
        vote = meta.get("vote")
        if vote == "up":
            count_up += 1
        elif vote == "down":
            count_down += 1
            reasons = meta.get("reasons") if isinstance(meta, dict) else None
            if isinstance(reasons, list):
                for reason in reasons:
                    if not isinstance(reason, str) or not reason:
                        continue
                    down_reasons[reason] = down_reasons.get(reason, 0) + 1

        user_id = row.user_id
        if user_id:
            user_entry = by_user.setdefault(user_id, {"count_up": 0, "count_down": 0})
            if vote == "up":
                user_entry["count_up"] += 1
            elif vote == "down":
                user_entry["count_down"] += 1

        landuse_method = meta.get("landuse_method") if isinstance(meta, dict) else None
        if isinstance(landuse_method, str) and landuse_method:
            landuse_entry = by_landuse_method.setdefault(landuse_method, {"count_up": 0, "count_down": 0})
            if vote == "up":
                landuse_entry["count_up"] += 1
            elif vote == "down":
                landuse_entry["count_down"] += 1

        provider = meta.get("provider") if isinstance(meta, dict) else None
        if isinstance(provider, str) and provider:
            provider_entry = by_provider.setdefault(provider, {"count_up": 0, "count_down": 0})
            if vote == "up":
                provider_entry["count_up"] += 1
            elif vote == "down":
                provider_entry["count_down"] += 1

    total = count_up + count_down
    down_rate = count_down / total if total else 0.0
    summary = {"count_up": count_up, "count_down": count_down, "down_rate": down_rate}
    breakdowns = {
        "by_user": by_user,
        "by_landuse_method": by_landuse_method,
        "by_provider": by_provider,
    }
    return summary, down_reasons, breakdowns


@router.get("/summary")
def usage_summary(
    since: str | None = None,
    db: Session = Depends(get_db),
) -> dict:
    since_dt = _parse_since(since)
    base = db.query(UsageEvent).filter(UsageEvent.is_admin.is_(False))
    if since_dt:
        base = base.filter(UsageEvent.ts >= since_dt)

    total_requests = base.count()
    active_users = (
        base.filter(UsageEvent.user_id.isnot(None))
        .with_entities(func.count(func.distinct(UsageEvent.user_id)))
        .scalar()
        or 0
    )
    estimates = (
        base.filter(
            UsageEvent.method == "POST",
            UsageEvent.path == "/v1/estimates",
        ).count()
    )
    pdf_exports = (
        base.filter(
            UsageEvent.path.like("/v1/estimates/%/memo.pdf"),
            UsageEvent.status_code == 200,
        ).count()
    )
    errors = (
        base.filter(UsageEvent.status_code >= 500, UsageEvent.status_code < 600).count()
    )

    return {
        "since": since,
        "totals": {
            "active_users": active_users,
            "requests": total_requests,
            "estimates": estimates,
            "pdf_exports": pdf_exports,
            "error_rate": _error_rate(errors, total_requests),
        },
    }


@router.get("/users")
def usage_users(
    since: str | None = None,
    db: Session = Depends(get_db),
) -> dict:
    since_dt = _parse_since(since)
    base = db.query(
        UsageEvent.user_id.label("user_id"),
        func.count().label("requests"),
        func.sum(
            case(
                (
                    (UsageEvent.method == "POST")
                    & (UsageEvent.path == "/v1/estimates"),
                    1,
                ),
                else_=0,
            )
        ).label("estimates"),
        func.sum(
            case(
                (
                    UsageEvent.path.like("/v1/estimates/%/memo.pdf")
                    & (UsageEvent.status_code == 200),
                    1,
                ),
                else_=0,
            )
        ).label("pdf_exports"),
        func.max(UsageEvent.ts).label("last_seen"),
        func.sum(
            case(
                (
                    (UsageEvent.status_code >= 500)
                    & (UsageEvent.status_code < 600),
                    1,
                ),
                else_=0,
            )
        ).label("errors"),
    ).filter(UsageEvent.is_admin.is_(False), UsageEvent.user_id.isnot(None))
    if since_dt:
        base = base.filter(UsageEvent.ts >= since_dt)

    rows = (
        base.group_by(UsageEvent.user_id)
        .order_by(func.count().desc())
        .all()
    )
    items = []
    for row in rows:
        requests = int(row.requests or 0)
        errors = int(row.errors or 0)
        last_seen = row.last_seen.isoformat() if row.last_seen else None
        items.append(
            {
                "user_id": row.user_id,
                "requests": requests,
                "estimates": int(row.estimates or 0),
                "pdf_exports": int(row.pdf_exports or 0),
                "last_seen": last_seen,
                "error_rate": _error_rate(errors, requests),
            }
        )
    return {"since": since, "items": items}


@router.get("/user/{user_id}")
def usage_user(
    user_id: str,
    since: str | None = None,
    db: Session = Depends(get_db),
) -> dict:
    since_dt = _parse_since(since)
    base = db.query(UsageEvent).filter(UsageEvent.user_id == user_id)
    if since_dt:
        base = base.filter(UsageEvent.ts >= since_dt)

    total_requests = base.count()
    estimates = (
        base.filter(
            UsageEvent.method == "POST",
            UsageEvent.path == "/v1/estimates",
        ).count()
    )
    pdf_exports = (
        base.filter(
            UsageEvent.path.like("/v1/estimates/%/memo.pdf"),
            UsageEvent.status_code == 200,
        ).count()
    )
    errors = (
        base.filter(UsageEvent.status_code >= 500, UsageEvent.status_code < 600).count()
    )

    top_paths_query = db.query(UsageEvent.path, func.count().label("count")).filter(
        UsageEvent.user_id == user_id
    )
    if since_dt:
        top_paths_query = top_paths_query.filter(UsageEvent.ts >= since_dt)
    top_paths_rows = (
        top_paths_query.group_by(UsageEvent.path)
        .order_by(func.count().desc())
        .limit(10)
        .all()
    )

    daily_rows = (
        db.query(
            func.date(UsageEvent.ts).label("date"),
            func.count().label("requests"),
            func.sum(
                case(
                    (
                        (UsageEvent.method == "POST")
                        & (UsageEvent.path == "/v1/estimates"),
                        1,
                    ),
                    else_=0,
                )
            ).label("estimates"),
            func.sum(
                case(
                    (
                        UsageEvent.path.like("/v1/estimates/%/memo.pdf")
                        & (UsageEvent.status_code == 200),
                        1,
                    ),
                    else_=0,
                )
            ).label("pdf_exports"),
            func.sum(
                case(
                    (
                        (UsageEvent.status_code >= 500)
                        & (UsageEvent.status_code < 600),
                        1,
                    ),
                    else_=0,
                )
            ).label("errors"),
        )
        .filter(UsageEvent.user_id == user_id)
    )
    if since_dt:
        daily_rows = daily_rows.filter(UsageEvent.ts >= since_dt)
    daily_rows = daily_rows.group_by(func.date(UsageEvent.ts)).order_by(
        func.date(UsageEvent.ts)
    )
    daily = []
    for row in daily_rows.all():
        date_value = row.date
        if isinstance(date_value, str):
            date_str = date_value
        else:
            date_str = date_value.isoformat()
        daily.append(
            {
                "date": date_str,
                "requests": int(row.requests or 0),
                "estimates": int(row.estimates or 0),
                "pdf_exports": int(row.pdf_exports or 0),
                "errors": int(row.errors or 0),
            }
        )

    return {
        "user_id": user_id,
        "since": since,
        "metrics": {
            "requests": total_requests,
            "estimates": estimates,
            "pdf_exports": pdf_exports,
            "error_rate": _error_rate(errors, total_requests),
        },
        "top_paths": [
            {"path": row.path, "count": int(row.count or 0)}
            for row in top_paths_rows
        ],
        "daily": daily,
    }


@router.get("/insights")
def usage_insights(
    since: str | None = None,
    db: Session = Depends(get_db),
) -> dict:
    since_dt = _parse_since(since)
    filters = [UsageEvent.is_admin.is_(False)]
    if since_dt:
        filters.append(UsageEvent.ts >= since_dt)

    estimate_event_rows = (
        db.query(UsageEvent.user_id, UsageEvent.meta)
        .filter(*filters, UsageEvent.event_name == "estimate_result")
        .all()
    )
    estimate_users: set[str] = set()
    land_price_override_users: set[str] = set()
    far_override_users: set[str] = set()
    land_price_deltas: list[float] = []
    for row in estimate_event_rows:
        user_id = row.user_id
        if user_id:
            estimate_users.add(user_id)
        meta = row.meta if isinstance(row.meta, dict) else {}
        if user_id and meta.get("land_price_overridden") is True:
            land_price_override_users.add(user_id)
        if user_id and meta.get("far_overridden") is True:
            far_override_users.add(user_id)
        if meta.get("land_price_overridden") is True and meta.get("land_price_delta_pct") is not None:
            try:
                land_price_deltas.append(float(meta["land_price_delta_pct"]))
            except Exception:
                pass

    estimate_count = (
        db.query(func.count())
        .select_from(UsageEvent)
        .filter(
            *filters,
            or_(
                UsageEvent.event_name == "estimate_create",
                (UsageEvent.method == "POST") & (UsageEvent.path == "/v1/estimates"),
            ),
        )
        .scalar()
        or 0
    )
    pdf_exports = (
        db.query(func.count())
        .select_from(UsageEvent)
        .filter(
            *filters,
            or_(
                UsageEvent.event_name == "pdf_export",
                (UsageEvent.path.like("/v1/estimates/%/memo.pdf"))
                & (UsageEvent.status_code == 200),
            ),
        )
        .scalar()
        or 0
    )
    conversion_rate = pdf_exports / estimate_count if estimate_count else 0.0

    error_rows = (
        db.query(
            UsageEvent.user_id.label("user_id"),
            func.count().label("requests"),
            func.sum(
                case(
                    (
                        (UsageEvent.status_code >= 500)
                        & (UsageEvent.status_code < 600),
                        1,
                    ),
                    else_=0,
                )
            ).label("errors"),
        )
        .filter(*filters, UsageEvent.user_id.isnot(None))
        .group_by(UsageEvent.user_id)
        .all()
    )
    error_rate_by_user = []
    repeated_error_users = 0
    for row in error_rows:
        requests = int(row.requests or 0)
        errors = int(row.errors or 0)
        if errors >= 3:
            repeated_error_users += 1
        error_rate_by_user.append(
            {
                "user_id": row.user_id,
                "requests": requests,
                "errors": errors,
                "error_rate": _error_rate(errors, requests),
            }
        )

    error_rate_by_user.sort(key=lambda item: item["error_rate"], reverse=True)

    top_5xx_paths = (
        db.query(UsageEvent.path, func.count().label("count"))
        .filter(
            *filters,
            UsageEvent.status_code >= 500,
            UsageEvent.status_code < 600,
        )
        .group_by(UsageEvent.path)
        .order_by(func.count().desc())
        .limit(5)
        .all()
    )

    duration_rows = (
        db.query(UsageEvent.path, UsageEvent.duration_ms)
        .filter(*filters)
        .all()
    )
    durations: list[int] = []
    path_durations: dict[str, list[int]] = {}
    for path, duration in duration_rows:
        if duration is None:
            continue
        durations.append(int(duration))
        path_durations.setdefault(path, []).append(int(duration))

    p95_duration_ms = _percentile(durations, 95)
    slow_paths = [
        {"path": path, "p95_duration_ms": _percentile(values, 95)}
        for path, values in path_durations.items()
    ]
    slow_paths.sort(key=lambda item: item["p95_duration_ms"], reverse=True)

    estimate_failure_count = (
        db.query(func.count())
        .select_from(UsageEvent)
        .filter(
            *filters,
            or_(
                UsageEvent.event_name == "estimate_create",
                (UsageEvent.method == "POST") & (UsageEvent.path == "/v1/estimates"),
            ),
            UsageEvent.status_code >= 500,
        )
        .scalar()
        or 0
    )
    estimate_retries = (
        db.query(
            UsageEvent.user_id,
            func.count().label("estimates"),
        )
        .filter(
            *filters,
            or_(
                UsageEvent.event_name == "estimate_create",
                (UsageEvent.method == "POST") & (UsageEvent.path == "/v1/estimates"),
            ),
            UsageEvent.user_id.isnot(None),
        )
        .group_by(UsageEvent.user_id)
        .all()
    )
    retry_users = sum(1 for row in estimate_retries if int(row.estimates or 0) > 1)

    user_count = len(estimate_users)
    land_price_override_rate = (
        len(land_price_override_users) / user_count if user_count else 0.0
    )
    far_override_rate = len(far_override_users) / user_count if user_count else 0.0
    avg_land_price_delta = (
        sum(land_price_deltas) / len(land_price_deltas) if land_price_deltas else 0.0
    )

    highlights = [
        {
            "title": "Land price trust gap",
            "detail": (
                f"{land_price_override_rate:.0%} of users override land price; "
                f"avg delta {avg_land_price_delta:+.1%}."
                if user_count
                else "No land price override signals yet."
            ),
            "metric": {
                "override_user_pct": land_price_override_rate,
                "avg_delta_pct": avg_land_price_delta,
                "users": user_count,
            },
        },
        {
            "title": "Memo conversion",
            "detail": (
                f"{pdf_exports} of {estimate_count} estimates exported to PDF "
                f"({conversion_rate:.1%})."
                if estimate_count
                else "No estimate-to-memo conversions yet."
            ),
            "metric": {
                "estimates": estimate_count,
                "pdf_exports": pdf_exports,
                "conversion_rate": conversion_rate,
            },
        },
        {
            "title": "Top friction endpoints",
            "detail": (
                f"P95 latency {p95_duration_ms:.0f} ms. "
                f"{len(top_5xx_paths)} endpoints have 5xx spikes."
            ),
            "metric": {
                "p95_duration_ms": p95_duration_ms,
                "top_5xx_paths": [
                    {"path": row.path, "count": int(row.count or 0)}
                    for row in top_5xx_paths
                ],
            },
        },
    ]

    return {
        "since": since,
        "highlights": highlights,
        "support": {
            "overrides": {
                "users_with_estimates": user_count,
                "land_price_override_user_pct": land_price_override_rate,
                "land_price_avg_delta_pct": avg_land_price_delta,
                "far_override_user_pct": far_override_rate,
            },
            "value_funnel": {
                "estimates": estimate_count,
                "pdf_exports": pdf_exports,
                "conversion_rate": conversion_rate,
            },
            "friction": {
                "error_rate_by_user": error_rate_by_user,
                "repeated_error_users": repeated_error_users,
                "top_5xx_paths": [
                    {"path": row.path, "count": int(row.count or 0)}
                    for row in top_5xx_paths
                ],
                "p95_duration_ms": p95_duration_ms,
                "slow_paths_p95": slow_paths[:5],
                "estimate_failures": estimate_failure_count,
                "estimate_retry_users": retry_users,
            },
        },
    }


@router.get("/feedback")
def usage_feedback(
    since: str | None = None,
    db: Session = Depends(get_db),
) -> dict:
    since_dt = _parse_since(since)
    filters = [UsageEvent.is_admin.is_(False)]
    if since_dt:
        filters.append(UsageEvent.ts >= since_dt)

    feedback_summary, _, _ = _feedback_rollups(db, filters)
    feedback_total = feedback_summary["count_up"] + feedback_summary["count_down"]

    estimate_event_rows = (
        db.query(UsageEvent.user_id, UsageEvent.meta)
        .filter(*filters, UsageEvent.event_name == "estimate_result")
        .all()
    )
    estimate_users: set[str] = set()
    land_price_override_users: set[str] = set()
    far_override_users: set[str] = set()
    land_price_deltas: list[float] = []
    suhail_overlay_count = 0
    for row in estimate_event_rows:
        user_id = row.user_id
        if user_id:
            estimate_users.add(user_id)
        meta = row.meta if isinstance(row.meta, dict) else {}
        if user_id and meta.get("land_price_overridden") is True:
            land_price_override_users.add(user_id)
        if user_id and meta.get("far_overridden") is True:
            far_override_users.add(user_id)
        if meta.get("land_price_overridden") is True and meta.get(
            "land_price_delta_pct"
        ) is not None:
            try:
                land_price_deltas.append(float(meta["land_price_delta_pct"]))
            except Exception:
                pass
        if meta.get("landuse_method") == "suhail_overlay":
            suhail_overlay_count += 1

    user_count = len(estimate_users)
    land_price_override_rate = (
        len(land_price_override_users) / user_count if user_count else 0.0
    )
    far_override_rate = len(far_override_users) / user_count if user_count else 0.0
    avg_land_price_delta = (
        sum(land_price_deltas) / len(land_price_deltas) if land_price_deltas else 0.0
    )
    total_estimate_results = len(estimate_event_rows)
    suhail_overlay_rate = (
        suhail_overlay_count / total_estimate_results if total_estimate_results else 0.0
    )

    estimate_count = (
        db.query(func.count())
        .select_from(UsageEvent)
        .filter(
            *filters,
            or_(
                UsageEvent.event_name == "estimate_create",
                (UsageEvent.method == "POST") & (UsageEvent.path == "/v1/estimates"),
            ),
        )
        .scalar()
        or 0
    )
    pdf_exports = (
        db.query(func.count())
        .select_from(UsageEvent)
        .filter(
            *filters,
            or_(
                UsageEvent.event_name == "pdf_export",
                (UsageEvent.path.like("/v1/estimates/%/memo.pdf"))
                & (UsageEvent.status_code == 200),
            ),
        )
        .scalar()
        or 0
    )
    conversion_rate = pdf_exports / estimate_count if estimate_count else 0.0

    error_rows = (
        db.query(
            UsageEvent.user_id.label("user_id"),
            func.sum(
                case(
                    (
                        (UsageEvent.status_code >= 500)
                        & (UsageEvent.status_code < 600),
                        1,
                    ),
                    else_=0,
                )
            ).label("errors"),
        )
        .filter(*filters, UsageEvent.user_id.isnot(None))
        .group_by(UsageEvent.user_id)
        .all()
    )
    repeated_error_users = sum(1 for row in error_rows if int(row.errors or 0) >= 3)

    top_5xx_paths = (
        db.query(UsageEvent.path, func.count().label("count"))
        .filter(
            *filters,
            UsageEvent.status_code >= 500,
            UsageEvent.status_code < 600,
        )
        .group_by(UsageEvent.path)
        .order_by(func.count().desc())
        .limit(5)
        .all()
    )

    estimate_failure_count = (
        db.query(func.count())
        .select_from(UsageEvent)
        .filter(
            *filters,
            or_(
                UsageEvent.event_name == "estimate_create",
                (UsageEvent.method == "POST") & (UsageEvent.path == "/v1/estimates"),
            ),
            UsageEvent.status_code >= 500,
        )
        .scalar()
        or 0
    )

    if land_price_override_rate >= 0.50:
        land_price_severity = "high"
    elif land_price_override_rate >= 0.25:
        land_price_severity = "medium"
    else:
        land_price_severity = "low"

    if far_override_rate >= 0.40:
        far_severity = "high"
    elif far_override_rate >= 0.20:
        far_severity = "medium"
    else:
        far_severity = "low"

    if estimate_count >= 10 and conversion_rate < 0.20:
        conversion_severity = "high"
    elif estimate_count >= 10 and conversion_rate < 0.40:
        conversion_severity = "medium"
    else:
        conversion_severity = "low"

    friction_severity = (
        "high"
        if repeated_error_users >= 2 or estimate_failure_count >= 3
        else "low"
    )

    suhail_severity = "high" if suhail_overlay_rate >= 0.30 else "low"

    if user_count:
        land_price_summary = (
            f"{land_price_override_rate:.0%} of users override land price; "
            f"avg delta {avg_land_price_delta:+.1%}."
        )
        far_summary = f"{far_override_rate:.0%} of users override FAR."
    else:
        land_price_summary = "No estimate results to evaluate land price overrides."
        far_summary = "No estimate results to evaluate FAR overrides."

    if estimate_count:
        conversion_summary = (
            f"{pdf_exports} of {estimate_count} estimates exported to PDF "
            f"({conversion_rate:.1%})."
        )
    else:
        conversion_summary = "No estimate creation events in the selected window."

    if repeated_error_users or estimate_failure_count:
        friction_summary = (
            f"{repeated_error_users} users hit 3+ 5xx responses; "
            f"{estimate_failure_count} estimate requests failed."
        )
    else:
        friction_summary = "No repeated 5xx spikes detected in this window."

    if total_estimate_results:
        suhail_summary = (
            f"Suhail overlay used in {suhail_overlay_count} of "
            f"{total_estimate_results} estimate results ({suhail_overlay_rate:.1%})."
        )
    else:
        suhail_summary = "No estimate results to evaluate landuse fallback."

    items = [
        {
            "id": "land_price_trust_gap",
            "severity": land_price_severity,
            "title": "Land price trust gap",
            "summary": land_price_summary,
            "evidence": {
                "users_with_estimates": user_count,
                "override_users": len(land_price_override_users),
                "override_rate": land_price_override_rate,
                "avg_delta_pct": avg_land_price_delta,
                **(
                    {
                        "feedback_up_count": feedback_summary["count_up"],
                        "feedback_down_count": feedback_summary["count_down"],
                        "feedback_down_rate": feedback_summary["down_rate"],
                    }
                    if feedback_total
                    else {}
                ),
            },
            "recommended_actions": [
                "Review blended_v1 calibration in districts with frequent overrides",
                "Expose price confidence band in UI to reduce manual overrides",
            ],
        },
        {
            "id": "far_trust_gap",
            "severity": far_severity,
            "title": "FAR trust/fit gap",
            "summary": far_summary,
            "evidence": {
                "users_with_estimates": user_count,
                "override_users": len(far_override_users),
                "override_rate": far_override_rate,
                **(
                    {
                        "feedback_up_count": feedback_summary["count_up"],
                        "feedback_down_count": feedback_summary["count_down"],
                        "feedback_down_rate": feedback_summary["down_rate"],
                    }
                    if feedback_total
                    else {}
                ),
            },
            "recommended_actions": [
                "Audit FAR overrides by zoning to tune FAR defaults",
                "Show source of FAR assumption to reinforce trust",
            ],
        },
        {
            "id": "value_conversion_gap",
            "severity": conversion_severity,
            "title": "Value conversion gap",
            "summary": conversion_summary,
            "evidence": {
                "estimate_count": estimate_count,
                "pdf_exports": pdf_exports,
                "conversion_rate": conversion_rate,
                **(
                    {
                        "feedback_up_count": feedback_summary["count_up"],
                        "feedback_down_count": feedback_summary["count_down"],
                        "feedback_down_rate": feedback_summary["down_rate"],
                    }
                    if feedback_total
                    else {}
                ),
            },
            "recommended_actions": [
                "Add in-product prompts to export the memo after estimating",
                "Surface memo preview value propositions in the estimate flow",
            ],
        },
        {
            "id": "friction_errors_retries",
            "severity": friction_severity,
            "title": "Friction: high errors and retries",
            "summary": friction_summary,
            "evidence": {
                "repeated_error_users": repeated_error_users,
                "estimate_failures": estimate_failure_count,
                "top_5xx_path_count": len(top_5xx_paths),
                **(
                    {
                        "feedback_up_count": feedback_summary["count_up"],
                        "feedback_down_count": feedback_summary["count_down"],
                        "feedback_down_rate": feedback_summary["down_rate"],
                    }
                    if feedback_total
                    else {}
                ),
            },
            "recommended_actions": [
                "Prioritize fixes on endpoints with repeated 5xx spikes",
                "Add retry-safe UX messaging for estimate failures",
            ],
        },
        {
            "id": "data_source_confusion_suhail",
            "severity": suhail_severity,
            "title": "Data source confusion: Suhail fallback rate",
            "summary": suhail_summary,
            "evidence": {
                "total_estimate_results": total_estimate_results,
                "suhail_overlay_count": suhail_overlay_count,
                "suhail_overlay_pct": suhail_overlay_rate,
                **(
                    {
                        "feedback_up_count": feedback_summary["count_up"],
                        "feedback_down_count": feedback_summary["count_down"],
                        "feedback_down_rate": feedback_summary["down_rate"],
                    }
                    if feedback_total
                    else {}
                ),
            },
            "recommended_actions": [
                "Audit ArcGIS label signal; reduce Suhail fallback when ArcGIS label present",
                "Log landuse_method transitions to confirm source selection",
            ],
        },
    ]

    return {"since": since, "items": items}


@router.get("/feedback_inbox")
def usage_feedback_inbox(
    since: str | None = None,
    db: Session = Depends(get_db),
) -> dict:
    since_dt = _parse_since(since)
    filters = [UsageEvent.is_admin.is_(False)]
    if since_dt:
        filters.append(UsageEvent.ts >= since_dt)

    summary, down_reasons, breakdowns = _feedback_rollups(db, filters)
    total = summary["count_up"] + summary["count_down"]
    top_reasons = sorted(down_reasons.items(), key=lambda item: item[1], reverse=True)[:10]

    def _format_breakdown(
        source: dict[str, dict[str, int]],
        key_name: str,
    ) -> list[dict[str, float | int | str]]:
        items: list[dict[str, float | int | str]] = []
        for key, counts in source.items():
            count_up = counts.get("count_up", 0)
            count_down = counts.get("count_down", 0)
            total_count = count_up + count_down
            items.append(
                {
                    key_name: key,
                    "count_up": count_up,
                    "count_down": count_down,
                    "down_rate": count_down / total_count if total_count else 0.0,
                }
            )
        items.sort(key=lambda item: item["count_down"], reverse=True)
        return items

    return {
        "since": since,
        "totals": summary,
        "top_reasons": [{"reason": reason, "count": count} for reason, count in top_reasons],
        "by_user": _format_breakdown(breakdowns["by_user"], "user_id"),
        "by_landuse_method": _format_breakdown(breakdowns["by_landuse_method"], "landuse_method"),
        "by_provider": _format_breakdown(breakdowns["by_provider"], "provider"),
        "total_responses": total,
    }


@router.get("/funnel")
def usage_funnel(
    since: str | None = None,
    db: Session = Depends(get_db),
) -> dict:
    since_dt = _parse_since(since)
    filters = [UsageEvent.is_admin.is_(False)]
    if since_dt:
        filters.append(UsageEvent.ts >= since_dt)

    unique_users = (
        db.query(func.count(func.distinct(UsageEvent.user_id)))
        .filter(*filters, UsageEvent.user_id.isnot(None))
        .scalar()
        or 0
    )

    parcel_filter = UsageEvent.event_name == "ui_parcel_selected"
    estimate_started_filter = UsageEvent.event_name == "ui_estimate_started"
    estimate_completed_filter = UsageEvent.event_name.in_(
        ["ui_estimate_completed", "estimate_result"]
    )
    pdf_opened_filter = UsageEvent.event_name.in_(["ui_pdf_opened", "pdf_export"])
    feedback_filter = UsageEvent.event_name == "feedback_vote"

    def _distinct_user_count(event_filter) -> int:
        return (
            db.query(func.count(func.distinct(UsageEvent.user_id)))
            .filter(*filters, UsageEvent.user_id.isnot(None), event_filter)
            .scalar()
            or 0
        )

    parcel_selected_users = _distinct_user_count(parcel_filter)
    estimate_started_users = _distinct_user_count(estimate_started_filter)
    estimate_completed_users = _distinct_user_count(estimate_completed_filter)
    pdf_opened_users = _distinct_user_count(pdf_opened_filter)
    feedback_users = _distinct_user_count(feedback_filter)

    parcel_selected_events = (
        db.query(func.count())
        .select_from(UsageEvent)
        .filter(*filters, parcel_filter)
        .scalar()
        or 0
    )
    estimate_started_events = (
        db.query(func.count())
        .select_from(UsageEvent)
        .filter(*filters, estimate_started_filter)
        .scalar()
        or 0
    )
    estimate_completed_events = (
        db.query(func.count())
        .select_from(UsageEvent)
        .filter(*filters, estimate_completed_filter)
        .scalar()
        or 0
    )
    pdf_opened_events = (
        db.query(func.count())
        .select_from(UsageEvent)
        .filter(*filters, pdf_opened_filter)
        .scalar()
        or 0
    )
    feedback_events = (
        db.query(func.count())
        .select_from(UsageEvent)
        .filter(*filters, feedback_filter)
        .scalar()
        or 0
    )

    time_event_names = [
        "ui_parcel_selected",
        "ui_estimate_completed",
        "estimate_result",
        "ui_pdf_opened",
        "pdf_export",
    ]
    time_rows = (
        db.query(
            UsageEvent.user_id.label("user_id"),
            func.min(
                case((UsageEvent.event_name == "ui_parcel_selected", UsageEvent.ts))
            ).label("first_parcel_ts"),
            func.min(
                case((UsageEvent.event_name.in_(["ui_estimate_completed", "estimate_result"]), UsageEvent.ts))
            ).label("first_estimate_ts"),
            func.min(
                case((UsageEvent.event_name.in_(["ui_pdf_opened", "pdf_export"]), UsageEvent.ts))
            ).label("first_pdf_ts"),
            func.max(UsageEvent.ts).label("last_seen"),
        )
        .filter(
            *filters,
            UsageEvent.user_id.isnot(None),
            UsageEvent.event_name.in_(time_event_names),
        )
        .group_by(UsageEvent.user_id)
        .all()
    )

    parcel_to_estimate_values: list[float] = []
    estimate_to_pdf_values: list[float] = []
    samples: list[dict[str, float | str | None]] = []
    for row in time_rows:
        parcel_to_estimate = None
        estimate_to_pdf = None
        if row.first_parcel_ts and row.first_estimate_ts:
            delta = (row.first_estimate_ts - row.first_parcel_ts).total_seconds() / 60
            parcel_to_estimate = float(delta)
            parcel_to_estimate_values.append(parcel_to_estimate)
        if row.first_estimate_ts and row.first_pdf_ts:
            delta = (row.first_pdf_ts - row.first_estimate_ts).total_seconds() / 60
            estimate_to_pdf = float(delta)
            estimate_to_pdf_values.append(estimate_to_pdf)
        if parcel_to_estimate is not None or estimate_to_pdf is not None:
            samples.append(
                {
                    "user_id": row.user_id,
                    "parcel_to_estimate_min": parcel_to_estimate,
                    "estimate_to_pdf_min": estimate_to_pdf,
                    "last_seen": row.last_seen,
                }
            )

    samples.sort(
        key=lambda item: item.get("last_seen") or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    per_user_samples = [
        {
            "user_id": item["user_id"],
            "parcel_to_estimate_min": item["parcel_to_estimate_min"],
            "estimate_to_pdf_min": item["estimate_to_pdf_min"],
        }
        for item in samples[:10]
    ]

    return {
        "since": since,
        "totals": {
            "unique_users": unique_users,
            "parcel_selected_users": parcel_selected_users,
            "estimate_started_users": estimate_started_users,
            "estimate_completed_users": estimate_completed_users,
            "pdf_opened_users": pdf_opened_users,
            "feedback_users": feedback_users,
            "events": {
                "parcel_selected": parcel_selected_events,
                "estimate_started": estimate_started_events,
                "estimate_completed": estimate_completed_events,
                "pdf_opened": pdf_opened_events,
                "feedback_votes": feedback_events,
            },
        },
        "conversion": {
            "parcel_to_estimate_started": _conversion_rate(
                estimate_started_users, parcel_selected_users
            ),
            "estimate_started_to_completed": _conversion_rate(
                estimate_completed_users, estimate_started_users
            ),
            "completed_to_pdf": _conversion_rate(pdf_opened_users, estimate_completed_users),
            "pdf_to_feedback": _conversion_rate(feedback_users, pdf_opened_users),
        },
        "time_to_value": {
            "median_minutes_parcel_to_first_estimate": _percentile_float(
                parcel_to_estimate_values, 50
            ),
            "p80_minutes_parcel_to_first_estimate": _percentile_float(
                parcel_to_estimate_values, 80
            ),
            "median_minutes_estimate_to_pdf": _percentile_float(
                estimate_to_pdf_values, 50
            ),
            "p80_minutes_estimate_to_pdf": _percentile_float(
                estimate_to_pdf_values, 80
            ),
        },
        "per_user_samples": per_user_samples,
    }
