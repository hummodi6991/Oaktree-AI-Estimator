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


def _percentile(values: list[int], pct: float) -> float:
    if not values:
        return 0.0
    values_sorted = sorted(values)
    index = max(0, math.ceil((pct / 100) * len(values_sorted)) - 1)
    return float(values_sorted[index])


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
