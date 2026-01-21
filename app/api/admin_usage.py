from datetime import date, datetime, time, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import case, func
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
