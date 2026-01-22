from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db.deps import get_db
from app.models.tables import UsageEvent
from app.security import auth


router = APIRouter(prefix="/analytics", tags=["analytics"])


class AnalyticsEventRequest(BaseModel):
    event_name: str = Field(min_length=1, max_length=128)
    estimate_id: str | None = None
    meta: dict[str, Any] | None = None


@router.post("/event")
def log_analytics_event(
    payload: AnalyticsEventRequest,
    request: Request,
    db: Session = Depends(get_db),
) -> dict[str, str]:
    try:
        auth_payload = getattr(request.state, "auth", None) or {}
        if auth.MODE == "disabled":
            user_id = "anonymous"
            is_admin = False
        else:
            user_id = auth_payload.get("sub")
            is_admin = bool(auth_payload.get("is_admin", False))

        event = UsageEvent(
            ts=datetime.now(timezone.utc),
            user_id=user_id,
            is_admin=is_admin,
            event_name=payload.event_name,
            method="POST",
            path=request.url.path,
            status_code=200,
            duration_ms=0,
            estimate_id=payload.estimate_id,
            meta=payload.meta,
        )
        db.add(event)
        db.commit()
    except SQLAlchemyError:
        db.rollback()
    except Exception:
        db.rollback()
    return {"status": "ok"}
