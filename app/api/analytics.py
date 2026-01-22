from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db.deps import get_db
from app.models.tables import UsageEvent
from app.security import auth


router = APIRouter(prefix="/analytics", tags=["analytics"])

_MAX_META_BYTES = 8 * 1024
_ALLOWED_EVENT_PREFIXES = ("ui_", "feedback_")


def _meta_size_bytes(meta: dict[str, Any] | None) -> int:
    if not meta:
        return 0
    try:
        return len(json.dumps(meta, ensure_ascii=False).encode("utf-8"))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="Invalid meta payload") from exc


def _validate_event_name(event_name: str) -> None:
    if event_name.startswith(_ALLOWED_EVENT_PREFIXES):
        return
    raise HTTPException(status_code=400, detail="Unsupported event name")


def _extract_estimate_id(payload: AnalyticsEventRequest) -> str | None:
    if payload.estimate_id:
        return payload.estimate_id
    if payload.meta and isinstance(payload.meta, dict):
        estimate_id = payload.meta.get("estimate_id")
        if isinstance(estimate_id, str) and estimate_id:
            return estimate_id
    return None


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
    _validate_event_name(payload.event_name)
    if _meta_size_bytes(payload.meta) > _MAX_META_BYTES:
        raise HTTPException(status_code=413, detail="Meta payload too large")

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
            path="/v1/analytics/event",
            status_code=200,
            duration_ms=0,
            estimate_id=_extract_estimate_id(payload),
            meta=payload.meta,
        )
        db.add(event)
        db.commit()
    except SQLAlchemyError:
        db.rollback()
    except Exception:
        db.rollback()
    return {"status": "ok"}
