import logging
import re
import time
from datetime import datetime, timezone

from fastapi import FastAPI, Request, Response
from sqlalchemy.exc import SQLAlchemyError
from starlette.middleware.base import BaseHTTPMiddleware

from app.db import session as db_session
from app.models.tables import UsageEvent
from app.security import auth

logger = logging.getLogger(__name__)

_SKIP_PATHS = {
    "/v1/health",
    "/v1/openapi.json",
    "/v1/docs",
    "/v1/redoc",
}
_SKIP_PREFIXES: tuple[str, ...] = ("/v1/admin/usage",)
_ESTIMATE_RE = re.compile(r"^/v1/estimates/(?P<estimate_id>[^/]+)")
_ESTIMATE_PDF_RE = re.compile(r"^/v1/estimates/[^/]+/memo\.pdf$")


def _resolve_event_name(method: str, path: str) -> str | None:
    if method == "POST" and path == "/v1/estimates":
        return "estimate_create"
    if method == "GET" and _ESTIMATE_PDF_RE.match(path):
        return "pdf_export"
    if method == "GET" and path == "/v1/pricing/land":
        return "land_price_fetch"
    if method == "POST" and path == "/v1/geo/identify":
        return "parcel_identify"
    if path.startswith("/v1/tiles/"):
        return "tile_fetch"
    return None


def _should_log(path: str) -> bool:
    if not path.startswith("/v1/"):
        return False
    if path in _SKIP_PATHS:
        return False
    return not path.startswith(_SKIP_PREFIXES)


def _extract_estimate_id(path: str) -> str | None:
    match = _ESTIMATE_RE.match(path)
    if not match:
        return None
    return match.group("estimate_id")


class UsageEventMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path
        if not _should_log(path):
            return await call_next(request)

        start = time.perf_counter()
        status_code = 500
        response: Response | None = None
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            duration_ms = int((time.perf_counter() - start) * 1000)
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
                    event_name=_resolve_event_name(request.method, path),
                    method=request.method,
                    path=path,
                    status_code=status_code,
                    duration_ms=duration_ms,
                    estimate_id=_extract_estimate_id(path),
                    meta={"query": request.url.query} if request.url.query else None,
                )
                with db_session.SessionLocal() as db:
                    db.add(event)
                    db.commit()
            except SQLAlchemyError as exc:
                logger.warning("Usage event insert failed: %s", exc)
            except Exception as exc:
                logger.warning("Usage event logging failed: %s", exc)


def add_usage_event_middleware(app: FastAPI) -> None:
    app.add_middleware(UsageEventMiddleware)
