import logging

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from starlette.staticfiles import StaticFiles

from app.api.comps import router as comps_router
from app.api.estimates import router as estimates_router
from app.api import pricing as pricing_router
from app.api.geo_portal import router as geo_router
from app.api.health import router as health_router
from app.api.indices import router as indices_router
from app.api.ingest import router as ingest_router
from app.api.metadata import router as metadata_router
from app.api.tiles import router as tiles_router
from app.telemetry import setup_otel_if_configured
from app.security.auth import require as auth_require
from app.core.config import settings
from app.db.session import SessionLocal

app = FastAPI(title="Oaktree Estimator API", version="0.1.0")
setup_otel_if_configured(app)
logger = logging.getLogger(__name__)


@app.on_event("startup")
def log_parcel_identify_settings() -> None:
    logger.info(
        "Parcel identify settings: table=%s geom_column=%s tolerance_m=%s target_srid=%s",
        settings.PARCEL_IDENTIFY_TABLE,
        settings.PARCEL_IDENTIFY_GEOM_COLUMN,
        settings.PARCEL_IDENTIFY_TOLERANCE_M,
        settings.PARCEL_TARGET_SRID,
    )
    try:
        with SessionLocal() as db:
            table_exists = db.execute(
                text("SELECT to_regclass(:table_name)"),
                {"table_name": settings.PARCEL_IDENTIFY_TABLE},
            ).scalar()
    except SQLAlchemyError as exc:
        logger.warning("Parcel identify table check failed: %s", exc)
        return

    if table_exists is None:
        logger.warning(
            "Parcel identify table is missing: %s", settings.PARCEL_IDENTIFY_TABLE
        )


@app.on_event("startup")
def log_route_counts() -> None:
    routes = list(app.router.routes)
    tiles_routes = [route for route in routes if "/tiles" in getattr(route, "path", "")]
    logger.info(
        "API route counts: total=%s tiles=%s",
        len(routes),
        len(tiles_routes),
    )

# Allow cross-origin requests for the API (tighten in production as needed).
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router, prefix="")  # public
deps: list = []  # stays empty unless AUTH_MODE != disabled (kept simple)
try:
    # if not disabled, enforce dependency
    from app.security.auth import MODE as _MODE

    if _MODE != "disabled":
        deps = [Depends(auth_require)]
except Exception:
    pass

app.include_router(indices_router, prefix="/v1", dependencies=deps)
app.include_router(comps_router, prefix="/v1", dependencies=deps)
app.include_router(estimates_router, prefix="/v1", dependencies=deps)
app.include_router(pricing_router.router, prefix="/v1", dependencies=deps)
app.include_router(metadata_router, prefix="/v1", dependencies=deps)
app.include_router(ingest_router, dependencies=deps)
app.include_router(tiles_router, prefix="/v1", dependencies=deps)
# Always expose geo routes; they already try PostGIS first and fall back to ArcGIS/external.
app.include_router(geo_router, prefix="/v1", dependencies=deps)

# Serve the compiled React app (frontend/dist) from the same container.
# UI will be reachable at "/" on the same LoadBalancer as the API.
try:
    app.mount("/", StaticFiles(directory="frontend/dist", html=True), name="web")
except Exception:
    # In dev without a build the directory may not exist; ignore.
    pass
