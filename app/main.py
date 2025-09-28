from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.comps import router as comps_router
from app.api.estimates import router as estimates_router
from app.api.geo_portal import router as geo_router
from app.api.health import router as health_router
from app.api.indices import router as indices_router
from app.api.ingest import router as ingest_router
from app.api.metadata import router as metadata_router

app = FastAPI(title="Oaktree Estimator API", version="0.1.0")

# Allow cross-origin requests for the API (tighten in production as needed).
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router, prefix="")
app.include_router(indices_router, prefix="/v1")
app.include_router(comps_router, prefix="/v1")
app.include_router(estimates_router, prefix="/v1")
app.include_router(metadata_router, prefix="/v1")
app.include_router(ingest_router)
app.include_router(geo_router)
