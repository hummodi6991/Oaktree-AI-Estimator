from fastapi import FastAPI

from app.api.comps import router as comps_router
from app.api.estimates import router as estimates_router
from app.api.health import router as health_router
from app.api.indices import router as indices_router

app = FastAPI(title="Oaktree Estimator API", version="0.1.0")

app.include_router(health_router, prefix="")
app.include_router(indices_router, prefix="/v1")
app.include_router(comps_router, prefix="/v1")
app.include_router(estimates_router, prefix="/v1")
