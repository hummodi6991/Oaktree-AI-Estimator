"""
Ingestion pipeline for population density data.

Downloads and processes HDX/Meta high-resolution population density
rasters into the ``population_density`` table using H3 indexing.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from app.connectors.population import load_hdx_population_raster
from app.models.tables import PopulationDensity

logger = logging.getLogger(__name__)


def ingest_population_hdx(db: Session, geotiff_path: str | Path) -> int:
    """
    Read a HDX population density GeoTIFF and upsert into population_density table.

    Returns the number of H3 cells upserted.
    """
    n = 0
    for cell in load_hdx_population_raster(geotiff_path):
        row = db.query(PopulationDensity).filter_by(h3_index=cell["h3_index"]).first()
        if row:
            row.population = cell["population"]
            row.observed_at = datetime.now(timezone.utc)
        else:
            db.add(
                PopulationDensity(
                    h3_index=cell["h3_index"],
                    lat=cell["lat"],
                    lon=cell["lon"],
                    population=cell["population"],
                    source="hdx_meta",
                    observed_at=datetime.now(timezone.utc),
                )
            )
        n += 1
        if n % 1000 == 0:
            db.flush()

    db.commit()
    logger.info("Ingested %d population density H3 cells", n)
    return n


if __name__ == "__main__":
    import sys

    from app.db.session import SessionLocal

    if len(sys.argv) < 2:
        print("Usage: python -m app.ingest.population_density <geotiff_path>")
        sys.exit(1)

    db = SessionLocal()
    try:
        n = ingest_population_hdx(db, sys.argv[1])
        print(f"Population density ingestion complete: {n} cells")
    finally:
        db.close()
