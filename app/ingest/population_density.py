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


def _validate_geotiff(path: str | Path) -> None:
    """Validate that a file is a readable GeoTIFF before ingestion."""
    import subprocess

    import rasterio

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"GeoTIFF not found: {p}")

    try:
        with rasterio.open(p) as ds:
            print(f"Raster OK: {ds.driver} {ds.width}x{ds.height} bands={ds.count} crs={ds.crs}")
    except Exception as exc:
        file_type = subprocess.run(
            ["file", str(p)], capture_output=True, text=True
        ).stdout.strip()
        head_bytes = p.read_bytes()[:200]
        head_text = head_bytes.decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Cannot open {p} as a raster.\n"
            f"  file type: {file_type}\n"
            f"  first 200 bytes: {head_text!r}\n"
            f"  original error: {exc}\n"
            f"Hint: The file may be an HTML error page or a corrupt download. "
            f"Re-run the download step or check the source URL."
        ) from exc


if __name__ == "__main__":
    import sys

    from app.db.session import SessionLocal

    if len(sys.argv) < 2:
        print("Usage: python -m app.ingest.population_density <geotiff_path>")
        sys.exit(1)

    geotiff_path = sys.argv[1]
    _validate_geotiff(geotiff_path)

    db = SessionLocal()
    try:
        n = ingest_population_hdx(db, geotiff_path)
        print(f"Population density ingestion complete: {n} cells")
    finally:
        db.close()
