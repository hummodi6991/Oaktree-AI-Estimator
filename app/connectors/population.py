"""
Connector for population density data.

Reads Meta/HDX high-resolution population density GeoTIFF rasters
and aggregates into H3 hexagonal cells.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Iterator

logger = logging.getLogger(__name__)

# Riyadh metro bounding box
RIYADH_BBOX = (46.20, 24.20, 47.30, 25.10)

H3_RESOLUTION = 9  # ~174m edge length


def load_hdx_population_raster(
    geotiff_path: str | Path,
    h3_resolution: int = H3_RESOLUTION,
) -> Iterator[dict[str, Any]]:
    """
    Read a population density GeoTIFF and aggregate pixel values
    into H3 hexagonal cells.

    Each yielded dict has: h3_index, lat, lon, population.

    Requires: rasterio, h3, numpy
    """
    try:
        import h3
        import numpy as np
        import rasterio
        from rasterio.windows import from_bounds
    except ImportError as exc:
        logger.error("Missing dependency for population raster: %s", exc)
        return

    path = Path(geotiff_path)
    if not path.exists():
        logger.error("GeoTIFF not found: %s", path)
        return

    min_lon, min_lat, max_lon, max_lat = RIYADH_BBOX
    h3_cells: dict[str, float] = {}

    with rasterio.open(path) as src:
        window = from_bounds(min_lon, min_lat, max_lon, max_lat, src.transform)
        data = src.read(1, window=window)
        transform = src.window_transform(window)

        rows, cols = data.shape
        for r in range(rows):
            for c in range(cols):
                val = float(data[r, c])
                if val <= 0 or np.isnan(val):
                    continue

                # Pixel center → lon/lat
                lon = transform.c + c * transform.a + r * transform.b
                lat = transform.f + c * transform.d + r * transform.e

                h3_idx = h3.latlng_to_cell(lat, lon, h3_resolution)
                h3_cells[h3_idx] = h3_cells.get(h3_idx, 0.0) + val

    logger.info(
        "Aggregated population into %d H3 cells from %s",
        len(h3_cells),
        path.name,
    )

    for h3_idx, pop in h3_cells.items():
        lat, lon = h3.cell_to_latlng(h3_idx)
        yield {
            "h3_index": h3_idx,
            "lat": lat,
            "lon": lon,
            "population": round(pop, 1),
        }
