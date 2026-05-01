"""
Connector for NASA Black Marble VNP46A3 monthly nighttime-radiance data.

Single tile h22v06 covers all of Riyadh. Data is fetched from the LAADS DAAC
archive (collection 5200, Black Marble Collection 2.0). Quality filter is
"lenient" (quality < 2, Good + Poor combined); confidence floor is 10 valid
pixels per district per month.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator

import requests

logger = logging.getLogger(__name__)

# Riyadh metro bounding box (west, south, east, north).
RIYADH_BBOX = (46.20, 24.20, 47.30, 25.10)

# Single VIIRS tile covering all of Riyadh.
TILE = "h22v06"
# h22v06 lon/lat envelope (10 deg x 10 deg sinusoidal grid expressed in WGS84).
TILE_BOUNDS = (40.0, 20.0, 50.0, 30.0)  # west, south, east, north

QUALITY_FILTER_LABEL = "lenient_qa_lt_2"
SOURCE_LABEL = "nasa_blackmarble_vnp46a3_c2"

# Confidence floor: a district needs >= this many valid pixels in a given
# month to produce a confident radiance signal. Below the floor, the leg
# falls through (no growth_rescue).
PIXEL_COUNT_FLOOR = 10

RADIANCE_BAND_PATH = (
    "HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields/NearNadir_Composite_Snow_Free"
)
QUALITY_BAND_PATH = (
    "HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields/NearNadir_Composite_Snow_Free_Quality"
)

# Confirmed from POC.
RADIANCE_FILL_VALUE = -999.9

LAADS_BASE_URL = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5200/VNP46A3"

# Magic bytes at the start of every HDF5 file.
_HDF5_SIGNATURE = b"\x89HDF\r\n\x1a\n"
_MIN_FILE_BYTES = 50 * 1024 * 1024  # 50 MB


class BlackMarbleError(Exception):
    """Base error for Black Marble connector."""


class BlackMarbleNotAvailableError(BlackMarbleError):
    """Raised when a requested month has not yet been published to LAADS."""


class BlackMarbleDownloadCorruptError(BlackMarbleError):
    """Raised when a downloaded H5 file fails magic-byte or size checks."""


class _LAADSSession(requests.Session):
    """Session that re-attaches the bearer token across cross-origin redirects.

    LAADS file downloads return a 302 to a signed URL on a different subdomain.
    Default ``requests`` strips the Authorization header on cross-origin
    redirects (RFC 7235); override ``rebuild_auth`` to keep it attached.
    """

    def __init__(self, token: str):
        super().__init__()
        self._token = token
        self.headers.update({"Authorization": f"Bearer {token}"})

    def rebuild_auth(self, prepared_request, response):
        # Re-attach the bearer token unconditionally on any redirect hop.
        prepared_request.headers["Authorization"] = f"Bearer {self._token}"


def discover_h5_url(year_month: date, token: str) -> str:
    """Resolve the LAADS download URL for the h22v06 file of ``year_month``.

    Uses LAADS's ``.json`` directory listing under
    ``/archive/allData/5200/VNP46A3/<year>/<DOY>/`` to find the h22v06 file.
    Raises :class:`BlackMarbleNotAvailableError` if the month has not yet
    been published.
    """
    ym = date(year_month.year, year_month.month, 1)
    doy = ym.timetuple().tm_yday  # 1..365 for first-of-month
    listing_url = f"{LAADS_BASE_URL}/{ym.year}/{doy:03d}.json"

    session = _LAADSSession(token)
    resp = session.get(listing_url, timeout=60)
    if resp.status_code == 404:
        raise BlackMarbleNotAvailableError(
            f"LAADS listing not found for {ym.isoformat()} (url={listing_url})"
        )
    resp.raise_for_status()

    try:
        payload = resp.json()
    except ValueError as exc:
        raise BlackMarbleError(
            f"LAADS listing for {ym.isoformat()} did not return JSON"
        ) from exc

    # LAADS .json listings are wrapped: {"content": [{"name": ..., ...}, ...]}
    # Defensive: also accept a bare list in case any LAADS endpoint variant
    # returns one. See LAADS DAAC's official bash download script
    # (jq '.content | .[] | .name') for the canonical wrapper shape.
    if isinstance(payload, dict):
        entries = payload.get("content") or []
    elif isinstance(payload, list):
        entries = payload
    else:
        entries = []

    for entry in entries:
        name = entry.get("name") if isinstance(entry, dict) else None
        if not name:
            continue
        if name.endswith(".h5") and TILE in name:
            return f"{LAADS_BASE_URL}/{ym.year}/{doy:03d}/{name}"

    raise BlackMarbleNotAvailableError(
        f"No {TILE} file found in LAADS listing for {ym.isoformat()}"
    )


def download_h5(url: str, token: str, dest_path: str | Path) -> None:
    """Download an H5 file from LAADS to ``dest_path``, streaming.

    Validates magic bytes and a minimum size after write; raises
    :class:`BlackMarbleDownloadCorruptError` on failure.
    """
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)

    session = _LAADSSession(token)
    with session.get(url, stream=True, timeout=600, allow_redirects=True) as resp:
        resp.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                if chunk:
                    fh.write(chunk)

    size = dest.stat().st_size
    if size < _MIN_FILE_BYTES:
        raise BlackMarbleDownloadCorruptError(
            f"Downloaded file too small ({size} bytes < {_MIN_FILE_BYTES}): {dest}"
        )

    with dest.open("rb") as fh:
        head = fh.read(len(_HDF5_SIGNATURE))
    if head != _HDF5_SIGNATURE:
        raise BlackMarbleDownloadCorruptError(
            f"Downloaded file is not HDF5 (bad magic bytes): {dest}"
        )


def aggregate_per_district(
    h5_path: str | Path,
    district_polygons: list[dict[str, Any]],
    year_month: date,
) -> Iterator[dict[str, Any]]:
    """Aggregate per-district radiance stats from a Black Marble H5 file.

    ``district_polygons`` is a list of ``{"district_key": str, "geometry":
    shapely_geom_in_4326}``. Yields one dict per district with keys matching
    :class:`DistrictRadianceMonthly` columns. ``pixel_count_valid`` is
    reported regardless of the threshold; the threshold check is enforced at
    consume-time.
    """
    try:
        import h5py
        import numpy as np
        from rasterio.features import geometry_mask
        from rasterio.transform import from_bounds
    except ImportError as exc:
        logger.error("Missing dependency for Black Marble aggregation: %s", exc)
        return

    ym = date(year_month.year, year_month.month, 1)
    path = Path(h5_path)
    if not path.exists():
        logger.error("H5 file not found: %s", path)
        return

    with h5py.File(path, "r") as fh:
        radiance_ds = fh[RADIANCE_BAND_PATH]
        quality_ds = fh[QUALITY_BAND_PATH]
        radiance = np.asarray(radiance_ds[...], dtype=np.float64)
        quality = np.asarray(quality_ds[...])

    if radiance.shape != quality.shape:
        raise BlackMarbleError(
            f"Radiance/quality shape mismatch in {path}: "
            f"{radiance.shape} vs {quality.shape}"
        )

    height, width = radiance.shape
    west, south, east, north = TILE_BOUNDS
    transform = from_bounds(west, south, east, north, width, height)

    valid_mask_global = (radiance != RADIANCE_FILL_VALUE) & (quality < 2)

    out_shape = radiance.shape

    for poly in district_polygons:
        dk = poly.get("district_key")
        geom = poly.get("geometry")
        if not dk or geom is None:
            continue

        try:
            poly_mask = geometry_mask(
                [geom.__geo_interface__],
                out_shape=out_shape,
                transform=transform,
                invert=True,
                all_touched=False,
            )
        except Exception as exc:  # noqa: BLE001 - one bad polygon shouldn't fail batch
            logger.warning("geometry_mask failed for district_key=%s: %s", dk, exc)
            continue

        pixel_count_total = int(poly_mask.sum())
        if pixel_count_total == 0:
            yield {
                "district_key": dk,
                "year_month": ym,
                "radiance_mean": None,
                "radiance_median": None,
                "radiance_sum": None,
                "radiance_p90": None,
                "pixel_count_total": 0,
                "pixel_count_valid": 0,
                "quality_filter": QUALITY_FILTER_LABEL,
                "source": SOURCE_LABEL,
                "tile": TILE,
            }
            continue

        valid_in_poly = poly_mask & valid_mask_global
        pixel_count_valid = int(valid_in_poly.sum())

        if pixel_count_valid == 0:
            yield {
                "district_key": dk,
                "year_month": ym,
                "radiance_mean": None,
                "radiance_median": None,
                "radiance_sum": None,
                "radiance_p90": None,
                "pixel_count_total": pixel_count_total,
                "pixel_count_valid": 0,
                "quality_filter": QUALITY_FILTER_LABEL,
                "source": SOURCE_LABEL,
                "tile": TILE,
            }
            continue

        values = radiance[valid_in_poly]
        yield {
            "district_key": dk,
            "year_month": ym,
            "radiance_mean": float(np.mean(values)),
            "radiance_median": float(np.median(values)),
            "radiance_sum": float(np.sum(values)),
            "radiance_p90": float(np.percentile(values, 90)),
            "pixel_count_total": pixel_count_total,
            "pixel_count_valid": pixel_count_valid,
            "quality_filter": QUALITY_FILTER_LABEL,
            "source": SOURCE_LABEL,
            "tile": TILE,
        }
