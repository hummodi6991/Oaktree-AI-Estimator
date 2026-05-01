"""Lightweight tests for the Black Marble VNP46A3 connector.

Network-dependent tests are skipped. Tests focus on URL construction and
quality-filter / pixel-count behavior of the aggregator using a small
synthetic raster + polygon.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import patch, MagicMock

import pytest


@pytest.mark.skip(reason="requires NASA EDL token and network")
def test_download_h5_real():
    pass


def test_discover_h5_url_constructs_correct_path():
    from app.connectors import blackmarble

    # Mock LAADS directory listing JSON for 2026-03 (DOY 60 for first-of-month).
    fake_entries = [
        {"name": "VNP46A3.A2026060.h22v06.002.2026105050000.h5", "size": 80_000_000},
        {"name": "VNP46A3.A2026060.h21v06.002.2026105050000.h5", "size": 80_000_000},
    ]

    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.json.return_value = fake_entries
    fake_resp.raise_for_status.return_value = None

    fake_session = MagicMock()
    fake_session.get.return_value = fake_resp

    with patch.object(blackmarble, "_LAADSSession", return_value=fake_session):
        url = blackmarble.discover_h5_url(date(2026, 3, 1), token="dummy")

    assert "ladsweb.modaps.eosdis.nasa.gov" in url
    assert "VNP46A3" in url
    assert "/2026/060/" in url
    assert "h22v06" in url
    # Listing URL passed to GET should be the .json variant for that DOY.
    listing_url = fake_session.get.call_args[0][0]
    assert listing_url.endswith("/2026/060.json")


def test_discover_h5_url_raises_when_not_published():
    from app.connectors import blackmarble

    fake_resp = MagicMock()
    fake_resp.status_code = 404

    fake_session = MagicMock()
    fake_session.get.return_value = fake_resp

    with patch.object(blackmarble, "_LAADSSession", return_value=fake_session):
        with pytest.raises(blackmarble.BlackMarbleNotAvailableError):
            blackmarble.discover_h5_url(date(2099, 1, 1), token="dummy")


def _build_synthetic_h5(tmp_path, radiance, quality):
    """Write a tiny H5 with the expected band paths."""
    import h5py

    p = tmp_path / "synth.h5"
    with h5py.File(p, "w") as fh:
        # h5py creates intermediate groups implicitly when paths are nested.
        from app.connectors.blackmarble import RADIANCE_BAND_PATH, QUALITY_BAND_PATH

        fh.create_dataset(RADIANCE_BAND_PATH, data=radiance)
        fh.create_dataset(QUALITY_BAND_PATH, data=quality)
    return p


def test_aggregate_per_district_filters_quality_correctly():
    # Skip if optional deps missing.
    h5py = pytest.importorskip("h5py")
    np = pytest.importorskip("numpy")
    pytest.importorskip("rasterio")
    shapely_geom = pytest.importorskip("shapely.geometry")

    from app.connectors import blackmarble

    # 4x4 synthetic raster.
    # Quality: top-left 2x2 = 0 (Good), top-right 2x2 = 1 (Poor),
    # bottom-left 2x2 = 2 (Bad), bottom-right 2x2 = 0 (Good).
    radiance = np.array(
        [
            [10.0, 11.0, 20.0, 21.0],
            [12.0, 13.0, 22.0, 23.0],
            [30.0, 31.0, 40.0, 41.0],
            [32.0, 33.0, 42.0, 43.0],
        ],
        dtype=float,
    )
    quality = np.array(
        [
            [0, 0, 1, 1],
            [0, 0, 1, 1],
            [2, 2, 0, 0],
            [2, 2, 0, 0],
        ],
        dtype=int,
    )

    # Polygon covering the entire tile envelope.
    poly = shapely_geom.box(40.0, 20.0, 50.0, 30.0)

    import tempfile
    import os

    with tempfile.TemporaryDirectory() as tmp:
        from pathlib import Path

        h5_path = _build_synthetic_h5(Path(tmp), radiance, quality)

        rows = list(
            blackmarble.aggregate_per_district(
                h5_path,
                [{"district_key": "all", "geometry": poly}],
                year_month=date(2026, 3, 1),
            )
        )

    assert len(rows) == 1
    r = rows[0]
    # Lenient filter (quality < 2) excludes the bottom-left 2x2 (quality=2).
    # 16 total, 12 valid.
    assert r["pixel_count_total"] == 16
    assert r["pixel_count_valid"] == 12
    # Mean of the 12 valid pixels.
    valid_vals = [10, 11, 20, 21, 12, 13, 22, 23, 40, 41, 42, 43]
    assert abs(r["radiance_mean"] - sum(valid_vals) / len(valid_vals)) < 1e-6
    assert r["quality_filter"] == "lenient_qa_lt_2"
    assert r["source"] == "nasa_blackmarble_vnp46a3_c2"
    assert r["tile"] == "h22v06"


def test_aggregate_per_district_pixel_count_floor():
    # Verifies pixel_count_valid is reported regardless of the
    # PIXEL_COUNT_FLOOR; the floor check happens at consume-time.
    pytest.importorskip("h5py")
    np = pytest.importorskip("numpy")
    pytest.importorskip("rasterio")
    shapely_geom = pytest.importorskip("shapely.geometry")

    from app.connectors import blackmarble

    radiance = np.full((4, 4), -999.9, dtype=float)
    radiance[0, 0] = 5.0  # one valid pixel
    quality = np.zeros((4, 4), dtype=int)

    poly = shapely_geom.box(40.0, 20.0, 50.0, 30.0)

    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmp:
        h5_path = _build_synthetic_h5(Path(tmp), radiance, quality)
        rows = list(
            blackmarble.aggregate_per_district(
                h5_path,
                [{"district_key": "tiny", "geometry": poly}],
                year_month=date(2026, 3, 1),
            )
        )

    assert len(rows) == 1
    r = rows[0]
    assert r["pixel_count_total"] == 16
    # Below the consume-time floor of 10, but we still report it.
    assert r["pixel_count_valid"] == 1
    assert r["pixel_count_valid"] < blackmarble.PIXEL_COUNT_FLOOR
