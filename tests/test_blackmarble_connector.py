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

    # Real LAADS .json listing shape: {"content": [{"name": ..., ...}, ...]}
    fake_payload = {
        "content": [
            {"name": "VNP46A3.A2026060.h22v06.002.2026105050000.h5", "size": 80_000_000},
            {"name": "VNP46A3.A2026060.h21v06.002.2026105050000.h5", "size": 80_000_000},
        ]
    }

    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.json.return_value = fake_payload
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


def test_discover_h5_url_accepts_bare_list_fallback():
    """Defensive: if a LAADS endpoint variant returns a flat list, the parser
    should still find the tile. Production hits the wrapper shape (see
    test_discover_h5_url_constructs_correct_path), but the parser supports
    either."""
    from app.connectors import blackmarble

    fake_payload = [
        {"name": "VNP46A3.A2026060.h22v06.002.2026105050000.h5", "size": 80_000_000},
    ]

    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.json.return_value = fake_payload
    fake_resp.raise_for_status.return_value = None

    fake_session = MagicMock()
    fake_session.get.return_value = fake_resp

    with patch.object(blackmarble, "_LAADSSession", return_value=fake_session):
        url = blackmarble.discover_h5_url(date(2026, 3, 1), token="dummy")

    assert "h22v06" in url
    assert "/2026/060/" in url


def test_discover_h5_url_raises_when_content_empty():
    """Empty content list should raise BlackMarbleNotAvailableError, same as a
    listing that legitimately has no h22v06 entry."""
    from app.connectors import blackmarble

    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.json.return_value = {"content": []}
    fake_resp.raise_for_status.return_value = None

    fake_session = MagicMock()
    fake_session.get.return_value = fake_resp

    with patch.object(blackmarble, "_LAADSSession", return_value=fake_session):
        with pytest.raises(blackmarble.BlackMarbleNotAvailableError):
            blackmarble.discover_h5_url(date(2026, 3, 1), token="dummy")


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


# ── Patch A1: area-based confidence guards ──────────────────────────────


def test_small_district_below_floor_is_unconfident():
    from app.connectors import blackmarble

    confident, reason = blackmarble.evaluate_confidence(
        pixels_cur=25,
        pixels_prev=30,
        area_km2=0.3,
        district_key="tiny_district",
    )
    assert confident is False
    assert reason == "small_district"


def test_small_district_at_floor_is_confident():
    """Boundary: area exactly at SMALL_DISTRICT_FLOOR_KM2 is allowed."""
    from app.connectors import blackmarble

    confident, reason = blackmarble.evaluate_confidence(
        pixels_cur=25,
        pixels_prev=30,
        area_km2=blackmarble.SMALL_DISTRICT_FLOOR_KM2,
        district_key="boundary_district",
    )
    assert confident is True
    assert reason is None


def test_pixel_floor_takes_precedence_over_area():
    from app.connectors import blackmarble

    confident, reason = blackmarble.evaluate_confidence(
        pixels_cur=5,  # below floor
        pixels_prev=30,
        area_km2=10.0,  # ample area
        district_key="thin_pixel_district",
    )
    assert confident is False
    assert reason == "pixel_floor"


def test_missing_area_falls_back_to_pixel_only():
    from app.connectors import blackmarble

    # Pixels above floor + unknown area → confident.
    confident, reason = blackmarble.evaluate_confidence(
        pixels_cur=25,
        pixels_prev=30,
        area_km2=None,
        district_key="unknown_area_district",
    )
    assert confident is True
    assert reason is None

    # Pixels below floor + unknown area → unconfident with pixel_floor.
    confident, reason = blackmarble.evaluate_confidence(
        pixels_cur=5,
        pixels_prev=30,
        area_km2=None,
        district_key="unknown_area_district",
    )
    assert confident is False
    assert reason == "pixel_floor"


def test_large_district_logs_outlier(caplog):
    import logging

    from app.connectors import blackmarble

    caplog.set_level(logging.WARNING, logger="app.connectors.blackmarble")

    confident, reason = blackmarble.evaluate_confidence(
        pixels_cur=25,
        pixels_prev=30,
        area_km2=750.0,
        district_key="huge_merged_district",
    )

    # Large-district outliers are unconfident: their district-wide radiance
    # YoY averages over too much empty land to be meaningful at a candidate's
    # micro-location. The WARNING log is retained for observability.
    assert confident is False
    assert reason == "large_district"

    outlier_records = [
        r for r in caplog.records
        if r.getMessage() == "blackmarble.large_district_outlier"
    ]
    assert len(outlier_records) == 1
    rec = outlier_records[0]
    assert rec.levelno == logging.WARNING
    assert getattr(rec, "district_key", None) == "huge_merged_district"
    assert getattr(rec, "area_km2", None) == 750.0


def test_large_district_at_threshold_is_confident(caplog):
    """Boundary: area exactly at LARGE_DISTRICT_OUTLIER_KM2 is allowed.

    The rule fires strictly above the threshold; equality stays confident
    and emits no WARNING.
    """
    import logging

    from app.connectors import blackmarble

    caplog.set_level(logging.WARNING, logger="app.connectors.blackmarble")

    confident, reason = blackmarble.evaluate_confidence(
        pixels_cur=25,
        pixels_prev=30,
        area_km2=blackmarble.LARGE_DISTRICT_OUTLIER_KM2,
        district_key="threshold_district",
    )

    assert confident is True
    assert reason is None
    assert not [
        r for r in caplog.records
        if r.getMessage() == "blackmarble.large_district_outlier"
    ]


def test_large_district_does_not_log_when_below_threshold(caplog):
    """Boundary: area = 499 km² is below the 500 km² outlier threshold."""
    import logging

    from app.connectors import blackmarble

    caplog.set_level(logging.WARNING, logger="app.connectors.blackmarble")

    confident, reason = blackmarble.evaluate_confidence(
        pixels_cur=25,
        pixels_prev=30,
        area_km2=499.0,
        district_key="just_under_outlier",
    )

    assert confident is True
    assert reason is None
    assert not [
        r for r in caplog.records
        if r.getMessage() == "blackmarble.large_district_outlier"
    ]


# ---------------------------------------------------------------------------
# B1.5 — rolling-6 window minimum pixel gate (evaluate_confidence reuse).
# The rolling-6 caller passes MIN(pixel_count_valid) over the 6-month window
# as pixels_cur / pixels_prev. These tests confirm the gate still fires
# correctly when the input represents a window minimum rather than a single
# month's count.
# ---------------------------------------------------------------------------


def test_evaluate_confidence_window_minimum_input_below_floor():
    """When the windowed MIN pixel count is below PIXEL_COUNT_FLOOR, the gate must fail.

    Mirrors the per-point pixel_floor branch but is reachable via window
    callers (B1.5): MIN over 6 months = 8 means one cloudy month slipped
    through, so the 6-window is not fully confident.
    """
    from app.connectors import blackmarble

    confident, reason = blackmarble.evaluate_confidence(
        pixels_cur=8,   # window min, below floor
        pixels_prev=20,
        area_km2=10.0,
        district_key="cur_window_dropped_one_cloudy_month",
    )
    assert confident is False
    assert reason == "pixel_floor"


def test_evaluate_confidence_window_minimum_input_above_floor():
    """When both windowed MIN pixel counts are at/above the floor, gate passes.

    MIN=10 means every month in the 6-window independently satisfied the
    PIXEL_COUNT_FLOOR=10 requirement.
    """
    from app.connectors import blackmarble

    confident, reason = blackmarble.evaluate_confidence(
        pixels_cur=10,  # window min, exactly at floor
        pixels_prev=15,
        area_km2=10.0,
        district_key="all_months_confident",
    )
    assert confident is True
    assert reason is None
