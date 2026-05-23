"""Tests for Expansion Advisor — Google Places parking ingest.

Mirrors the structure of ``tests/test_expansion_advisor_parking_ingest.py``
but mocks ``AsyncGooglePlacesClient.nearby_search`` rather than executing
real HTTP requests, and uses a tiny synthetic bounding box so the grid
generation cost stays bounded.
"""
from __future__ import annotations

import asyncio
import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

from app.ingest.expansion_advisor_parking_google import (
    RIYADH_BUILT_UP_BBOX,
    SPACING_M,
    delete_checkpoint,
    generate_grid_points,
    load_checkpoint,
    run_ingest,
    save_checkpoint,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# A small bbox keeps tests fast (~3-4 cells with 500m spacing).
SMALL_BBOX = {
    "min_lon": 46.60,
    "min_lat": 24.60,
    "max_lon": 46.61,
    "max_lat": 24.61,
}


def _mock_db():
    """Return a MagicMock DB session that captures all execute() calls.

    Mirrors the pattern in ``tests/test_expansion_advisor_parking_ingest.py``.
    """
    db = MagicMock()
    executed: list[tuple[str, object]] = []

    def execute_side(stmt, params=None):
        sql_str = str(stmt)
        executed.append((sql_str, params))
        result = MagicMock()
        # Make scalar() always return 0 so log_table_counts can do int(...)
        result.scalar.return_value = 0
        result.rowcount = len(params) if isinstance(params, list) else 0
        return result

    db.execute.side_effect = execute_side
    db._executed = executed
    return db


def _empty_ok():
    return {"status": "OK", "results": []}


def _ok_with_results(results):
    return {"status": "OK", "results": results}


def _single_result(place_id="p1", name="Lot A", lat=24.605, lng=46.605):
    return _ok_with_results([
        {
            "place_id": place_id,
            "name": name,
            "geometry": {"location": {"lat": lat, "lng": lng}},
        }
    ])


def _run(
    db,
    response,
    *,
    checkpoint_path,
    replace=True,
    write_stats_path=None,
    bbox=None,
    side_effect=None,
):
    """Run ``run_ingest`` with ``nearby_search`` patched to *response*."""
    with patch(
        "app.ingest.expansion_advisor_parking_google.AsyncGooglePlacesClient.nearby_search",
        new_callable=AsyncMock,
    ) as mock_search:
        if side_effect is not None:
            mock_search.side_effect = side_effect
        else:
            mock_search.return_value = response
        stats = asyncio.run(
            run_ingest(
                db,
                city="riyadh",
                replace=replace,
                bbox=bbox if bbox is not None else SMALL_BBOX,
                spacing_m=500,
                radius_m=350,
                checkpoint_path=checkpoint_path,
                write_stats_path=write_stats_path,
            )
        )
        return stats, mock_search


# ---------------------------------------------------------------------------
# 1. Grid generation
# ---------------------------------------------------------------------------

class TestGridGeneration:
    def test_grid_generation_produces_expected_count(self):
        grid = generate_grid_points(RIYADH_BUILT_UP_BBOX, SPACING_M)
        # ~67 rows x 81 cols = 5,427 (endpoint inclusion math; budget 5,200–5,500)
        assert 5_200 <= len(grid) <= 5_500
        cell_keys = [ck for _lat, _lon, ck in grid]
        assert len(cell_keys) == len(set(cell_keys))

    def test_cell_key_format(self):
        grid = generate_grid_points(RIYADH_BUILT_UP_BBOX, SPACING_M)
        for lat, lon, ck in grid[:5]:
            assert ck == f"{lat:.4f}_{lon:.4f}"

    def test_grid_lat_lon_in_bbox(self):
        grid = generate_grid_points(RIYADH_BUILT_UP_BBOX, SPACING_M)
        for lat, lon, _ck in grid:
            assert RIYADH_BUILT_UP_BBOX["min_lat"] <= lat <= RIYADH_BUILT_UP_BBOX["max_lat"]
            assert RIYADH_BUILT_UP_BBOX["min_lon"] <= lon <= RIYADH_BUILT_UP_BBOX["max_lon"]

    def test_small_bbox(self):
        grid = generate_grid_points(SMALL_BBOX, 500)
        assert 4 <= len(grid) <= 25


# ---------------------------------------------------------------------------
# 2. Checkpoint helpers
# ---------------------------------------------------------------------------

class TestCheckpoint:
    def test_load_missing_returns_empty_set(self, tmp_path):
        p = str(tmp_path / "missing.json")
        assert load_checkpoint(p) == set()

    def test_save_and_load_roundtrip(self, tmp_path):
        p = str(tmp_path / "cp.json")
        cells = {"24.6000_46.6000", "24.6050_46.6050"}
        save_checkpoint(p, cells)
        assert load_checkpoint(p) == cells

    def test_delete_checkpoint_removes_file(self, tmp_path):
        p = str(tmp_path / "cp.json")
        save_checkpoint(p, {"a"})
        assert os.path.exists(p)
        delete_checkpoint(p)
        assert not os.path.exists(p)

    def test_delete_checkpoint_tolerates_missing(self, tmp_path):
        p = str(tmp_path / "never_existed.json")
        # Should not raise
        delete_checkpoint(p)


# ---------------------------------------------------------------------------
# 3. Dedup within run
# ---------------------------------------------------------------------------

class TestDedup:
    def test_dedup_within_run(self, tmp_path):
        db = _mock_db()
        cp = str(tmp_path / "cp.json")
        # Every cell returns the same place_id → only one row should be inserted
        stats, mock_search = _run(db, _single_result("PID_SHARED"), checkpoint_path=cp)
        assert stats["pois_inserted"] == 1
        assert stats["unique_pois_found"] == 1
        # Every cell was still queried
        grid = generate_grid_points(SMALL_BBOX, 500)
        assert mock_search.call_count == len(grid)


# ---------------------------------------------------------------------------
# 4. Checkpoint skips completed cells
# ---------------------------------------------------------------------------

class TestCheckpointSkip:
    def test_checkpoint_skips_completed_cells(self, tmp_path):
        db = _mock_db()
        cp = str(tmp_path / "cp.json")
        grid = generate_grid_points(SMALL_BBOX, 500)
        all_keys = [ck for _lat, _lon, ck in grid]
        # Pre-complete all but the last cell
        save_checkpoint(cp, set(all_keys[:-1]))

        _stats, mock_search = _run(db, _empty_ok(), checkpoint_path=cp)
        assert mock_search.call_count == 1


# ---------------------------------------------------------------------------
# 5. Checkpoint deleted on success
# ---------------------------------------------------------------------------

class TestCheckpointDeletedOnSuccess:
    def test_checkpoint_deleted_on_success(self, tmp_path):
        db = _mock_db()
        cp = str(tmp_path / "cp.json")
        _run(db, _empty_ok(), checkpoint_path=cp)
        assert not os.path.exists(cp)


# ---------------------------------------------------------------------------
# 6. Replace logic
# ---------------------------------------------------------------------------

class TestReplaceLogic:
    def test_replace_true_with_no_checkpoint_deletes_existing_rows(self, tmp_path):
        db = _mock_db()
        cp = str(tmp_path / "cp.json")
        _run(db, _empty_ok(), checkpoint_path=cp, replace=True)
        delete_stmts = [
            sql for sql, _ in db._executed
            if sql.strip().upper().startswith("DELETE")
        ]
        assert any("source = 'google_places'" in s for s in delete_stmts), (
            "Expected DELETE … source='google_places' to run with replace=True and no checkpoint"
        )

    def test_replace_true_with_checkpoint_does_not_delete(self, tmp_path):
        db = _mock_db()
        cp = str(tmp_path / "cp.json")
        save_checkpoint(cp, {"24.6000_46.6000"})  # checkpoint exists → resume mode
        _run(db, _empty_ok(), checkpoint_path=cp, replace=True)
        delete_stmts = [
            sql for sql, _ in db._executed
            if sql.strip().upper().startswith("DELETE")
        ]
        assert not any("source = 'google_places'" in s for s in delete_stmts), (
            "DELETE must be skipped when a checkpoint indicates a resumed run"
        )

    def test_replace_false_does_not_delete(self, tmp_path):
        db = _mock_db()
        cp = str(tmp_path / "cp.json")
        _run(db, _empty_ok(), checkpoint_path=cp, replace=False)
        delete_stmts = [
            sql for sql, _ in db._executed
            if sql.strip().upper().startswith("DELETE")
        ]
        assert not any("source = 'google_places'" in s for s in delete_stmts)


# ---------------------------------------------------------------------------
# 7. Stats payload keys
# ---------------------------------------------------------------------------

class TestStatsPayloadKeys:
    def test_stats_payload_keys(self, tmp_path):
        db = _mock_db()
        cp = str(tmp_path / "cp.json")
        stats_path = str(tmp_path / "stats.json")
        with patch(
            "app.ingest.expansion_advisor_parking_google.log_table_counts",
            return_value={"expansion_parking_asset": 0},
        ):
            _run(db, _empty_ok(), checkpoint_path=cp, write_stats_path=stats_path)
        with open(stats_path) as f:
            payload = json.load(f)
        required = [
            "grid_points_total",
            "grid_points_queried_successfully",
            "grid_points_with_results",
            "grid_points_empty",
            "grid_points_saturated_at_20",
            "unique_pois_found",
            "pois_inserted",
            "api_errors",
            "total_api_calls_billed",
        ]
        for key in required:
            assert key in payload, f"missing stats key: {key}"


# ---------------------------------------------------------------------------
# 8. API error status increments counter
# ---------------------------------------------------------------------------

class TestApiErrorStatus:
    def test_api_error_status_increments_counter(self, tmp_path):
        db = _mock_db()
        cp = str(tmp_path / "cp.json")
        with patch(
            "app.ingest.expansion_advisor_parking_google.log_table_counts",
            return_value={},
        ):
            stats, _ = _run(
                db, {"status": "OVER_QUERY_LIMIT", "results": []},
                checkpoint_path=cp,
            )
        grid = generate_grid_points(SMALL_BBOX, 500)
        assert stats["api_errors"] == len(grid)
        assert stats["pois_inserted"] == 0
        # No INSERT statements should have been executed
        insert_stmts = [
            sql for sql, _ in db._executed
            if sql.strip().upper().startswith("INSERT")
        ]
        assert insert_stmts == []


# ---------------------------------------------------------------------------
# 9. Saturation at 20 tracked
# ---------------------------------------------------------------------------

class TestSaturation:
    def test_saturation_at_20_tracked(self, tmp_path):
        db = _mock_db()
        cp = str(tmp_path / "cp.json")
        results = [
            {
                "place_id": f"p{i}",
                "name": f"Lot {i}",
                "geometry": {"location": {"lat": 24.6 + i * 0.0001, "lng": 46.6 + i * 0.0001}},
            }
            for i in range(20)
        ]
        with patch(
            "app.ingest.expansion_advisor_parking_google.log_table_counts",
            return_value={},
        ):
            stats, _ = _run(db, _ok_with_results(results), checkpoint_path=cp)
        grid = generate_grid_points(SMALL_BBOX, 500)
        # Every cell returned 20 results → every cell counts as saturated
        assert stats["grid_points_saturated_at_20"] == len(grid)


# ---------------------------------------------------------------------------
# 10. Module presence / shape
# ---------------------------------------------------------------------------

class TestModulePresence:
    def test_module_exports(self):
        import app.ingest.expansion_advisor_parking_google as mod

        for attr in (
            "main",
            "run_ingest",
            "generate_grid_points",
            "load_checkpoint",
            "save_checkpoint",
            "delete_checkpoint",
            "RIYADH_BUILT_UP_BBOX",
            "SPACING_M",
            "RADIUS_M",
        ):
            assert hasattr(mod, attr), f"missing module attribute: {attr}"


# ---------------------------------------------------------------------------
# 11. Empty-DB safety
# ---------------------------------------------------------------------------

class TestEmptyRunSafety:
    def test_runs_clean_with_no_results_anywhere(self, tmp_path):
        db = _mock_db()
        cp = str(tmp_path / "cp.json")
        stats_path = str(tmp_path / "stats.json")
        with patch(
            "app.ingest.expansion_advisor_parking_google.log_table_counts",
            return_value={"expansion_parking_asset": 0},
        ):
            stats, _ = _run(db, _empty_ok(), checkpoint_path=cp, write_stats_path=stats_path)
        assert stats["pois_inserted"] == 0
        assert stats["unique_pois_found"] == 0
        assert stats["api_errors"] == 0
