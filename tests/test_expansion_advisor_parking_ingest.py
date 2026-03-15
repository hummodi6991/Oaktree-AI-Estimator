"""Tests for Expansion Advisor — Parking Context Ingestion.

Covers schema-aware column detection and SQL generation:
- Source table with `parking` column present
- Source table without `parking` column (fallback to amenity/tags)
- Fallback to amenity-only parking detection
- Safe zero-row behaviour and stats output
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.ingest.expansion_advisor_parking import (
    _build_where_filter,
    _col_expr,
    _hstore_or_col,
    _ingest_from_polygons,
    _ingest_from_points,
    _parking_expr,
)


# ---------------------------------------------------------------------------
# Unit tests for SQL fragment builders
# ---------------------------------------------------------------------------

class TestParkingExpr:
    """_parking_expr returns the correct SQL expression for the parking tag."""

    def test_direct_parking_column(self):
        assert _parking_expr("op", {"parking", "amenity"}) == "op.parking"

    def test_fallback_tags_hstore(self):
        assert _parking_expr("op", {"amenity", "tags"}) == "op.tags->'parking'"

    def test_fallback_other_tags_hstore(self):
        assert _parking_expr("op", {"amenity", "other_tags"}) == "op.other_tags->'parking'"

    def test_no_parking_source(self):
        assert _parking_expr("op", {"amenity", "name"}) == "NULL"

    def test_prefers_parking_over_tags(self):
        result = _parking_expr("op", {"parking", "tags", "other_tags"})
        assert result == "op.parking"

    def test_prefers_tags_over_other_tags(self):
        result = _parking_expr("op", {"tags", "other_tags"})
        assert result == "op.tags->'parking'"


class TestColExpr:
    def test_column_exists(self):
        assert _col_expr("op", "name", {"name", "amenity"}) == "op.name"

    def test_column_missing(self):
        assert _col_expr("op", "name", {"amenity"}) == "NULL"


class TestHstoreOrCol:
    def test_direct_column(self):
        assert _hstore_or_col("op", "capacity", {"capacity"}) == "op.capacity"

    def test_tags_fallback(self):
        assert _hstore_or_col("op", "capacity", {"tags"}) == "op.tags->'capacity'"

    def test_other_tags_fallback(self):
        assert _hstore_or_col("op", "capacity", {"other_tags"}) == "op.other_tags->'capacity'"

    def test_no_source(self):
        assert _hstore_or_col("op", "capacity", {"name"}) == "NULL"


class TestBuildWhereFilter:
    """_build_where_filter produces correct SQL predicates."""

    def test_with_both_amenity_and_parking(self):
        result = _build_where_filter("op", {"amenity", "parking"})
        assert "amenity" in result
        assert "parking" in result.lower()
        assert result.startswith("(")

    def test_amenity_only(self):
        result = _build_where_filter("op", {"amenity", "name"})
        assert "amenity" in result
        # Should NOT reference a parking column
        assert "op.parking" not in result

    def test_parking_only_no_amenity(self):
        result = _build_where_filter("op", {"parking"})
        assert "op.parking" in result
        assert "op.amenity" not in result

    def test_tags_fallback(self):
        result = _build_where_filter("op", {"amenity", "tags"})
        assert "tags->'parking'" in result

    def test_no_useful_columns(self):
        assert _build_where_filter("op", {"name", "way"}) == "FALSE"


# ---------------------------------------------------------------------------
# Integration-style tests (mocked DB)
# ---------------------------------------------------------------------------

def _mock_db(table_exists_val=True, columns=None, rowcount=0):
    """Build a mock DB session with controlled column introspection."""
    db = MagicMock()
    columns = columns or set()

    # table_exists -> bool
    def scalar_side_effect(*args, **kwargs):
        return True  # default

    # For get_table_columns we need fetchall
    col_rows = [(c,) for c in columns]

    execute_results = []

    def execute_side(sql, params=None):
        sql_str = str(sql) if not isinstance(sql, str) else sql
        mock_result = MagicMock()

        # information_schema.tables (table_exists)
        if "information_schema.tables" in sql_str:
            mock_result.scalar.return_value = table_exists_val
            return mock_result

        # information_schema.columns (get_table_columns)
        if "information_schema.columns" in sql_str:
            mock_result.fetchall.return_value = col_rows
            return mock_result

        # Find_SRID (detect_srid)
        if "Find_SRID" in sql_str:
            mock_result.scalar.return_value = 4326
            return mock_result

        # DELETE
        if sql_str.strip().upper().startswith("DELETE"):
            mock_result.rowcount = 0
            return mock_result

        # INSERT (the main query)
        if "INSERT INTO" in sql_str.upper():
            mock_result.rowcount = rowcount
            execute_results.append(sql_str)
            return mock_result

        # fallback
        mock_result.scalar.return_value = None
        mock_result.rowcount = 0
        return mock_result

    db.execute.side_effect = execute_side
    db._execute_results = execute_results
    return db


class TestIngestFromPolygonsWithParkingColumn:
    """planet_osm_polygon has the flattened `parking` column."""

    ALL_COLS = {"way", "name", "amenity", "parking", "capacity", "covered", "access"}

    def test_returns_rowcount(self):
        db = _mock_db(columns=self.ALL_COLS, rowcount=42)
        count = _ingest_from_polygons(db, replace=True)
        assert count == 42

    def test_sql_references_parking_column(self):
        db = _mock_db(columns=self.ALL_COLS, rowcount=5)
        _ingest_from_polygons(db, replace=False)
        insert_sql = db._execute_results[0]
        assert "op.parking" in insert_sql

    def test_no_hstore_extraction_when_parking_exists(self):
        db = _mock_db(columns=self.ALL_COLS | {"tags"}, rowcount=1)
        _ingest_from_polygons(db, replace=False)
        insert_sql = db._execute_results[0]
        # Should use op.parking directly, not tags
        assert "tags->'parking'" not in insert_sql


class TestIngestFromPolygonsWithoutParkingColumn:
    """planet_osm_polygon lacks the `parking` column."""

    COLS_NO_PARKING = {"way", "name", "amenity", "capacity", "covered", "access", "tags"}

    def test_returns_rowcount(self):
        db = _mock_db(columns=self.COLS_NO_PARKING, rowcount=10)
        count = _ingest_from_polygons(db, replace=True)
        assert count == 10

    def test_sql_uses_tags_hstore(self):
        db = _mock_db(columns=self.COLS_NO_PARKING, rowcount=1)
        _ingest_from_polygons(db, replace=False)
        insert_sql = db._execute_results[0]
        assert "tags->'parking'" in insert_sql
        assert "op.parking" not in insert_sql

    def test_amenity_filter_present(self):
        db = _mock_db(columns=self.COLS_NO_PARKING, rowcount=0)
        _ingest_from_polygons(db, replace=False)
        insert_sql = db._execute_results[0]
        # Must still filter on amenity='parking'
        assert "amenity" in insert_sql.lower()


class TestIngestAmenityOnlyFallback:
    """Only amenity column available — no parking, no tags."""

    COLS_AMENITY_ONLY = {"way", "name", "amenity"}

    def test_polygon_amenity_only(self):
        db = _mock_db(columns=self.COLS_AMENITY_ONLY, rowcount=3)
        count = _ingest_from_polygons(db, replace=False)
        assert count == 3

    def test_sql_has_no_parking_ref(self):
        db = _mock_db(columns=self.COLS_AMENITY_ONLY, rowcount=1)
        _ingest_from_polygons(db, replace=False)
        insert_sql = db._execute_results[0]
        assert "op.parking" not in insert_sql
        assert "tags->'parking'" not in insert_sql

    def test_point_amenity_only(self):
        db = _mock_db(columns=self.COLS_AMENITY_ONLY, rowcount=7)
        count = _ingest_from_points(db, replace=False)
        assert count == 7


class TestIngestFromPointsWithParkingColumn:
    ALL_COLS = {"way", "name", "amenity", "parking", "capacity", "covered", "access"}

    def test_returns_rowcount(self):
        db = _mock_db(columns=self.ALL_COLS, rowcount=15)
        count = _ingest_from_points(db, replace=True)
        assert count == 15


class TestIngestFromPointsWithoutParkingColumn:
    COLS_NO_PARKING = {"way", "name", "amenity", "tags"}

    def test_falls_back_to_tags(self):
        db = _mock_db(columns=self.COLS_NO_PARKING, rowcount=4)
        count = _ingest_from_points(db, replace=True)
        assert count == 4
        insert_sql = db._execute_results[0]
        assert "tags->'parking'" in insert_sql


class TestZeroRowBehaviour:
    """Ingest completes gracefully when no rows match."""

    def test_polygon_zero_rows(self):
        db = _mock_db(columns={"way", "amenity"}, rowcount=0)
        count = _ingest_from_polygons(db, replace=True)
        assert count == 0

    def test_point_zero_rows(self):
        db = _mock_db(columns={"way", "amenity"}, rowcount=0)
        count = _ingest_from_points(db, replace=True)
        assert count == 0

    def test_table_missing(self):
        db = _mock_db(table_exists_val=False, rowcount=0)
        assert _ingest_from_polygons(db, replace=True) == 0
        assert _ingest_from_points(db, replace=True) == 0


class TestStatsWrittenOnPartialResults:
    """main() writes stats JSON even when counts are zero."""

    @patch("app.ingest.expansion_advisor_parking.get_session")
    @patch("app.ingest.expansion_advisor_parking.validate_db_env")
    @patch("app.ingest.expansion_advisor_parking.write_stats")
    @patch("app.ingest.expansion_advisor_parking.log_table_counts", return_value={"expansion_parking_asset": 0})
    @patch("app.ingest.expansion_advisor_parking._ingest_from_points", return_value=0)
    @patch("app.ingest.expansion_advisor_parking._ingest_from_polygons", return_value=0)
    def test_stats_written_on_zero_rows(
        self, mock_poly, mock_pt, mock_log, mock_write, mock_validate, mock_session
    ):
        import sys
        from app.ingest.expansion_advisor_parking import main

        mock_session.return_value = MagicMock()
        with patch.object(sys, "argv", ["prog", "--write-stats", "/tmp/test_stats.json"]):
            main()

        mock_write.assert_called_once()
        stats = mock_write.call_args[0][1]
        assert stats["total_inserted"] == 0
        assert stats["polygon_count"] == 0
        assert stats["point_count"] == 0
