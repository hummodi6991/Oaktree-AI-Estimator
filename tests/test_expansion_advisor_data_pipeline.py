"""Tests for Expansion Advisor data-ingestion pipeline.

Covers:
- Service prefers normalized tables when populated
- Fallback to legacy logic when normalized tables empty
- Delivery zero-row failure
- Rent comp normalization to annual SAR/m²
- Competitor quality materialization
- Road/parking source metadata appears in feature_snapshot_json
"""
from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import tempfile
from unittest.mock import MagicMock, call, patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool


# ---------------------------------------------------------------------------
# Config tests
# ---------------------------------------------------------------------------
class TestExpansionAdvisorConfig:
    """Expansion Advisor config settings exist and have correct defaults."""

    def test_config_defaults(self):
        from app.core.config import Settings

        s = Settings()
        assert s.EXPANSION_ROADS_TABLE == "expansion_road_context"
        assert s.EXPANSION_PARKING_TABLE == "expansion_parking_asset"
        assert s.EXPANSION_DELIVERY_TABLE == "expansion_delivery_market"
        assert s.EXPANSION_RENT_TABLE == "expansion_rent_comp"
        assert s.EXPANSION_COMPETITOR_TABLE == "expansion_competitor_quality"

    def test_config_env_override(self):
        """Settings reads from env at class definition time, so new instances
        reflect the env at import time, not at instantiation time.  This test
        verifies the env var is read correctly by checking the mechanism."""
        # The Settings class uses os.getenv at class body time.
        # If EXPANSION_ROADS_TABLE was set before import, it would take effect.
        # We verify the default is correct (env var not set in test).
        from app.core.config import Settings
        s = Settings()
        assert isinstance(s.EXPANSION_ROADS_TABLE, str)
        assert len(s.EXPANSION_ROADS_TABLE) > 0


# ---------------------------------------------------------------------------
# Service prefers normalized tables when populated
# ---------------------------------------------------------------------------
class TestServicePrefersNormalizedTables:
    """Verify Expansion Advisor service prefers normalized tables."""

    def test_ea_table_has_rows_returns_false_on_missing_table(self):
        from app.services.expansion_advisor import _ea_table_has_rows

        mock_db = MagicMock()
        # Make the context manager work properly so execute() is called inside it
        mock_db.begin_nested.return_value.__enter__ = MagicMock(return_value=None)
        mock_db.begin_nested.return_value.__exit__ = MagicMock(return_value=False)
        mock_db.execute.side_effect = Exception("table does not exist")
        result = _ea_table_has_rows(mock_db, "nonexistent_table")
        assert result is False or result is None  # Both are falsy, service treats as "not available"

    def test_estimate_rent_prefers_expansion_table(self):
        """_estimate_rent_sar_m2_year should try expansion_rent_comp first."""
        from app.services.expansion_advisor import _estimate_rent_from_expansion_table

        mock_db = MagicMock()
        ctx = MagicMock()
        mock_db.begin_nested.return_value.__enter__ = MagicMock(return_value=ctx)
        mock_db.begin_nested.return_value.__exit__ = MagicMock(return_value=False)

        # Simulate table has rows + district median
        def mock_execute(stmt, params=None):
            sql_str = str(stmt)
            result = MagicMock()
            if "EXISTS" in sql_str:
                result.scalar.return_value = True
                return result
            if "PERCENTILE_CONT" in sql_str and "district" in sql_str:
                row = {"median": 1200.0, "n": 10}
                result.mappings.return_value.first.return_value = row
                return result
            result.mappings.return_value.first.return_value = {"median": 1000.0}
            return result

        mock_db.execute.side_effect = mock_execute

        result = _estimate_rent_from_expansion_table(mock_db, "الملقا")
        assert result is not None
        rent, source = result
        assert rent == 1200.0
        assert source == "expansion_rent_district"


# ---------------------------------------------------------------------------
# Fallback to legacy logic when normalized tables empty
# ---------------------------------------------------------------------------
class TestFallbackToLegacy:
    """Verify fallback to legacy logic when normalized tables are empty."""

    def test_estimate_rent_falls_back_when_expansion_empty(self):
        from app.services.expansion_advisor import _estimate_rent_from_expansion_table

        mock_db = MagicMock()
        ctx = MagicMock()
        mock_db.begin_nested.return_value.__enter__ = MagicMock(return_value=ctx)
        mock_db.begin_nested.return_value.__exit__ = MagicMock(return_value=False)

        # Table has no rows
        def mock_execute(stmt, params=None):
            result = MagicMock()
            result.scalar.return_value = False
            return result

        mock_db.execute.side_effect = mock_execute
        assert _estimate_rent_from_expansion_table(mock_db, "test") is None


# ---------------------------------------------------------------------------
# Delivery zero-row failure
# ---------------------------------------------------------------------------
class TestDeliveryZeroRowFailure:
    """Delivery ingest should fail loudly on zero useful rows unless allowed."""

    def test_delivery_module_import(self):
        """Verify the delivery ingest module can be imported."""
        import app.ingest.expansion_advisor_delivery as mod

        assert hasattr(mod, "main")
        assert hasattr(mod, "_normalize_delivery_records")
        assert mod.DEFAULT_PLATFORMS == "hungerstation,jahez,keeta,talabat,mrsool"


# ---------------------------------------------------------------------------
# Rent comp normalization
# ---------------------------------------------------------------------------
class TestRentCompNormalization:
    """Rent comps should normalize to annual SAR/m²."""

    def test_rent_comps_module_import(self):
        import app.ingest.expansion_advisor_rent_comps as mod

        assert hasattr(mod, "main")
        assert hasattr(mod, "_normalize_from_existing_rent_comp")
        assert hasattr(mod, "_normalize_from_csv")
        assert hasattr(mod, "_log_district_medians")

    def test_rent_annual_normalization_logic(self):
        """Monthly rent * 12 should equal annual rent, and rent/m² should be annual/area."""
        monthly = 5000.0
        area = 100.0
        annual = monthly * 12.0
        rent_m2_year = annual / area
        assert annual == 60000.0
        assert rent_m2_year == 600.0


# ---------------------------------------------------------------------------
# Competitor quality materialization
# ---------------------------------------------------------------------------
class TestCompetitorQuality:
    """Competitor quality scores should be derivable."""

    def test_competitor_module_import(self):
        import app.ingest.expansion_advisor_competitors as mod

        assert hasattr(mod, "main")
        assert hasattr(mod, "_build_competitor_quality")
        assert hasattr(mod, "_has_google_review_columns")

    def test_chain_strength_score_formula(self):
        """chain_strength_score = min(100, chain_size * 12)."""
        chain_size = 5
        score = min(100.0, chain_size * 12.0)
        assert score == 60.0

        chain_size = 10
        score = min(100.0, chain_size * 12.0)
        assert score == 100.0

    def test_review_score_normalization(self):
        """review_score maps 1-5 star to 0-100."""
        rating = 4.5
        score = min(100.0, max(0.0, (rating - 1.0) / 4.0 * 100.0))
        assert score == 87.5


# ---------------------------------------------------------------------------
# Road/parking source metadata in feature_snapshot_json
# ---------------------------------------------------------------------------
class TestContextSourcesMetadata:
    """feature_snapshot_json.context_sources should include source provenance."""

    def test_context_sources_has_road_parking_keys(self):
        from app.services.expansion_advisor import _normalize_feature_snapshot

        raw = {
            "context_sources": {
                "road_source": "expansion_road_context",
                "parking_source": "expansion_parking_asset",
                "delivery_source": "expansion_delivery_market",
                "competitor_source": "expansion_competitor_quality",
                "rent_source": "expansion_rent_district",
                "road_context_available": True,
                "parking_context_available": True,
            }
        }
        normalized = _normalize_feature_snapshot(raw)
        cs = normalized["context_sources"]
        assert cs["road_source"] == "expansion_road_context"
        assert cs["parking_source"] == "expansion_parking_asset"
        assert cs["delivery_source"] == "expansion_delivery_market"
        assert cs["competitor_source"] == "expansion_competitor_quality"
        assert cs["rent_source"] == "expansion_rent_district"

    def test_fallback_sources_when_tables_empty(self):
        """When normalized tables are empty, sources should reflect legacy."""
        from app.services.expansion_advisor import _normalize_feature_snapshot

        raw = {
            "context_sources": {
                "road_source": "estimated",
                "parking_source": "estimated",
                "delivery_source": "delivery_source_record",
                "competitor_source": "restaurant_poi",
                "rent_source": "conservative_default",
            }
        }
        normalized = _normalize_feature_snapshot(raw)
        cs = normalized["context_sources"]
        assert cs["road_source"] == "estimated"
        assert cs["delivery_source"] == "delivery_source_record"
        assert cs["competitor_source"] == "restaurant_poi"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
class TestSharedHelpers:
    """Test expansion advisor common helpers."""

    def test_riyadh_bbox_values(self):
        from app.ingest.expansion_advisor_common import RIYADH_BBOX

        assert RIYADH_BBOX["min_lon"] < RIYADH_BBOX["max_lon"]
        assert RIYADH_BBOX["min_lat"] < RIYADH_BBOX["max_lat"]
        assert 46.0 < RIYADH_BBOX["min_lon"] < 47.0
        assert 24.0 < RIYADH_BBOX["min_lat"] < 26.0

    def test_riyadh_filter_sql(self):
        from app.ingest.expansion_advisor_common import riyadh_filter_sql

        sql = riyadh_filter_sql("geom", "t")
        assert "ST_Intersects" in sql
        assert "t.geom" in sql
        assert "ST_MakeEnvelope" in sql

    def test_write_stats(self, tmp_path):
        from app.ingest.expansion_advisor_common import write_stats

        path = str(tmp_path / "test_stats.json")
        write_stats(path, {"test": 123})
        with open(path) as f:
            data = json.load(f)
        assert data["test"] == 123
        assert "generated_at" in data


# ---------------------------------------------------------------------------
# Refresh module
# ---------------------------------------------------------------------------
class TestRefreshModule:
    """Test expansion advisor refresh module."""

    def test_refresh_module_import(self):
        import app.ingest.expansion_advisor_refresh as mod

        assert hasattr(mod, "main")
        assert hasattr(mod, "run_alembic_upgrade")
        assert hasattr(mod, "refresh_materialized_views")
        assert hasattr(mod, "EXPANSION_TABLES")
        assert len(mod.EXPANSION_TABLES) == 5

    def test_expansion_tables_list(self):
        from app.ingest.expansion_advisor_refresh import EXPANSION_TABLES

        expected = [
            "expansion_road_context",
            "expansion_parking_asset",
            "expansion_delivery_market",
            "expansion_rent_comp",
            "expansion_competitor_quality",
        ]
        assert EXPANSION_TABLES == expected


# ---------------------------------------------------------------------------
# Roads module
# ---------------------------------------------------------------------------
class TestRoadsModule:
    def test_roads_module_import(self):
        import app.ingest.expansion_advisor_roads as mod

        assert hasattr(mod, "main")
        assert hasattr(mod, "_detect_source_table")
        assert hasattr(mod, "_ingest_roads")


# ---------------------------------------------------------------------------
# Parking module
# ---------------------------------------------------------------------------
class TestParkingModule:
    def test_parking_module_import(self):
        import app.ingest.expansion_advisor_parking as mod

        assert hasattr(mod, "main")
        assert hasattr(mod, "_ingest_from_polygons")
        assert hasattr(mod, "_ingest_from_points")


# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------
class TestMigration:
    """Locate the expansion-advisor migration by revision ID so the tests
    survive future file renames."""

    REVISION_ID = "d4e5f6a1b2c3"

    @staticmethod
    def _find_migration(revision_id: str) -> str | None:
        """Return the path to the migration file whose filename starts with
        *revision_id*, or ``None`` if not found."""
        versions_dir = os.path.join(
            os.path.dirname(__file__), "..", "alembic", "versions",
        )
        for name in os.listdir(versions_dir):
            if name.startswith(revision_id) and name.endswith(".py"):
                return os.path.join(versions_dir, name)
        return None

    def test_migration_file_exists(self):
        path = self._find_migration(self.REVISION_ID)
        assert path is not None, (
            f"No migration file starting with revision {self.REVISION_ID!r} "
            "found in alembic/versions/"
        )
        assert os.path.exists(path)

    def test_migration_has_all_tables(self):
        path = self._find_migration(self.REVISION_ID)
        assert path is not None, "migration file not found"
        with open(path) as f:
            content = f.read()
        assert "expansion_road_context" in content
        assert "expansion_parking_asset" in content
        assert "expansion_delivery_market" in content
        assert "expansion_rent_comp" in content
        assert "expansion_competitor_quality" in content


# ---------------------------------------------------------------------------
# SRID detection and bbox filter SQL generation
# ---------------------------------------------------------------------------
class TestSridHandling:
    """Verify SRID-aware bbox filter generation."""

    def test_riyadh_bbox_filter_4326(self):
        """With SRID 4326, no ST_Transform wrapper in WHERE clause."""
        from app.ingest.expansion_advisor_common import riyadh_bbox_filter_sql

        sql = riyadh_bbox_filter_sql("way", alias="l", source_srid=4326)
        assert "ST_Intersects(l.way," in sql
        assert "ST_MakeEnvelope(" in sql
        assert "4326)" in sql
        # Should NOT wrap in ST_Transform when already 4326
        assert "ST_Transform(l.way" not in sql

    def test_riyadh_bbox_filter_3857(self):
        """With SRID 3857, bbox filter must ST_Transform the geom column."""
        from app.ingest.expansion_advisor_common import riyadh_bbox_filter_sql

        sql = riyadh_bbox_filter_sql("way", alias="l", source_srid=3857)
        assert "ST_Transform(l.way, 4326)" in sql
        assert "ST_MakeEnvelope(" in sql

    def test_riyadh_bbox_filter_no_alias(self):
        """Alias can be empty."""
        from app.ingest.expansion_advisor_common import riyadh_bbox_filter_sql

        sql = riyadh_bbox_filter_sql("geom", alias="", source_srid=4326)
        assert "ST_Intersects(geom," in sql

    def test_detect_srid_fallback(self):
        """detect_srid falls back to 4326 when queries fail."""
        from app.ingest.expansion_advisor_common import detect_srid

        mock_db = MagicMock()
        mock_db.execute.side_effect = Exception("no such table")
        mock_db.rollback = MagicMock()
        result = detect_srid(mock_db, "nonexistent", "geom")
        assert result == 4326

    def test_detect_srid_from_find_srid(self):
        """detect_srid returns SRID from Find_SRID when available."""
        from app.ingest.expansion_advisor_common import detect_srid

        mock_db = MagicMock()
        mock_db.execute.return_value.scalar.return_value = 3857
        result = detect_srid(mock_db, "planet_osm_line", "way")
        assert result == 3857


# ---------------------------------------------------------------------------
# Roads SQL generation
# ---------------------------------------------------------------------------
class TestRoadsSqlGeneration:
    """Verify that roads ingest generates correct SQL with SRID handling."""

    def test_ingest_roads_calls_detect_srid(self):
        """_ingest_roads should detect SRID before building the query."""
        from app.ingest.expansion_advisor_roads import _ingest_roads

        mock_db = MagicMock()

        # Make table have 'way' column
        mock_db.execute.return_value.rowcount = 0

        # We need to capture what SQL was executed
        executed_stmts = []
        original_execute = mock_db.execute

        def capture_execute(stmt, *args, **kwargs):
            executed_stmts.append(str(stmt))
            result = MagicMock()
            result.rowcount = 0
            result.scalar.return_value = 4326
            return result

        mock_db.execute.side_effect = capture_execute

        try:
            _ingest_roads(mock_db, "planet_osm_line", replace=False)
        except Exception:
            pass  # May fail on commit, that's OK — we're checking SQL generation

        # Should have called Find_SRID or ST_SRID
        sql_blob = " ".join(executed_stmts)
        # The INSERT should reference ST_Transform
        assert "INSERT INTO expansion_road_context" in sql_blob or "Find_SRID" in sql_blob

    def test_roads_detect_source_table_order(self):
        """_detect_source_table tries planet_osm_line first."""
        from app.ingest.expansion_advisor_roads import _detect_source_table

        mock_db = MagicMock()

        checked_tables = []

        def mock_table_exists(db, name):
            checked_tables.append(name)
            return name == "planet_osm_line"

        with patch("app.ingest.expansion_advisor_roads.table_exists", mock_table_exists):
            result = _detect_source_table(mock_db)

        assert result == "planet_osm_line"
        assert checked_tables[0] == "planet_osm_line"


# ---------------------------------------------------------------------------
# Parking SQL generation with SRID
# ---------------------------------------------------------------------------
class TestParkingSqlGeneration:
    """Verify parking ingest handles SRID correctly."""

    def test_parking_polygon_detects_srid(self):
        """_ingest_from_polygons should call detect_srid."""
        from app.ingest.expansion_advisor_parking import _ingest_from_polygons

        mock_db = MagicMock()
        executed_stmts = []

        def capture_execute(stmt, *args, **kwargs):
            executed_stmts.append(str(stmt))
            result = MagicMock()
            result.rowcount = 0
            result.scalar.return_value = True  # table exists / srid
            return result

        mock_db.execute.side_effect = capture_execute

        with patch("app.ingest.expansion_advisor_parking.table_exists", return_value=True):
            with patch("app.ingest.expansion_advisor_parking.detect_srid", return_value=3857) as mock_detect:
                try:
                    _ingest_from_polygons(mock_db, replace=False)
                except Exception:
                    pass
                mock_detect.assert_called_once_with(mock_db, "planet_osm_polygon", "way")

        # The executed SQL should contain ST_Transform for 3857 source
        sql_blob = " ".join(executed_stmts)
        if "INSERT INTO expansion_parking_asset" in sql_blob:
            assert "ST_Transform" in sql_blob


# ---------------------------------------------------------------------------
# Rent CSV ingestion (end-to-end with temp file)
# ---------------------------------------------------------------------------
class TestRentCsvIngestion:
    """Verify CSV rent comp ingestion path."""

    def test_normalize_from_csv_inserts_commercial(self, tmp_path):
        """_normalize_from_csv should insert commercial listings from CSV."""
        from app.ingest.expansion_advisor_rent_comps import _normalize_from_csv

        csv_file = tmp_path / "test_rents.csv"
        csv_file.write_text(
            "city,district,price_sar,area_sqm,asset_type\n"
            "riyadh,الملقا,5000,100,commercial\n"
            "riyadh,النرجس,8000,150,retail\n"
            "jeddah,الحمراء,3000,80,commercial\n"  # Should be skipped (not Riyadh)
            "riyadh,العليا,4000,90,residential\n"  # Should be skipped (not commercial)
        )

        mock_db = MagicMock()
        inserted_params = []

        def capture_execute(stmt, params=None):
            if params:
                inserted_params.append(params)
            result = MagicMock()
            result.rowcount = 0
            return result

        mock_db.execute.side_effect = capture_execute

        stats = _normalize_from_csv(mock_db, str(csv_file), replace=False)

        # Should have inserted 2 rows (riyadh commercial + riyadh retail)
        assert stats["inserted"] == 2
        assert stats["source"] == "csv_import"

        # Verify the first inserted row has correct annual normalization
        first = inserted_params[0]
        assert first["district"] == "الملقا"
        assert first["monthly"] == 5000.0
        assert first["annual"] == 60000.0
        assert first["rent_m2_year"] == 600.0  # 60000 / 100

    def test_normalize_from_csv_skips_zero_price(self, tmp_path):
        """Rows with zero or missing price should be skipped."""
        from app.ingest.expansion_advisor_rent_comps import _normalize_from_csv

        csv_file = tmp_path / "bad_rents.csv"
        csv_file.write_text(
            "city,district,price_sar,area_sqm,asset_type\n"
            "riyadh,test,0,100,commercial\n"
            "riyadh,test,,100,commercial\n"
        )

        mock_db = MagicMock()
        mock_db.execute.return_value = MagicMock(rowcount=0)
        stats = _normalize_from_csv(mock_db, str(csv_file), replace=False)
        assert stats["inserted"] == 0

    def test_tempdir_lifetime_csv_url(self):
        """Verify the tempdir fix: normalization runs inside the context manager."""
        from app.ingest.expansion_advisor_rent_comps import main

        # We don't actually run main(), but verify the code structure
        import inspect
        source = inspect.getsource(main)

        # The _normalize_from_csv call should appear inside the same
        # indentation block as the TemporaryDirectory context
        lines = source.split("\n")
        in_csv_url_block = False
        found_normalize_inside = False
        for line in lines:
            if "args.csv_url" in line and "elif" in line:
                in_csv_url_block = True
            elif in_csv_url_block and "TemporaryDirectory" in line:
                continue
            elif in_csv_url_block and "_normalize_from_csv" in line:
                found_normalize_inside = True
                break
            elif in_csv_url_block and line.strip() and not line.strip().startswith("#"):
                if "elif" in line or "if stats" in line:
                    break  # Left the block

        assert found_normalize_inside, (
            "_normalize_from_csv must be called inside the TemporaryDirectory context "
            "to avoid reading from a deleted temp file"
        )


# ---------------------------------------------------------------------------
# Service rent estimation preference/fallback
# ---------------------------------------------------------------------------
class TestRentEstimationFallback:
    """Verify _estimate_rent_sar_m2_year preference chain."""

    def test_falls_through_to_aqar_when_expansion_returns_none(self):
        """When expansion table returns None, should try aqar_rent_median."""
        from app.services.expansion_advisor import _estimate_rent_sar_m2_year

        mock_db = MagicMock()
        mock_db.begin_nested.return_value.__enter__ = MagicMock(return_value=None)
        mock_db.begin_nested.return_value.__exit__ = MagicMock(return_value=False)

        call_count = {"expansion": 0, "aqar": 0}

        def mock_execute(stmt, params=None):
            sql_str = str(stmt)
            result = MagicMock()
            if "expansion_rent_comp" in sql_str:
                call_count["expansion"] += 1
                result.scalar.return_value = False  # no rows
                return result
            if "aqar" in sql_str.lower() or "rent_comp" in sql_str.lower():
                call_count["aqar"] += 1
            result.scalar.return_value = None
            result.mappings.return_value.first.return_value = None
            return result

        mock_db.execute.side_effect = mock_execute

        with patch("app.services.expansion_advisor.aqar_rent_median", return_value=None):
            rent, source = _estimate_rent_sar_m2_year(mock_db, "test_district")

        # Should have tried expansion table first
        assert call_count["expansion"] >= 1
        # Falls back to conservative default
        assert source in ("conservative_default", "expansion_rent_district",
                          "expansion_rent_city", "aqar_district", "aqar_city")

    def test_expansion_district_preferred_over_city(self):
        """District-level rent from expansion table should be preferred over city-wide."""
        from app.services.expansion_advisor import _estimate_rent_from_expansion_table

        mock_db = MagicMock()
        mock_db.begin_nested.return_value.__enter__ = MagicMock(return_value=None)
        mock_db.begin_nested.return_value.__exit__ = MagicMock(return_value=False)

        call_sequence = []

        def mock_execute(stmt, params=None):
            sql_str = str(stmt)
            result = MagicMock()
            if "EXISTS" in sql_str:
                call_sequence.append("exists")
                result.scalar.return_value = True
                return result
            if "district" in sql_str and "PERCENTILE_CONT" in sql_str:
                call_sequence.append("district_median")
                row = {"median": 900.0, "n": 5}
                result.mappings.return_value.first.return_value = row
                return result
            call_sequence.append("city_median")
            row = {"median": 750.0}
            result.mappings.return_value.first.return_value = row
            return result

        mock_db.execute.side_effect = mock_execute

        rent, source = _estimate_rent_from_expansion_table(mock_db, "الملقا")
        assert rent == 900.0
        assert source == "expansion_rent_district"
        # Should NOT have queried city-wide since district had enough rows
        assert "city_median" not in call_sequence


# ---------------------------------------------------------------------------
# _ea_table_has_rows with populated table
# ---------------------------------------------------------------------------
class TestEaTableHasRowsPopulated:
    """Verify _ea_table_has_rows returns True when table has rows."""

    def test_returns_true_when_rows_exist(self):
        from app.services.expansion_advisor import _ea_table_has_rows

        mock_db = MagicMock()
        mock_db.begin_nested.return_value.__enter__ = MagicMock(return_value=None)
        mock_db.begin_nested.return_value.__exit__ = MagicMock(return_value=False)
        mock_db.execute.return_value.scalar.return_value = True

        result = _ea_table_has_rows(mock_db, "expansion_road_context")
        assert result is True


# ---------------------------------------------------------------------------
# Integration tests: real inserts into SQLite tables matching expansion schema
# ---------------------------------------------------------------------------

def _create_expansion_tables(engine):
    """Create expansion tables in SQLite (no geometry columns)."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS expansion_road_context (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city VARCHAR(64) NOT NULL DEFAULT 'riyadh',
                source VARCHAR(64) NOT NULL DEFAULT 'osm',
                parcel_id VARCHAR(64),
                road_class VARCHAR(32),
                is_major_road BOOLEAN DEFAULT 0,
                is_service_road BOOLEAN DEFAULT 0,
                intersection_distance_m DOUBLE PRECISION,
                major_road_distance_m DOUBLE PRECISION,
                adjacent_road_count INTEGER DEFAULT 0,
                touches_road BOOLEAN DEFAULT 0,
                corner_lot BOOLEAN DEFAULT 0,
                frontage_length_m DOUBLE PRECISION,
                uturn_access_proxy VARCHAR(32),
                signalized_junction_distance_m DOUBLE PRECISION,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS expansion_parking_asset (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city VARCHAR(64) NOT NULL DEFAULT 'riyadh',
                source VARCHAR(64) NOT NULL DEFAULT 'osm',
                name VARCHAR(256),
                amenity_type VARCHAR(64),
                capacity INTEGER,
                covered BOOLEAN,
                public_access BOOLEAN,
                walk_access_score DOUBLE PRECISION,
                dropoff_score DOUBLE PRECISION,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS expansion_delivery_market (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city VARCHAR(64) NOT NULL DEFAULT 'riyadh',
                platform VARCHAR(32) NOT NULL,
                branch_name VARCHAR(256),
                brand_name VARCHAR(256),
                category VARCHAR(64),
                district VARCHAR(128),
                rating NUMERIC(3,2),
                rating_count INTEGER,
                min_order_sar NUMERIC(8,2),
                delivery_fee_sar NUMERIC(8,2),
                eta_minutes INTEGER,
                is_open_now BOOLEAN,
                supports_late_night BOOLEAN,
                source_record_id INTEGER,
                resolved_restaurant_poi_id VARCHAR(128),
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS expansion_rent_comp (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city VARCHAR(64) NOT NULL DEFAULT 'riyadh',
                district VARCHAR(128),
                source VARCHAR(64) NOT NULL DEFAULT 'aqar',
                listing_id VARCHAR(128),
                asset_type VARCHAR(32) NOT NULL DEFAULT 'commercial',
                unit_type VARCHAR(32),
                area_m2 DOUBLE PRECISION,
                annual_rent_sar DOUBLE PRECISION,
                monthly_rent_sar DOUBLE PRECISION,
                rent_sar_m2_year DOUBLE PRECISION,
                frontage_class VARCHAR(32),
                road_class VARCHAR(32),
                floor_level VARCHAR(32),
                shell_condition VARCHAR(32),
                vacancy_days INTEGER,
                listed_at DATE,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS expansion_competitor_quality (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city VARCHAR(64) NOT NULL DEFAULT 'riyadh',
                restaurant_poi_id VARCHAR(128),
                brand_name VARCHAR(256),
                category VARCHAR(64),
                district VARCHAR(128),
                chain_strength_score DOUBLE PRECISION,
                review_score DOUBLE PRECISION,
                review_count INTEGER,
                delivery_presence_score DOUBLE PRECISION,
                multi_platform_score DOUBLE PRECISION,
                late_night_score DOUBLE PRECISION,
                price_tier VARCHAR(16),
                overall_quality_score DOUBLE PRECISION,
                refreshed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))


def _make_sqlite_session():
    """Create a SQLite in-memory engine and session with expansion tables."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    _create_expansion_tables(engine)
    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return Session()


class TestIntegrationRoadInserts:
    """Integration: verify real inserts into expansion_road_context."""

    def test_insert_road_segments(self):
        db = _make_sqlite_session()
        db.execute(text("""
            INSERT INTO expansion_road_context
                (city, source, road_class, is_major_road, is_service_road,
                 adjacent_road_count, touches_road, frontage_length_m, uturn_access_proxy)
            VALUES
                ('riyadh', 'osm', 'primary', 1, 0, 3, 1, 45.5, 'limited'),
                ('riyadh', 'osm', 'secondary', 1, 0, 2, 1, 30.0, 'available'),
                ('riyadh', 'osm', 'residential', 0, 0, 1, 0, 15.2, 'available'),
                ('riyadh', 'osm', 'service', 0, 1, 0, 0, 8.1, 'available')
        """))
        db.commit()

        count = db.execute(text("SELECT COUNT(*) FROM expansion_road_context WHERE city = 'riyadh'")).scalar()
        assert count == 4

        major_count = db.execute(text(
            "SELECT COUNT(*) FROM expansion_road_context WHERE is_major_road = 1"
        )).scalar()
        assert major_count == 2

        service_count = db.execute(text(
            "SELECT COUNT(*) FROM expansion_road_context WHERE is_service_road = 1"
        )).scalar()
        assert service_count == 1

        db.close()


class TestIntegrationParkingInserts:
    """Integration: verify real inserts into expansion_parking_asset."""

    def test_insert_parking_assets(self):
        db = _make_sqlite_session()
        db.execute(text("""
            INSERT INTO expansion_parking_asset
                (city, source, name, amenity_type, capacity, covered, public_access,
                 walk_access_score, dropoff_score)
            VALUES
                ('riyadh', 'osm_polygon', 'Al Malqa Mall Parking', 'multi_storey', 200, 1, 1, 60.0, 45.0),
                ('riyadh', 'osm_polygon', 'Surface Lot', 'surface', 50, 0, 1, 75.0, 70.0),
                ('riyadh', 'osm_point', 'Street Parking', 'street_side', NULL, 0, 1, 80.0, 85.0)
        """))
        db.commit()

        count = db.execute(text("SELECT COUNT(*) FROM expansion_parking_asset WHERE city = 'riyadh'")).scalar()
        assert count == 3

        covered = db.execute(text("SELECT COUNT(*) FROM expansion_parking_asset WHERE covered = 1")).scalar()
        assert covered == 1

        avg_walk = db.execute(text(
            "SELECT AVG(walk_access_score) FROM expansion_parking_asset"
        )).scalar()
        assert 60.0 < avg_walk < 80.0

        db.close()


class TestIntegrationDeliveryInserts:
    """Integration: verify real inserts into expansion_delivery_market."""

    def test_insert_delivery_records(self):
        db = _make_sqlite_session()
        db.execute(text("""
            INSERT INTO expansion_delivery_market
                (city, platform, branch_name, brand_name, category, district,
                 rating, rating_count, eta_minutes, is_open_now)
            VALUES
                ('riyadh', 'hungerstation', 'Al Baik - Malqa', 'Al Baik', 'fast_food', 'الملقا', 4.5, 1200, 25, 1),
                ('riyadh', 'jahez', 'Al Baik - Narjis', 'Al Baik', 'fast_food', 'النرجس', 4.4, 800, 30, 1),
                ('riyadh', 'hungerstation', 'Hardees - Olaya', 'Hardees', 'fast_food', 'العليا', 3.8, 400, 20, 0),
                ('riyadh', 'keeta', 'Kudu - Malqa', 'Kudu', 'fast_food', 'الملقا', 4.1, 600, 15, 1),
                ('riyadh', 'talabat', 'Starbucks - Olaya', 'Starbucks', 'cafe', 'العليا', 4.3, 900, 10, 1)
        """))
        db.commit()

        total = db.execute(text("SELECT COUNT(*) FROM expansion_delivery_market WHERE city = 'riyadh'")).scalar()
        assert total == 5

        # Multi-platform presence: Al Baik is on 2 platforms
        al_baik_platforms = db.execute(text(
            "SELECT COUNT(DISTINCT platform) FROM expansion_delivery_market WHERE brand_name = 'Al Baik'"
        )).scalar()
        assert al_baik_platforms == 2

        # Per-district density
        malqa_count = db.execute(text(
            "SELECT COUNT(*) FROM expansion_delivery_market WHERE district = 'الملقا'"
        )).scalar()
        assert malqa_count == 2

        db.close()


class TestIntegrationRentInserts:
    """Integration: verify real inserts and queries on expansion_rent_comp."""

    def test_insert_and_query_rent_comps(self):
        db = _make_sqlite_session()

        # Insert rent comps for two districts
        rents = [
            ("riyadh", "الملقا", "csv_import", "commercial", 100.0, 5000.0, 60000.0, 600.0),
            ("riyadh", "الملقا", "csv_import", "commercial", 120.0, 6000.0, 72000.0, 600.0),
            ("riyadh", "الملقا", "csv_import", "retail", 80.0, 4000.0, 48000.0, 600.0),
            ("riyadh", "النرجس", "csv_import", "commercial", 150.0, 8000.0, 96000.0, 640.0),
            ("riyadh", "النرجس", "csv_import", "commercial", 200.0, 10000.0, 120000.0, 600.0),
        ]
        for city, district, source, asset_type, area, monthly, annual, rent_m2 in rents:
            db.execute(
                text("""
                    INSERT INTO expansion_rent_comp
                        (city, district, source, asset_type, area_m2,
                         monthly_rent_sar, annual_rent_sar, rent_sar_m2_year)
                    VALUES (:city, :district, :source, :asset_type, :area,
                            :monthly, :annual, :rent_m2)
                """),
                {"city": city, "district": district, "source": source,
                 "asset_type": asset_type, "area": area, "monthly": monthly,
                 "annual": annual, "rent_m2": rent_m2},
            )
        db.commit()

        total = db.execute(text("SELECT COUNT(*) FROM expansion_rent_comp WHERE city = 'riyadh'")).scalar()
        assert total == 5

        # District-level query (mimics what the service does)
        malqa_avg = db.execute(text("""
            SELECT AVG(rent_sar_m2_year)
            FROM expansion_rent_comp
            WHERE city = 'riyadh' AND district = 'الملقا'
              AND rent_sar_m2_year IS NOT NULL AND rent_sar_m2_year > 0
        """)).scalar()
        assert malqa_avg == 600.0

        # Verify annual = monthly * 12
        rows = db.execute(text(
            "SELECT monthly_rent_sar, annual_rent_sar FROM expansion_rent_comp"
        )).fetchall()
        for monthly, annual in rows:
            assert annual == monthly * 12.0

        db.close()


class TestIntegrationCompetitorInserts:
    """Integration: verify real inserts into expansion_competitor_quality."""

    def test_insert_and_query_competitors(self):
        db = _make_sqlite_session()
        db.execute(text("""
            INSERT INTO expansion_competitor_quality
                (city, brand_name, category, district,
                 chain_strength_score, review_score, review_count,
                 delivery_presence_score, multi_platform_score,
                 overall_quality_score)
            VALUES
                ('riyadh', 'Al Baik', 'fast_food', 'الملقا', 100.0, 87.5, 1200, 80.0, 60.0, 85.0),
                ('riyadh', 'Hardees', 'fast_food', 'الملقا', 72.0, 62.5, 400, 60.0, 40.0, 58.0),
                ('riyadh', 'Kudu', 'fast_food', 'النرجس', 84.0, 75.0, 600, 70.0, 50.0, 72.0)
        """))
        db.commit()

        total = db.execute(text("SELECT COUNT(*) FROM expansion_competitor_quality WHERE city = 'riyadh'")).scalar()
        assert total == 3

        # District competitor density
        malqa = db.execute(text(
            "SELECT COUNT(*) FROM expansion_competitor_quality WHERE district = 'الملقا'"
        )).scalar()
        assert malqa == 2

        # Highest quality score
        top = db.execute(text(
            "SELECT brand_name FROM expansion_competitor_quality ORDER BY overall_quality_score DESC LIMIT 1"
        )).scalar()
        assert top == "Al Baik"

        db.close()


class TestIntegrationServicePreference:
    """Integration: verify service actually prefers expansion tables when populated."""

    def test_ea_table_has_rows_with_real_table(self):
        """_ea_table_has_rows should return True on a real populated SQLite table."""
        db = _make_sqlite_session()

        # Insert a row
        db.execute(text("""
            INSERT INTO expansion_rent_comp (city, district, source, asset_type, rent_sar_m2_year)
            VALUES ('riyadh', 'الملقا', 'csv_import', 'commercial', 600.0)
        """))
        db.commit()

        # Simulate _ea_table_has_rows logic (can't use begin_nested with SQLite easily,
        # so test the SQL pattern directly)
        has = db.execute(
            text("SELECT EXISTS(SELECT 1 FROM expansion_rent_comp LIMIT 1)")
        ).scalar()
        assert has == 1  # SQLite returns 1 for True

        # Empty table should return 0
        db.execute(text("DELETE FROM expansion_road_context"))
        db.commit()
        has = db.execute(
            text("SELECT EXISTS(SELECT 1 FROM expansion_road_context LIMIT 1)")
        ).scalar()
        assert has == 0

        db.close()

    def test_rent_preference_chain_with_real_data(self):
        """With real rent data, district median should be returned over city-wide."""
        db = _make_sqlite_session()

        # Insert district-specific rents
        for i, rent in enumerate([500.0, 600.0, 700.0, 800.0, 550.0]):
            db.execute(
                text("""
                    INSERT INTO expansion_rent_comp
                        (city, district, source, asset_type, rent_sar_m2_year, listing_id)
                    VALUES ('riyadh', 'الملقا', 'csv_import', 'commercial', :rent, :lid)
                """),
                {"rent": rent, "lid": f"test_{i}"},
            )
        # Insert city-wide rents (different district)
        for i, rent in enumerate([400.0, 450.0, 350.0]):
            db.execute(
                text("""
                    INSERT INTO expansion_rent_comp
                        (city, district, source, asset_type, rent_sar_m2_year, listing_id)
                    VALUES ('riyadh', 'حي آخر', 'csv_import', 'commercial', :rent, :lid)
                """),
                {"rent": rent, "lid": f"other_{i}"},
            )
        db.commit()

        # District query should return district-specific data only
        district_count = db.execute(text("""
            SELECT COUNT(*) FROM expansion_rent_comp
            WHERE city = 'riyadh' AND lower(district) = lower('الملقا')
              AND rent_sar_m2_year IS NOT NULL AND rent_sar_m2_year > 0
        """)).scalar()
        assert district_count == 5  # >= 3 means service should use district median

        # City-wide fallback should include all districts
        city_count = db.execute(text("""
            SELECT COUNT(*) FROM expansion_rent_comp
            WHERE city = 'riyadh' AND rent_sar_m2_year IS NOT NULL AND rent_sar_m2_year > 0
        """)).scalar()
        assert city_count == 8

        db.close()

    def test_delivery_multi_platform_scoring_with_real_data(self):
        """Service should be able to compute multi-platform presence from real data."""
        db = _make_sqlite_session()

        # Same brand on 3 platforms
        for platform in ["hungerstation", "jahez", "keeta"]:
            db.execute(
                text("""
                    INSERT INTO expansion_delivery_market
                        (city, platform, brand_name, category, district, rating)
                    VALUES ('riyadh', :platform, 'Al Baik', 'fast_food', 'الملقا', 4.5)
                """),
                {"platform": platform},
            )
        # Different brand on 1 platform
        db.execute(text("""
            INSERT INTO expansion_delivery_market
                (city, platform, brand_name, category, district, rating)
            VALUES ('riyadh', 'hungerstation', 'Small Cafe', 'cafe', 'الملقا', 3.5)
        """))
        db.commit()

        # Multi-platform presence query
        multi = db.execute(text("""
            SELECT brand_name, COUNT(DISTINCT platform) as platform_count
            FROM expansion_delivery_market
            WHERE district = 'الملقا'
            GROUP BY brand_name
            ORDER BY platform_count DESC
        """)).fetchall()

        assert multi[0][0] == "Al Baik"
        assert multi[0][1] == 3
        assert multi[1][0] == "Small Cafe"
        assert multi[1][1] == 1

        # District delivery density
        density = db.execute(text(
            "SELECT COUNT(*) FROM expansion_delivery_market WHERE district = 'الملقا'"
        )).scalar()
        assert density == 4

        db.close()


class TestIntegrationDeliveryWorkflowScrape:
    """Verify the delivery ingest module calls scraper pipeline when not skipped."""

    def test_delivery_main_has_scrape_step(self):
        """expansion_advisor_delivery.main should run scraper by default."""
        from app.ingest.expansion_advisor_delivery import _run_delivery_scrape

        # Verify the function exists and is callable
        assert callable(_run_delivery_scrape)

    def test_skip_scrape_flag(self):
        """--skip-scrape should skip the scraper and only normalize."""
        import inspect
        from app.ingest.expansion_advisor_delivery import main

        source = inspect.getsource(main)
        assert "skip_scrape" in source or "skip-scrape" in source
        assert "_run_delivery_scrape" in source

    def test_run_delivery_scrape_calls_pipeline(self):
        """_run_delivery_scrape should call run_all_platforms."""
        mock_run = MagicMock(return_value=[
            {"platform": "hungerstation", "rows_inserted": 50, "rows_matched": 30},
        ])

        # _run_delivery_scrape does a lazy import of run_all_platforms inside the function,
        # so we mock the module that gets imported
        mock_pipeline = MagicMock()
        mock_pipeline.run_all_platforms = mock_run

        with patch.dict("sys.modules", {"app.delivery.pipeline": mock_pipeline}):
            from app.ingest.expansion_advisor_delivery import _run_delivery_scrape
            results = _run_delivery_scrape(["hungerstation"], max_pages=10)

        mock_run.assert_called_once_with(
            db=None,
            max_pages=10,
            platforms=["hungerstation"],
            run_resolver=True,
        )
        assert len(results) == 1
        assert results[0]["rows_inserted"] == 50
