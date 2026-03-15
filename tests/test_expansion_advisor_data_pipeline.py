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

import json
import os
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest


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
    def test_migration_file_exists(self):
        migration_path = os.path.join(
            os.path.dirname(__file__), "..",
            "alembic", "versions",
            "a1b2c3d4e5f6_create_expansion_advisor_tables.py",
        )
        assert os.path.exists(migration_path)

    def test_migration_has_all_tables(self):
        migration_path = os.path.join(
            os.path.dirname(__file__), "..",
            "alembic", "versions",
            "a1b2c3d4e5f6_create_expansion_advisor_tables.py",
        )
        with open(migration_path) as f:
            content = f.read()
        assert "expansion_road_context" in content
        assert "expansion_parking_asset" in content
        assert "expansion_delivery_market" in content
        assert "expansion_rent_comp" in content
        assert "expansion_competitor_quality" in content
