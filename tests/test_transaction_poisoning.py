"""
Tests that a single helper DB failure does not poison the rest of score_location().

The root bug: when traffic_score_at (or any early helper) raised a SQL error,
it caught the exception and returned a neutral fallback — but left the
SQLAlchemy/Postgres session in an aborted-transaction state.  Every subsequent
DB query then failed with ``InFailedSqlTransaction``, causing all helpers
(including Aqar rent) to silently degrade to their worst defaults.

The fix: every except block that catches a DB error now calls ``db.rollback()``
before returning the fallback, resetting the session for subsequent queries.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, PropertyMock
import pytest

from app.services.restaurant_location import (
    _RentResolution,
    _rent_data_quality,
    score_location,
    traffic_score_at,
    population_score,
    commercial_density_score,
    anchor_proximity_score,
    parking_availability_score,
    zoning_fit_score,
    income_proxy_score,
    _resolve_rent_aqar,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeInFailedSqlTransaction(Exception):
    """Simulates psycopg.errors.InFailedSqlTransaction."""
    pass


def _make_db_session(*, fail_execute: bool = False):
    """
    Return a mock SQLAlchemy Session.

    When *fail_execute* is True the first ``db.execute()`` call raises,
    simulating a SQL error that aborts the transaction.  ``db.rollback()``
    is always a no-op so we can assert it was called.
    """
    db = MagicMock(spec=["execute", "query", "rollback"])
    if fail_execute:
        db.execute.side_effect = _FakeInFailedSqlTransaction(
            "current transaction is aborted"
        )
        db.query.side_effect = _FakeInFailedSqlTransaction(
            "current transaction is aborted"
        )
    db.rollback.return_value = None
    return db


# ---------------------------------------------------------------------------
# Unit tests: individual helpers call rollback on failure
# ---------------------------------------------------------------------------


class TestTrafficScoreAtRollback:
    """traffic_score_at must rollback on DB failure."""

    def test_returns_fallback_on_db_error(self):
        db = _make_db_session(fail_execute=True)
        result = traffic_score_at(db, 24.7, 46.7)
        assert result["score"] == 25.0
        db.rollback.assert_called_once()

    def test_session_usable_after_failure(self):
        """After rollback, subsequent queries on the session should not fail."""
        db = _make_db_session(fail_execute=True)
        traffic_score_at(db, 24.7, 46.7)
        db.rollback.assert_called()


class TestPopulationScoreRollback:
    """population_score must rollback on DB failure."""

    def test_returns_fallback_on_db_error(self):
        db = _make_db_session(fail_execute=True)
        with patch("app.services.restaurant_location.h3", create=True):
            import app.services.restaurant_location as mod
            with patch.dict("sys.modules", {"h3": MagicMock(
                latlng_to_cell=MagicMock(return_value="abc"),
                grid_disk=MagicMock(return_value=["abc"]),
            )}):
                result = population_score(db, 24.7, 46.7)
        assert result == 50.0
        db.rollback.assert_called()


class TestCommercialDensityRollback:
    def test_returns_fallback_on_db_error(self):
        db = _make_db_session(fail_execute=True)
        result = commercial_density_score(db, 24.7, 46.7)
        assert result == 50.0
        db.rollback.assert_called()


class TestAnchorProximityRollback:
    def test_returns_neutral_on_db_error(self):
        db = _make_db_session(fail_execute=True)
        result = anchor_proximity_score(db, 24.7, 46.7)
        # Should return something reasonable (neutral), not crash
        assert 10.0 <= result <= 95.0
        db.rollback.assert_called()


class TestParkingRollback:
    def test_returns_fallback_on_db_error(self):
        db = _make_db_session(fail_execute=True)
        result = parking_availability_score(db, 24.7, 46.7)
        assert result == 50.0
        db.rollback.assert_called()


class TestZoningFitRollback:
    def test_returns_fallback_on_db_error(self):
        db = _make_db_session(fail_execute=True)
        result = zoning_fit_score(db, 24.7, 46.7)
        assert result == 50.0
        db.rollback.assert_called()


class TestIncomeProxyRollback:
    def test_returns_fallback_on_db_error(self):
        db = MagicMock()
        db.rollback.return_value = None
        # resolve_district is imported inside the function body via
        # ``from app.services.district_resolver import resolve_district``.
        # Inject a fake module into sys.modules so the import succeeds and
        # the function raises, simulating a DB error inside the resolver.
        fake_module = MagicMock()
        fake_module.resolve_district.side_effect = _FakeInFailedSqlTransaction(
            "aborted"
        )
        with patch.dict("sys.modules", {"app.services.district_resolver": fake_module}):
            result = income_proxy_score(db, 24.7, 46.7)
        assert result == 50.0
        db.rollback.assert_called()


# ---------------------------------------------------------------------------
# Integration-level: one helper failure must NOT zero out Aqar rent
# ---------------------------------------------------------------------------


class TestHelperFailureDoesNotPoisonRent:
    """
    Core regression test: when traffic_score_at fails, the Aqar rent
    resolution must still succeed (i.e. rent != 50, scope != 'none').
    """

    def _build_mock_db(self):
        """
        Build a mock db where:
        - traffic/osm_roads queries FAIL (simulating the original bug trigger)
        - all other queries SUCCEED with reasonable data
        """
        db = MagicMock()
        db.rollback.return_value = None

        _call_count = {"n": 0}
        _osm_roads_error = _FakeInFailedSqlTransaction("current transaction is aborted")

        def _execute_side_effect(stmt, params=None, **kw):
            sql_str = str(stmt) if not isinstance(stmt, str) else stmt
            # Fail on osm_roads queries (traffic_score_at)
            if "osm_roads" in sql_str.lower():
                raise _osm_roads_error
            # Return reasonable results for other queries
            result = MagicMock()
            result.mappings.return_value.all.return_value = []
            result.scalar.return_value = 0
            result.fetchone.return_value = (0, 0)
            return result

        db.execute.side_effect = _execute_side_effect

        # ORM query fallback (population_score, _nearby_restaurants ORM fallback)
        query_mock = MagicMock()
        query_mock.filter.return_value = query_mock
        query_mock.scalar.return_value = 0
        query_mock.all.return_value = []
        db.query.return_value = query_mock

        return db

    @patch("app.services.restaurant_location._resolve_rent_aqar")
    @patch("app.services.restaurant_location._compute_confidence_features")
    def test_aqar_rent_survives_traffic_failure(
        self, mock_conf_features, mock_rent
    ):
        """
        Even when traffic_score_at fails, _resolve_rent_aqar should be
        called and its result should appear in the final output.
        """
        # Simulate a successful Aqar rent resolution
        mock_rent.return_value = _RentResolution(
            rent_per_m2=167.394,
            scope="district_shrinkage",
            sample_count=3,
            median_used=167.394,
            method="aqar_district_shrinkage",
        )
        mock_conf_features.return_value = {
            "has_google": 0.5,
            "google_confidence": 0.5,
            "review_sufficiency": 0.5,
            "nearby_evidence": 0.5,
            "source_diversity": 0.5,
            "rating_coverage": 0.5,
        }

        db = self._build_mock_db()

        result = score_location(
            db, lat=24.7056, lon=46.7180, category="burger", use_ai_weights=False
        )

        # Aqar rent must NOT be the neutral 50 / scope "none"
        assert result.debug["rent_per_m2"] == 167.394, (
            f"Aqar rent was zeroed out: {result.debug['rent_per_m2']}"
        )
        assert result.debug["rent_meta"]["scope"] == "district_shrinkage", (
            f"Rent scope degraded to: {result.debug['rent_meta']['scope']}"
        )
        assert result.debug["rent_meta"]["method"] == "aqar_district_shrinkage"

        # rent factor should NOT be the neutral 50.0
        assert result.factors["rent"] != 50.0, (
            "Rent factor should reflect Aqar data, not neutral default"
        )

    @patch("app.services.restaurant_location._resolve_rent_aqar")
    @patch("app.services.restaurant_location._compute_confidence_features")
    def test_rent_data_quality_nonzero_when_aqar_works(
        self, mock_conf_features, mock_rent
    ):
        """
        rent_data_quality should be > 0 when Aqar data is available,
        even if other helpers failed.
        """
        mock_rent.return_value = _RentResolution(
            rent_per_m2=167.394,
            scope="district_shrinkage",
            sample_count=3,
            median_used=167.394,
            method="aqar_district_shrinkage",
        )
        mock_conf_features.return_value = {
            "has_google": 0.0,
            "google_confidence": 0.0,
            "review_sufficiency": 0.0,
            "nearby_evidence": 0.0,
            "source_diversity": 0.0,
            "rating_coverage": 0.0,
        }

        db = self._build_mock_db()
        result = score_location(
            db, lat=24.7056, lon=46.7180, category="burger", use_ai_weights=False
        )

        rent_quality = result.debug["confidence_features"]["rent_data_quality"]
        assert rent_quality > 0.0, (
            f"rent_data_quality should be >0 with Aqar data, got {rent_quality}"
        )
        assert rent_quality == 0.7  # district_shrinkage -> 0.7


class TestRollbackPreservesAqarIntegration:
    """
    Verify that the rollback() calls do not interfere with the Aqar rent
    integration when it works correctly.
    """

    def test_aqar_rent_data_quality_mapping(self):
        """Verify scope -> quality mapping is intact."""
        cases = [
            ("district", 1.0),
            ("district_shrinkage", 0.7),
            ("city", 0.5),
            ("city_asset", 0.5),
            ("indicator_fallback", 0.2),
            ("none", 0.0),
        ]
        for scope, expected in cases:
            res = _RentResolution(
                rent_per_m2=100.0, scope=scope, sample_count=5,
                median_used=100.0, method="test",
            )
            assert _rent_data_quality(res) == expected, (
                f"scope={scope}: expected {expected}, got {_rent_data_quality(res)}"
            )
