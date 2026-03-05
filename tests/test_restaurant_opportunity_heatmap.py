"""Tests for the restaurant opportunity heatmap service and endpoint."""

import math
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

from app.services.restaurant_opportunity_heatmap import (
    _score_cell,
    _GridIndex,
    _population_factor,
    _foot_traffic_proxy,
    _chain_gap_simple,
    _CACHE_TTL,
)
from app.services.restaurant_location import DEMAND_WEIGHTS


# ---------------------------------------------------------------------------
# Unit tests: scoring helpers
# ---------------------------------------------------------------------------


class TestPopulationFactor:
    def test_zero_population(self):
        assert _population_factor(0) == 10.0

    def test_high_population(self):
        score = _population_factor(50000)
        assert score > 80.0

    def test_moderate_population(self):
        score = _population_factor(5000)
        assert 40.0 < score < 90.0


class TestFootTrafficProxy:
    def test_empty_area(self):
        assert _foot_traffic_proxy(0, 0, 0) <= 15.0

    def test_busy_area(self):
        score = _foot_traffic_proxy(40, 15, 10000)
        assert score > 60.0


class TestChainGapSimple:
    def test_no_competitors(self):
        assert _chain_gap_simple(0) == 90.0

    def test_few_competitors(self):
        assert _chain_gap_simple(2) == 80.0

    def test_many_competitors(self):
        assert _chain_gap_simple(20) == 20.0


# ---------------------------------------------------------------------------
# Grid index
# ---------------------------------------------------------------------------


class TestGridIndex:
    def test_finds_nearby(self):
        pois = [
            {"lat": 24.7, "lon": 46.7, "category": "burger", "source": "test",
             "rating": 4.0, "review_count": 10, "chain_name": None,
             "google_place_id": None, "google_confidence": None},
        ]
        idx = _GridIndex(pois)
        found = idx.neighbors(24.7, 46.7, 500)
        assert len(found) == 1

    def test_excludes_far(self):
        pois = [
            {"lat": 25.0, "lon": 47.0, "category": "burger", "source": "test",
             "rating": 4.0, "review_count": 10, "chain_name": None,
             "google_place_id": None, "google_confidence": None},
        ]
        idx = _GridIndex(pois)
        found = idx.neighbors(24.7, 46.7, 500)
        assert len(found) == 0


# ---------------------------------------------------------------------------
# Score cell
# ---------------------------------------------------------------------------


class TestScoreCell:
    def test_returns_required_fields(self):
        nearby = [
            {
                "category": "burger",
                "source": "hungerstation",
                "rating": 3.5,
                "review_count": 50,
                "chain_name": None,
                "google_place_id": "abc",
                "google_confidence": 0.9,
                "lat": 24.7,
                "lon": 46.7,
                "distance_m": 200,
            },
        ]
        result = _score_cell(24.7, 46.7, "burger", 1200, nearby, 5000.0, DEMAND_WEIGHTS)

        assert "opportunity_score" in result
        assert "confidence_score" in result
        assert "final_score" in result
        assert "demand_sum_reviews" in result
        assert "competitor_count" in result
        assert "population" in result
        assert "underserved_index" in result
        assert "debug_factors" in result

    def test_no_nearby_high_opportunity(self):
        result = _score_cell(24.7, 46.7, "burger", 1200, [], 5000.0, DEMAND_WEIGHTS)
        # No competitors → high competition score → decent opportunity
        assert result["opportunity_score"] > 50.0
        assert result["competitor_count"] == 0

    def test_final_score_bounded(self):
        nearby = [
            {
                "category": "burger",
                "source": "test",
                "rating": 4.0,
                "review_count": 100,
                "chain_name": None,
                "google_place_id": "x",
                "google_confidence": 0.8,
                "lat": 24.7,
                "lon": 46.7,
                "distance_m": 100,
            },
        ] * 5
        result = _score_cell(24.7, 46.7, "burger", 1200, nearby, 3000.0, DEMAND_WEIGHTS)
        assert 0.0 <= result["final_score"] <= 100.0

    def test_underserved_index_uses_log_population(self):
        nearby = [
            {
                "category": "burger",
                "source": "test",
                "rating": 4.0,
                "review_count": 200,
                "chain_name": None,
                "google_place_id": None,
                "google_confidence": None,
                "lat": 24.7,
                "lon": 46.7,
                "distance_m": 100,
            },
        ]
        result = _score_cell(24.7, 46.7, "burger", 1200, nearby, 10000.0, DEMAND_WEIGHTS)
        expected = (200 / 1) * math.log1p(10000.0)
        assert abs(result["underserved_index"] - round(expected, 2)) < 0.1


# ---------------------------------------------------------------------------
# Cache table read/write (mock DB)
# ---------------------------------------------------------------------------


class TestCacheReadWrite:
    def test_cache_miss_returns_none(self):
        from app.services.restaurant_opportunity_heatmap import _get_cached

        mock_db = MagicMock()
        mock_db.query.return_value.filter_by.return_value.first.return_value = None
        assert _get_cached(mock_db, "burger", 1200) is None

    def test_cache_hit_returns_payload(self):
        from app.services.restaurant_opportunity_heatmap import _get_cached

        mock_row = MagicMock()
        mock_row.payload = {"type": "FeatureCollection", "features": []}
        mock_row.computed_at = datetime.now(timezone.utc)

        mock_db = MagicMock()
        mock_db.query.return_value.filter_by.return_value.first.return_value = mock_row
        result = _get_cached(mock_db, "burger", 1200)
        assert result is not None
        assert result["type"] == "FeatureCollection"

    def test_cache_expired_returns_none(self):
        from app.services.restaurant_opportunity_heatmap import _get_cached

        mock_row = MagicMock()
        mock_row.payload = {"type": "FeatureCollection", "features": []}
        mock_row.computed_at = datetime.now(timezone.utc) - timedelta(days=8)

        mock_db = MagicMock()
        mock_db.query.return_value.filter_by.return_value.first.return_value = mock_row
        assert _get_cached(mock_db, "burger", 1200) is None

    def test_set_cache_inserts_new(self):
        from app.services.restaurant_opportunity_heatmap import _set_cache

        mock_db = MagicMock()
        mock_db.query.return_value.filter_by.return_value.first.return_value = None

        payload = {"type": "FeatureCollection", "features": []}
        _set_cache(mock_db, "burger", 1200, payload)

        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()

    def test_set_cache_updates_existing(self):
        from app.services.restaurant_opportunity_heatmap import _set_cache

        existing = MagicMock()
        mock_db = MagicMock()
        mock_db.query.return_value.filter_by.return_value.first.return_value = existing

        payload = {"type": "FeatureCollection", "features": [{"new": True}]}
        _set_cache(mock_db, "burger", 1200, payload)

        assert existing.payload == payload
        mock_db.add.assert_not_called()
        mock_db.commit.assert_called_once()


# ---------------------------------------------------------------------------
# Endpoint returns FeatureCollection (mock service)
# ---------------------------------------------------------------------------


class TestOpportunityHeatmapEndpoint:
    def test_endpoint_returns_feature_collection(self):
        """Verify the endpoint wiring returns proper GeoJSON."""
        from fastapi.testclient import TestClient
        from unittest.mock import patch as mock_patch

        fake_payload = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [46.7, 24.7]},
                    "properties": {
                        "h3": "882a10a1cbfffff",
                        "opportunity_score": 72.5,
                        "confidence_score": 45.0,
                        "final_score": 58.0,
                        "demand_sum_reviews": 120,
                        "competitor_count": 3,
                        "population": 4500.0,
                        "underserved_index": 320.5,
                        "debug_factors": {},
                    },
                }
            ],
            "metadata": {
                "category": "burger",
                "radius_m": 1200,
                "cell_count": 1,
            },
        }

        with mock_patch(
            "app.api.restaurant_location.generate_opportunity_heatmap",
            return_value=fake_payload,
        ):
            from app.main import app

            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/v1/restaurant/opportunity-heatmap?category=burger")

            if resp.status_code == 200:
                data = resp.json()
                assert data["type"] == "FeatureCollection"
                assert len(data["features"]) == 1
                props = data["features"][0]["properties"]
                assert "h3" in props
                assert "opportunity_score" in props
                assert "confidence_score" in props
                assert "final_score" in props
                assert "demand_sum_reviews" in props
                assert "competitor_count" in props
                assert "population" in props
