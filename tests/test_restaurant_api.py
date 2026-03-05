"""Tests for restaurant location API endpoints."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    from app.main import app

    return TestClient(app, raise_server_exceptions=False)


class TestCategoriesEndpoint:
    def test_returns_category_list(self, client):
        resp = client.get("/v1/restaurant/categories")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0
        assert "key" in data[0]
        assert "name_en" in data[0]
        assert "name_ar" in data[0]

    def test_has_burger_category(self, client):
        resp = client.get("/v1/restaurant/categories")
        keys = [c["key"] for c in resp.json()]
        assert "burger" in keys
        assert "traditional" in keys


class TestScoreEndpoint:
    def test_score_requires_body(self, client):
        resp = client.post("/v1/restaurant/score")
        assert resp.status_code == 422  # validation error

    def test_score_validates_lat_range(self, client):
        resp = client.post(
            "/v1/restaurant/score",
            json={"lat": 0, "lon": 46.7, "category": "burger"},
        )
        assert resp.status_code == 422

    @patch("app.api.restaurant_location.score_location")
    def test_score_returns_result(self, mock_score, client):
        from app.services.restaurant_location import LocationScoreResult

        mock_score.return_value = LocationScoreResult(
            opportunity_score=72.5,
            demand_score=75.0,
            cost_penalty=62.5,
            factors={"competition": 80, "traffic": 65},
            contributions=[
                {"factor": "competition", "score": 80, "weight": 0.25, "weighted_contribution": 20.0},
            ],
            confidence=0.8,
            confidence_score=65.0,
            final_score=61.6,
            contributions_confidence=[
                {"factor": "has_google", "score": 0.8, "weight": 0.35, "weighted_contribution": 0.28},
            ],
            nearby_competitors=[],
        )
        resp = client.post(
            "/v1/restaurant/score",
            json={"lat": 24.7, "lon": 46.7, "category": "burger"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["opportunity_score"] == 72.5
        assert data["demand_score"] == 75.0
        assert data["cost_penalty"] == 62.5
        assert data["confidence_score"] == 65.0
        assert data["final_score"] == 61.6
        assert "factors" in data
        assert "contributions" in data
        assert "contributions_confidence" in data
        assert "confidence" in data


class TestHeatmapEndpoint:
    def test_heatmap_requires_category(self, client):
        resp = client.get("/v1/restaurant/heatmap")
        assert resp.status_code == 422

    def test_heatmap_bbox_too_large(self, client):
        resp = client.get(
            "/v1/restaurant/heatmap",
            params={
                "category": "burger",
                "min_lon": 44,
                "min_lat": 20,
                "max_lon": 48,
                "max_lat": 26,
            },
        )
        assert resp.status_code == 400


class TestScoreParcelEndpoint:
    def test_requires_parcel_or_geometry(self, client):
        resp = client.post(
            "/v1/restaurant/score-parcel",
            json={"category": "burger"},
        )
        assert resp.status_code == 400

    @patch("app.api.restaurant_location.score_location")
    def test_with_geometry(self, mock_score, client):
        from app.services.restaurant_location import LocationScoreResult

        mock_score.return_value = LocationScoreResult(
            opportunity_score=65.0,
            demand_score=68.0,
            cost_penalty=55.0,
            factors={"competition": 70},
            contributions=[
                {"factor": "competition", "score": 70, "weight": 0.25, "weighted_contribution": 17.5},
            ],
            confidence=0.6,
            confidence_score=50.0,
            final_score=52.0,
            contributions_confidence=[],
        )
        resp = client.post(
            "/v1/restaurant/score-parcel",
            json={
                "category": "burger",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [46.675, 24.713],
                            [46.676, 24.713],
                            [46.676, 24.714],
                            [46.675, 24.714],
                            [46.675, 24.713],
                        ]
                    ],
                },
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["opportunity_score"] == 65.0
        assert data["category"] == "burger"
