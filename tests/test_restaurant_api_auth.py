"""Tests for restaurant API endpoints under AUTH_MODE=api_key.

auth.require() reads AUTH_MODE from env at runtime, so we use monkeypatch to
set AUTH_MODE=api_key and API_KEY=test-key for the scope of each test.  This
keeps auth tests self-contained and prevents env leakage to other tests.
"""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

_TEST_API_KEY = "test-key"
AUTH_HEADER = {"x-api-key": _TEST_API_KEY}


@pytest.fixture()
def auth_client(monkeypatch):
    """TestClient with AUTH_MODE=api_key forced via monkeypatch."""
    monkeypatch.setenv("AUTH_MODE", "api_key")
    monkeypatch.setenv("API_KEY", _TEST_API_KEY)

    from app.main import app

    return TestClient(app, raise_server_exceptions=False)


class TestUnauthenticatedReturns401:
    """Without an API key, protected restaurant endpoints must return 401."""

    def test_categories_requires_key(self, auth_client):
        resp = auth_client.get("/v1/restaurant/categories")
        assert resp.status_code == 401

    def test_score_requires_key(self, auth_client):
        resp = auth_client.post(
            "/v1/restaurant/score",
            json={"lat": 24.7, "lon": 46.7, "category": "burger"},
        )
        assert resp.status_code == 401

    def test_heatmap_requires_key(self, auth_client):
        resp = auth_client.get(
            "/v1/restaurant/heatmap", params={"category": "burger"}
        )
        assert resp.status_code == 401


class TestAuthenticatedAccess:
    """With a valid API key, endpoints should process requests normally."""

    def test_categories_with_key(self, auth_client):
        resp = auth_client.get("/v1/restaurant/categories", headers=AUTH_HEADER)
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0

    def test_score_validation_with_key(self, auth_client):
        resp = auth_client.post(
            "/v1/restaurant/score",
            headers=AUTH_HEADER,
        )
        # 422 validation error, not 401
        assert resp.status_code == 422

    @patch("app.api.restaurant_location.score_location")
    def test_score_with_key(self, mock_score, auth_client):
        from app.services.restaurant_location import LocationScoreResult

        mock_score.return_value = LocationScoreResult(
            opportunity_score=70.0,
            demand_score=72.0,
            cost_penalty=60.0,
            factors={"competition": 75},
            contributions=[
                {"factor": "competition", "score": 75, "weight": 0.25, "weighted_contribution": 18.75},
            ],
            confidence=0.7,
            nearby_competitors=[],
        )
        resp = auth_client.post(
            "/v1/restaurant/score",
            headers=AUTH_HEADER,
            json={"lat": 24.7, "lon": 46.7, "category": "burger"},
        )
        assert resp.status_code == 200
        assert resp.json()["opportunity_score"] == 70.0

    def test_heatmap_bbox_too_large_with_key(self, auth_client):
        resp = auth_client.get(
            "/v1/restaurant/heatmap",
            headers=AUTH_HEADER,
            params={
                "category": "burger",
                "min_lon": 44,
                "min_lat": 20,
                "max_lon": 48,
                "max_lat": 26,
            },
        )
        # 400 business logic error, not 401
        assert resp.status_code == 400
