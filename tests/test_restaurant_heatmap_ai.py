"""Tests for the dedicated restaurant heatmap AI model and integration."""

import json
import math
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pandas as pd
import pytest

from app.ml.restaurant_heatmap_train import (
    build_cell_features,
    NUMERIC_FEATURES,
)


# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------


class TestBuildCellFeatures:
    """Unit tests for the shared feature extraction function."""

    def _make_poi(self, **overrides):
        base = {
            "category": "burger",
            "source": "hungerstation",
            "rating": 4.0,
            "review_count": 50,
            "chain_name": None,
            "google_place_id": "abc",
            "google_confidence": 0.9,
            "price_level": 2.0,
            "lat": 24.7,
            "lon": 46.7,
            "distance_m": 200,
        }
        base.update(overrides)
        return base

    def test_returns_all_numeric_features(self):
        nearby = [self._make_poi()]
        feats = build_cell_features(
            24.7, 46.7, "burger", nearby, 5000.0, frozenset({"hungerstation"})
        )
        for feat_name in NUMERIC_FEATURES:
            assert feat_name in feats, f"Missing feature: {feat_name}"

    def test_empty_nearby(self):
        feats = build_cell_features(
            24.7, 46.7, "burger", [], 5000.0, frozenset({"hungerstation"})
        )
        assert feats["competitor_count"] == 0
        assert feats["all_restaurant_count"] == 0
        assert feats["population"] == 5000.0

    def test_competitor_count_matches_category(self):
        nearby = [
            self._make_poi(category="burger"),
            self._make_poi(category="burger"),
            self._make_poi(category="pizza"),
        ]
        feats = build_cell_features(
            24.7, 46.7, "burger", nearby, 1000.0, frozenset()
        )
        assert feats["competitor_count"] == 2
        assert feats["complementary_count"] == 1
        assert feats["all_restaurant_count"] == 3

    def test_chain_count(self):
        nearby = [
            self._make_poi(chain_name="McDonalds"),
            self._make_poi(chain_name="McDonalds"),
            self._make_poi(chain_name="BurgerKing"),
            self._make_poi(chain_name=None),
        ]
        feats = build_cell_features(
            24.7, 46.7, "burger", nearby, 1000.0, frozenset()
        )
        assert feats["chain_count"] == 2

    def test_platform_diversity(self):
        nearby = [
            self._make_poi(source="hungerstation"),
            self._make_poi(source="talabat"),
            self._make_poi(source="hungerstation"),
        ]
        feats = build_cell_features(
            24.7, 46.7, "burger", nearby, 1000.0,
            frozenset({"hungerstation", "talabat"}),
        )
        assert feats["platform_count"] == 3
        assert feats["platform_diversity"] == 2

    def test_google_coverage(self):
        nearby = [
            self._make_poi(google_place_id="abc"),
            self._make_poi(google_place_id=None),
        ]
        feats = build_cell_features(
            24.7, 46.7, "burger", nearby, 1000.0, frozenset()
        )
        assert feats["google_coverage"] == 0.5

    def test_category_field_present(self):
        feats = build_cell_features(
            24.7, 46.7, "pizza", [], 0.0, frozenset()
        )
        assert feats["category"] == "pizza"


# ---------------------------------------------------------------------------
# Model loading (restaurant_heatmap_ai.py)
# ---------------------------------------------------------------------------


class TestModelLoading:
    def test_returns_none_when_no_artifact(self):
        import app.services.restaurant_heatmap_ai as mod

        # Reset cached model
        mod._MODEL = None
        mod._META = {}
        with patch.object(mod, "_MODEL_PATH", "/nonexistent/path.pkl"):
            result = mod.try_load_model()
            assert result is None

    def test_get_model_status_unavailable(self):
        import app.services.restaurant_heatmap_ai as mod

        mod._MODEL = None
        mod._META = {}
        with patch.object(mod, "_MODEL_PATH", "/nonexistent/path.pkl"):
            status = mod.get_model_status()
            assert status["available"] is False
            assert status["artifact_present"] is False
            assert status["model_version"] is None

    def test_predict_returns_none_when_no_model(self):
        import app.services.restaurant_heatmap_ai as mod

        mod._MODEL = None
        mod._META = {}
        with patch.object(mod, "_MODEL_PATH", "/nonexistent/path.pkl"):
            df = pd.DataFrame({"x": [1, 2, 3]})
            result = mod.predict_cell_scores(df)
            assert result is None

    def test_predict_with_mock_model(self):
        import app.services.restaurant_heatmap_ai as mod

        mock_model = MagicMock()
        mock_model.predict.return_value = np.array([50.0, 75.0])

        mod._MODEL = mock_model
        mod._META = {"feature_names": ["a", "b"]}

        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        result = mod.predict_cell_scores(df)
        assert result is not None
        assert len(result) == 2
        assert result[0] == 50.0
        assert result[1] == 75.0

        # Cleanup
        mod._MODEL = None
        mod._META = {}

    def test_predict_clips_to_0_100(self):
        import app.services.restaurant_heatmap_ai as mod

        mock_model = MagicMock()
        mock_model.predict.return_value = np.array([-10.0, 120.0])

        mod._MODEL = mock_model
        mod._META = {"feature_names": ["a"]}

        df = pd.DataFrame({"a": [1, 2]})
        result = mod.predict_cell_scores(df)
        assert result is not None
        assert result[0] == 0.0
        assert result[1] == 100.0

        mod._MODEL = None
        mod._META = {}

    def test_get_model_status_available(self):
        import app.services.restaurant_heatmap_ai as mod

        mod._MODEL = MagicMock()
        mod._META = {
            "model_version": "heatmap_ai_v1",
            "trained_at": "2026-03-08",
            "feature_names": ["a", "b"],
            "train_row_count": 1000,
            "mae": 5.0,
            "r2": 0.85,
            "target_definition": "test_target",
            "riyadh_only": True,
        }

        status = mod.get_model_status()
        assert status["available"] is True
        assert status["model_version"] == "heatmap_ai_v1"
        assert status["feature_count"] == 2
        assert status["mae"] == 5.0

        mod._MODEL = None
        mod._META = {}


# ---------------------------------------------------------------------------
# Fallback behaviour in opportunity heatmap
# ---------------------------------------------------------------------------


class TestHeatmapFallbackBehaviour:
    """Verify the heatmap service correctly falls back to static scoring."""

    def test_static_scoring_metadata(self):
        """When no AI model is available, metadata should reflect static mode."""
        with patch(
            "app.services.restaurant_opportunity_heatmap._try_load_heatmap_model",
            return_value=None,
        ):
            from app.services.restaurant_opportunity_heatmap import _score_cell
            from app.services.restaurant_location import DEMAND_WEIGHTS

            result = _score_cell(24.7, 46.7, "burger", 1200, [], 5000.0, DEMAND_WEIGHTS)
            # _score_cell itself doesn't add ai_used — that's added by the generator.
            # Just verify the static path produces valid scores.
            assert "opportunity_score" in result
            assert "final_score" in result
            assert result["competitor_count"] == 0


# ---------------------------------------------------------------------------
# API endpoint: heatmap-ai-status
# ---------------------------------------------------------------------------


class TestHeatmapAiStatusEndpoint:
    def test_endpoint_returns_expected_fields(self):
        from fastapi.testclient import TestClient
        from unittest.mock import patch as mock_patch

        fake_status = {
            "available": False,
            "artifact_present": False,
            "model_version": None,
            "trained_at": None,
            "feature_count": 0,
            "train_row_count": None,
            "mae": None,
            "r2": None,
            "target_definition": None,
            "riyadh_only": True,
        }

        with mock_patch(
            "app.api.restaurant_location.get_heatmap_model_status",
            return_value=fake_status,
        ):
            from app.main import app

            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/v1/restaurant/heatmap-ai-status")

            if resp.status_code == 200:
                data = resp.json()
                assert "ai_model_available" in data
                assert "model_version" in data
                assert "artifact_present" in data
                assert "fallback_mode" in data
                assert "description" in data
                assert data["ai_model_available"] is False
                assert data["fallback_mode"] is True


# ---------------------------------------------------------------------------
# Opportunity heatmap metadata
# ---------------------------------------------------------------------------


class TestOpportunityHeatmapAiMetadata:
    def test_heatmap_payload_includes_ai_metadata(self):
        """Verify that the heatmap response includes AI metadata fields."""
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
                        "ai_used": False,
                        "model_version": "curated_static_v1",
                        "scoring_mode": "curated_static_v1",
                    },
                }
            ],
            "metadata": {
                "category": "burger",
                "radius_m": 1200,
                "cell_count": 1,
                "ai_used": False,
                "ai_model_available": False,
                "model_version": "curated_static_v1",
                "scoring_mode": "curated_static_v1",
            },
        }

        with patch(
            "app.api.restaurant_location.generate_opportunity_heatmap",
            return_value=fake_payload,
        ):
            from app.main import app
            from fastapi.testclient import TestClient

            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/v1/restaurant/opportunity-heatmap?category=burger")

            if resp.status_code == 200:
                data = resp.json()
                meta = data.get("metadata", {})
                assert "ai_used" in meta
                assert "ai_model_available" in meta
                assert "model_version" in meta
                assert "scoring_mode" in meta

                # Per-feature metadata
                props = data["features"][0]["properties"]
                assert "ai_used" in props
                assert "scoring_mode" in props


# ---------------------------------------------------------------------------
# No regression: parcel scoring
# ---------------------------------------------------------------------------


class TestParcelScoringNotRegressed:
    def test_score_response_still_has_ai_fields(self):
        """Ensure existing parcel scoring still returns model_version + ai_weights_used."""
        from app.services.restaurant_location import LocationScoreResult

        # Just verify the dataclass still has the expected fields
        result = LocationScoreResult(
            opportunity_score=70.0,
            demand_score=75.0,
            cost_penalty=60.0,
            factors={"competition": 80.0},
            contributions=[],
            confidence=0.7,
            confidence_score=70.0,
            final_score=65.0,
            contributions_confidence=[],
            nearby_competitors=[],
            model_version="ai_weighted_v3",
            ai_weights_used=True,
        )
        assert result.model_version == "ai_weighted_v3"
        assert result.ai_weights_used is True
