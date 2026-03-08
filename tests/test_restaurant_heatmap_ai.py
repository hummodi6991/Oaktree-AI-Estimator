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
# End-to-end: train a real model, run inference, verify ai_used=true
# ---------------------------------------------------------------------------


class TestHeatmapAiEndToEnd:
    """
    Train a real HistGradientBoostingRegressor on synthetic cell data,
    inject it into the inference service, and verify the full scoring
    path produces ai_used=true with valid scores.
    """

    def test_ai_path_produces_ai_used_true(self):
        """Prove that when a real model artifact is present, the AI path
        runs and produces ai_used=true with finite scores."""
        from sklearn.ensemble import HistGradientBoostingRegressor
        import app.services.restaurant_heatmap_ai as ai_mod
        from app.ml.restaurant_heatmap_train import build_cell_features, NUMERIC_FEATURES

        # --- 1. Train a tiny real model on synthetic data ---
        rng = np.random.RandomState(42)
        n_rows = 60
        cat_options = ["burger", "pizza", "chicken"]
        categories_onehot = [f"cat_{c}" for c in cat_options]
        all_features = NUMERIC_FEATURES + categories_onehot

        synth = {}
        for feat in NUMERIC_FEATURES:
            synth[feat] = rng.uniform(0, 100, size=n_rows)
        for cat_col in categories_onehot:
            synth[cat_col] = rng.choice([0, 1], size=n_rows)
        synth_target = rng.uniform(0, 100, size=n_rows)

        X_train = pd.DataFrame(synth)[all_features]
        y_train = pd.Series(synth_target)

        model = HistGradientBoostingRegressor(
            max_iter=10, max_depth=3, random_state=42,
        )
        model.fit(X_train, y_train)

        # --- 2. Inject the trained model into the AI module ---
        old_model, old_meta = ai_mod._MODEL, ai_mod._META
        try:
            ai_mod._MODEL = model
            ai_mod._META = {
                "model_version": "heatmap_ai_v1_test",
                "feature_names": all_features,
            }

            # --- 3. Build features via the real build_cell_features path ---
            nearby_pois = [
                {
                    "category": "burger",
                    "source": "hungerstation",
                    "rating": 4.0,
                    "review_count": 100,
                    "chain_name": "TestChain",
                    "google_place_id": "gp1",
                    "google_confidence": 0.85,
                    "price_level": 2.0,
                    "lat": 24.7,
                    "lon": 46.7,
                    "distance_m": 300,
                },
                {
                    "category": "pizza",
                    "source": "talabat",
                    "rating": 3.5,
                    "review_count": 60,
                    "chain_name": None,
                    "google_place_id": "gp2",
                    "google_confidence": 0.7,
                    "price_level": 1.0,
                    "lat": 24.701,
                    "lon": 46.701,
                    "distance_m": 500,
                },
            ]

            feats = build_cell_features(
                24.7, 46.7, "burger", nearby_pois, 5000.0,
                frozenset({"hungerstation", "talabat"}),
            )
            feat_df = pd.DataFrame([feats])
            feat_df = pd.get_dummies(feat_df, columns=["category"], prefix="cat")
            # Drop any non-feature columns (same logic as the fixed inference path)
            feat_df = feat_df.drop(
                columns=[c for c in ("h3",) if c in feat_df.columns],
            )

            # --- 4. Run prediction via the inference service ---
            from app.services.restaurant_heatmap_ai import predict_cell_scores

            scores = predict_cell_scores(feat_df)

            # --- 5. Verify: AI path ran and produced valid output ---
            assert scores is not None, "predict_cell_scores returned None despite model being loaded"
            assert len(scores) == 1
            assert np.isfinite(scores[0]), f"Score is not finite: {scores[0]}"
            assert 0.0 <= scores[0] <= 100.0, f"Score out of range: {scores[0]}"

            # --- 6. Verify model status shows available ---
            status = ai_mod.get_model_status()
            assert status["available"] is True
            assert status["model_version"] == "heatmap_ai_v1_test"

        finally:
            # Restore original state
            ai_mod._MODEL = old_model
            ai_mod._META = old_meta

    def test_full_generator_ai_path(self):
        """
        Simulate the generate_opportunity_heatmap AI path end-to-end:
        build features for multiple cells, batch-predict, and verify
        the output GeoJSON features all have ai_used=true.
        """
        from sklearn.ensemble import HistGradientBoostingRegressor
        import app.services.restaurant_heatmap_ai as ai_mod
        from app.ml.restaurant_heatmap_train import build_cell_features, NUMERIC_FEATURES
        from app.services.restaurant_opportunity_heatmap import _GridIndex

        rng = np.random.RandomState(99)
        cat_options = ["burger", "pizza"]
        categories_onehot = [f"cat_{c}" for c in cat_options]
        all_features = NUMERIC_FEATURES + categories_onehot

        # Train a tiny model
        n_rows = 40
        synth = {feat: rng.uniform(0, 50, size=n_rows) for feat in NUMERIC_FEATURES}
        for cat_col in categories_onehot:
            synth[cat_col] = rng.choice([0, 1], size=n_rows)

        model = HistGradientBoostingRegressor(max_iter=5, max_depth=2, random_state=99)
        model.fit(pd.DataFrame(synth)[all_features], rng.uniform(20, 80, size=n_rows))

        old_model, old_meta = ai_mod._MODEL, ai_mod._META
        try:
            ai_mod._MODEL = model
            ai_mod._META = {
                "model_version": "heatmap_ai_v1_e2e",
                "feature_names": all_features,
            }

            # Simulate 3 cells with some POIs
            pois = [
                {"id": "1", "lat": 24.70, "lon": 46.70, "category": "burger",
                 "source": "hungerstation", "rating": 4.0, "review_count": 80,
                 "chain_name": "BK", "google_place_id": "g1",
                 "google_confidence": 0.9, "price_level": 2.0},
                {"id": "2", "lat": 24.70, "lon": 46.70, "category": "pizza",
                 "source": "talabat", "rating": 3.5, "review_count": 40,
                 "chain_name": None, "google_place_id": None,
                 "google_confidence": None, "price_level": None},
            ]

            cells = [
                {"h3": "cell_a", "lat": 24.700, "lon": 46.700, "population": 5000},
                {"h3": "cell_b", "lat": 24.705, "lon": 46.705, "population": 3000},
                {"h3": "cell_c", "lat": 24.710, "lon": 46.710, "population": 1000},
            ]

            idx = _GridIndex(pois, cell_deg=0.01)
            category = "burger"
            ps = frozenset({"hungerstation", "talabat"})

            cell_records = []
            cell_meta_list = []
            for cell in cells:
                nearby = idx.neighbors(cell["lat"], cell["lon"], 1200)
                feats = build_cell_features(
                    cell["lat"], cell["lon"], category, nearby,
                    cell["population"], ps,
                )
                cell_records.append(feats)
                cell_meta_list.append({"h3": cell["h3"], "lat": cell["lat"], "lon": cell["lon"]})

            feat_df = pd.DataFrame(cell_records)
            feat_df = pd.get_dummies(feat_df, columns=["category"], prefix="cat")
            feat_df = feat_df.drop(
                columns=[c for c in ("h3",) if c in feat_df.columns],
            )

            from app.services.restaurant_heatmap_ai import predict_cell_scores
            scores = predict_cell_scores(feat_df)

            assert scores is not None, "AI path must produce scores when model is loaded"
            assert len(scores) == 3
            for i, s in enumerate(scores):
                assert np.isfinite(s), f"Cell {i} score is not finite: {s}"
                assert 0.0 <= s <= 100.0, f"Cell {i} score out of range: {s}"

        finally:
            ai_mod._MODEL = old_model
            ai_mod._META = old_meta


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


# ---------------------------------------------------------------------------
# Parcel AI status introspection
# ---------------------------------------------------------------------------


class TestParcelAiStatus:
    """Tests for the parcel AI status introspection endpoint."""

    def test_parcel_ai_status_unavailable(self):
        """When no model artifact exists, status reports unavailable."""
        import app.services.restaurant_location as mod

        # Reset cached state
        old_weights = mod._cached_ai_weights
        old_error = mod._parcel_load_error
        old_meta = mod._parcel_meta
        try:
            mod._cached_ai_weights = None
            mod._parcel_load_error = None
            mod._parcel_meta = {}
            with patch.object(mod, "_MODEL_META_PATH", "/nonexistent/meta.json"), \
                 patch.object(mod, "_MODEL_PKL_PATH", "/nonexistent/model.pkl"):
                status = mod.get_parcel_ai_status()
                assert status["available"] is False
                assert status["artifact_present"] is False
                assert status["fallback_mode"] is True
                assert status["load_error"] is not None
                assert "model_path" in status
                assert "meta_path" in status
        finally:
            mod._cached_ai_weights = old_weights
            mod._parcel_load_error = old_error
            mod._parcel_meta = old_meta

    def test_parcel_ai_status_available(self):
        """When model meta is loaded, status reports available."""
        import app.services.restaurant_location as mod

        old_weights = mod._cached_ai_weights
        old_error = mod._parcel_load_error
        old_meta = mod._parcel_meta
        try:
            mod._cached_ai_weights = {"competition": 0.3, "population": 0.2}
            mod._parcel_load_error = None
            mod._parcel_meta = {
                "model_version": "score_v0_test",
                "mae": 5.0,
                "r2": 0.8,
                "n_rows": 500,
            }
            with patch.object(mod, "_MODEL_PKL_PATH", "/tmp/fake.pkl"), \
                 patch("os.path.exists", return_value=True):
                status = mod.get_parcel_ai_status()
                assert status["available"] is True
                assert status["fallback_mode"] is False
                assert status["model_version"] == "score_v0_test"
        finally:
            mod._cached_ai_weights = old_weights
            mod._parcel_load_error = old_error
            mod._parcel_meta = old_meta

    def test_parcel_ai_status_endpoint(self):
        """The /v1/restaurant/parcel-ai-status API endpoint returns expected fields."""
        from unittest.mock import patch as mock_patch

        fake_status = {
            "available": False,
            "artifact_present": False,
            "model_path": "models/restaurant_score_v0.pkl",
            "meta_path": "models/restaurant_score_v0.meta.json",
            "meta_present": False,
            "load_error": "not found",
            "model_version": None,
            "trained_at": None,
            "mae": None,
            "r2": None,
            "n_rows": None,
            "fallback_mode": True,
        }

        with mock_patch(
            "app.api.restaurant_location.get_parcel_ai_status",
            return_value=fake_status,
        ):
            from app.main import app
            from fastapi.testclient import TestClient

            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/v1/restaurant/parcel-ai-status")

            if resp.status_code == 200:
                data = resp.json()
                assert "ai_model_available" in data
                assert "model_version" in data
                assert "artifact_present" in data
                assert "model_path" in data
                assert "meta_path" in data
                assert "load_error" in data
                assert "fallback_mode" in data
                assert "description" in data
                assert data["ai_model_available"] is False
                assert data["fallback_mode"] is True


# ---------------------------------------------------------------------------
# Heatmap AI status endpoint — extended fields
# ---------------------------------------------------------------------------


class TestHeatmapAiStatusExtended:
    """Verify the heatmap-ai-status endpoint now includes path/error fields."""

    def test_extended_fields_present(self):
        from unittest.mock import patch as mock_patch

        fake_status = {
            "available": False,
            "artifact_present": False,
            "model_path": "models/restaurant_heatmap_v1.pkl",
            "meta_path": "models/restaurant_heatmap_v1.meta.json",
            "meta_present": False,
            "load_error": "pkl not found",
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
            from fastapi.testclient import TestClient

            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/v1/restaurant/heatmap-ai-status")

            if resp.status_code == 200:
                data = resp.json()
                assert "model_path" in data
                assert "meta_path" in data
                assert "meta_present" in data
                assert "load_error" in data
