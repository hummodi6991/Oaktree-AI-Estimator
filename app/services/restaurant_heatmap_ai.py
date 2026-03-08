"""
Inference service for the dedicated restaurant heatmap AI model.

Loads the heatmap-specific model artifact and provides prediction
capabilities for cell-level restaurant opportunity scoring.

This is intentionally separate from the parcel-level scoring model
(loaded in ``restaurant_location.py``).  The heatmap AI predicts a
cell-level opportunity score directly, while the parcel AI adjusts
demand-factor weights.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Module-level cached model and metadata
_MODEL: Any = None
_META: dict[str, Any] = {}

_BASE = os.environ.get("MODEL_DIR", "models")
_MODEL_PATH = os.path.join(_BASE, "restaurant_heatmap_v1.pkl")
_META_PATH = os.path.join(_BASE, "restaurant_heatmap_v1.meta.json")


def try_load_model() -> Any:
    """
    Lazy-load the heatmap AI model.  Returns the model object on success
    or ``None`` if the artifact is not present / cannot be loaded.
    """
    global _MODEL, _META
    if _MODEL is not None:
        return _MODEL

    if not os.path.exists(_MODEL_PATH):
        logger.info("Heatmap AI model not found at %s — will use static fallback", _MODEL_PATH)
        return None

    try:
        import joblib

        _MODEL = joblib.load(_MODEL_PATH)
        if os.path.exists(_META_PATH):
            with open(_META_PATH, "r") as f:
                _META = json.load(f)
        logger.info(
            "Heatmap AI model loaded: version=%s",
            _META.get("model_version", "unknown"),
        )
        return _MODEL
    except Exception as exc:
        logger.warning("Failed to load heatmap AI model: %s", exc)
        return None


def predict_cell_scores(features_df: pd.DataFrame) -> Optional[np.ndarray]:
    """
    Predict opportunity scores for a batch of cells.

    ``features_df`` must contain all columns listed in the model metadata's
    ``feature_names``.  Returns a 1-D array of predicted scores (0–100 scale)
    or ``None`` if the model is not available.
    """
    model = try_load_model()
    if model is None:
        return None

    expected_cols = _META.get("feature_names")
    if expected_cols:
        # Ensure column order matches training; fill missing one-hot cols with 0
        for col in expected_cols:
            if col not in features_df.columns:
                features_df[col] = 0
        features_df = features_df[expected_cols]

    try:
        preds = model.predict(features_df)
        # Clip to 0–100
        preds = np.clip(preds, 0.0, 100.0)
        return preds
    except Exception as exc:
        logger.warning("Heatmap AI prediction failed: %s", exc)
        return None


def get_model_status() -> dict[str, Any]:
    """
    Return introspection info for the heatmap AI model.
    Used by the ``/v1/restaurant/heatmap-ai-status`` endpoint.
    """
    model = try_load_model()
    available = model is not None
    return {
        "available": available,
        "artifact_present": os.path.exists(_MODEL_PATH),
        "model_version": _META.get("model_version") if available else None,
        "trained_at": _META.get("trained_at") if available else None,
        "feature_count": len(_META.get("feature_names", [])) if available else 0,
        "train_row_count": _META.get("train_row_count") if available else None,
        "mae": _META.get("mae") if available else None,
        "r2": _META.get("r2") if available else None,
        "target_definition": _META.get("target_definition") if available else None,
        "riyadh_only": _META.get("riyadh_only", True),
    }
