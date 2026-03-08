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
_LOAD_ERROR: str | None = None

# Startup logging — runs once at import time
logger.info(
    "Heatmap AI loader: MODEL_DIR=%s, pkl=%s (exists=%s), meta=%s (exists=%s)",
    _BASE,
    _MODEL_PATH,
    os.path.exists(_MODEL_PATH),
    _META_PATH,
    os.path.exists(_META_PATH),
)


def try_load_model() -> Any:
    """
    Lazy-load the heatmap AI model.  Returns the model object on success
    or ``None`` if the artifact is not present / cannot be loaded.

    Re-checks the filesystem each call when the model is not yet loaded,
    so a newly deployed artifact is picked up without a server restart.
    """
    global _MODEL, _META, _LOAD_ERROR
    if _MODEL is not None:
        return _MODEL

    if not os.path.exists(_MODEL_PATH):
        _LOAD_ERROR = f"Model pkl not found at {_MODEL_PATH}"
        logger.debug("Heatmap AI: %s — will use static fallback", _LOAD_ERROR)
        return None

    try:
        import joblib

        logger.info("Heatmap AI: loading model from %s ...", _MODEL_PATH)
        _MODEL = joblib.load(_MODEL_PATH)
        if os.path.exists(_META_PATH):
            with open(_META_PATH, "r") as f:
                _META = json.load(f)
        _LOAD_ERROR = None
        logger.info(
            "Heatmap AI model loaded successfully: version=%s, features=%d, trained_at=%s",
            _META.get("model_version", "unknown"),
            len(_META.get("feature_names", [])),
            _META.get("trained_at", "unknown"),
        )
        return _MODEL
    except Exception as exc:
        _LOAD_ERROR = f"Failed to load: {exc}"
        logger.warning("Heatmap AI: %s", _LOAD_ERROR)
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
        "ai_model_available": available,
        "artifact_present": os.path.exists(_MODEL_PATH),
        "model_path": _MODEL_PATH,
        "meta_path": _META_PATH,
        "meta_present": os.path.exists(_META_PATH),
        "load_error": _LOAD_ERROR,
        "model_version": _META.get("model_version") if available else None,
        "trained_at": _META.get("trained_at") if available else None,
        "feature_count": len(_META.get("feature_names", [])) if available else 0,
        "train_row_count": _META.get("train_row_count") if available else None,
        "mae": _META.get("mae") if available else None,
        "r2": _META.get("r2") if available else None,
        "target_definition": _META.get("target_definition") if available else None,
        "riyadh_only": _META.get("riyadh_only", True),
    }
