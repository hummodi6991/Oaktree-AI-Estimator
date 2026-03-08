"""
ML model training for the citywide restaurant heatmap AI.

Trains a HistGradientBoostingRegressor to predict cell-level restaurant
opportunity scores for Riyadh H3 cells.  This is a SEPARATE model from
the parcel-level scoring model (restaurant_score_train.py).

The parcel AI adjusts demand-factor *weights*; this heatmap AI directly
predicts a cell-level opportunity score used by the citywide heatmap.

Target: demand-gap proxy — measures underserved opportunity per H3 cell.
    demand_signal = log1p(sum_review_count) * log1p(population)
    supply_signal = log1p(competitor_count)
    raw_target   = demand_signal / (1 + supply_signal)
    target       = max-scaled to 0–100  (divided by training-set max)

High target ⇒ area has strong demand signals but limited competition.
This is an explicit heuristic proxy; true merchant-outcome data is not
available.  The proxy is documented here and in the metadata artifact.

Training is scoped to Riyadh (lat 24.20–25.10, lon 46.20–47.30).
"""

from __future__ import annotations

import json
import logging
import math
import os
from datetime import date
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

logger = logging.getLogger(__name__)

MODEL_DIR = os.environ.get("MODEL_DIR", "models")
MODEL_PATH = os.path.join(MODEL_DIR, "restaurant_heatmap_v1.pkl")
META_PATH = os.path.join(MODEL_DIR, "restaurant_heatmap_v1.meta.json")

# Riyadh bounding box — matches the Google enrichment pipeline scope
RIYADH_LAT_MIN, RIYADH_LAT_MAX = 24.20, 25.10
RIYADH_LON_MIN, RIYADH_LON_MAX = 46.20, 47.30

# Radius (metres) used for neighbour lookups during feature building
CELL_RADIUS_M = 1200

# Numeric feature columns (order matters — must match inference)
NUMERIC_FEATURES = [
    "population",
    "competitor_count",
    "all_restaurant_count",
    "chain_count",
    "avg_rating",
    "sum_review_count",
    "avg_review_count",
    "avg_price_level",
    "platform_count",
    "platform_diversity",
    "google_coverage",
    "avg_google_confidence",
    "poi_density",
    "complementary_count",
]


# ---------------------------------------------------------------------------
# Shared feature extraction — used by both training and inference
# ---------------------------------------------------------------------------


def build_cell_features(
    lat: float,
    lon: float,
    category: str,
    nearby: list[dict],
    population: float,
    platform_sources: set[str] | frozenset[str],
) -> dict[str, Any]:
    """
    Compute the feature vector for a single (cell, category) pair.

    ``nearby`` is a list of POI dicts within ``CELL_RADIUS_M`` of the cell
    centre — each must have keys: category, source, rating, review_count,
    chain_name, google_place_id, google_confidence, price_level.

    ``platform_sources`` is the set of known delivery-platform source keys.

    Returns a flat dict of feature values (all numeric + ``category``).
    """
    same_cat = [p for p in nearby if p.get("category") == category]
    diff_cat = [p for p in nearby if p.get("category") != category]

    competitor_count = len(same_cat)
    all_restaurant_count = len(nearby)
    complementary_count = len(diff_cat)

    # Chains
    chain_names = {
        p["chain_name"] for p in same_cat
        if p.get("chain_name")
    }
    chain_count = len(chain_names)

    # Ratings
    rated = [p for p in same_cat if p.get("rating") is not None]
    avg_rating = (
        sum(float(p["rating"]) for p in rated) / len(rated)
        if rated else float("nan")
    )

    # Reviews
    review_vals = [int(p.get("review_count") or 0) for p in same_cat]
    sum_review_count = sum(review_vals)
    avg_review_count = (
        sum_review_count / len(same_cat) if same_cat else 0.0
    )

    # Price level
    priced = [
        p for p in same_cat
        if p.get("price_level") is not None and float(p["price_level"]) > 0
    ]
    avg_price_level = (
        sum(float(p["price_level"]) for p in priced) / len(priced)
        if priced else float("nan")
    )

    # Platform coverage
    platform_pois = [
        p for p in nearby if p.get("source") in platform_sources
    ]
    platform_count = len(platform_pois)
    platform_diversity = len({p["source"] for p in platform_pois})

    # Google coverage
    google_pois = [p for p in nearby if p.get("google_place_id")]
    google_coverage = len(google_pois) / max(1, all_restaurant_count)
    gconf_vals = [
        float(p["google_confidence"])
        for p in google_pois
        if p.get("google_confidence") is not None
    ]
    avg_google_confidence = (
        sum(gconf_vals) / len(gconf_vals) if gconf_vals else float("nan")
    )

    # POI density (simple proxy: total nearby / 1)
    poi_density = float(all_restaurant_count)

    return {
        "population": population,
        "competitor_count": competitor_count,
        "all_restaurant_count": all_restaurant_count,
        "chain_count": chain_count,
        "avg_rating": avg_rating,
        "sum_review_count": sum_review_count,
        "avg_review_count": avg_review_count,
        "avg_price_level": avg_price_level,
        "platform_count": platform_count,
        "platform_diversity": platform_diversity,
        "google_coverage": google_coverage,
        "avg_google_confidence": avg_google_confidence,
        "poi_density": poi_density,
        "complementary_count": complementary_count,
        "category": category,
    }


# ---------------------------------------------------------------------------
# Training data builder
# ---------------------------------------------------------------------------


def _build_training_df_from_db() -> pd.DataFrame:
    """Load data from the database and build a training DataFrame."""
    from sqlalchemy.orm import Session
    from app.db.session import SessionLocal
    from app.models.tables import PopulationDensity, RestaurantPOI
    from app.services.restaurant_categories import CATEGORIES
    from app.services.restaurant_location import PLATFORM_SOURCES
    from app.services.restaurant_opportunity_heatmap import _GridIndex

    db: Session = SessionLocal()
    try:
        return _build_training_df(db, CATEGORIES, PLATFORM_SOURCES, _GridIndex)
    finally:
        db.close()


def _build_training_df(
    db: Any,
    categories: list[str],
    platform_sources: set[str],
    grid_index_cls: type,
) -> pd.DataFrame:
    """
    Build cell-level training data from PopulationDensity + RestaurantPOI.

    1. Load all population H3 cells (Riyadh).
    2. Load all restaurant POIs in Riyadh bbox.
    3. Build spatial index.
    4. For each (cell, category): compute features + target.
    """
    from sqlalchemy import text
    from app.models.tables import PopulationDensity, RestaurantPOI

    # --- Load population cells ---
    pop_rows = (
        db.query(PopulationDensity)
        .filter(PopulationDensity.lat.isnot(None))
        .filter(PopulationDensity.lon.isnot(None))
        .all()
    )
    cells = [
        {
            "h3": r.h3_index,
            "lat": float(r.lat),
            "lon": float(r.lon),
            "population": float(r.population or 0),
        }
        for r in pop_rows
    ]
    if not cells:
        raise RuntimeError("No population cells found for Riyadh")

    logger.info("Loaded %d population cells", len(cells))

    # --- Load all POIs in Riyadh bbox ---
    expand_deg = CELL_RADIUS_M / 111_000 + 0.02
    rows = db.execute(
        text("""
            SELECT id, lat, lon, category, rating, review_count,
                   source, chain_name, google_place_id, google_confidence,
                   price_level
            FROM restaurant_poi
            WHERE lat BETWEEN :min_lat AND :max_lat
              AND lon BETWEEN :min_lon AND :max_lon
        """),
        {
            "min_lat": RIYADH_LAT_MIN - expand_deg,
            "max_lat": RIYADH_LAT_MAX + expand_deg,
            "min_lon": RIYADH_LON_MIN - expand_deg,
            "max_lon": RIYADH_LON_MAX + expand_deg,
        },
    ).mappings().all()

    pois = [
        {
            "id": r["id"],
            "lat": float(r["lat"]),
            "lon": float(r["lon"]),
            "category": r["category"],
            "rating": float(r["rating"]) if r["rating"] else None,
            "review_count": int(r["review_count"]) if r["review_count"] else 0,
            "source": r["source"],
            "chain_name": r["chain_name"],
            "google_place_id": r["google_place_id"],
            "google_confidence": (
                float(r["google_confidence"]) if r["google_confidence"] else None
            ),
            "price_level": (
                float(r["price_level"]) if r["price_level"] else None
            ),
        }
        for r in rows
    ]
    if not pois:
        raise RuntimeError("No restaurant POIs found for Riyadh bbox")

    logger.info("Loaded %d POIs", len(pois))

    # --- Build spatial index ---
    idx = grid_index_cls(pois)

    # --- Build feature rows for each (cell, category) ---
    items: list[dict] = []
    ps_frozen = frozenset(platform_sources)

    for cell in cells:
        nearby = idx.neighbors(cell["lat"], cell["lon"], CELL_RADIUS_M)
        for cat in categories:
            feats = build_cell_features(
                cell["lat"],
                cell["lon"],
                cat,
                nearby,
                cell["population"],
                ps_frozen,
            )

            # --- Target: demand-gap proxy ---
            # demand_signal = log1p(sum_review_count) * log1p(population)
            # supply_signal = log1p(competitor_count)
            # raw = demand_signal / (1 + supply_signal)
            demand_signal = math.log1p(feats["sum_review_count"]) * math.log1p(
                feats["population"]
            )
            supply_signal = math.log1p(feats["competitor_count"])
            raw_target = demand_signal / (1.0 + supply_signal)

            feats["target"] = raw_target
            feats["h3"] = cell["h3"]
            items.append(feats)

    df = pd.DataFrame(items)

    # Max-scale target to 0–100 (divide by training-set max)
    t_max = df["target"].max()
    if t_max > 0:
        df["target"] = (df["target"] / t_max) * 100.0

    logger.info(
        "Built training DataFrame: %d rows (%d cells × %d categories)",
        len(df),
        len(cells),
        len(categories),
    )
    return df


# ---------------------------------------------------------------------------
# Train and persist
# ---------------------------------------------------------------------------


def train_and_save() -> dict:
    """Train the heatmap AI model and save artifacts to disk."""
    os.makedirs(MODEL_DIR, exist_ok=True)

    df = _build_training_df_from_db()

    if df.empty or len(df) < 20:
        logger.warning("Insufficient training data (%d rows), skipping", len(df))
        return {"model_path": None, "metrics": {"error": "insufficient_data"}}

    # One-hot encode category
    df_encoded = pd.get_dummies(df, columns=["category"], prefix="cat")
    cat_cols = sorted(c for c in df_encoded.columns if c.startswith("cat_"))
    all_features = NUMERIC_FEATURES + cat_cols

    X = df_encoded[all_features]
    y = df_encoded["target"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42,
    )

    model = HistGradientBoostingRegressor(
        max_iter=200,
        max_depth=6,
        learning_rate=0.08,
        min_samples_leaf=8,
        random_state=42,
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mae = float(mean_absolute_error(y_test, y_pred))
    r2 = float(r2_score(y_test, y_pred))

    # Feature importances (permutation-based for HistGBR would be costly;
    # use the built-in split-based importances via internal attribute)
    try:
        raw_importances = model.feature_importances_  # type: ignore[attr-defined]
        importances = dict(zip(all_features, raw_importances.tolist()))
    except AttributeError:
        importances = {}

    joblib.dump(model, MODEL_PATH, compress=3)

    meta = {
        "model_version": "heatmap_ai_v1",
        "trained_at": str(date.today()),
        "feature_names": all_features,
        "train_row_count": len(df),
        "n_train": len(X_train),
        "n_test": len(X_test),
        "mae": mae,
        "r2": r2,
        "target_definition": (
            "demand_gap: log1p(sum_review_count)*log1p(population) "
            "/ (1+log1p(competitor_count)), max-scaled 0-100"
        ),
        "category_handling": "one-hot encoded, model trains across all categories",
        "riyadh_only": True,
        "feature_importances": importances,
        "training_bbox": {
            "lat_min": RIYADH_LAT_MIN,
            "lat_max": RIYADH_LAT_MAX,
            "lon_min": RIYADH_LON_MIN,
            "lon_max": RIYADH_LON_MAX,
            "label": "Riyadh",
        },
    }
    with open(META_PATH, "w") as f:
        json.dump(meta, f, indent=2)

    # MLflow logging (non-critical)
    try:
        import mlflow

        with mlflow.start_run(run_name="restaurant_heatmap_ai_v1"):
            mlflow.log_params({
                "model": "HistGradientBoostingRegressor",
                "max_iter": 200,
                "max_depth": 6,
                "learning_rate": 0.08,
                "features": ",".join(all_features),
            })
            mlflow.log_metrics({"mae": mae, "r2": r2, "n_rows": len(df)})
            mlflow.log_artifact(MODEL_PATH)
            mlflow.log_artifact(META_PATH)
    except Exception as exc:
        logger.warning("MLflow logging failed (non-critical): %s", exc)

    logger.info(
        "Heatmap AI model trained: MAE=%.2f, R2=%.3f, n=%d", mae, r2, len(df)
    )

    # Smoke-test summary
    sorted_imp = sorted(importances.items(), key=lambda x: x[1], reverse=True)

    print("\n" + "=" * 60)
    print("HEATMAP AI TRAINING SMOKE TEST")
    print("=" * 60)
    print(f"  Training rows:          {len(df)}")
    print(f"  Train / Test:           {len(X_train)} / {len(X_test)}")
    print(f"  MAE:                    {mae:.2f}")
    print(f"  R2:                     {r2:.3f}")
    print(f"\n  Top feature importances:")
    for rank, (feat, imp) in enumerate(sorted_imp[:15], 1):
        print(f"    {rank:2d}. {feat:<30s} {imp:.4f}")
    print("=" * 60 + "\n")

    return {"model_path": MODEL_PATH, "metrics": meta}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = train_and_save()
    print(json.dumps(result.get("metrics", {}), indent=2, default=str))
