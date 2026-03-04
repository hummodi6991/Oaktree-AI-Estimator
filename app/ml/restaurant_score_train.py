"""
ML model training for restaurant location demand-potential scoring.

Trains a GradientBoostingRegressor to calibrate location demand-potential
scores based on features like competition density, population,
traffic, commercial density, delivery platform coverage, anchor proximity,
and income proxy.

The trained model's feature importances are used by the scoring engine
to dynamically weight demand factors (instead of static weights). This
allows the system to adapt weights to actual market data patterns.

IMPORTANT: This model learns a *demand-potential proxy* — it predicts
where restaurant demand is likely based on existing POI density and
ratings. It is NOT a profitability predictor. True profitability
requires outcome signals (merchant sales, order volumes) that are
not available in public data.
"""

from __future__ import annotations

import json
import logging
import math
import os
from datetime import date

import joblib
import mlflow
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.models.tables import LocationScore, PopulationDensity, RestaurantPOI
from app.services.restaurant_categories import CATEGORIES
from app.services.restaurant_location import PLATFORM_SOURCES

logger = logging.getLogger(__name__)

MODEL_DIR = os.environ.get("MODEL_DIR", "models")
MODEL_PATH = os.path.join(MODEL_DIR, "restaurant_score_v0.pkl")
META_PATH = os.path.join(MODEL_DIR, "restaurant_score_v0.meta.json")


def _build_training_df(db: Session) -> pd.DataFrame:
    """
    Build training data from restaurant POI density.

    Training target: demand-potential proxy — areas with many highly-rated
    restaurants of a given category indicate proven demand. We use
    restaurant count x avg rating as a composite proxy.

    Features include:
    - restaurant_count: total restaurants in the H3 cell
    - avg_rating: average rating of restaurants
    - platform_count: number of delivery platform listings
    - platform_diversity: number of distinct delivery platforms
    - neighbor_competition: same-category restaurants in surrounding cells
    - neighbor_total: total restaurants in surrounding cells
    - population: population in the H3 cell
    - category (one-hot encoded)
    """
    try:
        import h3
    except ImportError:
        raise RuntimeError("h3 library required for training")

    # Get all restaurant POIs with coordinates
    pois = (
        db.query(
            RestaurantPOI.lat,
            RestaurantPOI.lon,
            RestaurantPOI.category,
            RestaurantPOI.rating,
            RestaurantPOI.source,
        )
        .filter(RestaurantPOI.lat.isnot(None))
        .filter(RestaurantPOI.lon.isnot(None))
        .all()
    )

    if not pois:
        raise RuntimeError("No restaurant POI data available for training")

    # Aggregate by H3 cell x category
    h3_data: dict[tuple[str, str], dict] = {}

    for lat, lon, category, rating, source in pois:
        lat_f, lon_f = float(lat), float(lon)
        h3_idx = h3.latlng_to_cell(lat_f, lon_f, 8)
        key = (h3_idx, category)

        if key not in h3_data:
            h3_data[key] = {
                "h3_index": h3_idx,
                "category": category,
                "lat": lat_f,
                "lon": lon_f,
                "count": 0,
                "rating_sum": 0.0,
                "rated_count": 0,
                "platform_count": 0,
                "platform_sources": set(),
            }

        h3_data[key]["count"] += 1
        if rating:
            h3_data[key]["rating_sum"] += float(rating)
            h3_data[key]["rated_count"] += 1
        if source in PLATFORM_SOURCES:
            h3_data[key]["platform_count"] += 1
            h3_data[key]["platform_sources"].add(source)

    # Build feature rows
    items = []
    for (h3_idx, category), data in h3_data.items():
        avg_rating = (
            data["rating_sum"] / data["rated_count"] if data["rated_count"] > 0 else 3.5
        )

        # Get population for this cell
        pop_row = (
            db.query(PopulationDensity.population)
            .filter_by(h3_index=h3_idx)
            .first()
        )
        population = float(pop_row[0]) if pop_row and pop_row[0] else 0.0

        # Compute neighboring cell stats
        neighbors = h3.grid_disk(h3_idx, 1)
        neighbor_same_cat = sum(
            h3_data.get((n, category), {}).get("count", 0)
            for n in neighbors
            if n != h3_idx
        )
        neighbor_total = sum(
            sum(
                h3_data.get((n, cat), {}).get("count", 0)
                for cat in CATEGORIES
            )
            for n in neighbors
            if n != h3_idx
        )

        # Target: normalized demand proxy
        target = min(100.0, data["count"] * avg_rating * 5)

        items.append({
            "h3_index": h3_idx,
            "category": category,
            "restaurant_count": data["count"],
            "avg_rating": avg_rating,
            "platform_count": data["platform_count"],
            "platform_diversity": len(data["platform_sources"]),
            "neighbor_competition": neighbor_same_cat,
            "neighbor_total": neighbor_total,
            "population": population,
            "target": target,
        })

    df = pd.DataFrame(items)
    logger.info("Built training DataFrame with %d rows", len(df))
    return df


def train_and_save() -> dict:
    """Train the restaurant location demand-potential model and save artifacts."""
    os.makedirs(MODEL_DIR, exist_ok=True)

    db = SessionLocal()
    try:
        df = _build_training_df(db)
    finally:
        db.close()

    if df.empty or len(df) < 10:
        logger.warning("Insufficient training data (%d rows), skipping", len(df))
        return {"model_path": None, "metrics": {"error": "insufficient_data"}}

    feature_cols = [
        "restaurant_count",
        "avg_rating",
        "platform_count",
        "platform_diversity",
        "neighbor_competition",
        "neighbor_total",
        "population",
    ]

    # One-hot encode category
    df_encoded = pd.get_dummies(df, columns=["category"], prefix="cat")
    cat_cols = [c for c in df_encoded.columns if c.startswith("cat_")]
    all_features = feature_cols + cat_cols

    X = df_encoded[all_features]
    y = df_encoded["target"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = GradientBoostingRegressor(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.08,
        min_samples_leaf=8,
        subsample=0.8,
        random_state=42,
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mae = float(mean_absolute_error(y_test, y_pred))
    r2 = float(r2_score(y_test, y_pred))

    # Extract feature importances
    importances = dict(zip(all_features, model.feature_importances_.tolist()))

    joblib.dump(model, MODEL_PATH, compress=3)

    meta = {
        "mae": mae,
        "r2": r2,
        "n_rows": len(df),
        "n_train": len(X_train),
        "n_test": len(X_test),
        "features": all_features,
        "feature_importances": importances,
        "trained_at": str(date.today()),
        "model_version": "v3_enhanced",
        "platform_sources": sorted(PLATFORM_SOURCES),
    }
    with open(META_PATH, "w") as f:
        json.dump(meta, f, indent=2)

    try:
        with mlflow.start_run(run_name="restaurant_demand_potential_v3"):
            mlflow.log_params({
                "model": "GradientBoostingRegressor",
                "n_estimators": 200,
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
        "Restaurant score model trained: MAE=%.2f, R2=%.3f, n=%d",
        mae, r2, len(df),
    )
    return {"model_path": MODEL_PATH, "metrics": meta}


if __name__ == "__main__":
    print(train_and_save())
