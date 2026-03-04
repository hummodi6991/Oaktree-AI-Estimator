"""
ML model training for restaurant location demand-potential scoring.

Trains a GradientBoostingRegressor to calibrate location demand-potential
scores based on features like competition density, population,
traffic, and commercial density.

IMPORTANT: This model learns a *demand-potential proxy* — it predicts
where restaurant demand is likely based on existing POI density and
ratings. It is NOT a profitability predictor. True profitability
requires outcome signals (merchant sales, order volumes) that are
not available in public data. Feature importances from the trained
model reveal which observable factors correlate most with proven
restaurant demand per category.

Follows the same pattern as hedonic_train.py.
"""

from __future__ import annotations

import json
import logging
import math
import os
from datetime import date, timedelta

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

logger = logging.getLogger(__name__)

MODEL_DIR = os.environ.get("MODEL_DIR", "models")
MODEL_PATH = os.path.join(MODEL_DIR, "restaurant_score_v0.pkl")
META_PATH = os.path.join(MODEL_DIR, "restaurant_score_v0.meta.json")


def _build_training_df(db: Session) -> pd.DataFrame:
    """
    Build training data from restaurant POI density.

    Training target: demand-potential proxy — areas with many highly-rated
    restaurants of a given category indicate proven demand. We use
    restaurant count x avg rating as a composite proxy. This is NOT a
    profitability label; true profitability requires merchant sales data.
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

    # Aggregate by H3 cell × category
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
            }

        h3_data[key]["count"] += 1
        if rating:
            h3_data[key]["rating_sum"] += float(rating)
            h3_data[key]["rated_count"] += 1
        if source in ("hungerstation", "talabat", "mrsool"):
            h3_data[key]["platform_count"] += 1

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

        # Compute neighboring cell stats (competition in surrounding cells)
        neighbors = h3.grid_disk(h3_idx, 1)
        neighbor_same_cat = sum(
            h3_data.get((n, category), {}).get("count", 0)
            for n in neighbors
            if n != h3_idx
        )

        # Target: normalized demand proxy
        # Higher count × higher rating = more demand proven
        target = min(100.0, data["count"] * avg_rating * 5)

        items.append({
            "h3_index": h3_idx,
            "category": category,
            "restaurant_count": data["count"],
            "avg_rating": avg_rating,
            "platform_count": data["platform_count"],
            "neighbor_competition": neighbor_same_cat,
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
        "neighbor_competition",
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
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        min_samples_leaf=10,
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
    }
    with open(META_PATH, "w") as f:
        json.dump(meta, f, indent=2)

    with mlflow.start_run(run_name="restaurant_demand_potential_v0"):
        mlflow.log_params({
            "model": "GradientBoostingRegressor",
            "n_estimators": 100,
            "max_depth": 5,
            "learning_rate": 0.1,
            "features": ",".join(all_features),
        })
        mlflow.log_metrics({"mae": mae, "r2": r2, "n_rows": len(df)})
        mlflow.log_artifact(MODEL_PATH)
        mlflow.log_artifact(META_PATH)

    logger.info(
        "Restaurant score model trained: MAE=%.2f, R²=%.3f, n=%d",
        mae, r2, len(df),
    )
    return {"model_path": MODEL_PATH, "metrics": meta}


if __name__ == "__main__":
    print(train_and_save())
