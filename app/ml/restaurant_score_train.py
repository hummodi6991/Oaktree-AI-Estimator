"""
ML model training for restaurant location demand-potential scoring.

Trains a GradientBoostingRegressor to calibrate location demand-potential
scores based on features like competition density, population,
traffic, commercial density, delivery platform coverage, anchor proximity,
income proxy, and Google-enriched signals (rating, review count,
price level, confidence).

Training is scoped to Riyadh (lat 24.20–25.10, lon 46.20–47.30) to
match the Google enrichment pipeline's coverage area.

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
import numpy as np
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

# Riyadh bounding box — matches the Google enrichment pipeline scope
RIYADH_LAT_MIN, RIYADH_LAT_MAX = 24.20, 25.10
RIYADH_LON_MIN, RIYADH_LON_MAX = 46.20, 47.30


def _build_training_df(db: Session) -> pd.DataFrame:
    """
    Build training data from restaurant POI density (Riyadh only).

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
    - google_rating: average Google rating in the cell
    - google_review_count: total Google review count in the cell
    - log_review_count: log1p(google_review_count)
    - google_price_level: average Google price level in the cell
    - google_confidence: average Google match confidence in the cell
    - has_google: fraction of POIs with a Google place ID
    - category (one-hot encoded)
    """
    try:
        import h3
    except ImportError:
        raise RuntimeError("h3 library required for training")

    # Get all restaurant POIs within Riyadh bbox
    pois = (
        db.query(
            RestaurantPOI.lat,
            RestaurantPOI.lon,
            RestaurantPOI.category,
            RestaurantPOI.rating,
            RestaurantPOI.source,
            RestaurantPOI.review_count,
            RestaurantPOI.price_level,
            RestaurantPOI.google_place_id,
            RestaurantPOI.google_confidence,
        )
        .filter(RestaurantPOI.lat.isnot(None))
        .filter(RestaurantPOI.lon.isnot(None))
        .filter(RestaurantPOI.lat >= RIYADH_LAT_MIN)
        .filter(RestaurantPOI.lat <= RIYADH_LAT_MAX)
        .filter(RestaurantPOI.lon >= RIYADH_LON_MIN)
        .filter(RestaurantPOI.lon <= RIYADH_LON_MAX)
        .all()
    )

    if not pois:
        raise RuntimeError("No restaurant POI data available for training (Riyadh bbox)")

    # Aggregate by H3 cell x category
    h3_data: dict[tuple[str, str], dict] = {}

    for row in pois:
        lat, lon, category, rating, source = row[0], row[1], row[2], row[3], row[4]
        review_count, price_level, google_place_id, google_conf = (
            row[5], row[6], row[7], row[8],
        )
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
                # Google-enriched accumulators
                "google_rating_sum": 0.0,
                "google_rated_count": 0,
                "google_review_total": 0,
                "google_price_sum": 0.0,
                "google_price_count": 0,
                "google_conf_sum": 0.0,
                "google_conf_count": 0,
                "google_count": 0,
            }

        h3_data[key]["count"] += 1
        if rating:
            h3_data[key]["rating_sum"] += float(rating)
            h3_data[key]["rated_count"] += 1
        if source in PLATFORM_SOURCES:
            h3_data[key]["platform_count"] += 1
            h3_data[key]["platform_sources"].add(source)

        # Google-enriched fields
        if google_place_id:
            h3_data[key]["google_count"] += 1
            if rating:
                h3_data[key]["google_rating_sum"] += float(rating)
                h3_data[key]["google_rated_count"] += 1
            if review_count:
                h3_data[key]["google_review_total"] += int(review_count)
            if price_level:
                h3_data[key]["google_price_sum"] += float(price_level)
                h3_data[key]["google_price_count"] += 1
            if google_conf:
                h3_data[key]["google_conf_sum"] += float(google_conf)
                h3_data[key]["google_conf_count"] += 1

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

        # Google-derived features
        g = data
        google_rating = (
            g["google_rating_sum"] / g["google_rated_count"]
            if g["google_rated_count"] > 0 else 0.0
        )
        google_review_count = g["google_review_total"]
        log_review_count = float(np.log1p(google_review_count))
        google_price_level = (
            g["google_price_sum"] / g["google_price_count"]
            if g["google_price_count"] > 0 else 0.0
        )
        google_confidence = (
            g["google_conf_sum"] / g["google_conf_count"]
            if g["google_conf_count"] > 0 else 0.0
        )
        has_google = g["google_count"] / g["count"] if g["count"] > 0 else 0.0

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
            "google_rating": google_rating,
            "google_review_count": google_review_count,
            "log_review_count": log_review_count,
            "google_price_level": google_price_level,
            "google_confidence": google_confidence,
            "has_google": has_google,
            "target": target,
        })

    df = pd.DataFrame(items)
    logger.info("Built training DataFrame with %d rows (Riyadh only)", len(df))
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
        "google_rating",
        "google_review_count",
        "log_review_count",
        "google_price_level",
        "google_confidence",
        "has_google",
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
        "model_version": "v4_google_riyadh",
        "platform_sources": sorted(PLATFORM_SOURCES),
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

    try:
        with mlflow.start_run(run_name="restaurant_demand_potential_v4_google"):
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

    # Smoke-test summary
    has_google_pct = (
        df["has_google"].mean() * 100.0 if "has_google" in df.columns else 0.0
    )
    sorted_imp = sorted(importances.items(), key=lambda x: x[1], reverse=True)
    top15 = sorted_imp[:15]

    print("\n" + "=" * 60)
    print("TRAINING SMOKE TEST")
    print("=" * 60)
    print(f"  Training rows:          {len(df)}")
    print(f"  has_google = true:      {has_google_pct:.1f}%")
    print(f"  MAE:                    {mae:.2f}")
    print(f"  R2:                     {r2:.3f}")
    print(f"\n  Top 15 feature importances:")
    for rank, (feat, imp) in enumerate(top15, 1):
        print(f"    {rank:2d}. {feat:<30s} {imp:.4f}")
    print("=" * 60 + "\n")

    return {"model_path": MODEL_PATH, "metrics": meta}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = train_and_save()
    print(json.dumps(result.get("metrics", {}), indent=2))
