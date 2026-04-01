"""
Train a profitability model for candidate_location.

Training approach:
  1. Compute spatial features for all primary candidates via batch SQL
  2. Compute success_proxy target for Tier 2 candidates
  3. Train XGBoost regressor on Tier 2 data
  4. Score ALL candidates (Tier 1, 2, 3)
  5. Update candidate_location with scores

Success proxy (target variable):
  success_index = normalize(
      0.5 * log1p(total_rating_count)
    + 0.3 * (avg_rating - 1) / 4
    + 0.2 * platform_count / max_platforms
  ) * 100

Features (all computable for any location):
  - population_1km: sum of population within 1km
  - competitor_count_1km: restaurant_poi count within 1km
  - delivery_density_1km: delivery_source_record count within 1km
  - rent_sar_m2_month: from Patch 2 interpolation
  - area_sqm: actual or inferred
  - district_encoded: target-encoded district
  - source_tier: 1, 2, or 3

Usage:
    python -m app.ml.profitability_train
"""

import json
import logging
import os
import time
from datetime import date, datetime
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import SessionLocal

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

MODEL_DIR = "models"
MODEL_PATH = os.path.join(MODEL_DIR, "profitability_v1.pkl")
META_PATH = os.path.join(MODEL_DIR, "profitability_v1.meta.json")


# ---------------------------------------------------------------------------
# Step 1: Compute spatial features for all primary candidates
# ---------------------------------------------------------------------------

def _compute_features(db: Session) -> pd.DataFrame:
    """Batch-compute spatial features for all primary candidates.

    Uses aggregate spatial joins instead of per-row LATERAL to keep
    query count constant regardless of candidate count.
    """
    logger.info("Computing spatial features for all primary candidates...")

    # Base candidate data
    base_sql = text("""
        SELECT
            cl.id,
            cl.source_tier,
            cl.lat::float AS lat,
            cl.lon::float AS lon,
            COALESCE(cl.area_sqm, 120)::float AS area_sqm,
            COALESCE(cl.rent_sar_m2_month, 75)::float AS rent_m2_month,
            cl.district_ar,
            cl.avg_rating::float AS avg_rating,
            cl.total_rating_count,
            COALESCE(cl.platform_count, 0) AS platform_count,
            cl.current_category
        FROM candidate_location cl
        WHERE cl.is_cluster_primary = TRUE
          AND cl.geom IS NOT NULL
    """)
    rows = db.execute(base_sql).mappings().all()
    df = pd.DataFrame([dict(r) for r in rows])
    logger.info("Base data: %d candidates", len(df))

    if df.empty:
        return df

    # Population within 1km
    logger.info("Computing population density...")
    pop_sql = text("""
        SELECT cl.id, COALESCE(SUM(pd.population), 0)::float AS pop_1km
        FROM candidate_location cl
        LEFT JOIN population_density pd
            ON ST_DWithin(cl.geom::geography, pd.geom::geography, 1000)
        WHERE cl.is_cluster_primary = TRUE AND cl.geom IS NOT NULL
        GROUP BY cl.id
    """)
    pop_rows = db.execute(pop_sql).mappings().all()
    pop_df = pd.DataFrame([dict(r) for r in pop_rows])
    if not pop_df.empty:
        df = df.merge(pop_df, on="id", how="left")
    else:
        df["pop_1km"] = 0.0
    df["pop_1km"] = df["pop_1km"].fillna(0.0)

    # Competitor count within 1km
    logger.info("Computing competitor density...")
    comp_sql = text("""
        SELECT cl.id, COUNT(rp.id)::int AS competitor_count_1km
        FROM candidate_location cl
        LEFT JOIN restaurant_poi rp
            ON ST_DWithin(cl.geom::geography, rp.geom::geography, 1000)
        WHERE cl.is_cluster_primary = TRUE AND cl.geom IS NOT NULL
        GROUP BY cl.id
    """)
    comp_rows = db.execute(comp_sql).mappings().all()
    comp_df = pd.DataFrame([dict(r) for r in comp_rows])
    if not comp_df.empty:
        df = df.merge(comp_df, on="id", how="left")
    else:
        df["competitor_count_1km"] = 0
    df["competitor_count_1km"] = df["competitor_count_1km"].fillna(0)

    # Delivery density within 1km
    logger.info("Computing delivery density...")
    del_sql = text("""
        SELECT cl.id, COUNT(dsr.id)::int AS delivery_density_1km
        FROM candidate_location cl
        LEFT JOIN delivery_source_record dsr
            ON ST_DWithin(cl.geom::geography, dsr.geom::geography, 1000)
            AND dsr.lat IS NOT NULL
        WHERE cl.is_cluster_primary = TRUE AND cl.geom IS NOT NULL
        GROUP BY cl.id
    """)
    del_rows = db.execute(del_sql).mappings().all()
    del_df = pd.DataFrame([dict(r) for r in del_rows])
    if not del_df.empty:
        df = df.merge(del_df, on="id", how="left")
    else:
        df["delivery_density_1km"] = 0
    df["delivery_density_1km"] = df["delivery_density_1km"].fillna(0)

    logger.info(
        "Feature computation complete: %d candidates with %d features",
        len(df),
        len(df.columns),
    )
    return df


# ---------------------------------------------------------------------------
# Step 2: Compute success proxy target for Tier 2
# ---------------------------------------------------------------------------

def _compute_success_proxy(df: pd.DataFrame) -> pd.DataFrame:
    """Compute the success_proxy target for Tier 2 candidates.

    success_index = normalize(
        0.5 * log1p(total_rating_count)
      + 0.3 * (avg_rating - 1) / 4  [normalized 0-1]
      + 0.2 * platform_count / max_platforms
    ) * 100
    """
    tier2 = df[df["source_tier"] == 2].copy()

    if tier2.empty:
        logger.warning("No Tier 2 candidates for training")
        return df

    max_platforms = max(tier2["platform_count"].max(), 1)
    max_log_reviews = max(np.log1p(tier2["total_rating_count"].fillna(0)).max(), 1)

    tier2["success_proxy"] = (
        0.5 * np.log1p(tier2["total_rating_count"].fillna(0)) / max_log_reviews
        + 0.3 * (tier2["avg_rating"].fillna(3.0) - 1.0) / 4.0
        + 0.2 * tier2["platform_count"].fillna(0) / max_platforms
    ) * 100.0

    # Clip to 0-100
    tier2["success_proxy"] = tier2["success_proxy"].clip(0, 100)

    # Merge back
    df = df.merge(
        tier2[["id", "success_proxy"]],
        on="id",
        how="left",
        suffixes=("", "_computed"),
    )
    if "success_proxy_computed" in df.columns:
        df["success_proxy"] = df["success_proxy_computed"].combine_first(
            df.get("success_proxy", pd.Series(dtype=float))
        )
        df.drop(columns=["success_proxy_computed"], inplace=True)

    return df


# ---------------------------------------------------------------------------
# Step 3: Train model
# ---------------------------------------------------------------------------

FEATURE_COLS = [
    "pop_1km",
    "competitor_count_1km",
    "delivery_density_1km",
    "rent_m2_month",
    "area_sqm",
    "source_tier",
]


def _train_model(df: pd.DataFrame) -> tuple[Any, dict]:
    """Train GradientBoostingRegressor on Tier 2 data."""
    os.makedirs(MODEL_DIR, exist_ok=True)

    # Filter to Tier 2 with valid success_proxy
    train_df = df[(df["source_tier"] == 2) & (df["success_proxy"].notna())].copy()

    if len(train_df) < 50:
        logger.warning("Insufficient training data (%d rows)", len(train_df))
        return None, {"error": "insufficient_data", "n_rows": len(train_df)}

    # Target-encode district (mean success_proxy per district)
    district_means = train_df.groupby("district_ar")["success_proxy"].mean()
    global_mean = train_df["success_proxy"].mean()
    df["district_encoded"] = df["district_ar"].map(district_means).fillna(global_mean)
    train_df["district_encoded"] = train_df["district_ar"].map(district_means).fillna(
        global_mean
    )

    all_features = FEATURE_COLS + ["district_encoded"]

    X = train_df[all_features].fillna(0)
    y = train_df["success_proxy"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = GradientBoostingRegressor(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.08,
        min_samples_leaf=10,
        subsample=0.8,
        random_state=42,
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mae = float(mean_absolute_error(y_test, y_pred))
    r2 = float(r2_score(y_test, y_pred))

    # Feature importances
    importances = dict(zip(all_features, model.feature_importances_.tolist()))

    # Save model
    joblib.dump(model, MODEL_PATH, compress=3)

    meta = {
        "mae": mae,
        "r2": r2,
        "n_rows": len(train_df),
        "n_train": len(X_train),
        "n_test": len(X_test),
        "features": all_features,
        "feature_importances": importances,
        "trained_at": str(date.today()),
        "model_version": "profitability_v1",
        "district_encoding": {k: round(v, 2) for k, v in district_means.items()},
        "global_mean": round(global_mean, 2),
    }
    with open(META_PATH, "w") as f:
        json.dump(meta, f, indent=2)

    logger.info("Model trained: MAE=%.2f, R2=%.3f, n=%d", mae, r2, len(train_df))

    # Print summary
    sorted_imp = sorted(importances.items(), key=lambda x: x[1], reverse=True)
    print("\n" + "=" * 60)
    print("PROFITABILITY MODEL TRAINING SUMMARY")
    print("=" * 60)
    print(f"  Training rows:    {len(train_df)}")
    print(f"  Train / Test:     {len(X_train)} / {len(X_test)}")
    print(f"  MAE:              {mae:.2f}")
    print(f"  R2:               {r2:.3f}")
    print(f"\n  Feature importances:")
    for rank, (feat, imp) in enumerate(sorted_imp, 1):
        print(f"    {rank}. {feat:<25s} {imp:.4f}")
    print("=" * 60 + "\n")

    return model, meta


# ---------------------------------------------------------------------------
# Step 4: Score all candidates
# ---------------------------------------------------------------------------

def _score_all_candidates(
    db: Session, model: Any, df: pd.DataFrame, meta: dict
) -> int:
    """Apply the model to all primary candidates and update the table."""
    all_features = meta["features"]

    # Ensure district_encoded exists for all rows
    if "district_encoded" not in df.columns:
        district_encoding = meta.get("district_encoding", {})
        global_mean = meta.get("global_mean", 50.0)
        df["district_encoded"] = (
            df["district_ar"].map(district_encoding).fillna(global_mean)
        )

    X_all = df[all_features].fillna(0)
    predictions = model.predict(X_all)

    # Clip to 0-100
    predictions = np.clip(predictions, 0, 100)

    # Build update data
    now = datetime.utcnow().isoformat()
    version = meta.get("model_version", "profitability_v1")

    updated = 0
    batch_size = 500
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i : i + batch_size]
        batch_preds = predictions[i : i + batch_size]

        values = []
        params: dict[str, Any] = {}
        for j, (_, row) in enumerate(batch.iterrows()):
            idx = i + j

            params[f"id_{idx}"] = int(row["id"])
            params[f"score_{idx}"] = round(float(batch_preds[j]), 2)
            params[f"proxy_{idx}"] = (
                round(float(row["success_proxy"]), 2)
                if pd.notna(row.get("success_proxy"))
                else None
            )
            params[f"features_{idx}"] = json.dumps(
                {feat: round(float(row.get(feat, 0)), 4) for feat in all_features}
            )

            values.append(
                f"(:id_{idx}, :score_{idx}, :proxy_{idx}, :features_{idx})"
            )

        if not values:
            continue

        values_sql = ", ".join(values)
        sql = text(f"""
            UPDATE candidate_location cl
            SET profitability_score = v.score::numeric,
                success_proxy = v.proxy::numeric,
                model_features = v.features::jsonb,
                model_version = :version,
                model_scored_at = :scored_at
            FROM (VALUES {values_sql}) AS v(id, score, proxy, features)
            WHERE cl.id = v.id
        """)
        params["version"] = version
        params["scored_at"] = now

        db.execute(sql, params)
        updated += len(batch)

    db.commit()
    logger.info("Scored %d candidates with profitability model", updated)
    return updated


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def train_and_score() -> dict:
    """Full pipeline: compute features -> train -> score all candidates."""
    t_start = time.time()

    db = SessionLocal()
    try:
        # Step 1: Features
        df = _compute_features(db)
        if df.empty:
            return {"error": "no_candidates"}

        # Step 2: Success proxy
        df = _compute_success_proxy(df)

        # Step 3: Train
        model, meta = _train_model(df)
        if model is None:
            return meta

        # Step 4: Score all
        scored = _score_all_candidates(db, model, df, meta)

        meta["candidates_scored"] = scored
        meta["elapsed_s"] = round(time.time() - t_start, 1)

        # Save updated meta
        with open(META_PATH, "w") as f:
            json.dump(meta, f, indent=2)

        return meta

    finally:
        db.close()


def main():
    result = train_and_score()
    print("\n=== Profitability Model Results ===")
    for k, v in result.items():
        if k not in ("features", "feature_importances", "district_encoding"):
            print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
