from __future__ import annotations
from typing import Any
import os, json, math
from datetime import date
import joblib
import pandas as pd
import mlflow
from sqlalchemy import func
from sqlalchemy.orm import Session
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_percentage_error

from app.db.session import SessionLocal
from app.models.tables import LandUseResidentialShare, SaleComp

MODEL_DIR = os.environ.get("MODEL_DIR", "models")
MODEL_PATH = os.path.join(MODEL_DIR, "hedonic_v0.pkl")
META_PATH  = os.path.join(MODEL_DIR, "hedonic_v0.meta.json")


def _norm(s: str | None) -> str:
    return (s or "").strip()


def _load_df(db: Session) -> pd.DataFrame:
    """
    Build the training dataframe from sale_comp.

    - Uses Riyadh land comps 2024 + Kaggle aqar land rows
    - Drops REGA indicator rows (they are not sale comps)
    - Cleans obvious junk and outliers
    """

    rows = (
        db.query(
            SaleComp.asof_date,
            SaleComp.date,
            SaleComp.city,
            SaleComp.district,
            SaleComp.net_area_m2,
            SaleComp.price_per_m2,
            SaleComp.source,
            LandUseResidentialShare.residential_share,
        )
        .filter(SaleComp.asset_type == "land")
        .filter(SaleComp.price_per_m2.isnot(None))
        .filter(SaleComp.net_area_m2.isnot(None))
        .filter(SaleComp.price_per_m2 > 0)
        .filter(SaleComp.net_area_m2 > 0)
        .filter(
            SaleComp.source.in_(
                ["kaggle_aqar", "riyadh_land_comps_2024"]
            )
        )
        .outerjoin(
            LandUseResidentialShare,
            func.lower(SaleComp.city)
            == func.lower(LandUseResidentialShare.city),
        )
        .all()
    )

    items: list[dict[str, Any]] = []
    for r in rows:
        city = _norm(r.city)
        district = _norm(r.district) or f"{city} – citywide"

        if not city:
            continue

        asof = r.asof_date or r.date or date(2024, 1, 1)
        ym = asof.strftime("%Y-%m")

        area = float(r.net_area_m2)
        price_per_m2 = float(r.price_per_m2)

        if area <= 0 or price_per_m2 <= 0:
            continue

        items.append(
            {
                "city": city,
                "district": district,
                "ym": ym,
                "log_area": math.log(area),
                "price_per_m2": price_per_m2,
                "residential_share": float(r.residential_share or 0.4),
                "source": r.source,
            }
        )

    df = pd.DataFrame(items)
    if df.empty:
        raise RuntimeError("No training data for hedonic model")

    # Robust outlier clip so one crazy comp doesn’t dominate
    lo, hi = df["price_per_m2"].quantile([0.01, 0.99])
    df = df[(df["price_per_m2"] >= lo) & (df["price_per_m2"] <= hi)]

    return df


def train_and_save() -> dict:
    os.makedirs(MODEL_DIR, exist_ok=True)
    db = SessionLocal()
    try:
        df = _load_df(db)
    finally:
        db.close()

    # Target: prefer explicit price_per_m2; fall back to legacy 'ppm2'
    if "price_per_m2" in df.columns:
        y = df["price_per_m2"].astype(float)
    elif "ppm2" in df.columns:
        y = df["ppm2"].astype(float)
    else:
        raise ValueError(
            "Training dataframe is missing both 'price_per_m2' and 'ppm2' columns"
        )

    feature_cols = ["city", "district", "ym", "log_area", "residential_share"]
    X = df[feature_cols]

    pre = ColumnTransformer(
        [
            ("cat", OneHotEncoder(handle_unknown="ignore"), ["city", "district", "ym"]),
            ("num", "passthrough", ["log_area", "residential_share"]),
        ]
    )

    # SMALL, SHALLOW FOREST → tiny .pkl, still decent accuracy
    rf = RandomForestRegressor(
        n_estimators=80,        # was much higher
        max_depth=14,          # keep trees shallow
        min_samples_leaf=20,   # no tiny leaves
        n_jobs=-1,
        random_state=42,
    )

    model = Pipeline([("pre", pre), ("rf", rf)])

    n = len(df)
    cut = max(10, int(n * 0.85))
    model.fit(X.iloc[:cut], y.iloc[:cut])

    mape = float(
        mean_absolute_percentage_error(
            y.iloc[cut:], model.predict(X.iloc[cut:])
        )
    ) if n > cut else None

    # IMPORTANT: compress so the file is small on disk
    joblib.dump(model, MODEL_PATH, compress=3)

    meta = {"mape_holdout": mape, "n_rows": n}
    with open(META_PATH, "w") as f:
        json.dump(meta, f)

    with mlflow.start_run(run_name="hedonic_v0"):
        mlflow.log_params(
            {
                "model": "RandomForestRegressor",
                "n_estimators": rf.n_estimators,
                "max_depth": rf.max_depth,
                "min_samples_leaf": rf.min_samples_leaf,
                "features": "city,district,ym,log_area,residential_share",
            }
        )
        metrics = {"n_rows": n}
        if mape is not None:
            metrics["mape_holdout"] = mape
        mlflow.log_metrics(metrics)
        mlflow.log_artifact(MODEL_PATH)
        mlflow.log_artifact(META_PATH)

    return {"model_path": MODEL_PATH, "metrics": meta}


if __name__ == "__main__":
    print(train_and_save())
