from __future__ import annotations
from typing import Any, List
import os, json, math
import joblib
import pandas as pd
import mlflow
from sqlalchemy.orm import Session
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_percentage_error

from app.db.session import SessionLocal
from app.models.tables import SaleComp

MODEL_DIR = os.environ.get("MODEL_DIR", "models")
MODEL_PATH = os.path.join(MODEL_DIR, "hedonic_v0.pkl")
META_PATH  = os.path.join(MODEL_DIR, "hedonic_v0.meta.json")


def _norm(s: str | None) -> str:
    return (s or "").strip().lower()


def _load_df(db: Session) -> pd.DataFrame:
    q = (
        db.query(SaleComp)
        .filter(SaleComp.asset_type == "land")
        .filter(SaleComp.price_per_m2.isnot(None))
    )
    # If you want to be extra explicit about sources:
    # .filter(SaleComp.source.in_(["kaggle_aqar", "REGA_indicators", "riyadh_land_comps_2024"]))

    rows: List[SaleComp] = q.all()

    items: list[dict[str, Any]] = []
    for r in rows:
        if not r.city or not r.price_per_m2 or r.price_per_m2 <= 0:
            continue

        dt = r.date
        ym = dt.strftime("%Y-%m")

        items.append(
            {
                "date": dt,
                "city": _norm(r.city),
                "district": _norm(r.district),
                "ym": ym,
                "log_area": math.log(float(r.net_area_m2))
                if r.net_area_m2 and r.net_area_m2 > 0
                else 6.5,
                "residential_share": 0.0,  # until land_use stats are wired
                "ppm2": float(r.price_per_m2),
            }
        )

    df = pd.DataFrame(items)
    if df.empty:
        raise RuntimeError("No sale_comp rows available for hedonic training")

    return df


def train_and_save() -> dict:
    os.makedirs(MODEL_DIR, exist_ok=True)
    db = SessionLocal()
    try:
        df = _load_df(db)
    finally:
        db.close()

    y = df["ppm2"]
    X = df[["city", "district", "ym", "log_area", "residential_share"]]

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
