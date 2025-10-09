from __future__ import annotations
import os, json, math
import joblib
import pandas as pd
import mlflow
from sqlalchemy import text
from sqlalchemy.orm import Session
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_percentage_error

from app.db.session import SessionLocal
from app.models.tables import SaleComp, LandUseResidentialShare

MODEL_DIR = os.environ.get("MODEL_DIR", "models")
MODEL_PATH = os.path.join(MODEL_DIR, "hedonic_v0.pkl")
META_PATH  = os.path.join(MODEL_DIR, "hedonic_v0.meta.json")


def _ensure_residential_share_view(db: Session) -> None:
    db.execute(
        text(
            """
            CREATE OR REPLACE VIEW land_use_residential_share AS
            SELECT
                city,
                sub_municipality,
                CASE
                    WHEN NULLIF(SUM(CASE WHEN metric ILIKE '%area%' THEN value ELSE 0 END), 0) IS NULL
                        THEN NULL
                    ELSE SUM(
                        CASE
                            WHEN metric ILIKE '%area%' AND category ILIKE '%residential%'
                                THEN value
                            ELSE 0
                        END
                    ) / NULLIF(SUM(CASE WHEN metric ILIKE '%area%' THEN value ELSE 0 END), 0)
                END AS residential_share
            FROM land_use_stat
            WHERE value IS NOT NULL
            GROUP BY city, sub_municipality
            """
        )
    )
    db.commit()


def _load_df(db: Session) -> pd.DataFrame:
    _ensure_residential_share_view(db)
    rows = (
        db.query(
            SaleComp.date,
            SaleComp.city,
            SaleComp.district,
            SaleComp.asset_type,
            SaleComp.net_area_m2,
            SaleComp.price_per_m2,
            LandUseResidentialShare.residential_share,
        )
        .outerjoin(
            LandUseResidentialShare,
            (SaleComp.city == LandUseResidentialShare.city)
            & (SaleComp.district == LandUseResidentialShare.sub_municipality),
        )
        .all()
    )
    items = []
    for r in rows:
        items.append({
            "date": r.date,
            "city": r.city,
            "district": r.district,
            "asset_type": r.asset_type,
            "net_area_m2": float(r.net_area_m2) if r.net_area_m2 else None,
            "price_per_m2": float(r.price_per_m2) if r.price_per_m2 else None,
            "residential_share": float(r.residential_share) if r.residential_share is not None else None,
        })
    df = pd.DataFrame(items).dropna(subset=["price_per_m2", "city"])
    if df.empty:
        raise RuntimeError("No sale_comp rows with price_per_m2")
    df["ym"] = pd.to_datetime(df["date"]).dt.to_period("M").astype(str)
    df["log_area"] = df["net_area_m2"].fillna(df["net_area_m2"].median() or 1.0).apply(lambda x: math.log(max(1.0, x)))
    df["residential_share"] = pd.to_numeric(df["residential_share"], errors="coerce")
    if df["residential_share"].notna().any():
        df["residential_share"] = df["residential_share"].fillna(df["residential_share"].median())
    else:
        df["residential_share"] = 0.0
    return df


def train_and_save() -> dict:
    os.makedirs(MODEL_DIR, exist_ok=True)
    db = SessionLocal()
    try:
        df = _load_df(db)
    finally:
        db.close()

    y = df["price_per_m2"]
    X = df[["city","district","ym","log_area","residential_share"]]
    pre = ColumnTransformer([
        ("cat", OneHotEncoder(handle_unknown="ignore"), ["city","district","ym"]),
        ("num", "passthrough", ["log_area", "residential_share"]),
    ])
    model = Pipeline([("pre", pre), ("rf", RandomForestRegressor(n_estimators=300, random_state=42))])

    n = len(df)
    cut = max(10, int(n * 0.85))
    model.fit(X.iloc[:cut], y.iloc[:cut])
    mape = float(mean_absolute_percentage_error(y.iloc[cut:], model.predict(X.iloc[cut:]))) if n > cut else None

    joblib.dump(model, MODEL_PATH)
    meta = {"mape_holdout": mape, "n_rows": n}
    with open(META_PATH, "w") as f:
        json.dump(meta, f)

    with mlflow.start_run(run_name="hedonic_v0"):
        mlflow.log_params({
            "model": "RandomForestRegressor",
            "n_estimators": 300,
            "features": "city,district,ym,log_area,residential_share",
        })
        metrics = {"n_rows": n}
        if mape is not None:
            metrics["mape_holdout"] = mape
        mlflow.log_metrics(metrics)
        mlflow.log_artifact(MODEL_PATH)
        mlflow.log_artifact(META_PATH)
    return {"model_path": MODEL_PATH, "metrics": meta}


if __name__ == "__main__":
    print(train_and_save())
