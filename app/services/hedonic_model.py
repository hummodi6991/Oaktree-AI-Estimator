import os, json
import joblib
from datetime import date
from typing import Any, Dict, Optional
import pandas as pd

_MODEL, _META = None, {}
_BASE = os.environ.get("MODEL_DIR", "models")
_MODEL_PATH = os.path.join(_BASE, "hedonic_v0.pkl")
_META_PATH  = os.path.join(_BASE, "hedonic_v0.meta.json")


def _norm(s: str | None) -> str:
    return (s or "").strip().lower()


def try_load_model():
    global _MODEL, _META
    if _MODEL is None and os.path.exists(_MODEL_PATH):
        _MODEL = joblib.load(_MODEL_PATH)
        if os.path.exists(_META_PATH):
            with open(_META_PATH, "r") as f:
                _META = json.load(f)
    return _MODEL

def predict_ppm2(
    city: str | None,
    district: str | None = None,
    on: date | None = None,
    default_city: str = "Riyadh",
) -> tuple[Optional[float], Dict[str, Any]]:
    model = try_load_model()
    if model is None:
        return None, {"model_used": False, "reason": "model_not_loaded"}

    if not city:
        city = default_city

    ref_date = on or date.today()
    ym = ref_date.strftime("%Y-%m")

    df = pd.DataFrame(
        [
            {
                "city": _norm(city),
                "district": _norm(district),
                "ym": ym,
                "log_area": 6.5,  # about 665 m², neutral parcel size
                "residential_share": 0.0,  # until land_use_stat is fully populated
            }
        ]
    )

    try:
        val = float(model.predict(df)[0])
        return val, {"model_used": True, "ym": ym, **_META}
    except Exception as exc:
        return None, {"model_used": False, "reason": f"predict_failed: {exc}"}
