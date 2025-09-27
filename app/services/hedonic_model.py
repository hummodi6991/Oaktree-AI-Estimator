import os, json
import joblib
from datetime import date
import pandas as pd

_MODEL, _META = None, {}
_BASE = os.environ.get("MODEL_DIR", "models")
_MODEL_PATH = os.path.join(_BASE, "hedonic_v0.pkl")
_META_PATH  = os.path.join(_BASE, "hedonic_v0.meta.json")

def try_load_model():
    global _MODEL, _META
    if _MODEL is None and os.path.exists(_MODEL_PATH):
        _MODEL = joblib.load(_MODEL_PATH)
        if os.path.exists(_META_PATH):
            with open(_META_PATH, "r") as f:
                _META = json.load(f)
    return _MODEL

def predict_ppm2(city: str | None, district: str | None, on: date | None):
    m = try_load_model()
    if m is None or city is None:
        return None, {"model_used": False}
    ym = (on or date.today()).strftime("%Y-%m")
    df = pd.DataFrame([{"city": city, "district": district or "", "ym": ym, "log_area": 6.5}])  # neutral area prior
    try:
        val = float(m.predict(df)[0])
        return val, {"model_used": True, **_META}
    except Exception:
        return None, {"model_used": False}
