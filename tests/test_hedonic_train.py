import math
import os

import joblib
import pandas as pd

from app.ml import hedonic_train


class _DummySession:
    def close(self):
        pass


class _DummyRun:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_trainer_uses_residential_share(tmp_path, monkeypatch):
    fake_df = pd.DataFrame(
        {
            "price_per_m2": [1000.0, 1100.0, 1050.0, 1200.0],
            "city": ["Riyadh", "Riyadh", "Jeddah", "Jeddah"],
            "district": ["Olaya", "Olaya", "Aziziyah", "Aziziyah"],
            "ym": ["2024-01", "2024-02", "2024-01", "2024-02"],
            "log_area": [math.log(150.0), math.log(200.0), math.log(175.0), math.log(160.0)],
            "residential_share": [0.35, 0.4, 0.25, 0.3],
        }
    )

    monkeypatch.setattr(hedonic_train, "_load_df", lambda db: fake_df)
    monkeypatch.setattr(hedonic_train, "SessionLocal", lambda: _DummySession())

    model_dir = tmp_path / "models"
    monkeypatch.setattr(hedonic_train, "MODEL_DIR", str(model_dir))
    monkeypatch.setattr(hedonic_train, "MODEL_PATH", str(model_dir / "hedonic_v0.pkl"))
    monkeypatch.setattr(hedonic_train, "META_PATH", str(model_dir / "hedonic_v0.meta.json"))

    monkeypatch.setattr(hedonic_train.mlflow, "start_run", lambda run_name=None: _DummyRun())
    monkeypatch.setattr(hedonic_train.mlflow, "log_params", lambda params: None)
    monkeypatch.setattr(hedonic_train.mlflow, "log_metrics", lambda metrics: None)
    monkeypatch.setattr(hedonic_train.mlflow, "log_artifact", lambda path: None)

    result = hedonic_train.train_and_save()
    assert os.path.exists(result["model_path"])

    model = joblib.load(result["model_path"])
    sample = pd.DataFrame(
        {
            "city": ["Riyadh"],
            "district": ["Olaya"],
            "ym": ["2024-03"],
            "log_area": [math.log(180.0)],
            "residential_share": [0.42],
        }
    )
    prediction = model.predict(sample)
    assert prediction.shape == (1,)
