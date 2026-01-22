import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tests.excel_inputs import sample_excel_inputs


def _load_app(monkeypatch: pytest.MonkeyPatch, db_url: str, **env: str):
    keys = [
        "AUTH_MODE",
        "API_KEYS_JSON",
        "ADMIN_API_KEYS_JSON",
        "API_KEY",
        "DATABASE_URL",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DATABASE_URL", db_url)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    import app.security.auth as auth  # type: ignore
    import app.db.session as session  # type: ignore
    import app.main as main  # type: ignore

    importlib.reload(auth)
    importlib.reload(session)
    return importlib.reload(main).app


def _init_db(db_path: Path) -> None:
    from app.db import session as db_session
    from app.models.tables import MarketIndicator, Rate, TaxRule, UsageEvent

    UsageEvent.__table__.create(db_session.engine, checkfirst=True)
    Rate.__table__.create(db_session.engine, checkfirst=True)
    TaxRule.__table__.create(db_session.engine, checkfirst=True)
    MarketIndicator.__table__.create(db_session.engine, checkfirst=True)


def test_usage_event_logging_and_summary(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "usage.db"
    db_url = f"sqlite:///{db_path}?check_same_thread=false"
    app = _load_app(
        monkeypatch,
        db_url,
        AUTH_MODE="api_key",
        ADMIN_API_KEYS_JSON='{"ceo": "ceo-key"}',
        API_KEYS_JSON='{"tester": "tester-key"}',
    )
    _init_db(db_path)
    client = TestClient(app)

    response = client.get("/v1/indices/rates", headers={"X-API-Key": "tester-key"})
    assert response.status_code == 200

    from app.db.session import SessionLocal
    from app.models.tables import UsageEvent

    with SessionLocal() as db:
        events = (
            db.query(UsageEvent)
            .filter(UsageEvent.user_id == "tester", UsageEvent.is_admin.is_(False))
            .all()
        )
    assert len(events) >= 1

    summary = client.get("/v1/admin/usage/summary", headers={"X-API-Key": "ceo-key"})
    assert summary.status_code == 200
    payload = summary.json()
    assert payload["totals"]["requests"] >= 1
    assert payload["totals"]["active_users"] >= 1


def test_estimate_result_usage_event(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "usage.db"
    db_url = f"sqlite:///{db_path}?check_same_thread=false"
    app = _load_app(
        monkeypatch,
        db_url,
        AUTH_MODE="api_key",
        ADMIN_API_KEYS_JSON='{"ceo": "ceo-key"}',
        API_KEYS_JSON='{"tester": "tester-key"}',
    )
    _init_db(db_path)
    client = TestClient(app)

    poly = {
        "type": "Polygon",
        "coordinates": [
            [
                [46.675, 24.713],
                [46.676, 24.713],
                [46.676, 24.714],
                [46.675, 24.714],
                [46.675, 24.713],
            ]
        ],
    }
    payload = {
        "geometry": poly,
        "asset_program": "residential_midrise",
        "unit_mix": [{"type": "1BR", "count": 10}],
        "finish_level": "mid",
        "timeline": {"start": "2025-10-01", "months": 18},
        "financing_params": {"margin_bps": 250, "ltv": 0.6},
        "strategy": "build_to_sell",
        "excel_inputs": sample_excel_inputs(),
    }

    response = client.post("/v1/estimates", json=payload, headers={"X-API-Key": "tester-key"})
    assert response.status_code == 200

    from app.db.session import SessionLocal
    from app.models.tables import UsageEvent

    with SessionLocal() as db:
        event = (
            db.query(UsageEvent)
            .filter(UsageEvent.event_name == "estimate_result")
            .order_by(UsageEvent.ts.desc())
            .first()
        )

    assert event is not None
    assert isinstance(event.meta, dict)
    assert "land_price_overridden" in event.meta
