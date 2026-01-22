from datetime import datetime, timezone
import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


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
    from app.models.tables import UsageEvent

    UsageEvent.__table__.create(db_session.engine, checkfirst=True)


def test_analytics_event_requires_auth(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_url = f"sqlite:///{tmp_path / 'usage.db'}?check_same_thread=false"
    app = _load_app(
        monkeypatch,
        db_url,
        AUTH_MODE="api_key",
        ADMIN_API_KEYS_JSON='{"ceo": "ceo-key"}',
        API_KEYS_JSON='{"tester": "tester-key"}',
    )
    _init_db(tmp_path / "usage.db")
    client = TestClient(app)

    response = client.post("/v1/analytics/event", json={"event_name": "ui_test"})

    assert response.status_code in {401, 403}


def test_feedback_inbox_rollup(
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

    from app.db.session import SessionLocal
    from app.models.tables import UsageEvent

    with SessionLocal() as db:
        db.add_all(
            [
                UsageEvent(
                    ts=datetime.now(timezone.utc),
                    user_id="user-a",
                    is_admin=False,
                    event_name="feedback_vote",
                    method="POST",
                    path="/v1/analytics/event",
                    status_code=200,
                    duration_ms=0,
                    estimate_id="est-1",
                    meta={"vote": "up", "context": "estimate", "provider": "blended_v1"},
                ),
                UsageEvent(
                    ts=datetime.now(timezone.utc),
                    user_id="user-a",
                    is_admin=False,
                    event_name="feedback_vote",
                    method="POST",
                    path="/v1/analytics/event",
                    status_code=200,
                    duration_ms=0,
                    estimate_id="est-1",
                    meta={
                        "vote": "down",
                        "context": "estimate",
                        "reasons": ["land_price_wrong", "confusing"],
                        "landuse_method": "arcgis",
                    },
                ),
                UsageEvent(
                    ts=datetime.now(timezone.utc),
                    user_id="user-b",
                    is_admin=False,
                    event_name="feedback_vote",
                    method="POST",
                    path="/v1/analytics/event",
                    status_code=200,
                    duration_ms=0,
                    estimate_id="est-2",
                    meta={"vote": "down", "context": "pdf", "reasons": ["missing_data"], "provider": "suhail"},
                ),
            ]
        )
        db.commit()

    client = TestClient(app)
    response = client.get("/v1/admin/usage/feedback_inbox", headers={"X-API-Key": "ceo-key"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["totals"]["count_up"] == 1
    assert payload["totals"]["count_down"] == 2
    assert payload["top_reasons"]
    assert any(item["reason"] == "missing_data" for item in payload["top_reasons"])
    assert any(item["user_id"] == "user-a" for item in payload["by_user"])
