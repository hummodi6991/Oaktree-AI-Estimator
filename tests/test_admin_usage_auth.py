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
    from app.models.tables import Rate, UsageEvent

    UsageEvent.__table__.create(db_session.engine, checkfirst=True)
    Rate.__table__.create(db_session.engine, checkfirst=True)


def test_admin_usage_allows_admin_key(
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

    response = client.get("/v1/admin/usage/summary", headers={"X-API-Key": "ceo-key"})

    assert response.status_code == 200

    insights = client.get("/v1/admin/usage/insights", headers={"X-API-Key": "ceo-key"})
    assert insights.status_code == 200
    payload = insights.json()
    assert "highlights" in payload

    feedback = client.get("/v1/admin/usage/feedback", headers={"X-API-Key": "ceo-key"})
    assert feedback.status_code == 200
    feedback_payload = feedback.json()
    assert "items" in feedback_payload

    feedback_inbox = client.get(
        "/v1/admin/usage/feedback_inbox", headers={"X-API-Key": "ceo-key"}
    )
    assert feedback_inbox.status_code == 200
    inbox_payload = feedback_inbox.json()
    assert "totals" in inbox_payload


def test_admin_usage_rejects_tester_key(
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

    response = client.get(
        "/v1/admin/usage/summary", headers={"X-API-Key": "tester-key"}
    )

    assert response.status_code == 403

    insights = client.get("/v1/admin/usage/insights", headers={"X-API-Key": "tester-key"})
    assert insights.status_code == 403

    feedback = client.get("/v1/admin/usage/feedback", headers={"X-API-Key": "tester-key"})
    assert feedback.status_code == 403

    feedback_inbox = client.get(
        "/v1/admin/usage/feedback_inbox", headers={"X-API-Key": "tester-key"}
    )
    assert feedback_inbox.status_code == 403
