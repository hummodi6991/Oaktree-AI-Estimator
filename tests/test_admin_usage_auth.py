import importlib

import pytest
from fastapi.testclient import TestClient


def _load_app(monkeypatch: pytest.MonkeyPatch, **env: str):
    keys = ["AUTH_MODE", "API_KEYS_JSON", "ADMIN_API_KEYS_JSON", "API_KEY"]
    for key in keys:
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    import app.security.auth as auth  # type: ignore
    import app.main as main  # type: ignore

    importlib.reload(auth)
    return importlib.reload(main).app


def test_admin_usage_allows_admin_key(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _load_app(
        monkeypatch,
        AUTH_MODE="api_key",
        ADMIN_API_KEYS_JSON='{"ceo": "ceo-key"}',
        API_KEYS_JSON='{"tester": "tester-key"}',
    )
    client = TestClient(app)

    response = client.get("/v1/admin/usage/summary", headers={"X-API-Key": "ceo-key"})

    assert response.status_code == 200


def test_admin_usage_rejects_tester_key(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _load_app(
        monkeypatch,
        AUTH_MODE="api_key",
        ADMIN_API_KEYS_JSON='{"ceo": "ceo-key"}',
        API_KEYS_JSON='{"tester": "tester-key"}',
    )
    client = TestClient(app)

    response = client.get(
        "/v1/admin/usage/summary", headers={"X-API-Key": "tester-key"}
    )

    assert response.status_code == 403
