import asyncio
import importlib

import pytest
from fastapi import HTTPException


def _load_auth(monkeypatch: pytest.MonkeyPatch, **env: str) -> object:
    keys = ["AUTH_MODE", "API_KEYS_JSON", "ADMIN_API_KEYS_JSON", "API_KEY"]
    for key in keys:
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    import app.security.auth as auth  # type: ignore

    return importlib.reload(auth)


def test_valid_tester_key(monkeypatch: pytest.MonkeyPatch) -> None:
    auth = _load_auth(
        monkeypatch,
        AUTH_MODE="api_key",
        API_KEYS_JSON='{"tester-1": "tester-key"}',
    )

    payload = asyncio.run(auth.require(x_api_key="tester-key"))

    assert payload == {"sub": "tester-1", "is_admin": False, "mode": "api_key"}


def test_valid_admin_key(monkeypatch: pytest.MonkeyPatch) -> None:
    auth = _load_auth(
        monkeypatch,
        AUTH_MODE="api_key",
        ADMIN_API_KEYS_JSON='{"ceo": "ceo-key"}',
        API_KEYS_JSON='{"tester-1": "tester-key"}',
    )

    payload = asyncio.run(auth.require(x_api_key="ceo-key"))

    assert payload == {"sub": "ceo", "is_admin": True, "mode": "api_key"}


def test_invalid_key(monkeypatch: pytest.MonkeyPatch) -> None:
    auth = _load_auth(
        monkeypatch,
        AUTH_MODE="api_key",
        ADMIN_API_KEYS_JSON='{"ceo": "ceo-key"}',
        API_KEYS_JSON='{"tester-1": "tester-key"}',
    )

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(auth.require(x_api_key="nope"))

    assert excinfo.value.status_code == 401


def test_invalid_json_falls_back_to_legacy_key(monkeypatch: pytest.MonkeyPatch) -> None:
    auth = _load_auth(
        monkeypatch,
        AUTH_MODE="api_key",
        API_KEYS_JSON="not-json",
        API_KEY="legacy-key",
    )

    payload = asyncio.run(auth.require(x_api_key="legacy-key"))

    assert payload == {"sub": "api-key", "is_admin": False, "mode": "api_key"}
