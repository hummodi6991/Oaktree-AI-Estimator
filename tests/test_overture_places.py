"""Tests for ``app.connectors.overture_places`` module-level config."""

from __future__ import annotations

import importlib


def test_overture_release_default(monkeypatch):
    monkeypatch.delenv("OVERTURE_RELEASE", raising=False)
    module = importlib.reload(importlib.import_module("app.connectors.overture_places"))
    assert module.OVERTURE_RELEASE == "2026-05-20.0"
    assert module.OVERTURE_RELEASE in module.OVERTURE_S3_PATH


def test_overture_release_env_override(monkeypatch):
    monkeypatch.setenv("OVERTURE_RELEASE", "2099-01-01.0")
    module = importlib.reload(importlib.import_module("app.connectors.overture_places"))
    try:
        assert module.OVERTURE_RELEASE == "2099-01-01.0"
        assert "release/2099-01-01.0/theme=places/type=place/" in module.OVERTURE_S3_PATH
    finally:
        # Restore module to default state so other tests see a clean import.
        monkeypatch.delenv("OVERTURE_RELEASE", raising=False)
        importlib.reload(module)
