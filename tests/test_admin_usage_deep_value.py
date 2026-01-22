from datetime import datetime, timedelta, timezone
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


def test_admin_usage_deep_value_metrics(
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

    from app.db.session import SessionLocal
    from app.models.tables import UsageEvent

    base_time = datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc)
    with SessionLocal() as db:
        db.add_all(
            [
                UsageEvent(
                    ts=base_time,
                    user_id="user-a",
                    is_admin=False,
                    event_name="ui_override_land_price",
                    method="POST",
                    path="/v1/ui/events",
                    status_code=200,
                    duration_ms=5,
                ),
                UsageEvent(
                    ts=base_time + timedelta(minutes=3),
                    user_id="user-a",
                    is_admin=False,
                    event_name="ui_estimate_started",
                    method="POST",
                    path="/v1/ui/events",
                    status_code=200,
                    duration_ms=5,
                ),
                UsageEvent(
                    ts=base_time + timedelta(minutes=10),
                    user_id="user-a",
                    is_admin=False,
                    event_name="ui_override_far",
                    method="POST",
                    path="/v1/ui/events",
                    status_code=200,
                    duration_ms=5,
                ),
                UsageEvent(
                    ts=base_time + timedelta(minutes=1),
                    user_id="user-b",
                    is_admin=False,
                    event_name="ui_change_provider",
                    method="POST",
                    path="/v1/ui/events",
                    status_code=200,
                    duration_ms=5,
                ),
                UsageEvent(
                    ts=base_time + timedelta(minutes=21),
                    user_id="user-b",
                    is_admin=False,
                    event_name="ui_estimate_started",
                    method="POST",
                    path="/v1/ui/events",
                    status_code=200,
                    duration_ms=5,
                ),
                UsageEvent(
                    ts=base_time + timedelta(minutes=2),
                    user_id="user-a",
                    is_admin=False,
                    event_name=None,
                    method="POST",
                    path="/v1/estimates/est-1/scenario",
                    status_code=200,
                    duration_ms=12,
                ),
                UsageEvent(
                    ts=base_time + timedelta(minutes=4),
                    user_id="user-b",
                    is_admin=False,
                    event_name="scenario_run",
                    method="POST",
                    path="/v1/estimates/est-2/scenario",
                    status_code=200,
                    duration_ms=14,
                    estimate_id="est-2",
                ),
            ]
        )
        db.commit()

    response = client.get(
        "/v1/admin/usage/deep_value?since=2024-01-01", headers={"X-API-Key": "ceo-key"}
    )

    assert response.status_code == 200
    payload = response.json()
    scenario = payload["scenario"]
    assert scenario["scenario_requests"] == 2
    assert scenario["scenario_users"] == 2
    assert scenario["scenario_estimates"] == 2
    top_users = {row["user_id"]: row["count"] for row in scenario["top_users"]}
    assert top_users == {"user-a": 1, "user-b": 1}

    rerun = payload["rerun_after_override"]
    assert rerun["override_events"] == 3
    assert rerun["rerun_events"] == 1
    assert rerun["rerun_users"] == 1
    assert rerun["rerun_rate"] == pytest.approx(0.5)
    assert rerun["median_minutes_to_rerun"] == pytest.approx(3.0)
    assert rerun["p80_minutes_to_rerun"] == pytest.approx(3.0)

    by_override = rerun["by_override_type"]
    assert by_override["ui_override_land_price"]["override_users"] == 1
    assert by_override["ui_override_land_price"]["rerun_users"] == 1
    assert by_override["ui_override_land_price"]["median_min"] == pytest.approx(3.0)
    assert by_override["ui_override_land_price"]["p80_min"] == pytest.approx(3.0)
    assert by_override["ui_override_far"]["override_users"] == 1
    assert by_override["ui_override_far"]["rerun_users"] == 0
    assert by_override["ui_change_provider"]["override_users"] == 1
    assert by_override["ui_change_provider"]["rerun_users"] == 0
