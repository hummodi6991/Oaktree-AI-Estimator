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


def test_admin_usage_funnel_rollup(
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

    base_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    with SessionLocal() as db:
        db.add_all(
            [
                UsageEvent(
                    ts=base_time,
                    user_id="user-a",
                    is_admin=False,
                    event_name="ui_parcel_selected",
                    method="POST",
                    path="/v1/analytics/event",
                    status_code=200,
                    duration_ms=0,
                ),
                UsageEvent(
                    ts=base_time + timedelta(minutes=2),
                    user_id="user-a",
                    is_admin=False,
                    event_name="ui_estimate_started",
                    method="POST",
                    path="/v1/analytics/event",
                    status_code=200,
                    duration_ms=0,
                ),
                UsageEvent(
                    ts=base_time + timedelta(minutes=5),
                    user_id="user-a",
                    is_admin=False,
                    event_name="ui_estimate_completed",
                    method="POST",
                    path="/v1/analytics/event",
                    status_code=200,
                    duration_ms=0,
                ),
                UsageEvent(
                    ts=base_time + timedelta(minutes=8),
                    user_id="user-a",
                    is_admin=False,
                    event_name="ui_pdf_opened",
                    method="POST",
                    path="/v1/analytics/event",
                    status_code=200,
                    duration_ms=0,
                ),
                UsageEvent(
                    ts=base_time + timedelta(minutes=10),
                    user_id="user-a",
                    is_admin=False,
                    event_name="feedback_vote",
                    method="POST",
                    path="/v1/analytics/event",
                    status_code=200,
                    duration_ms=0,
                ),
                UsageEvent(
                    ts=base_time + timedelta(minutes=1),
                    user_id="user-b",
                    is_admin=False,
                    event_name="ui_parcel_selected",
                    method="POST",
                    path="/v1/analytics/event",
                    status_code=200,
                    duration_ms=0,
                ),
                UsageEvent(
                    ts=base_time + timedelta(minutes=2),
                    user_id="user-b",
                    is_admin=False,
                    event_name="ui_estimate_started",
                    method="POST",
                    path="/v1/analytics/event",
                    status_code=200,
                    duration_ms=0,
                ),
                UsageEvent(
                    ts=base_time + timedelta(minutes=4),
                    user_id="user-b",
                    is_admin=False,
                    event_name="estimate_result",
                    method="POST",
                    path="/v1/analytics/event",
                    status_code=200,
                    duration_ms=0,
                ),
                UsageEvent(
                    ts=base_time + timedelta(minutes=6),
                    user_id="user-b",
                    is_admin=False,
                    event_name="pdf_export",
                    method="POST",
                    path="/v1/analytics/event",
                    status_code=200,
                    duration_ms=0,
                ),
                UsageEvent(
                    ts=base_time + timedelta(minutes=3),
                    user_id="admin-user",
                    is_admin=True,
                    event_name="ui_parcel_selected",
                    method="POST",
                    path="/v1/analytics/event",
                    status_code=200,
                    duration_ms=0,
                ),
            ]
        )
        db.commit()

    client = TestClient(app)
    response = client.get("/v1/admin/usage/funnel", headers={"X-API-Key": "ceo-key"})

    assert response.status_code == 200
    payload = response.json()

    totals = payload["totals"]
    assert totals["unique_users"] == 2
    assert totals["parcel_selected_users"] == 2
    assert totals["estimate_started_users"] == 2
    assert totals["estimate_completed_users"] == 2
    assert totals["pdf_opened_users"] == 2
    assert totals["feedback_users"] == 1
    assert totals["events"]["parcel_selected"] == 2
    assert totals["events"]["estimate_started"] == 2
    assert totals["events"]["estimate_completed"] == 2
    assert totals["events"]["pdf_opened"] == 2
    assert totals["events"]["feedback_votes"] == 1

    conversion = payload["conversion"]
    assert conversion["parcel_to_estimate_started"] == 1.0
    assert conversion["estimate_started_to_completed"] == 1.0
    assert conversion["completed_to_pdf"] == 1.0
    assert conversion["pdf_to_feedback"] == 0.5

    time_to_value = payload["time_to_value"]
    assert time_to_value["median_minutes_parcel_to_first_estimate"] == 3.0
    assert time_to_value["p80_minutes_parcel_to_first_estimate"] == 5.0
    assert time_to_value["median_minutes_estimate_to_pdf"] == 2.0
    assert time_to_value["p80_minutes_estimate_to_pdf"] == 3.0
