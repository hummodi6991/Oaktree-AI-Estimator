import pytest

from app.services import land_price_engine
from app.services.district_resolver import DistrictResolution
from app.db.deps import get_db
from app.main import app


class DummyDB:
    pass


def _mock_resolution():
    return DistrictResolution(
        city_norm="riyadh",
        district_raw="Al Olaya",
        district_norm="al_olaya",
        method="provided",
        confidence=1.0,
    )


def _run_quote(monkeypatch, suhail_result, aqar_result):
    monkeypatch.setattr(land_price_engine, "resolve_district", lambda *args, **kwargs: _mock_resolution())
    monkeypatch.setattr(land_price_engine, "_suhail_land_signal", lambda *args, **kwargs: suhail_result)
    monkeypatch.setattr(land_price_engine, "_aqar_land_signal", lambda *args, **kwargs: aqar_result)
    return land_price_engine.quote_land_price_blended_v1(DummyDB(), city="Riyadh")


def test_blended_weights(monkeypatch):
    quote = _run_quote(monkeypatch, (1000, {"n": None}), (800, {"n": 50}))
    assert quote["method"] == "blended_v1"
    assert quote["value"] == pytest.approx(940)  # 0.7*1000 + 0.3*800
    assert quote["meta"]["guardrails"]["aqar_low_evidence"] is False
    assert quote["meta"]["reason"] is None


def test_blended_guardrail_low_evidence(monkeypatch):
    quote = _run_quote(monkeypatch, (1000, {"n": None}), (800, {"n": 10}))
    assert quote["method"] == "blended_v1"
    assert quote["value"] == pytest.approx(980)  # 0.9*1000 + 0.1*800
    assert quote["meta"]["guardrails"]["aqar_low_evidence"] is True
    assert quote["meta"]["reason"] is None


def test_blended_suhail_only(monkeypatch):
    quote = _run_quote(monkeypatch, (1000, {"n": None}), (None, {"n": 0}))
    assert quote["method"] == "blended_v1"
    assert quote["value"] is None
    assert quote["meta"]["reason"] == "missing_aqar"


def test_blended_aqar_only(monkeypatch):
    quote = _run_quote(monkeypatch, (None, {"n": None}), (800, {"n": 30}))
    assert quote["method"] == "blended_v1"
    assert quote["value"] is None
    assert quote["meta"]["reason"] == "missing_suhail"


def test_aqar_query_uses_raw_district(monkeypatch):
    class RecordingDB:
        def __init__(self):
            self.last_text = None
            self.last_params = None

        def execute(self, stmt, params):
            self.last_text = stmt.text if hasattr(stmt, "text") else str(stmt)
            self.last_params = params

            class Result:
                def mappings(self_inner):
                    return self_inner

                def first(self_inner):
                    return {"price_per_sqm": 1200, "n": 7}

            return Result()

    db = RecordingDB()
    value, meta = land_price_engine._aqar_land_signal(
        db, city="Riyadh", district_norm="ignored", district_raw="الملقا"
    )

    assert "district = :district_raw" in (db.last_text or "")
    assert "district_normalized" not in (db.last_text or "")
    assert db.last_params["aqar_city"] == "الرياض"
    assert value == pytest.approx(1200.0)
    assert meta["district_used"] == "الملقا"


class DummySession:
    def close(self):
        pass


def override_get_db():
    session = DummySession()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def pricing_client():
    from fastapi.testclient import TestClient

    app.dependency_overrides[get_db] = override_get_db
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_db, None)


def test_pricing_api_returns_reason_on_missing(monkeypatch, pricing_client):
    from app.api import pricing as pricing_mod

    def fake_quote_land_price_blended_v1(db, city, district=None, lon=None, lat=None, geom_geojson=None, land_use_group=None):
        return {
            "provider": "blended_v1",
            "method": "blended_v1",
            "value": None,
            "district_norm": None,
            "district_raw": "حي الاختبار",
            "district_resolution": {},
            "meta": {"reason": "missing_suhail", "city_used": "الرياض", "district_used": "حي الاختبار"},
        }

    monkeypatch.setattr(pricing_mod, "quote_land_price_blended_v1", fake_quote_land_price_blended_v1)

    resp = pricing_client.get("/v1/pricing/land", params={"city": "Riyadh", "district": "Test", "provider": "blended_v1"})
    assert resp.status_code == 404
    detail = resp.json()["detail"]
    assert detail["reason"] == "missing_suhail"
    assert detail["city_used"] == "الرياض"
    assert detail["district_used"] == "حي الاختبار"
