import pytest

from app.services import land_price_engine
from app.services.district_resolver import DistrictResolution
from app.db.deps import get_db
from app.main import app
from app.services.pricing import SUHAIL_RIYADH_PROVINCE_ID


class DummyDB:
    pass


class FakeResult:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def first(self):
        return self.row


class FakeSuhailDB:
    def __init__(self, rows):
        self.rows = rows

    def _match_rows(self, params):
        matching = [r for r in self.rows if r.get("district_norm") == params.get("district_norm")]
        if "province_id" in params:
            matching = [r for r in matching if r.get("province_id") == params["province_id"]]
        if "land_use_group" in params:
            matching = [r for r in matching if r.get("land_use_group") == params["land_use_group"]]
        elif "land_use_group_like" in params:
            needle = params["land_use_group_like"].strip("%").lower()
            matching = [r for r in matching if needle in (r.get("land_use_group") or "").lower()]
        return matching

    def execute(self, stmt, params):
        text = stmt.text if hasattr(stmt, "text") else str(stmt)
        if "percentile_disc" in text:
            matching = [r for r in self.rows if r.get("province_id") == params.get("province_id")]
            if "land_use_group" in params:
                matching = [r for r in matching if r.get("land_use_group") == params["land_use_group"]]
            elif "land_use_group_like" in params:
                needle = params["land_use_group_like"].strip("%").lower()
                matching = [r for r in matching if needle in (r.get("land_use_group") or "").lower()]
            if not matching:
                return FakeResult(None)
            latest = sorted(matching, key=lambda r: r.get("as_of_date") or "")[-1]
            return FakeResult(
                {
                    "median_ppm2": latest.get("median_ppm2"),
                    "as_of_date": latest.get("as_of_date"),
                    "land_use_group": params.get("land_use_group_used") or latest.get("land_use_group"),
                }
            )

        matching = self._match_rows(params)
        if not matching:
            return FakeResult(None)
        latest = sorted(matching, key=lambda r: r.get("as_of_date") or "")[-1]
        return FakeResult(latest)


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
    assert quote["value"] == pytest.approx(1000)
    assert quote["meta"]["reason"] == "missing_aqar"


def test_blended_aqar_only(monkeypatch):
    quote = _run_quote(monkeypatch, (None, {"n": None}), (800, {"n": 30}))
    assert quote["method"] == "blended_v1"
    assert quote["value"] == pytest.approx(800)
    assert quote["meta"]["reason"] == "missing_suhail"


def test_suhail_land_signal_fallbacks_to_all_group():
    db = FakeSuhailDB(
        [
            {
                "district_norm": "al_olaya",
                "province_id": SUHAIL_RIYADH_PROVINCE_ID,
                "land_use_group": "الكل",
                "median_ppm2": 1200,
                "as_of_date": "2024-01-01",
            }
        ]
    )
    value, meta = land_price_engine._suhail_land_signal(
        db, city_norm="riyadh", district_norm="al_olaya", land_use_group="سكني"
    )
    assert value == pytest.approx(1200)
    assert meta["land_use_group_used"] == "الكل"
    assert meta["used_fallback"] is True


def test_quote_prefers_requested_land_use_when_available(monkeypatch):
    db = FakeSuhailDB(
        [
            {
                "district_norm": "al_olaya",
                "province_id": SUHAIL_RIYADH_PROVINCE_ID,
                "land_use_group": "تجاري",
                "median_ppm2": 1500,
                "as_of_date": "2024-02-01",
            }
        ]
    )
    monkeypatch.setattr(land_price_engine, "resolve_district", lambda *args, **kwargs: _mock_resolution())
    monkeypatch.setattr(land_price_engine, "_aqar_land_signal", lambda *args, **kwargs: (None, {"n": 0}))

    quote = land_price_engine.quote_land_price_blended_v1(db, city="Riyadh", land_use_group="تجاري")

    assert quote["value"] == pytest.approx(1500)
    suhail_meta = quote["meta"]["components"]["suhail"]
    assert suhail_meta["land_use_group_used"] == "تجاري"
    assert suhail_meta["used_fallback"] is False


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
                    return {"district": "الملقا", "price_per_sqm": 1200, "n": 7}

            return Result()

    db = RecordingDB()
    value, meta = land_price_engine._aqar_land_signal(
        db, city="Riyadh", district_norm="ignored", district_raw="الملقا"
    )

    assert "FROM aqar.mv_city_price_per_sqm" in (db.last_text or "")
    assert db.last_params["aqar_city"] == "الرياض"
    assert value == pytest.approx(1200.0)
    assert meta["district_used"] == "الملقا"
    assert meta["method"] == "aqar_mv_exact"


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


def test_pricing_api_passes_land_use_group(monkeypatch, pricing_client):
    from app.api import pricing as pricing_mod

    recorded: dict[str, str | None] = {}

    def fake_quote_land_price_blended_v1(db, city, district=None, lon=None, lat=None, geom_geojson=None, land_use_group=None):
        recorded["land_use_group"] = land_use_group
        return {
            "provider": "blended_v1",
            "method": "blended_v1",
            "value": 1500,
            "district_norm": None,
            "district_raw": "حي الاختبار",
            "district_resolution": {},
            "meta": {"components": {"suhail": {"value": 1500, "land_use_group": land_use_group}}},
        }

    monkeypatch.setattr(pricing_mod, "quote_land_price_blended_v1", fake_quote_land_price_blended_v1)

    resp = pricing_client.get(
        "/v1/pricing/land",
        params={"city": "Riyadh", "district": "Test", "provider": "blended_v1", "land_use_group": "تجاري"},
    )
    assert resp.status_code == 200
    assert recorded["land_use_group"] == "تجاري"


def test_pricing_api_infers_land_use_group(monkeypatch, pricing_client):
    from app.api import pricing as pricing_mod

    recorded: dict[str, str | None] = {}

    def fake_identify_postgis(lng, lat, tol_m, db):
        return {
            "found": True,
            "parcel": {
                "landuse_raw": "commercial",
                "classification_raw": "parcel",
                "landuse_code": "m",
            },
        }

    def fake_quote_land_price_blended_v1(db, city, district=None, lon=None, lat=None, geom_geojson=None, land_use_group=None):
        recorded["land_use_group"] = land_use_group
        return {
            "provider": "blended_v1",
            "method": "blended_v1",
            "value": 1750,
            "district_norm": None,
            "district_raw": "حي الاختبار",
            "district_resolution": {},
            "meta": {"components": {"suhail": {"value": 1750, "land_use_group": land_use_group}}},
        }

    monkeypatch.setattr("app.api.geo_portal._identify_postgis", fake_identify_postgis)
    monkeypatch.setattr(pricing_mod, "quote_land_price_blended_v1", fake_quote_land_price_blended_v1)

    resp = pricing_client.get(
        "/v1/pricing/land",
        params={"city": "Riyadh", "district": "Test", "provider": "blended_v1", "lng": 46.7, "lat": 24.7},
    )
    assert resp.status_code == 200
    assert recorded["land_use_group"] == "تجاري"
