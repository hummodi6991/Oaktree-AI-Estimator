import pytest

from app.services import land_price_engine
from app.services.district_resolver import DistrictResolution


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


def test_blended_guardrail_low_evidence(monkeypatch):
    quote = _run_quote(monkeypatch, (1000, {"n": None}), (800, {"n": 10}))
    assert quote["method"] == "blended_v1"
    assert quote["value"] == pytest.approx(980)  # 0.9*1000 + 0.1*800
    assert quote["meta"]["guardrails"]["aqar_low_evidence"] is True


def test_blended_suhail_only(monkeypatch):
    quote = _run_quote(monkeypatch, (1000, {"n": None}), (None, {"n": 0}))
    assert quote["method"] == "blended_v1_suhail_only"
    assert quote["value"] == pytest.approx(1000)
    assert quote["meta"]["weights"]["suhail"] == 1.0


def test_blended_aqar_only(monkeypatch):
    quote = _run_quote(monkeypatch, (None, {"n": None}), (800, {"n": 30}))
    assert quote["method"] == "blended_v1_aqar_only"
    assert quote["value"] == pytest.approx(800)
    assert quote["meta"]["weights"]["aqar"] == 1.0
