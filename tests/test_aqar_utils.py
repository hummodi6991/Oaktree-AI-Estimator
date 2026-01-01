import pytest

from app.services import land_price_engine, pricing
from app.services.aqar_utils import norm_city_for_aqar


def test_norm_city_for_aqar_mappings():
    assert norm_city_for_aqar("Riyadh") == "الرياض"
    assert norm_city_for_aqar("  Ar Riyadh  ") == "الرياض"
    assert norm_city_for_aqar("JEDDAH") == "جدة"
    assert norm_city_for_aqar("Dammam") == "الدمام"
    assert norm_city_for_aqar("Abha") == "Abha"


def test_aqar_land_signal_normalizes_city():
    captured = {}

    class DummyDB:
        def execute(self, stmt, params):
            captured["city"] = params["aqar_city"]

            class DummyResult:
                def mappings(self_inner):
                    return self_inner

                def first(self_inner):
                    return {
                        "price_per_sqm": 750,
                        "n": 10,
                    }

            return DummyResult()

    value, meta = land_price_engine._aqar_land_signal(
        DummyDB(), city="Riyadh", district_norm="al_olaya", district_raw="Al Olaya"
    )

    assert captured["city"] == "الرياض"
    assert value == pytest.approx(750)
    assert meta["level"] == "district"


def test_price_from_aqar_normalizes_city():
    captured = {}

    class DummyDB:
        def execute(self, stmt, params):
            captured["city"] = params["city"]

            class DummyResult:
                def scalar(self_inner):
                    return 600.0

            return DummyResult()

    value, method = pricing.price_from_aqar(DummyDB(), city=" Ar-Riyadh ", district=None)

    assert captured["city"] == "الرياض"
    assert value == pytest.approx(600.0)
    assert method == "aqar.mv_city_price_per_sqm"
