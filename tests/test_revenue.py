from app.services import revenue


class DummySession:
    pass


def _run_and_get_line(monkeypatch, indicator_value):
    monkeypatch.setattr(
        revenue,
        "_indicator",
        lambda db, indicator_type, city, asset_type: indicator_value,
    )
    result = revenue.build_to_sell_revenue(DummySession(), 100.0, city=None)
    return result["price_per_m2"], result["lines"][0]["source_type"]


def test_build_to_sell_revenue_zero_indicator(monkeypatch):
    price, source = _run_and_get_line(monkeypatch, 0.0)
    assert price == 0.0
    assert source == "Observed"


def test_build_to_sell_revenue_fallback_when_none(monkeypatch):
    price, source = _run_and_get_line(monkeypatch, None)
    assert price == 5500.0
    assert source == "Manual"


def test_build_to_lease_revenue_zero_indicator(monkeypatch):
    monkeypatch.setattr(
        revenue,
        "_indicator",
        lambda db, indicator_type, city, asset_type: 0.0,
    )
    result = revenue.build_to_lease_revenue(DummySession(), 100.0, city=None)
    assert result["rent_per_m2"] == 0.0
    assert result["lines"][0]["source_type"] == "Observed"


def test_build_to_lease_revenue_fallback_when_none(monkeypatch):
    monkeypatch.setattr(
        revenue,
        "_indicator",
        lambda db, indicator_type, city, asset_type: None,
    )
    result = revenue.build_to_lease_revenue(DummySession(), 100.0, city=None)
    assert result["rent_per_m2"] == 220.0
    assert result["lines"][0]["source_type"] == "Manual"
