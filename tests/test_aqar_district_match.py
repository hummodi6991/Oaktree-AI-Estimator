from __future__ import annotations

import pytest

from app.services.aqar_district_match import (
    _reset_cache_for_tests,
    find_aqar_mv_district_price,
    normalize_district_key,
)


class DummyResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class DummyDB:
    def __init__(self, rows):
        self.rows = rows
        self.executed_queries = []

    def execute(self, _query, _params=None):
        self.executed_queries.append((_query, _params))
        return DummyResult(self.rows)


@pytest.fixture(autouse=True)
def clear_cache():
    _reset_cache_for_tests()
    yield
    _reset_cache_for_tests()


@pytest.mark.parametrize(
    "raw,expected",
    [
        (" حي  النرجس ", "النرجس"),
        ("أبرق الرغامة", "ابرق الرغامة"),
        ("حي المرسلات", "المرسلات"),
        ("حي-النرجس", "حي-النرجس"),
    ],
)
def test_normalize_district_key(raw, expected):
    assert normalize_district_key(raw) == expected


def test_exact_match_prefers_highest_n():
    rows = [
        {"district": "النرجس", "price_per_sqm": 1000, "n": 5},
        {"district": "النرجس", "price_per_sqm": 1200, "n": 10},
    ]
    db = DummyDB(rows)
    val, meta = find_aqar_mv_district_price(db, city_ar="الرياض", district_raw="النرجس")
    assert val == 1200
    assert meta["method"] == "aqar_mv_exact"
    assert meta["matched_district"] == "النرجس"
    assert meta["n"] == 10


def test_normalized_match_used_when_raw_missing():
    rows = [
        {"district": "ابو بكر الصديق", "price_per_sqm": 900, "n": 3},
    ]
    db = DummyDB(rows)
    val, meta = find_aqar_mv_district_price(db, city_ar="الرياض", district_raw="أبو بكر الصديق")
    assert val == 900
    assert meta["method"] == "aqar_mv_norm"
    assert meta["matched_district"] == "ابو بكر الصديق"


def test_variant_match_on_prefix_removed():
    rows = [
        {"district": "النرجس", "price_per_sqm": 800, "n": 7},
    ]
    db = DummyDB(rows)
    val, meta = find_aqar_mv_district_price(db, city_ar="الرياض", district_raw="حي-النرجس")
    assert val == 800
    assert meta["method"] == "aqar_mv_variant"
    assert meta["district_normed"] == "النرجس"


def test_falls_back_to_none_when_no_match():
    rows = [
        {"district": "النرجس", "price_per_sqm": 800, "n": 7},
    ]
    db = DummyDB(rows)
    val, meta = find_aqar_mv_district_price(db, city_ar="الرياض", district_raw="غير موجود")
    assert val is None
    assert meta["method"] is None
    assert meta["reason"] == "no_district_match"
