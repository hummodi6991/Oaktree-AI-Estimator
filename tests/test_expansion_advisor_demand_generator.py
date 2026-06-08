"""PR-1 — L1 demand-generator index (additive, emit-only).

Two layers of coverage:

1. Pure-function tests for the composite math (no DB) — range, variation,
   monotonicity, net-of-supply independence, and that the new module constants
   did not perturb the scoring weights.
2. FakeDB integration tests proving the feature is inert when the flag is OFF
   (no snapshot key, scores unchanged) and additive-only when ON (key emitted,
   final_score / ranking byte-for-byte unchanged). The FakeDB reports the
   externally-imported tables as absent, so the ON test also exercises the
   missing-table no-op path.
"""

from __future__ import annotations

from app.services import expansion_advisor as expansion_service
from app.services.expansion_advisor import (
    _DEMAND_GENERATOR_COMPOSITE_WEIGHTS,
    _DEMAND_GENERATOR_OSM_WEIGHTS,
    _demand_blend_weights,
    _demand_generator_index,
    _demand_generator_osm_subscore,
    clear_expansion_caches,
    run_expansion_search as _run_expansion_search_raw,
)

from tests.test_expansion_advisor_service import FakeDB


def run_expansion_search(*args, **kwargs):
    result = _run_expansion_search_raw(*args, **kwargs)
    return result["items"] if isinstance(result, dict) else result


# ---------------------------------------------------------------------------
# Pure-function tests
# ---------------------------------------------------------------------------

_FULL_OSM = {
    "offices": 12,
    "malls_retail": 3,
    "transit": 2,
    "mosques": 8,
    "schools": 6,
    "hospitals": 1,
    "hotels": 4,
}


def _index(**overrides):
    base = dict(
        population_reach=120000.0,
        osm_counts=_FULL_OSM,
        building_floors_proxy_sum=850.0,
        fnb_review_weighted=4200.0,
        fnb_venue_count=70,
        radius_m=3500,
    )
    base.update(overrides)
    return _demand_generator_index(**base)


def test_composite_in_range_and_all_subcomponents_present():
    idx = _index()
    assert 0.0 <= idx["composite_0_100"] <= 100.0
    # raw sub-values retained for PR-2 recalibration
    assert idx["population_reach"] == 120000
    assert idx["building_floors_proxy_sum"] == 850.0
    assert idx["fnb_review_weighted_density"] == 4200.0
    assert idx["fnb_venue_count"] == 70
    assert idx["radius_m"] == 3500
    assert idx["weights_version"]  # non-empty
    for k in (
        "offices",
        "malls_retail",
        "transit",
        "mosques",
        "schools",
        "hospitals",
        "hotels",
    ):
        assert idx["osm_generators"][k] == _FULL_OSM[k]
    for k in ("population", "osm_generators", "building_floors", "fnb_review_weighted"):
        assert 0.0 <= idx["subscores"][k] <= 100.0


def test_composite_varies_with_inputs():
    low = _index(
        population_reach=1000.0,
        osm_counts={},
        building_floors_proxy_sum=0.0,
        fnb_review_weighted=0.0,
        fnb_venue_count=0,
    )
    high = _index()
    assert high["composite_0_100"] > low["composite_0_100"]
    # not a constant / not mostly-zero across a realistic spread
    assert high["composite_0_100"] > 0.0


def test_zero_inputs_are_safe():
    idx = _index(
        population_reach=0.0,
        osm_counts={},
        building_floors_proxy_sum=0.0,
        fnb_review_weighted=0.0,
        fnb_venue_count=0,
    )
    assert idx["composite_0_100"] >= 0.0
    assert idx["subscores"]["fnb_review_weighted"] == 0.0
    assert idx["subscores"]["population"] == 0.0


def test_osm_subscore_monotonic_and_bounded():
    s0 = _demand_generator_osm_subscore({})
    s1 = _demand_generator_osm_subscore({"malls_retail": 1})
    s2 = _demand_generator_osm_subscore({"malls_retail": 10, "offices": 10})
    assert 0.0 <= s0 <= s1 <= s2 <= 100.0
    assert s1 > s0


def test_index_is_numerator_only_more_fnb_raises_score():
    """Net-of-supply: the index must NOT penalize density. Adding F&B venues
    (which correlates with competition) raises the composite — proving the
    index is a demand numerator, not a saturation signal. Competition stays in
    _competition_whitespace_score (verified structurally: the index takes no
    competitor argument)."""
    sparse = _index(fnb_review_weighted=100.0, fnb_venue_count=2)
    dense = _index(fnb_review_weighted=8000.0, fnb_venue_count=120)
    assert dense["composite_0_100"] >= sparse["composite_0_100"]


def test_composite_weights_sum_to_one():
    assert abs(sum(_DEMAND_GENERATOR_COMPOSITE_WEIGHTS.values()) - 1.0) < 1e-9
    assert set(_DEMAND_GENERATOR_OSM_WEIGHTS) == {
        "offices",
        "malls_retail",
        "transit",
        "mosques",
        "schools",
        "hospitals",
        "hotels",
    }


def test_demand_blend_weights_unchanged():
    # PR-1 must not touch the demand blend weights (that is PR-2 territory).
    assert _demand_blend_weights("dine_in") == (0.75, 0.25)
    assert _demand_blend_weights("cafe") == (0.55, 0.45)
    assert _demand_blend_weights("qsr") == (0.60, 0.40)
    assert _demand_blend_weights("delivery_first") == (0.40, 0.60)


# ---------------------------------------------------------------------------
# FakeDB integration tests — flag OFF vs ON
# ---------------------------------------------------------------------------


def _candidate_rows():
    return [
        {
            "parcel_id": "p1",
            "landuse_label": "Commercial",
            "landuse_code": "C",
            "area_m2": 220,
            "lon": 46.69,
            "lat": 24.69,
            "district": "حي العليا",
            "population_reach": 180000,
            "competitor_count": 4,
            "delivery_listing_count": 12,
        },
        {
            "parcel_id": "p2",
            "landuse_label": "Commercial",
            "landuse_code": "C",
            "area_m2": 240,
            "lon": 46.64,
            "lat": 24.75,
            "district": "حي النخيل",
            "population_reach": 90000,
            "competitor_count": 2,
            "delivery_listing_count": 6,
        },
    ]


def _run(db):
    return run_expansion_search(
        db,
        search_id="search-1",
        brand_name="Brand X",
        category="burger",
        service_model="dine_in",
        min_area_m2=100,
        max_area_m2=400,
        target_area_m2=220,
        limit=10,
    )


def test_flag_off_emits_no_index_key(disable_market_viability_floors, monkeypatch):
    monkeypatch.setattr(
        expansion_service.settings, "EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED", False
    )
    clear_expansion_caches()
    items = _run(FakeDB(candidate_rows=_candidate_rows()))
    assert items
    for it in items:
        fs = it.get("feature_snapshot_json") or {}
        assert "demand_generator_index" not in fs


def test_flag_on_emits_index_without_changing_scores(
    disable_market_viability_floors, monkeypatch
):
    # Baseline run with the flag OFF.
    monkeypatch.setattr(
        expansion_service.settings, "EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED", False
    )
    clear_expansion_caches()
    baseline = _run(FakeDB(candidate_rows=_candidate_rows()))
    baseline_scores = [(it["parcel_id"], it["final_score"]) for it in baseline]

    # Same search with the flag ON.
    monkeypatch.setattr(
        expansion_service.settings, "EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED", True
    )
    clear_expansion_caches()
    enriched = _run(FakeDB(candidate_rows=_candidate_rows()))
    enriched_scores = [(it["parcel_id"], it["final_score"]) for it in enriched]

    # Ranking + scores are byte-for-byte unchanged (emit-only).
    assert enriched_scores == baseline_scores

    # The index is emitted with all sub-components. FakeDB reports the
    # externally-imported tables as absent, so this also covers the
    # missing-table no-op (no exception; zero-filled sub-values).
    for it in enriched:
        fs = it.get("feature_snapshot_json") or {}
        idx = fs.get("demand_generator_index")
        assert idx is not None
        assert 0.0 <= idx["composite_0_100"] <= 100.0
        assert idx["radius_m"] == 3500
        assert set(idx["osm_generators"]) == {
            "offices",
            "malls_retail",
            "transit",
            "mosques",
            "schools",
            "hospitals",
            "hotels",
        }
        assert "fnb_review_weighted_density" in idx
        assert "building_floors_proxy_sum" in idx
        assert idx["weights_version"]
