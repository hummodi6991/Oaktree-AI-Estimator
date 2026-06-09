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

# Realistic dense-Riyadh catchment OSM counts (3.5 km). The PR-1a anchors expect
# hundreds of offices, so these are scaled to land in the calibrated band.
_FULL_OSM = {
    "offices": 600,
    "malls_retail": 40,
    "transit": 15,
    "mosques": 80,
    "schools": 60,
    "hospitals": 12,
    "hotels": 30,
}


def _index(**overrides):
    base = dict(
        population_reach=250000.0,
        population_local_reach=45000.0,
        osm_counts=_FULL_OSM,
        building_floors_proxy_sum=20000.0,
        fnb_review_weighted=80000.0,
        fnb_venue_count=70,
        radius_m=3500,
        pop_radius_m=1500,
    )
    base.update(overrides)
    return _demand_generator_index(**base)


def test_composite_in_range_and_all_subcomponents_present():
    idx = _index()
    assert 0.0 <= idx["composite_0_100"] <= 100.0
    # raw sub-values retained for the next recalibration (no re-enrich needed)
    assert idx["population_reach"] == 250000
    assert idx["population_local_reach"] == 45000
    assert idx["pop_radius_m"] == 1500
    assert idx["building_floors_proxy_sum"] == 20000.0
    assert idx["fnb_review_weighted_density"] == 80000.0
    assert idx["fnb_venue_count"] == 70
    assert idx["radius_m"] == 3500
    assert idx["weights_version"] == "l1_v2_2026-06"
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
    # PR-1a: dense-but-not-extreme catchment must leave HEADROOM (not peg at 100),
    # which is exactly the saturation v1 suffered from.
    assert idx["composite_0_100"] < 95.0


def test_composite_varies_with_inputs():
    low = _index(
        population_reach=1000.0,
        population_local_reach=1000.0,
        osm_counts={},
        building_floors_proxy_sum=0.0,
        fnb_review_weighted=0.0,
        fnb_venue_count=0,
    )
    high = _index()
    assert high["composite_0_100"] > low["composite_0_100"]
    # not a constant / not mostly-zero across a realistic spread
    assert high["composite_0_100"] > 0.0


def test_recalibrated_index_spreads_across_a_fixture():
    """PR-1a regression guard for the ceiling-pinning bug.

    The v1 index gave low-activity المربع (low everything) the SAME 98.75 as
    high-activity الورود (near-max everything). With the re-anchored normalization
    a realistic spread of catchments must produce a wide, well-ordered composite —
    materially > the v1 stddev of 1.92, many distinct values, and the low-activity
    candidate clearly below the high-activity one.
    """
    fixture = [
        # (label, population_local, osm_counts, floors, fnb_review_weighted)
        (
            "almurabba_low",
            9000,
            {"offices": 40, "malls_retail": 2, "schools": 5},
            3500,
            13000,
        ),
        (
            "mid_a",
            25000,
            {
                "offices": 200,
                "malls_retail": 10,
                "transit": 4,
                "mosques": 30,
                "schools": 25,
            },
            9000,
            45000,
        ),
        (
            "mid_b",
            38000,
            {
                "offices": 350,
                "malls_retail": 18,
                "transit": 6,
                "mosques": 45,
                "schools": 40,
                "hospitals": 5,
            },
            14000,
            70000,
        ),
        (
            "alward_high",
            70000,
            {
                "offices": 1200,
                "malls_retail": 60,
                "transit": 20,
                "mosques": 90,
                "schools": 70,
                "hospitals": 15,
                "hotels": 45,
            },
            30000,
            150000,
        ),
    ]
    composites = []
    by_label = {}
    for label, pop, osm, floors, fnb in fixture:
        idx = _demand_generator_index(
            population_reach=250000.0,
            population_local_reach=float(pop),
            osm_counts=osm,
            building_floors_proxy_sum=float(floors),
            fnb_review_weighted=float(fnb),
            fnb_venue_count=10,
            radius_m=3500,
            pop_radius_m=1500,
        )
        composites.append(idx["composite_0_100"])
        by_label[label] = idx["composite_0_100"]

    # Wide spread: uses much of 0-100 and beats the v1 stddev artifact (1.92).
    spread = max(composites) - min(composites)
    assert spread >= 30.0, composites
    assert len(set(round(c) for c in composites)) == len(composites), composites
    # Ordering sanity: low-activity must rank clearly below high-activity.
    assert by_label["almurabba_low"] < by_label["alward_high"] - 20.0
    assert (
        by_label["almurabba_low"]
        < by_label["mid_a"]
        < by_label["mid_b"]
        < by_label["alward_high"]
    )


def test_zero_inputs_are_safe():
    idx = _index(
        population_reach=0.0,
        population_local_reach=0.0,
        osm_counts={},
        building_floors_proxy_sum=0.0,
        fnb_review_weighted=0.0,
        fnb_venue_count=0,
    )
    assert idx["composite_0_100"] >= 0.0
    assert idx["subscores"]["fnb_review_weighted"] == 0.0
    assert idx["subscores"]["population"] == 0.0


def test_osm_subscore_monotonic_and_bounded():
    # Counts scaled into the calibrated band (offices in the hundreds).
    s0 = _demand_generator_osm_subscore({})
    s1 = _demand_generator_osm_subscore({"offices": 100, "malls_retail": 10})
    s2 = _demand_generator_osm_subscore({"offices": 800, "malls_retail": 50})
    assert 0.0 <= s0 <= s1 <= s2 <= 100.0
    assert s1 > s0
    assert s2 > s1


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
        # PR-1a: tighter-radius population sub-term retained raw alongside the
        # full 3500 m reach.
        assert "population_local_reach" in idx
        assert idx["pop_radius_m"] == 1500
        assert idx["weights_version"] == "l1_v2_2026-06"


# ---------------------------------------------------------------------------
# PR-2 — gated dine-in demand-blend swap (pop_score → dg_composite)
# ---------------------------------------------------------------------------


def _run_flags(monkeypatch, *, index, scoring, service_model="dine_in"):
    """Run the FakeDB search with the two demand-generator flags set."""
    monkeypatch.setattr(
        expansion_service.settings, "EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED", index
    )
    monkeypatch.setattr(
        expansion_service.settings,
        "EXPANSION_DEMAND_GENERATOR_SCORING_ENABLED",
        scoring,
        raising=False,
    )
    clear_expansion_caches()
    return run_expansion_search(
        FakeDB(candidate_rows=_candidate_rows()),
        search_id="search-pr2",
        brand_name="Brand X",
        category="burger",
        service_model=service_model,
        min_area_m2=100,
        max_area_m2=400,
        target_area_m2=220,
        limit=10,
    )


def test_pr2_scoring_flag_off_is_inert(disable_market_viability_floors, monkeypatch):
    """Scoring flag OFF → dine-in final_score + ordering byte-for-byte identical
    to the feature-absent (both-flags-off) baseline, and no transparency field."""
    off = _run_flags(monkeypatch, index=False, scoring=False)
    off_scores = [(it["parcel_id"], it["final_score"]) for it in off]

    # Index ON but scoring OFF must not perturb scores or emit the source field.
    inert = _run_flags(monkeypatch, index=True, scoring=False)
    inert_scores = [(it["parcel_id"], it["final_score"]) for it in inert]
    assert inert_scores == off_scores
    for it in inert:
        fs = it.get("feature_snapshot_json") or {}
        assert "demand_score_source" not in fs


def test_pr2_scoring_on_dine_in_uses_dg_index(
    disable_market_viability_floors, monkeypatch
):
    """Scoring flag ON + dine_in + composite present → blend uses dg_composite,
    demand_score_source == 'dg_index', and the final score actually moves."""
    baseline = _run_flags(monkeypatch, index=True, scoring=False)
    base_scores = {it["parcel_id"]: it["final_score"] for it in baseline}

    on = _run_flags(monkeypatch, index=True, scoring=True)
    on_by_pid = {it["parcel_id"]: it for it in on}

    for it in on:
        fs = it.get("feature_snapshot_json") or {}
        assert fs.get("demand_score_source") == "dg_index"
        # The swapped blend equals composite·0.75 + delivery·0.25; assert the
        # population numerator is the composite, not the saturated pop_score.
        idx = fs.get("demand_generator_index")
        assert idx is not None
        composite = idx["composite_0_100"]
        pop_score = expansion_service._population_score(
            fs["population_reach"], service_model="dine_in"
        )
        # In dense Riyadh the 250k-ref pop_score saturates (~98); the spread-out
        # composite must differ, otherwise the swap would be a no-op.
        assert abs(composite - pop_score) > 1.0

    # At least one candidate's final_score moved vs the pop_score baseline.
    moved = any(
        abs(on_by_pid[pid]["final_score"] - base_scores[pid]) > 1e-9
        for pid in base_scores
        if pid in on_by_pid
    )
    assert moved


def test_pr2_scoring_on_index_off_falls_back_to_pop_score(
    disable_market_viability_floors, monkeypatch
):
    """Scoring flag ON but index flag OFF → composite absent → fall back to
    pop_score silently (no exception), demand_score_source == 'pop_score',
    and final_score is identical to the feature-absent baseline."""
    off = _run_flags(monkeypatch, index=False, scoring=False)
    off_scores = [(it["parcel_id"], it["final_score"]) for it in off]

    fallback = _run_flags(monkeypatch, index=False, scoring=True)
    fb_scores = [(it["parcel_id"], it["final_score"]) for it in fallback]
    assert fb_scores == off_scores
    for it in fallback:
        fs = it.get("feature_snapshot_json") or {}
        assert fs.get("demand_score_source") == "pop_score"
        # Index never computed → no index key emitted.
        assert "demand_generator_index" not in fs


def test_pr2_non_dine_in_unchanged_with_flag_on(
    disable_market_viability_floors, monkeypatch
):
    """Blast-radius guard: with the scoring flag ON, cafe and qsr keep the
    pop_score demand path — scores identical to scoring-off, source 'pop_score'."""
    for service_model in ("cafe", "qsr"):
        base = _run_flags(
            monkeypatch, index=True, scoring=False, service_model=service_model
        )
        base_scores = [(it["parcel_id"], it["final_score"]) for it in base]

        on = _run_flags(
            monkeypatch, index=True, scoring=True, service_model=service_model
        )
        on_scores = [(it["parcel_id"], it["final_score"]) for it in on]

        assert on_scores == base_scores, f"{service_model} demand path must not swap"
        for it in on:
            fs = it.get("feature_snapshot_json") or {}
            assert fs.get("demand_score_source") == "pop_score"
