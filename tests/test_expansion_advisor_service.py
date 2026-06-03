from __future__ import annotations

from app.services import expansion_advisor as expansion_service
from app.services.expansion_advisor import (
    _brand_fit_score,
    _candidate_gate_status,
    _comparable_competitors,
    _confidence_grade,
    compare_candidates,
    get_candidate_memo,
    get_recommendation_report,
    get_search,
    run_expansion_search as _run_expansion_search_raw,
)


def run_expansion_search(*args, **kwargs):
    """Wrapper that unwraps the new dict return format to a plain list."""
    result = _run_expansion_search_raw(*args, **kwargs)
    return result["items"] if isinstance(result, dict) else result


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def scalar(self):
        """Return first column of first row (for COUNT queries etc)."""
        if self._rows and isinstance(self._rows[0], dict):
            return next(iter(self._rows[0].values()), None)
        if self._rows:
            return self._rows[0]
        return None

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None


class _FakeNestedTransaction:
    """Minimal stand-in for SQLAlchemy's nested (SAVEPOINT) context manager."""
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        return False  # propagate exceptions


class FakeDB:
    def __init__(self, candidate_rows=None, compare_rows=None, has_search=True, memo_row=None, brand_profile_row=None):
        self.candidate_rows = candidate_rows or []
        self.compare_rows = compare_rows or []
        self.has_search = has_search
        self.memo_row = memo_row
        self.inserted = []
        self.brand_profile_row = brand_profile_row

    def begin_nested(self):
        return _FakeNestedTransaction()

    def execute(self, stmt, params=None):
        sql = stmt.text if hasattr(stmt, "text") else str(stmt)
        if "FROM candidate_base" in sql:
            return _Result(self.candidate_rows)
        # candidate_location count → return 0 so code falls to commercial_unit path
        if "COUNT(*)" in sql and "candidate_location" in sql:
            return _Result([{"count": 0}])
        # Phase 3b _district_momentum_score — spatial CTE "WITH
        # listing_district AS". Match before the generic
        # "FROM commercial_unit" branch so every candidate resolves to
        # neutral 50.0 momentum in these fixtures.
        if "WITH listing_district AS" in sql:
            return _Result([])
        # commercial_unit queries → return candidate rows
        # Scope to top-level reads — the serve queries (compare/memo)
        # also reference commercial_unit inside an EXISTS guard subquery,
        # which we want routed to the expansion_candidate branches below.
        if "FROM commercial_unit" in sql and "FROM expansion_candidate" not in sql:
            return _Result(self.candidate_rows)
        if "INSERT INTO expansion_candidate" in sql:
            self.inserted.append(params)
            return _Result([])
        if "SELECT id FROM expansion_search" in sql:
            return _Result([{"id": "search-1"}] if self.has_search else [])
        if "FROM expansion_candidate" in sql and "id = ANY" in sql:
            return _Result(self.compare_rows)
        if "FROM expansion_candidate c" in sql and "JOIN expansion_search s" in sql:
            return _Result([self.memo_row] if self.memo_row else [])
        if "FROM expansion_brand_profile" in sql:
            return _Result([self.brand_profile_row] if self.brand_profile_row else [])
        return _Result([])


def test_district_filtering_narrows_results_and_sets_economics_fields(disable_market_viability_floors):
    db = FakeDB(
        candidate_rows=[
            {
                "parcel_id": "p1",
                "landuse_label": "Commercial",
                "landuse_code": "C",
                "area_m2": 180,
                "lon": 46.7,
                "lat": 24.7,
                "district": "حي العليا",
                "population_reach": 15000,
                "competitor_count": 2,
                "delivery_listing_count": 10,
            },
            {
                "parcel_id": "p2",
                "landuse_label": "Commercial",
                "landuse_code": "C",
                "area_m2": 170,
                "lon": 46.8,
                "lat": 24.8,
                "district": "الملقا",
                "population_reach": 13000,
                "competitor_count": 3,
                "delivery_listing_count": 8,
            },
        ]
    )

    items = run_expansion_search(
        db,
        search_id="search-1",
        brand_name="Brand X",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=300,
        target_area_m2=180,
        limit=10,
        target_districts=["العليا"],
        existing_branches=[{"name": "B1", "lat": 24.7005, "lon": 46.7005}],
    )

    assert len(items) == 1
    assert items[0]["parcel_id"] == "p1"
    assert items[0]["district"] == "حي العليا"
    assert items[0]["cannibalization_score"] is not None
    assert items[0]["distance_to_nearest_branch_m"] is not None
    assert items[0]["economics_score"] is not None
    assert "estimated_payback_months" not in items[0]
    assert "payback_band" not in items[0]
    assert 0.0 <= items[0]["final_score"] <= 100.0
    assert items[0]["compare_rank"] == 1


def test_compare_candidates_rejects_candidate_ids_from_other_search():
    db = FakeDB(
        compare_rows=[
            {
                "id": "c1",
                "parcel_id": "p1",
                "district": "Olaya",
                "area_m2": 150,
                "final_score": 80,
                "demand_score": 75,
                "whitespace_score": 70,
                "fit_score": 85,
                "confidence_score": 90,
                "cannibalization_score": 40,
                "distance_to_nearest_branch_m": 2300,
                "estimated_rent_sar_m2_year": 960,
                "estimated_annual_rent_sar": 144000,
                "estimated_fitout_cost_sar": 390000,
                "estimated_revenue_index": 71,
                "economics_score": 68,
                "competitor_count": 3,
                "delivery_listing_count": 12,
                "population_reach": 14000,
                "landuse_label": "Commercial",
            }
        ]
    )

    try:
        compare_candidates(db, "search-1", ["c1", "c2"])
        raised = False
    except ValueError:
        raised = True

    assert raised is True


def test_compare_candidates_includes_v5_fields_and_gate_summary_uses_actual_gate_data():
    db = FakeDB(
        compare_rows=[
            {
                "id": "c1",
                "parcel_id": "p1",
                "district": "Olaya",
                "area_m2": 150,
                "final_score": 80,
                "demand_score": 75,
                "whitespace_score": 70,
                "fit_score": 85,
                "zoning_fit_score": 88,
                "frontage_score": 66,
                "access_score": 64,
                "parking_score": 62,
                "access_visibility_score": 65,
                "confidence_score": 79,
                "confidence_grade": "B",
                "gate_status_json": {"overall_pass": False},
                "gate_reasons_json": {"failed": ["frontage_access_pass"]},
                "feature_snapshot_json": {"touches_road": False},
                "demand_thesis": "Demand is moderate",
                "cost_thesis": "Cost is manageable",
                "comparable_competitors_json": [{"id": "r1"}],
                "cannibalization_score": 40,
                "distance_to_nearest_branch_m": 2300,
                "estimated_rent_sar_m2_year": 960,
                "estimated_annual_rent_sar": 144000,
                "estimated_fitout_cost_sar": 390000,
                "estimated_revenue_index": 71,
                "economics_score": 68,
                "competitor_count": 3,
                "delivery_listing_count": 12,
                "population_reach": 14000,
                "landuse_label": "Commercial",
            },
            {
                "id": "c2",
                "parcel_id": "p2",
                "district": "Malqa",
                "area_m2": 170,
                "final_score": 74,
                "demand_score": 69,
                "whitespace_score": 62,
                "fit_score": 73,
                "zoning_fit_score": 80,
                "frontage_score": 70,
                "access_score": 72,
                "parking_score": 68,
                "access_visibility_score": 71,
                "confidence_score": 86,
                "confidence_grade": "A",
                "gate_status_json": {"overall_pass": True},
                "gate_reasons_json": {"passed": ["overall_pass"]},
                "feature_snapshot_json": {"touches_road": True},
                "demand_thesis": "Demand is strong",
                "cost_thesis": "Cost is higher",
                "comparable_competitors_json": [{"id": "r2"}],
                "cannibalization_score": 35,
                "distance_to_nearest_branch_m": 2500,
                "estimated_rent_sar_m2_year": 990,
                "estimated_annual_rent_sar": 168300,
                "estimated_fitout_cost_sar": 430000,
                "estimated_revenue_index": 70,
                "economics_score": 64,
                "competitor_count": 4,
                "delivery_listing_count": 11,
                "population_reach": 13200,
                "landuse_label": "Commercial",
            },
        ]
    )

    result = compare_candidates(db, "search-1", ["c1", "c2"])

    assert result["items"][0]["confidence_grade"] == "B"
    assert result["items"][0]["gate_status_json"] == {"overall_pass": False}
    assert result["items"][0]["demand_thesis"] == "Demand is moderate"
    assert result["items"][0]["zoning_fit_score"] == 88
    assert result["items"][0]["frontage_score"] == 66
    assert result["items"][0]["gate_reasons_json"]["failed"] == ["frontage/access"]
    assert result["items"][0]["gate_reasons_json"]["unknown"] == []
    assert result["items"][0]["gate_reasons_json"]["thresholds"] == {}
    assert result["items"][0]["cost_thesis"] == "Cost is manageable"
    assert result["items"][0]["comparable_competitors_json"] == [{"id": "r1"}]
    assert result["summary"]["best_gate_pass_candidate_id"] == "c2"


def test_get_candidate_memo_returns_recommendation_shape():
    db = FakeDB(
        memo_row={
            "candidate_id": "c1",
            "search_id": "search-1",
            "brand_name": "Brand X",
            "category": "burger",
            "service_model": "qsr",
            "parcel_id": "p1",
            "district": "Olaya",
            "area_m2": 180,
            "landuse_label": "Commercial",
            "final_score": 82,
            "economics_score": 75,
            "demand_score": 80,
            "whitespace_score": 70,
            "fit_score": 78,
            "zoning_fit_score": 82,
            "frontage_score": 67,
            "access_score": 69,
            "parking_score": 60,
            "access_visibility_score": 68,
            "confidence_score": 85,
            "cannibalization_score": 35,
            "distance_to_nearest_branch_m": 2200,
            "estimated_rent_sar_m2_year": 980,
            "estimated_annual_rent_sar": 176400,
            "estimated_fitout_cost_sar": 468000,
            "estimated_revenue_index": 74,
            "key_strengths_json": ["Strong demand index supports branch throughput"],
            "key_risks_json": ["Competitive density may pressure launch economics"],
            "decision_summary": "summary",
            "gate_status_json": {"overall_pass": True, "zoning_fit_pass": True},
            "gate_reasons_json": {"passed": ["zoning_fit_pass"], "failed": []},
            "feature_snapshot_json": {"parcel_area_m2": 180, "touches_road": True},
            "comparable_competitors_json": [{"id": "r1", "name": "Comp"}],
            "demand_thesis": "Demand looks strong",
            "cost_thesis": "Costs are manageable",
            "confidence_grade": "A",
        }
    )

    memo = get_candidate_memo(db, "c1")

    assert memo is not None
    assert memo["candidate_id"] == "c1"
    assert memo["recommendation"]["verdict"] in {"go", "consider", "caution"}
    assert memo["candidate"]["key_strengths"]
    assert memo["candidate"]["gate_reasons"]["passed"] == ["zoning fit"]
    assert memo["candidate"]["gate_reasons"]["unknown"] == []
    assert memo["candidate"]["score_breakdown_json"]["weights"] == {}
    assert memo["candidate"]["feature_snapshot"]["touches_road"] is True
    assert memo["candidate"]["comparable_competitors"][0]["id"] == "r1"


def test_run_expansion_search_caches_rent_resolution_by_district(monkeypatch, disable_market_viability_floors):
    db = FakeDB(
        candidate_rows=[
            {
                "parcel_id": "p1",
                "landuse_label": "Commercial",
                "landuse_code": "C",
                "area_m2": 160,
                "lon": 46.70,
                "lat": 24.70,
                "district": "حي العليا",
                "population_reach": 12000,
                "competitor_count": 4,
                "delivery_listing_count": 11,
            },
            {
                "parcel_id": "p2",
                "landuse_label": "Commercial",
                "landuse_code": "C",
                "area_m2": 170,
                "lon": 46.71,
                "lat": 24.71,
                "district": "العليا",
                "population_reach": 11800,
                "competitor_count": 4,
                "delivery_listing_count": 10,
            },
            {
                "parcel_id": "p3",
                "landuse_label": "Commercial",
                "landuse_code": "C",
                "area_m2": 180,
                "lon": 46.72,
                "lat": 24.72,
                "district": "الملقا",
                "population_reach": 12500,
                "competitor_count": 3,
                "delivery_listing_count": 12,
            },
        ]
    )

    calls: list[str | None] = []

    def _fake_rent(_db, district):
        calls.append(district)
        return (900.0, "test")

    monkeypatch.setattr(expansion_service, "_estimate_rent_sar_m2_year", _fake_rent)

    items = run_expansion_search(
        db,
        search_id="search-1",
        brand_name="Brand X",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=300,
        target_area_m2=170,
        limit=10,
    )

    assert len(items) == 3
    assert len(calls) == 2


def test_report_happy_path_returns_best_and_runner_up(monkeypatch):
    db = FakeDB(candidate_rows=[], brand_profile_row={"price_tier": "mid", "preferred_districts_json": [], "excluded_districts_json": []})
    import app.services.expansion_advisor as svc
    monkeypatch.setattr(svc, "get_search", lambda _db, _sid, **_kw: {"id": "search-1", "service_model": "qsr", "brand_profile": {"expansion_goal": "balanced"}})
    monkeypatch.setattr(svc, "get_candidates", lambda _db, _sid, district_lookup=None, **_kw: [
        {"id": "c1", "final_score": 90, "brand_fit_score": 82, "economics_score": 70, "area_m2": 170, "district": "Olaya", "key_risks_json": ["risk"]},
        {"id": "c2", "final_score": 86, "brand_fit_score": 79, "economics_score": 68, "area_m2": 180, "district": "Malqa", "key_risks_json": ["risk2"]},
    ])
    report = get_recommendation_report(db, "search-1")
    assert report is not None
    assert report["recommendation"]["best_candidate_id"] == "c1"
    assert report["meta"]["version"] == "expansion_advisor_v7"


def test_brand_provider_scores_bounded(disable_market_viability_floors):
    db = FakeDB(candidate_rows=[{
        "parcel_id": "p1", "landuse_label": "Commercial", "landuse_code": "C", "area_m2": 180, "lon": 46.7, "lat": 24.7, "district": "Olaya",
        "population_reach": 15000, "competitor_count": 20, "delivery_listing_count": 200, "provider_listing_count": 200, "provider_platform_count": 10, "delivery_competition_count": 400
    }])
    items = run_expansion_search(db, search_id="s", brand_name="b", category="burger", service_model="qsr", min_area_m2=100, max_area_m2=300, target_area_m2=180, limit=3)
    assert 0 <= items[0]["brand_fit_score"] <= 100
    assert 0 <= items[0]["provider_density_score"] <= 100
    assert 0 <= items[0]["provider_whitespace_score"] <= 100
    assert 0 <= items[0]["multi_platform_presence_score"] <= 100
    assert 0 <= items[0]["delivery_competition_score"] <= 100


def test_brand_fit_responds_to_multi_platform_presence():
    base_kwargs = dict(
        district="Olaya",
        area_m2=220,
        demand_score=72,
        fit_score=70,
        cannibalization_score=42,
        provider_density_score=65,
        provider_whitespace_score=58,
        delivery_competition_score=48,
        visibility_signal=74,
        parking_signal=62,
        brand_profile={"primary_channel": "delivery", "expansion_goal": "balanced"},
        service_model="qsr",
    )

    low_platform = _brand_fit_score(multi_platform_presence_score=20, **base_kwargs)
    high_platform = _brand_fit_score(multi_platform_presence_score=90, **base_kwargs)

    assert high_platform != low_platform
    assert high_platform > low_platform


def test_gate_status_logic():
    gates, reasons = _candidate_gate_status(
        fit_score=60,
        area_fit_score=80,
        area_m2=200,
        min_area_m2=100,
        max_area_m2=300,
        zoning_fit_score=80,
        landuse_available=True,
        frontage_score=70,
        access_score=66,
        parking_score=55,
        district="Olaya",
        distance_to_nearest_branch_m=2200,
        provider_density_score=50,
        multi_platform_presence_score=40,
        economics_score=65,
        brand_profile={"primary_channel": "delivery", "excluded_districts": ["Malqa"], "cannibalization_tolerance_m": 1800},
        road_context_available=True,
        parking_context_available=True,
    )
    assert gates["overall_pass"] is True
    assert gates["district_pass"] is True
    assert reasons["failed"] == []


def test_confidence_grade_bounds():
    assert _confidence_grade(confidence_score=88, district="Olaya", provider_platform_count=2, multi_platform_presence_score=50, rent_source="aqar_city", data_completeness_score=90) == "A"
    assert _confidence_grade(confidence_score=70, district=None, provider_platform_count=None, multi_platform_presence_score=None, rent_source="conservative_default") in {"B", "C"}
    assert _confidence_grade(confidence_score=30, district=None, provider_platform_count=None, multi_platform_presence_score=None, rent_source="conservative_default") == "D"


def test_competition_whitespace_unknown_confidence_is_neutral_not_open():
    """count=0 must only score wide-open (100) on confirmed evidence.

    Unknown confidence (``None`` — e.g. the ArcGIS-fallback pool path that
    bypasses bulk competitor enrichment) and explicit ``False`` (scan ran
    but found thin POI coverage) must both fall back to the neutral
    midpoint. Treating "we don't know" as "we know it's empty" fabricates
    evidence and pushes thin-coverage candidates up the ranking.
    """
    from app.services.expansion_advisor import _competition_whitespace_score

    # Unknown confidence + zero observed competitors -> neutral, NOT 100.
    assert _competition_whitespace_score(0, confident=None) == 50.0
    # Defensive (scan ran, thin coverage) -> neutral.
    assert _competition_whitespace_score(0, confident=False) == 50.0
    # Confirmed broader presence + zero same-category -> genuine greenfield.
    assert _competition_whitespace_score(0, confident=True) == 100.0
    # Sanity: positive counts are unaffected by the confidence flag and
    # decay below the wide-open ceiling.
    for _flag in (None, False, True):
        score = _competition_whitespace_score(3, confident=_flag)
        assert 15.0 <= score < 100.0


def test_comparable_competitors_payload_shape():
    class _DB:
        def begin_nested(self):
            return _FakeNestedTransaction()
        def execute(self, *_args, **_kwargs):
            return _Result([
                {"id": "r1", "name": "A", "category": "burger", "district": "Olaya", "rating": 4.2, "review_count": 100, "distance_m": 320.5, "source": "google"}
            ])

    items = _comparable_competitors(_DB(), category="burger", lat=24.7, lon=46.7)
    assert items
    assert {"id", "name", "category", "district", "rating", "review_count", "distance_m", "source"}.issubset(items[0].keys())


def test_report_includes_new_decision_outputs(monkeypatch):
    db = FakeDB(candidate_rows=[])
    import app.services.expansion_advisor as svc
    monkeypatch.setattr(svc, "get_search", lambda _db, _sid, **_kw: {"id": "search-1", "service_model": "qsr", "brand_profile": {"expansion_goal": "balanced"}})
    monkeypatch.setattr(svc, "get_candidates", lambda _db, _sid, district_lookup=None, **_kw: [
        {"id": "c1", "final_score": 90, "brand_fit_score": 82, "economics_score": 70, "area_m2": 170, "district": "Olaya", "key_risks_json": ["risk"], "confidence_grade": "A", "confidence_score": 85, "gate_status_json": {"overall_pass": True}, "demand_thesis": "d", "cost_thesis": "c", "comparable_competitors_json": [{"id": "x"}], "zoning_fit_score": 88, "frontage_score": 65, "access_score": 67, "parking_score": 62, "access_visibility_score": 66, "feature_snapshot_json": {"parcel_area_m2": 170, "data_completeness_score": 90}, "rank_position": 1, "score_breakdown_json": {"final_score": 90}, "top_positives_json": ["pos"], "top_risks_json": ["risk"]},
        {"id": "c2", "final_score": 86, "brand_fit_score": 79, "economics_score": 68, "area_m2": 180, "district": "Malqa", "key_risks_json": ["risk2"], "confidence_grade": "B", "confidence_score": 72, "gate_status_json": {"overall_pass": False}, "demand_thesis": "d2", "cost_thesis": "c2", "comparable_competitors_json": [], "zoning_fit_score": 78, "frontage_score": 61, "access_score": 60, "parking_score": 58, "access_visibility_score": 61, "feature_snapshot_json": {"parcel_area_m2": 180, "data_completeness_score": 80}, "rank_position": 2, "score_breakdown_json": {"final_score": 86}, "top_positives_json": ["pos2"], "top_risks_json": ["risk2"]},
    ])
    report = get_recommendation_report(db, "search-1")
    assert report["recommendation"]["best_pass_candidate_id"] == "c1"
    assert report["recommendation"]["best_confidence_candidate_id"] == "c1"
    assert "score_breakdown_json" in report["top_candidates"][0]
    assert "rank_position" in report["top_candidates"][0]
    assert "feature_snapshot_json" in report["top_candidates"][0]
    assert report["top_candidates"][0]["rank_position"] == 1
    assert "score_breakdown_json" in report["top_candidates"][0]


def test_v6_feature_scores_are_bounded():
    assert 0 <= expansion_service._zoning_fit_score("commercial", "C") <= 100
    assert 0 <= expansion_service._frontage_score(parcel_perimeter_m=240, touches_road=True, nearby_road_count=5, nearest_major_road_m=120) <= 100
    assert 0 <= expansion_service._access_score(touches_road=False, nearest_major_road_m=350, nearby_road_count=2) <= 100
    assert 0 <= expansion_service._parking_score(area_m2=180, service_model="qsr", nearby_parking_count=3, access_score=65) <= 100


def test_gate_status_uses_v6_scores_for_failure():
    gates, reasons = _candidate_gate_status(
        fit_score=75,
        area_fit_score=80,
        area_m2=200,
        min_area_m2=100,
        max_area_m2=300,
        zoning_fit_score=40,
        landuse_available=True,
        frontage_score=30,
        access_score=30,
        parking_score=20,
        district="Olaya",
        distance_to_nearest_branch_m=2600,
        provider_density_score=60,
        multi_platform_presence_score=70,
        economics_score=75,
        brand_profile={"excluded_districts": [], "cannibalization_tolerance_m": 1800},
        road_context_available=True,
        parking_context_available=True,
    )
    assert gates["overall_pass"] is False
    assert "zoning_fit_pass" in reasons["failed"]
    assert "frontage_access_pass" in reasons["failed"]
    assert "parking_pass" in reasons["failed"]


def test_missing_road_context_uses_neutral_scores_and_unknown_gate(monkeypatch, disable_market_viability_floors):
    db = FakeDB(candidate_rows=[{
        "parcel_id": "p1", "landuse_label": "Commercial", "landuse_code": "C", "area_m2": 180, "lon": 46.7, "lat": 24.7, "district": "Olaya",
        "population_reach": 15000, "competitor_count": 5, "delivery_listing_count": 12
    }])

    monkeypatch.setattr(expansion_service, "_table_available", lambda _db, _table: False)

    items = run_expansion_search(db, search_id="s", brand_name="b", category="burger", service_model="qsr", min_area_m2=100, max_area_m2=300, target_area_m2=180, limit=3)
    item = items[0]
    assert item["frontage_score"] == 50.0
    assert item["access_score"] == 50.0
    assert "frontage/access" in item["gate_reasons_json"]["unknown"]
    # With both road tables missing, frontage/parking gates are unknown (None),
    # so overall_pass is None (indeterminate), not True.
    assert item["gate_status_json"]["overall_pass"] is None


def test_missing_parking_context_uses_neutral_score_and_unknown_gate(monkeypatch, disable_market_viability_floors):
    db = FakeDB(candidate_rows=[{
        "parcel_id": "p1", "landuse_label": "Commercial", "landuse_code": "C", "area_m2": 180, "lon": 46.7, "lat": 24.7, "district": "Olaya",
        "population_reach": 15000, "competitor_count": 5, "delivery_listing_count": 12
    }])

    monkeypatch.setattr(expansion_service, "_table_available", lambda _db, table: table == "public.planet_osm_line")

    items = run_expansion_search(db, search_id="s", brand_name="b", category="burger", service_model="qsr", min_area_m2=100, max_area_m2=300, target_area_m2=180, limit=3)
    item = items[0]
    assert 0.0 <= item["parking_score"] <= 100.0
    assert "parking" in item["gate_reasons_json"]["unknown"]


def test_score_breakdown_matches_final_score():
    breakdown = expansion_service._score_breakdown(
        demand_score=80,
        whitespace_score=70,
        brand_fit_score=75,
        economics_score=60,
        provider_intelligence_composite=65,
        access_visibility_score=55,
        confidence_score=50,
        listing_quality_score=60,
    )
    weighted_sum = sum((breakdown.get("weighted_components") or {}).values())
    assert abs(weighted_sum - breakdown["final_score"]) < 0.01
    assert 0.0 <= breakdown["final_score"] <= 100.0


def _baseline_breakdown_kwargs():
    return dict(
        demand_score=80,
        whitespace_score=70,
        brand_fit_score=75,
        economics_score=60,
        provider_intelligence_composite=65,
        access_visibility_score=55,
        confidence_score=50,
        listing_quality_score=60,
    )


def test_brand_weight_reweight_neutral_profile_is_noop():
    """Neutral / empty / all-medium profiles must leave weights byte-identical."""
    baseline = expansion_service._score_breakdown(**_baseline_breakdown_kwargs())
    static_weights = baseline["weights"]

    # No profile at all.
    assert (
        expansion_service._score_breakdown(
            **_baseline_breakdown_kwargs(), brand_profile=None, service_model="qsr"
        )["weights"]
        == static_weights
    )

    # Explicitly neutral knobs.
    neutral = {
        "parking_sensitivity": "medium",
        "frontage_sensitivity": "medium",
        "visibility_sensitivity": "medium",
        "primary_channel": "balanced",
        "expansion_goal": "balanced",
    }
    neutral_weights = expansion_service._score_breakdown(
        **_baseline_breakdown_kwargs(), brand_profile=neutral, service_model="qsr"
    )["weights"]
    assert neutral_weights == static_weights
    assert abs(sum(neutral_weights.values()) - 100) < 1e-3


def test_brand_weight_reweight_gain_zero_disables(monkeypatch):
    baseline = expansion_service._score_breakdown(**_baseline_breakdown_kwargs())
    monkeypatch.setattr(
        expansion_service.settings, "EXPANSION_BRAND_WEIGHT_GAIN", 0.0, raising=False
    )
    aggressive = {
        "parking_sensitivity": "high",
        "primary_channel": "delivery",
        "expansion_goal": "delivery_led",
    }
    weights = expansion_service._score_breakdown(
        **_baseline_breakdown_kwargs(), brand_profile=aggressive, service_model="qsr"
    )["weights"]
    assert weights == baseline["weights"]


def test_brand_weight_reweight_high_parking_lifts_access_visibility(monkeypatch):
    monkeypatch.setattr(
        expansion_service.settings, "EXPANSION_BRAND_WEIGHT_GAIN", 0.35, raising=False
    )
    baseline = expansion_service._score_breakdown(**_baseline_breakdown_kwargs())
    weights = expansion_service._score_breakdown(
        **_baseline_breakdown_kwargs(),
        brand_profile={"parking_sensitivity": "high"},
        service_model="qsr",
    )["weights"]
    assert weights["access_visibility"] > baseline["weights"]["access_visibility"]
    assert abs(sum(weights.values()) - 100) < 1e-3


def test_brand_weight_reweight_delivery_channel_lifts_delivery_demand(monkeypatch):
    monkeypatch.setattr(
        expansion_service.settings, "EXPANSION_BRAND_WEIGHT_GAIN", 0.35, raising=False
    )
    baseline = expansion_service._score_breakdown(**_baseline_breakdown_kwargs())
    weights = expansion_service._score_breakdown(
        **_baseline_breakdown_kwargs(),
        brand_profile={"primary_channel": "delivery"},
        service_model="qsr",
    )["weights"]
    assert weights["delivery_demand"] > baseline["weights"]["delivery_demand"]
    assert abs(sum(weights.values()) - 100) < 1e-3


def test_compare_includes_v61_fields():
    db = FakeDB(
        compare_rows=[
            {
                "id": "c1", "parcel_id": "p1", "district": "Olaya", "area_m2": 150, "final_score": 80, "demand_score": 75,
                "whitespace_score": 70, "fit_score": 85, "zoning_fit_score": 88, "frontage_score": 66, "access_score": 64,
                "parking_score": 62, "access_visibility_score": 65, "confidence_score": 79, "confidence_grade": "B",
                "gate_status_json": {"overall_pass": True}, "gate_reasons_json": {"passed": ["zoning_fit_pass"], "unknown": []},
                "feature_snapshot_json": {"context_sources": {"road_context_available": True, "parking_context_available": True}},
                "score_breakdown_json": {"final_score": 80}, "top_positives_json": ["good"], "top_risks_json": ["risk"],
                "demand_thesis": "Demand is moderate", "cost_thesis": "Cost is manageable", "comparable_competitors_json": [],
                "cannibalization_score": 40, "distance_to_nearest_branch_m": 2300, "estimated_rent_sar_m2_year": 960,
                "estimated_annual_rent_sar": 144000, "estimated_fitout_cost_sar": 390000, "estimated_revenue_index": 71,
                "economics_score": 68, "brand_fit_score": 70, "provider_density_score": 50, "provider_whitespace_score": 60,
                "multi_platform_presence_score": 60, "delivery_competition_score": 50,
                "competitor_count": 3, "delivery_listing_count": 12, "population_reach": 14000,
                "landuse_label": "Commercial", "rank_position": 1,
            },
            {
                "id": "c2", "parcel_id": "p2", "district": "Malqa", "area_m2": 170, "final_score": 74, "demand_score": 69,
                "whitespace_score": 62, "fit_score": 73, "zoning_fit_score": 80, "frontage_score": 70, "access_score": 72,
                "parking_score": 68, "access_visibility_score": 71, "confidence_score": 86, "confidence_grade": "A",
                "gate_status_json": {"overall_pass": True}, "gate_reasons_json": {"passed": ["zoning_fit_pass"], "unknown": []},
                "feature_snapshot_json": {}, "score_breakdown_json": {"final_score": 74}, "top_positives_json": [], "top_risks_json": [],
                "demand_thesis": "Demand is strong", "cost_thesis": "Cost is higher", "comparable_competitors_json": [],
                "cannibalization_score": 35, "distance_to_nearest_branch_m": 2500, "estimated_rent_sar_m2_year": 990,
                "estimated_annual_rent_sar": 168300, "estimated_fitout_cost_sar": 430000, "estimated_revenue_index": 70,
                "economics_score": 64, "brand_fit_score": 69, "provider_density_score": 48, "provider_whitespace_score": 58,
                "multi_platform_presence_score": 58, "delivery_competition_score": 52,
                "competitor_count": 4, "delivery_listing_count": 11, "population_reach": 13200,
                "landuse_label": "Commercial", "rank_position": 2,
            },
        ]
    )
    result = compare_candidates(db, "search-1", ["c1", "c2"])
    assert "score_breakdown_json" in result["items"][0]
    assert "top_positives_json" in result["items"][0]
    assert "top_risks_json" in result["items"][0]
    assert result["items"][0]["rank_position"] == 1
    assert result["items"][0]["score_breakdown_json"]["weights"] == {}


def test_search_caches_context_table_checks_and_limits_snapshot_work(monkeypatch, disable_market_viability_floors):
    expansion_service.clear_expansion_caches()
    candidate_rows = []
    for idx in range(120):
        candidate_rows.append(
            {
                "parcel_id": f"p{idx}",
                "landuse_label": "Commercial",
                "landuse_code": "C",
                "area_m2": 140 + (idx % 30),
                "lon": 46.7 + idx * 0.0001,
                "lat": 24.7 + idx * 0.0001,
                "district": f"District_{idx % 20}",
                "population_reach": 12000,
                "competitor_count": 4,
                "delivery_listing_count": 10,
            }
        )
    db = FakeDB(candidate_rows=candidate_rows)

    table_calls: list[str] = []
    snapshot_calls = 0

    def _fake_table_available(_db, table_name):
        table_calls.append(table_name)
        return True

    def _fake_snapshot(*_args, **_kwargs):
        nonlocal snapshot_calls
        snapshot_calls += 1
        return {
            "parcel_area_m2": 150,
            "parcel_perimeter_m": 250,
            "district": f"District_{snapshot_calls % 20}",
            "landuse_label": "Commercial",
            "landuse_code": "C",
            "nearest_major_road_distance_m": 120,
            "nearby_road_segment_count": 4,
            "touches_road": True,
            "nearby_parking_amenity_count": 2,
            "provider_listing_count": 10,
            "provider_platform_count": 3,
            "competitor_count": 4,
            "nearest_branch_distance_m": 2000,
            "rent_source": "test",
            "estimated_rent_sar_m2_year": 900 + (snapshot_calls * 50),
            "economics_score": 60,
            "context_sources": {
                "roads_table_available": True,
                "parking_table_available": True,
                "road_context_available": True,
                "parking_context_available": True,
            },
            "missing_context": [],
            "data_completeness_score": 100,
        }

    def _fake_ea_table_has_rows(_db, table_name):
        return False

    monkeypatch.setattr(expansion_service, "_table_available", _fake_table_available)
    monkeypatch.setattr(expansion_service, "_ea_table_has_rows", _fake_ea_table_has_rows)
    monkeypatch.setattr(expansion_service, "_candidate_feature_snapshot", _fake_snapshot)

    items = run_expansion_search(
        db,
        search_id="s",
        brand_name="b",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=300,
        target_area_m2=180,
        limit=10,
    )

    assert len(items) == 10
    assert table_calls == ["public.planet_osm_line", "public.planet_osm_polygon"]
    assert snapshot_calls == 25


def test_feature_snapshot_queries_road_and_parking_independently():
    class _DB:
        def begin_nested(self):
            return _FakeNestedTransaction()
        def execute(self, stmt, _params=None):
            sql = stmt.text if hasattr(stmt, "text") else str(stmt)
            if "ST_Perimeter" in sql:
                return _Result([{"parcel_perimeter_m": 260.0}])
            if "FROM planet_osm_line" in sql:
                return _Result([{"nearest_major_road_distance_m": 120.0, "nearby_road_segment_count": 3, "touches_road": True}])
            if "FROM planet_osm_polygon" in sql:
                raise AssertionError("parking query should not run when parking table unavailable")
            return _Result([])

    snapshot = expansion_service._candidate_feature_snapshot(
        _DB(),
        parcel_id="p1",
        lat=24.7,
        lon=46.7,
        area_m2=180,
        district="Olaya",
        landuse_label="Commercial",
        landuse_code="C",
        provider_listing_count=5,
        provider_platform_count=2,
        competitor_count=3,
        nearest_branch_distance_m=2000,
        rent_source="test",
        estimated_rent_sar_m2_year=900,
        economics_score=60,
        roads_table_available=True,
        parking_table_available=False,
    )

    assert snapshot["nearby_road_segment_count"] == 3
    assert snapshot["touches_road"] is True
    assert snapshot["context_sources"]["road_context_available"] is True
    assert snapshot["context_sources"]["parking_context_available"] is False


def test_get_search_normalizes_sparse_legacy_row(monkeypatch):
    class _SearchDB:
        def execute(self, stmt, _params=None):
            sql = stmt.text if hasattr(stmt, "text") else str(stmt)
            if "FROM expansion_search" in sql:
                return _Result([
                    {
                        "id": "search-legacy",
                        "created_at": None,
                        "brand_name": "Brand",
                        "category": "burger",
                        "service_model": "qsr",
                        "target_districts": None,
                        "min_area_m2": 100,
                        "max_area_m2": 250,
                        "target_area_m2": None,
                        "bbox": None,
                        "request_json": None,
                        "notes": None,
                        "existing_branches": None,
                    }
                ])
            return _Result([])

    monkeypatch.setattr(expansion_service, "get_brand_profile", lambda *_args, **_kwargs: None)

    payload = get_search(_SearchDB(), "search-legacy")

    assert payload is not None
    assert payload["target_districts"] == []
    assert payload["request_json"] == {}
    assert payload["notes"] == {}
    assert payload["existing_branches"] == []
    assert payload["brand_profile"] == {}
    assert payload["meta"]["version"] == "expansion_advisor_v7"


def test_get_saved_search_normalizes_sparse_nested_payload(monkeypatch):
    class _SavedDB:
        def execute(self, stmt, _params=None):
            sql = stmt.text if hasattr(stmt, "text") else str(stmt)
            if "FROM expansion_saved_search" in sql:
                return _Result([
                    {
                        "id": "saved-1",
                        "search_id": "search-1",
                        "title": "Study",
                        "description": None,
                        "status": "draft",
                        "selected_candidate_ids": None,
                        "filters_json": None,
                        "ui_state_json": None,
                        "created_at": None,
                        "updated_at": None,
                    }
                ])
            return _Result([])

    monkeypatch.setattr(expansion_service, "get_search", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(expansion_service, "get_candidates", lambda *_args, **_kwargs: None)

    payload = expansion_service.get_saved_search(_SavedDB(), "saved-1")

    assert payload is not None
    assert payload["selected_candidate_ids"] == []
    assert payload["filters_json"] == {}
    assert payload["ui_state_json"] == {}
    assert payload["search"] is None
    assert payload["candidates"] == []


def test_compare_candidates_returns_full_summary_contract_for_empty_list():
    db = FakeDB(compare_rows=[])
    result = compare_candidates(db, "search-1", [])

    assert result["items"] == []
    assert set(result["summary"].keys()) == {
        "best_overall_candidate_id",
        "lowest_cannibalization_candidate_id",
        "highest_demand_candidate_id",
        "best_fit_candidate_id",
        "best_economics_candidate_id",
        "best_brand_fit_candidate_id",
        "strongest_delivery_market_candidate_id",
        "strongest_whitespace_candidate_id",
        "lowest_rent_burden_candidate_id",
        "best_value_candidate_id",
        "most_confident_candidate_id",
        "best_gate_pass_candidate_id",
    }
    assert all(value is None for value in result["summary"].values())


def test_get_recommendation_report_empty_state_is_deterministic(monkeypatch):
    monkeypatch.setattr(expansion_service, "get_search", lambda *_args, **_kwargs: {"id": "search-1", "brand_profile": {}})
    monkeypatch.setattr(expansion_service, "get_candidates", lambda *_args, **_kwargs: [])

    report = get_recommendation_report(FakeDB(), "search-1")

    assert report is not None
    assert report["meta"]["version"] == "expansion_advisor_v7"
    assert report["top_candidates"] == []
    assert set(report["recommendation"].keys()) == {
        "best_candidate_id",
        "runner_up_candidate_id",
        "best_pass_candidate_id",
        "best_confidence_candidate_id",
        "highest_demand_candidate_id",
        "best_economics_candidate_id",
        "best_brand_fit_candidate_id",
        "strongest_whitespace_candidate_id",
        "most_confident_candidate_id",
        "best_value_candidate_id",
        "why_best",
        "main_risk",
        "best_format",
        "summary",
        "report_summary",
    }
    assert "parcel_source" in report["assumptions"]


# ---------------------------------------------------------------------------
# Regression: full payload with brand_profile + existing_branches + districts
# ---------------------------------------------------------------------------

def test_run_expansion_search_with_brand_profile_and_branches(disable_market_viability_floors):
    """Regression test: the exact payload shape that triggered the 500.

    Ensures the scoring pipeline handles brand_profile, existing_branches,
    and target_districts together without raising.
    """
    db = FakeDB(
        candidate_rows=[
            {
                "parcel_id": "p1",
                "landuse_label": "Commercial",
                "landuse_code": "C",
                "area_m2": 200,
                "lon": 46.7,
                "lat": 24.7,
                "district": "حي العليا",
                "population_reach": 18000,
                "competitor_count": 4,
                "delivery_listing_count": 12,
            }
        ]
    )

    brand_profile = {
        "price_tier": "premium",
        "primary_channel": "delivery",
        "expansion_goal": "delivery_led",
        "preferred_districts": ["Olaya"],
        "excluded_districts": ["Malqa"],
    }
    existing_branches = [
        {"name": "HQ", "lat": 24.71, "lon": 46.68, "district": "Olaya"},
        {"name": "Branch 2", "lat": 24.75, "lon": 46.72, "district": "Malqa"},
    ]

    items = run_expansion_search(
        db,
        search_id="search-regression",
        brand_name="Brand X",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=350,
        target_area_m2=200,
        limit=10,
        target_districts=["العليا"],
        existing_branches=existing_branches,
        brand_profile=brand_profile,
    )

    assert len(items) == 1
    item = items[0]
    assert item["parcel_id"] == "p1"
    assert 0.0 <= item["final_score"] <= 100.0
    assert item["cannibalization_score"] is not None
    assert item["distance_to_nearest_branch_m"] is not None
    assert item["economics_score"] is not None
    assert "estimated_payback_months" not in item
    assert "payback_band" not in item
    assert "gate_status_json" in item
    assert "score_breakdown_json" in item
    assert "top_positives_json" in item
    assert "top_risks_json" in item
    assert "comparable_competitors_json" in item
    assert "feature_snapshot_json" in item


def test_run_expansion_search_no_candidates_returns_empty():
    """When the main query returns no rows, we should get an empty list—not a crash."""
    db = FakeDB(candidate_rows=[])

    items = run_expansion_search(
        db,
        search_id="search-empty",
        brand_name="Brand X",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=300,
        target_area_m2=180,
        limit=10,
    )

    assert items == []


def test_comparable_competitors_returns_empty_on_db_error():
    """_comparable_competitors should gracefully return [] if the DB query fails."""

    class BrokenDB:
        def execute(self, _stmt, _params=None):
            raise RuntimeError("connection lost")

    result = _comparable_competitors(BrokenDB(), category="burger", lat=24.7, lon=46.7)
    assert result == []


# ---------------------------------------------------------------------------
# Regression: empty existing_branches must not crash (production 500 trigger)
# ---------------------------------------------------------------------------


def test_run_expansion_search_empty_existing_branches(disable_market_viability_floors):
    """Regression: empty existing_branches list must score candidates without crash."""
    db = FakeDB(
        candidate_rows=[
            {
                "parcel_id": "p1",
                "landuse_label": "Commercial",
                "landuse_code": "C",
                "area_m2": 200,
                "lon": 46.7,
                "lat": 24.7,
                "district": "حي العليا",
                "population_reach": 18000,
                "competitor_count": 4,
                "delivery_listing_count": 12,
            }
        ]
    )

    items = run_expansion_search(
        db,
        search_id="search-empty-branches",
        brand_name="Test",
        category="Burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=15,
        existing_branches=[],
        brand_profile={
            "preferred_districts": ["Alolaya"],
        },
    )

    assert len(items) == 1
    item = items[0]
    assert item["distance_to_nearest_branch_m"] is None
    assert item["cannibalization_score"] == 0.0
    assert 0.0 <= item["final_score"] <= 100.0
    assert item["economics_score"] is not None
    assert "estimated_payback_months" not in item


def test_run_expansion_search_preferred_districts_typo_no_crash(disable_market_viability_floors):
    """Regression: misspelled preferred_districts must not crash; they simply have no effect."""
    db = FakeDB(
        candidate_rows=[
            {
                "parcel_id": "p1",
                "landuse_label": "Commercial",
                "landuse_code": "C",
                "area_m2": 200,
                "lon": 46.7,
                "lat": 24.7,
                "district": "حي العليا",
                "population_reach": 15000,
                "competitor_count": 3,
                "delivery_listing_count": 10,
            }
        ]
    )

    items = run_expansion_search(
        db,
        search_id="search-typo-district",
        brand_name="Test",
        category="Burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=15,
        existing_branches=[{"name": "HQ", "lat": 24.71, "lon": 46.68}],
        brand_profile={
            "preferred_districts": ["Alolaya"],
            "excluded_districts": ["Nonexistent"],
        },
    )

    assert len(items) == 1
    assert 0.0 <= items[0]["brand_fit_score"] <= 100.0
    assert 0.0 <= items[0]["final_score"] <= 100.0


def test_run_expansion_search_unmatched_target_districts_returns_empty():
    """When target_districts don't match any DB districts, return empty list—not a crash."""
    db = FakeDB(
        candidate_rows=[
            {
                "parcel_id": "p1",
                "landuse_label": "Commercial",
                "landuse_code": "C",
                "area_m2": 200,
                "lon": 46.7,
                "lat": 24.7,
                "district": "حي العليا",
                "population_reach": 15000,
                "competitor_count": 3,
                "delivery_listing_count": 10,
            }
        ]
    )

    items = run_expansion_search(
        db,
        search_id="search-unmatched",
        brand_name="Test",
        category="Burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=15,
        target_districts=["Nonexistent District"],
        existing_branches=[],
    )

    assert items == []


def test_run_expansion_search_exact_production_payload(disable_market_viability_floors):
    """Regression: exact payload shape that triggered the production 500."""
    db = FakeDB(
        candidate_rows=[
            {
                "parcel_id": "p1",
                "landuse_label": "Commercial",
                "landuse_code": "C",
                "area_m2": 200,
                "lon": 46.7,
                "lat": 24.7,
                "district": "Al Olaya",
                "population_reach": 18000,
                "competitor_count": 4,
                "delivery_listing_count": 12,
            }
        ]
    )

    brand_profile = {
        "preferred_districts": ["Alolaya"],
    }

    items = run_expansion_search(
        db,
        search_id="search-prod-repro",
        brand_name="Test",
        category="Burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=15,
        target_districts=["Al Olaya", "Al Malqa", "Al Nakheel"],
        existing_branches=[],
        brand_profile=brand_profile,
    )

    # "Al Olaya" in candidate matches "Al Olaya" in target_districts
    assert len(items) == 1
    item = items[0]
    assert item["distance_to_nearest_branch_m"] is None
    assert item["cannibalization_score"] == 0.0
    assert 0.0 <= item["final_score"] <= 100.0
    assert "payback_band" not in item
    assert "gate_status_json" in item
    assert "score_breakdown_json" in item


# ---------------------------------------------------------------------------
# Regression: production payload reproducing search_id=c3ace4a6-…
# ---------------------------------------------------------------------------


class _FailingNestedTransaction:
    """Simulates a SAVEPOINT that rolls back (DB error inside nested block)."""
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class FailingQueryDB(FakeDB):
    """FakeDB subclass that raises on specific queries to simulate production failures."""

    def __init__(self, *args, fail_on=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.fail_on = fail_on or []

    def execute(self, stmt, params=None):
        sql = stmt.text if hasattr(stmt, "text") else str(stmt)
        for pattern in self.fail_on:
            if pattern in sql:
                raise RuntimeError(f"Simulated DB error on: {pattern}")
        return super().execute(stmt, params)


def test_snapshot_db_failure_does_not_poison_session(monkeypatch, disable_market_viability_floors):
    """Regression: when _candidate_feature_snapshot sub-queries fail,
    the session must remain usable and candidates still get persisted."""
    db = FakeDB(candidate_rows=[{
        "parcel_id": "p1", "landuse_label": "Commercial", "landuse_code": "C",
        "area_m2": 180, "lon": 46.7, "lat": 24.7, "district": "Olaya",
        "population_reach": 15000, "competitor_count": 2, "delivery_listing_count": 10,
    }])

    original_snapshot = expansion_service._candidate_feature_snapshot

    def _failing_snapshot(db_arg, **kwargs):
        # Simulate a snapshot that internally catches query failures
        # (as the real one does with begin_nested + try/except)
        return {
            "parcel_area_m2": kwargs.get("area_m2", 0),
            "parcel_perimeter_m": None,
            "district": kwargs.get("district"),
            "landuse_label": kwargs.get("landuse_label"),
            "landuse_code": kwargs.get("landuse_code"),
            "nearest_major_road_distance_m": None,
            "nearby_road_segment_count": 0,
            "touches_road": False,
            "nearby_parking_amenity_count": 0,
            "provider_listing_count": kwargs.get("provider_listing_count", 0),
            "provider_platform_count": kwargs.get("provider_platform_count", 0),
            "competitor_count": kwargs.get("competitor_count", 0),
            "nearest_branch_distance_m": kwargs.get("nearest_branch_distance_m"),
            "rent_source": kwargs.get("rent_source", "conservative_default"),
            "estimated_rent_sar_m2_year": kwargs.get("estimated_rent_sar_m2_year", 900),
            "economics_score": kwargs.get("economics_score", 0),
            "context_sources": {
                "roads_table_available": False,
                "parking_table_available": False,
                "road_context_available": False,
                "parking_context_available": False,
            },
            "missing_context": ["roads_table_unavailable", "parking_table_unavailable"],
            "data_completeness_score": 40,
        }

    monkeypatch.setattr(expansion_service, "_candidate_feature_snapshot", _failing_snapshot)

    items = run_expansion_search(
        db, search_id="s-snap-fail", brand_name="b", category="burger",
        service_model="qsr", min_area_m2=100, max_area_m2=300,
        target_area_m2=180, limit=5,
    )
    assert len(items) >= 1
    assert items[0]["frontage_score"] == 50.0
    assert items[0]["access_score"] == 50.0


def test_candidate_insert_failure_skips_candidate_gracefully(monkeypatch, disable_market_viability_floors):
    """Bulk insert fails, row-wise fallback saves p2 but p1 fails individually."""
    insert_call = 0

    class InsertFailDB(FakeDB):
        def execute(self, stmt, params=None):
            nonlocal insert_call
            sql = stmt.text if hasattr(stmt, "text") else str(stmt)
            if "INSERT INTO expansion_candidate" in sql:
                insert_call += 1
                if insert_call == 1:
                    # First call is the bulk batch — fail it to trigger fallback
                    raise RuntimeError("Simulated bulk insert failure")
                # Row-wise fallback: fail p1, succeed p2
                if isinstance(params, dict) and params.get("parcel_id") == "p1":
                    raise RuntimeError("Simulated row insert failure for p1")
                self.inserted.append(params)
                return _Result([])
            return super().execute(stmt, params)

    db = InsertFailDB(candidate_rows=[
        {
            "parcel_id": "p1", "landuse_label": "Commercial", "landuse_code": "C",
            "area_m2": 180, "lon": 46.7, "lat": 24.7, "district": "Olaya",
            "population_reach": 15000, "competitor_count": 2, "delivery_listing_count": 10,
        },
        {
            "parcel_id": "p2", "landuse_label": "Commercial", "landuse_code": "C",
            "area_m2": 170, "lon": 46.71, "lat": 24.71, "district": "Malqa",
            "population_reach": 13000, "competitor_count": 3, "delivery_listing_count": 8,
        },
    ])

    items = run_expansion_search(
        db, search_id="s-insert-fail", brand_name="b", category="burger",
        service_model="qsr", min_area_m2=100, max_area_m2=300,
        target_area_m2=180, limit=5,
    )
    # Bulk insert fails → row-wise fallback: p1 fails individually, p2 succeeds
    assert len(items) == 1
    assert items[0]["parcel_id"] == "p2"


def test_district_mismatch_returns_empty_result_not_500(monkeypatch):
    """When target_districts don't match any candidate districts, return empty list (not crash)."""
    db = FakeDB(candidate_rows=[{
        "parcel_id": "p1", "landuse_label": "Commercial", "landuse_code": "C",
        "area_m2": 180, "lon": 46.7, "lat": 24.7, "district": "الملقا",
        "population_reach": 12000, "competitor_count": 3, "delivery_listing_count": 8,
    }])

    items = run_expansion_search(
        db, search_id="s-dist-mismatch", brand_name="b", category="burger",
        service_model="qsr", min_area_m2=100, max_area_m2=300,
        target_area_m2=180, limit=5,
        target_districts=["NonExistentDistrict", "حي_لا_يوجد"],
    )
    assert items == []


def test_rent_lookup_failure_falls_back_to_default(monkeypatch, disable_market_viability_floors):
    """When aqar_rent_median raises, rent falls back to conservative_default."""
    db = FakeDB(candidate_rows=[{
        "parcel_id": "p1", "landuse_label": "Commercial", "landuse_code": "C",
        "area_m2": 180, "lon": 46.7, "lat": 24.7, "district": "Olaya",
        "population_reach": 15000, "competitor_count": 2, "delivery_listing_count": 10,
    }])

    def _boom_rent(_db, _city, **_kwargs):
        raise RuntimeError("rent DB down")

    monkeypatch.setattr(expansion_service, "aqar_rent_median", _boom_rent)

    items = run_expansion_search(
        db, search_id="s-rent-fail", brand_name="b", category="burger",
        service_model="qsr", min_area_m2=100, max_area_m2=300,
        target_area_m2=180, limit=5,
    )
    assert len(items) == 1
    # Base fallback rent is 900.0; micro-location multiplier adjusts it
    # within [0.70, 1.35] range based on local commercial signals.
    assert 900.0 * 0.70 <= items[0]["estimated_rent_sar_m2_year"] <= 900.0 * 1.35


def test_production_payload_c3ace4a6_regression(monkeypatch):
    """Exact reproduction of the production payload that triggered search_id=c3ace4a6-…
    500 error. The search must succeed and return candidates or empty list."""
    db = FakeDB(candidate_rows=[
        {
            "parcel_id": "p-prod-1", "landuse_label": "Commercial", "landuse_code": "C",
            "area_m2": 220, "lon": 46.6812, "lat": 24.7136, "district": "حي العليا",
            "population_reach": 18200, "competitor_count": 6, "delivery_listing_count": 22,
            "provider_listing_count": 35, "provider_platform_count": 4, "delivery_competition_count": 15,
        },
        {
            "parcel_id": "p-prod-2", "landuse_label": None, "landuse_code": None,
            "area_m2": 150, "lon": 46.7243, "lat": 24.7401, "district": None,
            "population_reach": 8500, "competitor_count": 1, "delivery_listing_count": 5,
            "provider_listing_count": 8, "provider_platform_count": 2, "delivery_competition_count": 3,
        },
        {
            "parcel_id": "p-prod-3", "landuse_label": "Residential", "landuse_code": "R",
            "area_m2": 310, "lon": 46.6500, "lat": 24.7600, "district": "حي الملقا",
            "population_reach": 11000, "competitor_count": 4, "delivery_listing_count": 14,
            "provider_listing_count": 20, "provider_platform_count": 3, "delivery_competition_count": 8,
        },
    ])

    items = run_expansion_search(
        db,
        search_id="c3ace4a6-9e4f-405f-887c-7f4e9c9e98e6",
        brand_name="Test Burger Co",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=15,
        bbox={"min_lon": 46.5, "min_lat": 24.5, "max_lon": 46.9, "max_lat": 24.9},
        target_districts=["العليا", "الملقا"],
        existing_branches=[
            {"name": "Main Branch", "lat": 24.71, "lon": 46.68, "district": "Olaya"},
            {"name": "Branch 2", "lat": 24.75, "lon": 46.72},
        ],
        brand_profile={
            "price_tier": "premium",
            "primary_channel": "delivery",
            "expansion_goal": "delivery_led",
            "preferred_districts": ["العليا"],
            "excluded_districts": ["الملقا"],
            "parking_sensitivity": "low",
            "frontage_sensitivity": "high",
            "visibility_sensitivity": "high",
            "cannibalization_tolerance_m": 1500,
        },
    )

    # Must not crash; must return candidates or empty
    assert isinstance(items, list)
    for item in items:
        assert 0.0 <= item["final_score"] <= 100.0
        assert "payback_band" not in item
        assert "gate_status_json" in item
        assert "score_breakdown_json" in item
        assert "feature_snapshot_json" in item
        assert "top_positives_json" in item
        assert "top_risks_json" in item
        assert "decision_summary" in item
        assert "demand_thesis" in item
        assert "cost_thesis" in item


# ---------------------------------------------------------------------------
# Regression tests: bbox params must never cause AmbiguousParameter (GH #500)
# ---------------------------------------------------------------------------

_BBOX_CANDIDATE_ROW = {
    "parcel_id": "bbox-p1",
    "landuse_label": "Commercial",
    "landuse_code": "C",
    "area_m2": 200,
    "lon": 46.7,
    "lat": 24.7,
    "district": "Al Olaya",
    "population_reach": 12000,
    "competitor_count": 3,
    "delivery_listing_count": 8,
    "provider_listing_count": 5,
    "provider_platform_count": 2,
    "delivery_competition_count": 4,
}

_BBOX_BASE_KWARGS = dict(
    search_id="search-1",
    brand_name="Test",
    category="Burger",
    service_model="qsr",
    min_area_m2=100,
    max_area_m2=500,
    target_area_m2=200,
    limit=15,
    target_districts=["Al Olaya"],
    existing_branches=[],
)


def test_run_expansion_search_no_bbox():
    """bbox=None must not trigger AmbiguousParameter."""
    db = FakeDB(candidate_rows=[_BBOX_CANDIDATE_ROW])
    items = run_expansion_search(db, **_BBOX_BASE_KWARGS, bbox=None)
    assert isinstance(items, list)
    for item in items:
        assert 0.0 <= item["final_score"] <= 100.0


def test_run_expansion_search_empty_bbox():
    """bbox={} (no keys) must not trigger AmbiguousParameter."""
    db = FakeDB(candidate_rows=[_BBOX_CANDIDATE_ROW])
    items = run_expansion_search(db, **_BBOX_BASE_KWARGS, bbox={})
    assert isinstance(items, list)
    for item in items:
        assert 0.0 <= item["final_score"] <= 100.0


def test_run_expansion_search_partial_bbox():
    """One-sided bbox (only min_lon, min_lat) must work."""
    db = FakeDB(candidate_rows=[_BBOX_CANDIDATE_ROW])
    items = run_expansion_search(
        db,
        **_BBOX_BASE_KWARGS,
        bbox={"min_lon": 46.5, "min_lat": 24.5},
    )
    assert isinstance(items, list)
    for item in items:
        assert 0.0 <= item["final_score"] <= 100.0


def test_run_expansion_search_full_bbox():
    """Full bbox with all four bounds must work."""
    db = FakeDB(candidate_rows=[_BBOX_CANDIDATE_ROW])
    items = run_expansion_search(
        db,
        **_BBOX_BASE_KWARGS,
        bbox={"min_lon": 46.5, "min_lat": 24.5, "max_lon": 46.9, "max_lat": 24.9},
    )
    assert isinstance(items, list)
    for item in items:
        assert 0.0 <= item["final_score"] <= 100.0


def test_run_expansion_search_production_payload():
    """Exact production payload that triggered the 500 must return safely."""
    db = FakeDB(candidate_rows=[_BBOX_CANDIDATE_ROW])
    items = run_expansion_search(
        db,
        search_id="search-1",
        brand_name="Test",
        category="Burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=15,
        bbox=None,
        target_districts=["Al Olaya", "Al Malqa", "Al Nakheel"],
        existing_branches=[],
        brand_profile={
            "strategy": "balanced",
            "price_tier": "mid",
            "visibility_sensitivity": "35",
            "cannibalization_tolerance_m": 1500,
        },
    )
    assert isinstance(items, list)
    for item in items:
        assert 0.0 <= item["final_score"] <= 100.0
        assert "payback_band" not in item


def test_run_expansion_search_no_bbox_empty_result():
    """No matching candidates with null bbox returns empty list, not 500."""
    db = FakeDB(candidate_rows=[])
    items = run_expansion_search(db, **_BBOX_BASE_KWARGS, bbox=None)
    assert items == []


# ---------------------------------------------------------------------------
# Regression: gate logic fix — area_fit_pass uses area_fit directly, zoning
# gate treats missing landuse as unknown, and overall_pass is three-state.
# ---------------------------------------------------------------------------

_GATE_BASE = dict(
    fit_score=75,
    area_m2=200,
    min_area_m2=100,
    max_area_m2=300,
    frontage_score=70,
    access_score=66,
    parking_score=55,
    district="Olaya",
    distance_to_nearest_branch_m=2200,
    provider_density_score=50,
    multi_platform_presence_score=40,
    economics_score=65,
    brand_profile={"excluded_districts": [], "cannibalization_tolerance_m": 1800},
    road_context_available=True,
    parking_context_available=True,
)


def test_area_inside_range_passes_area_gate():
    """Candidate with real parcel area inside the requested range -> area_fit_pass True."""
    gates, reasons = _candidate_gate_status(
        **_GATE_BASE,
        area_fit_score=85.0,  # well inside range
        zoning_fit_score=80.0,
        landuse_available=True,
    )
    assert gates["area_fit_pass"] is True
    assert "area_fit_pass" in reasons["passed"]


def test_area_outside_range_fails_area_gate():
    """Candidate with parcel area outside range -> area_fit_pass False."""
    gates, reasons = _candidate_gate_status(
        **{**_GATE_BASE, "area_m2": 600},  # outside 100–300 range -> hard fail
        area_fit_score=20.0,
        zoning_fit_score=80.0,
        landuse_available=True,
    )
    assert gates["area_fit_pass"] is False
    assert "area_fit_pass" in reasons["failed"]


def test_missing_zoning_context_produces_unknown_not_fail():
    """Candidate with missing landuse context -> zoning gate unknown, not hard fail."""
    gates, reasons = _candidate_gate_status(
        **_GATE_BASE,
        area_fit_score=85.0,
        zoning_fit_score=45.0,  # below threshold, but landuse data is absent
        landuse_available=False,
    )
    assert gates["zoning_fit_pass"] is None
    assert "zoning_fit_pass" in reasons["unknown"]
    assert "zoning_fit_pass" not in reasons["failed"]
    # overall should NOT be False just because zoning is unknown
    assert gates["overall_pass"] is not False


def test_contradictory_zoning_fails_gate():
    """Candidate with clearly incompatible zoning (e.g. residential) -> zoning_fit_pass False."""
    gates, reasons = _candidate_gate_status(
        **_GATE_BASE,
        area_fit_score=85.0,
        zoning_fit_score=40.0,  # residential zone, below 60 threshold
        landuse_available=True,  # real data, legitimately fails
    )
    assert gates["zoning_fit_pass"] is False
    assert "zoning_fit_pass" in reasons["failed"]
    assert gates["overall_pass"] is False


def test_production_like_mixed_verdicts():
    """Production-like scenario: candidates with varying data should produce
    a mix of pass/unknown/fail instead of universal fail."""
    # Candidate 1: good area, good zoning (Commercial)
    g1, r1 = _candidate_gate_status(
        **_GATE_BASE,
        area_fit_score=90.0,
        zoning_fit_score=100.0,
        landuse_available=True,
    )

    # Candidate 2: good area, missing zoning
    g2, r2 = _candidate_gate_status(
        **_GATE_BASE,
        area_fit_score=85.0,
        zoning_fit_score=45.0,
        landuse_available=False,
    )

    # Candidate 3: area outside range, good zoning
    g3, r3 = _candidate_gate_status(
        **{**_GATE_BASE, "area_m2": 600},  # outside 100–300 range -> hard fail
        area_fit_score=20.0,
        zoning_fit_score=100.0,
        landuse_available=True,
    )

    # Should produce discriminative verdicts, not all False
    assert g1["overall_pass"] is True, "good candidate should pass"
    assert g2["overall_pass"] is None, "missing-zoning candidate should be unknown, not fail"
    assert g3["overall_pass"] is False, "out-of-range candidate should fail"

    # Verify they are distinct
    verdicts = {g1["overall_pass"], g2["overall_pass"], g3["overall_pass"]}
    assert len(verdicts) == 3, f"expected 3 distinct verdicts, got {verdicts}"


# ---------------------------------------------------------------------------
# New focused tests for expansion-advisor backend fix patch
# ---------------------------------------------------------------------------

from app.services.expansion_advisor import (
    _area_fit,
    _gate_key_to_label,
    _gate_verdict_label,
    _score_breakdown,
    _top_positives_and_risks,
)


def test_large_parcel_not_favored_when_target_is_small():
    """A 500 m² parcel with target 200 m² and min/max 100/500 should yield low
    area_fit and should NOT be the top-ranked candidate just because it is the
    max-size parcel."""
    area_fit_500 = _area_fit(500, target_area_m2=200, min_area_m2=100, max_area_m2=500)
    area_fit_200 = _area_fit(200, target_area_m2=200, min_area_m2=100, max_area_m2=500)
    area_fit_250 = _area_fit(250, target_area_m2=200, min_area_m2=100, max_area_m2=500)

    # 500 is 300 away from target in a 400-span, so ~25 — much lower than 200
    assert area_fit_200 == 100.0, "Exact target should score 100"
    assert area_fit_500 < 30, f"500 m² with target 200 should score low, got {area_fit_500}"
    assert area_fit_250 > area_fit_500, "250 m² should score better than 500 m²"


def test_report_best_pass_candidate_id_null_when_no_pass(monkeypatch):
    """get_recommendation_report() must set best_pass_candidate_id = None when
    no candidates pass all gates."""
    import app.services.expansion_advisor as svc

    db = FakeDB(candidate_rows=[], brand_profile_row={
        "price_tier": "mid",
        "preferred_districts_json": [],
        "excluded_districts_json": [],
    })
    monkeypatch.setattr(svc, "get_search", lambda _db, _sid, **_kw: {
        "id": "search-1",
        "service_model": "qsr",
        "brand_profile": {"expansion_goal": "balanced"},
    })
    # Both candidates have overall_pass=False
    monkeypatch.setattr(svc, "get_candidates", lambda _db, _sid, district_lookup=None, **_kw: [
        {
            "id": "c1", "final_score": 75, "brand_fit_score": 60, "economics_score": 55,
            "area_m2": 200, "district": "Olaya", "key_risks_json": ["risk"],
            "gate_status_json": {"overall_pass": False},
            "confidence_grade": "C", "confidence_score": 60,
            "rank_position": 1,
            "score_breakdown_json": {"final_score": 75},
            "top_positives_json": [], "top_risks_json": ["risk"],
            "feature_snapshot_json": {"parcel_area_m2": 200, "data_completeness_score": 70},
        },
        {
            "id": "c2", "final_score": 70, "brand_fit_score": 58, "economics_score": 52,
            "area_m2": 180, "district": "Malqa", "key_risks_json": ["risk2"],
            "gate_status_json": {"overall_pass": False},
            "confidence_grade": "C", "confidence_score": 55,
            "rank_position": 2,
            "score_breakdown_json": {"final_score": 70},
            "top_positives_json": [], "top_risks_json": ["risk2"],
            "feature_snapshot_json": {"parcel_area_m2": 180, "data_completeness_score": 60},
        },
    ])

    report = get_recommendation_report(db, "search-1")

    assert report is not None
    assert report["recommendation"]["best_pass_candidate_id"] is None
    # best_candidate_id should still be set (exploratory)
    assert report["recommendation"]["best_candidate_id"] == "c1"
    # Language should be explicitly exploratory
    assert "no" in report["recommendation"]["why_best"].lower() or "not" in report["recommendation"]["why_best"].lower()
    assert "pass" in report["recommendation"]["why_best"].lower() or "gate" in report["recommendation"]["why_best"].lower()


def test_gate_verdict_serializes_to_pass_fail_unknown():
    """_gate_verdict_label must map True/False/None to pass/fail/unknown."""
    assert _gate_verdict_label(True) == "pass"
    assert _gate_verdict_label(False) == "fail"
    assert _gate_verdict_label(None) == "unknown"
    # Edge case: non-bool values map to "unknown" since only exact True/False are matched
    assert _gate_verdict_label(0) == "unknown"
    assert _gate_verdict_label("") == "unknown"


def test_top_positives_and_risks_no_raw_gate_keys():
    """top_positives and top_risks must not contain raw internal gate keys
    like 'zoning_fit_pass' or 'frontage_access_pass'."""
    candidate = {
        "demand_score": 30,
        "whitespace_score": 30,
        "brand_fit_score": 30,
        "economics_score": 30,
        "delivery_competition_score": 30,
        "cannibalization_score": 80,
        "gate_status_json": {"overall_pass": False},
    }
    gate_reasons = {
        "passed": ["district_pass"],
        "failed": ["zoning_fit_pass", "frontage_access_pass"],
        "unknown": ["parking_pass"],
    }

    positives, risks, positives_structured, risks_structured = _top_positives_and_risks(
        candidate=candidate, gate_reasons=gate_reasons,
    )
    assert isinstance(positives_structured, list) and isinstance(risks_structured, list)

    all_text = " ".join(positives + risks)
    for raw_key in ["zoning_fit_pass", "area_fit_pass", "frontage_access_pass",
                     "parking_pass", "district_pass", "cannibalization_pass",
                     "delivery_market_pass", "economics_pass"]:
        assert raw_key not in all_text, f"Raw gate key '{raw_key}' leaked into user-facing text"

    # Verify human labels are used instead
    assert any("zoning fit" in r.lower() for r in risks), "Should mention 'zoning fit'"
    assert any("frontage/access" in r.lower() for r in risks), "Should mention 'frontage/access'"
    assert any("parking" in r.lower() for r in risks), "Should mention 'parking'"


def test_gate_key_to_label_mapping():
    """Verify the gate-key mapping covers all known gates."""
    assert _gate_key_to_label("zoning_fit_pass") == "zoning fit"
    assert _gate_key_to_label("area_fit_pass") == "area fit"
    assert _gate_key_to_label("frontage_access_pass") == "frontage/access"
    assert _gate_key_to_label("parking_pass") == "parking"
    assert _gate_key_to_label("district_pass") == "district"
    assert _gate_key_to_label("cannibalization_pass") == "cannibalization"
    assert _gate_key_to_label("delivery_market_pass") == "delivery market"
    assert _gate_key_to_label("economics_pass") == "economics"


def test_score_breakdown_has_display_structure():
    """score_breakdown must include a 'display' dict with raw_input_score,
    weight_percent, and weighted_points for each component."""
    breakdown = _score_breakdown(
        demand_score=80,
        whitespace_score=70,
        brand_fit_score=75,
        economics_score=60,
        provider_intelligence_composite=65,
        access_visibility_score=55,
        confidence_score=50,
        listing_quality_score=60,
    )

    assert "display" in breakdown
    assert "demand_potential" in breakdown["display"]
    assert "listing_quality" in breakdown["display"]

    dp = breakdown["display"]["demand_potential"]
    assert "raw_input_score" in dp
    assert "weight_percent" in dp
    assert "weighted_points" in dp
    assert dp["raw_input_score"] == 80.0
    # 2026-05-07 rebalance: 10 -> 8.7640 (rescaled by 78/89).
    assert abs(dp["weight_percent"] - 8.7640) < 1e-6
    assert dp["weighted_points"] == round(80.0 * 0.087640, 2)

    # Verify listing_quality entry (2026-05-07: 11 -> 22 to elevate
    # CEO-directive recency and momentum signals).
    lq = breakdown["display"]["listing_quality"]
    assert lq["raw_input_score"] == 60.0
    assert lq["weight_percent"] == 22.0
    assert lq["weighted_points"] == round(60.0 * 0.22, 2)

    # Patch 13: landlord_signal is a new first-class component.
    # 2026-05-07 rebalance: 8 -> 7.0112 (rescaled by 78/89).
    # When the optional landlord_signal_score arg is omitted it falls back
    # to a neutral 50.0 so rows missing the LLM signal aren't penalized.
    assert "landlord_signal" in breakdown["display"]
    ls = breakdown["display"]["landlord_signal"]
    assert ls["raw_input_score"] == 50.0
    assert abs(ls["weight_percent"] - 7.0112) < 1e-6
    assert ls["weighted_points"] == round(50.0 * 0.070112, 2)

    # Verify weighted_points != weight_percent (they are NOT the same thing)
    for name, entry in breakdown["display"].items():
        assert entry["weighted_points"] != entry["weight_percent"] or entry["raw_input_score"] == 100.0, \
            f"{name}: weighted_points should differ from weight_percent unless input is 100"


def test_report_gate_verdict_uses_tristate(monkeypatch):
    """top_candidates[].gate_verdict in reports must use tri-state mapping,
    not bool()."""
    import app.services.expansion_advisor as svc

    db = FakeDB(candidate_rows=[], brand_profile_row={
        "price_tier": "mid",
        "preferred_districts_json": [],
        "excluded_districts_json": [],
    })
    monkeypatch.setattr(svc, "get_search", lambda _db, _sid, **_kw: {
        "id": "search-1",
        "service_model": "qsr",
        "brand_profile": {"expansion_goal": "balanced"},
    })
    monkeypatch.setattr(svc, "get_candidates", lambda _db, _sid, district_lookup=None, **_kw: [
        {
            "id": "c1", "final_score": 85, "brand_fit_score": 70, "economics_score": 65,
            "area_m2": 200, "district": "Olaya", "key_risks_json": ["risk"],
            "gate_status_json": {"overall_pass": None},
            "confidence_grade": "B", "confidence_score": 72,
            "rank_position": 1,
            "score_breakdown_json": {"final_score": 85},
            "top_positives_json": ["pos"], "top_risks_json": ["risk"],
            "feature_snapshot_json": {"parcel_area_m2": 200, "data_completeness_score": 80},
        },
    ])

    report = get_recommendation_report(db, "search-1")

    # With overall_pass=None, gate_verdict must be "unknown", not "fail"
    assert report["top_candidates"][0]["gate_verdict"] == "unknown"


def test_memo_gate_verdict_uses_tristate():
    """Candidate memo gate_verdict must render None overall_pass as 'unknown'."""
    db = FakeDB(
        memo_row={
            "candidate_id": "c1",
            "search_id": "search-1",
            "brand_name": "Brand X",
            "category": "burger",
            "service_model": "qsr",
            "parcel_id": "p1",
            "district": "Olaya",
            "area_m2": 180,
            "landuse_label": "Commercial",
            "final_score": 72,
            "economics_score": 55,
            "demand_score": 60,
            "whitespace_score": 60,
            "fit_score": 65,
            "zoning_fit_score": 70,
            "frontage_score": 55,
            "access_score": 55,
            "parking_score": 50,
            "access_visibility_score": 55,
            "confidence_score": 65,
            "cannibalization_score": 40,
            "distance_to_nearest_branch_m": 2000,
            "estimated_rent_sar_m2_year": 900,
            "estimated_annual_rent_sar": 162000,
            "estimated_fitout_cost_sar": 468000,
            "estimated_revenue_index": 60,
            "key_strengths_json": ["strength"],
            "key_risks_json": ["risk"],
            "decision_summary": "summary",
            "gate_status_json": {"overall_pass": None, "zoning_fit_pass": True, "frontage_access_pass": None},
            "gate_reasons_json": {"passed": ["zoning_fit_pass"], "failed": [], "unknown": ["frontage_access_pass"]},
            "feature_snapshot_json": {"parcel_area_m2": 180},
            "comparable_competitors_json": [],
            "demand_thesis": "d",
            "cost_thesis": "c",
            "confidence_grade": "C",
            "brand_fit_score": 60,
            "provider_density_score": 50,
            "provider_whitespace_score": 55,
            "multi_platform_presence_score": 45,
            "delivery_competition_score": 40,
            "score_breakdown_json": {},
            "top_positives_json": [],
            "top_risks_json": [],
            "rank_position": 3,
        }
    )

    memo = get_candidate_memo(db, "c1")

    assert memo is not None
    # With overall_pass=None, verdict should be "unknown", not "fail"
    assert memo["recommendation"]["gate_verdict"] == "unknown"


# ─── _derive_site_fit_context tests ──────────────────────────────

from app.services.expansion_advisor import _derive_site_fit_context


def test_derive_site_fit_context_with_road_and_parking():
    snapshot = {
        "context_sources": {
            "road_context_available": True,
            "parking_context_available": True,
        }
    }
    ctx = _derive_site_fit_context(snapshot)
    assert ctx["road_context_available"] is True
    assert ctx["parking_context_available"] is True
    assert ctx["frontage_score_mode"] == "observed"
    assert ctx["access_score_mode"] == "observed"
    assert ctx["parking_score_mode"] == "observed"


def test_derive_site_fit_context_no_road_context():
    snapshot = {
        "context_sources": {
            "road_context_available": False,
            "parking_context_available": True,
        }
    }
    ctx = _derive_site_fit_context(snapshot)
    assert ctx["road_context_available"] is False
    assert ctx["frontage_score_mode"] == "estimated"
    assert ctx["access_score_mode"] == "estimated"
    assert ctx["parking_score_mode"] == "observed"


def test_derive_site_fit_context_no_parking_context():
    snapshot = {
        "context_sources": {
            "road_context_available": True,
            "parking_context_available": False,
        }
    }
    ctx = _derive_site_fit_context(snapshot)
    assert ctx["parking_context_available"] is False
    assert ctx["parking_score_mode"] == "estimated"
    assert ctx["frontage_score_mode"] == "observed"


def test_derive_site_fit_context_none_snapshot():
    ctx = _derive_site_fit_context(None)
    assert ctx["road_context_available"] is False
    assert ctx["parking_context_available"] is False
    assert ctx["frontage_score_mode"] == "estimated"
    assert ctx["access_score_mode"] == "estimated"
    assert ctx["parking_score_mode"] == "estimated"


def test_derive_site_fit_context_empty_snapshot():
    ctx = _derive_site_fit_context({})
    assert ctx["frontage_score_mode"] == "estimated"
    assert ctx["parking_score_mode"] == "estimated"


def test_report_compatible_with_legacy_two_arg_get_candidates(monkeypatch):
    """get_recommendation_report must work when get_candidates is a 2-arg callable (legacy monkeypatch)."""
    db = FakeDB(candidate_rows=[])
    import app.services.expansion_advisor as svc
    monkeypatch.setattr(svc, "get_search", lambda _db, _sid, **_kw: {
        "id": "search-1",
        "service_model": "qsr",
        "brand_profile": {"expansion_goal": "balanced"},
    })
    # Legacy 2-arg callable — must not raise TypeError. get_recommendation_report
    # falls back to the 2-arg call form when the lang/district_lookup kwargs
    # are rejected.
    monkeypatch.setattr(svc, "get_candidates", lambda _db, _sid: [
        {"id": "c1", "final_score": 80, "brand_fit_score": 70, "economics_score": 60,
         "area_m2": 150, "district": "Olaya", "key_risks_json": []},
    ])
    report = get_recommendation_report(db, "search-1")
    assert report is not None
    assert report["recommendation"]["best_candidate_id"] == "c1"


# ── Realized-demand signal (rating_count Δ) ──

def test_delivery_score_backwards_compatible_without_realized_demand():
    """Legacy call signature must return the original supply-proxy score."""
    from app.services.expansion_advisor import _delivery_score

    # Reference point from the existing calibration: 40 listings → 100
    assert _delivery_score(0) == 0.0
    assert _delivery_score(40) == 100.0
    assert 0.0 < _delivery_score(10) < _delivery_score(40)
    # Explicit None realized_demand must equal listing-only score
    assert _delivery_score(10, realized_demand=None) == _delivery_score(10)
    # Zero realized_demand means "no signal" and must not drag the score down
    assert _delivery_score(10, realized_demand=0.0) == _delivery_score(10)


def test_delivery_score_blends_realized_demand_when_provided():
    """When realized_demand is present, it blends with listing-count."""
    from app.services.expansion_advisor import _delivery_score

    listing_only = _delivery_score(10)  # ~50
    # Realized demand at the calibration reference (p75 = 263 Δ ratings)
    # ≈ 100 on the realized curve.
    blended = _delivery_score(10, realized_demand=263.0, blend_weight=0.5)
    # Blend pulls the score toward the stronger realized signal
    assert blended > listing_only
    # Full-realized weight = realized score only
    realized_only = _delivery_score(
        10, realized_demand=263.0, blend_weight=1.0
    )
    assert abs(realized_only - 100.0) < 0.01
    # Zero blend weight = listing-only
    ignore_realized = _delivery_score(
        10, realized_demand=263.0, blend_weight=0.0
    )
    assert abs(ignore_realized - listing_only) < 0.01


def test_delivery_score_realized_low_demand_pulls_score_down():
    """Saturated supply but no realized growth signals stagnation."""
    from app.services.expansion_advisor import _delivery_score

    saturated_listing_only = _delivery_score(80)  # high supply score
    # Tiny realized-demand delta: catchment is over-served
    saturated_with_low_demand = _delivery_score(
        80, realized_demand=5.0, blend_weight=0.5
    )
    assert saturated_with_low_demand < saturated_listing_only


# ---------------------------------------------------------------------------
# Phase 2 — bounded LLM shortlist reranking integration
#
# These tests exercise _apply_rerank_to_candidates as it sits in the search
# pipeline (the wiring between the search service and generate_rerank).
# LLM unit-level behavior is covered in tests/test_expansion_rerank.py.
# ---------------------------------------------------------------------------
from unittest.mock import patch  # noqa: E402 — grouped with Phase 2 tests

import pytest  # noqa: E402

from app.core.config import settings as _ea_settings  # noqa: E402
from app.services.expansion_advisor import (  # noqa: E402
    _apply_rerank_to_candidates,
)


def _build_candidates(n: int, prefix: str = "p") -> list[dict]:
    """Build n candidates in deterministic rank order. final_score is
    monotonically decreasing so the list is pre-sorted."""
    return [
        {
            "parcel_id": f"{prefix}{i}",
            "final_score": 1.0 - i * 0.001,
            "feature_snapshot": {"area_m2": 300 + i * 10},
        }
        for i in range(1, n + 1)
    ]


def _ok_reason() -> dict:
    return {
        "summary": "moved after reweighing realized-demand and landlord signal",
        "positives_cited": [],
        "negatives_cited": [],
        "comparison_to_displaced_candidate": "the displaced candidate has a weaker overall fit",
    }


@pytest.fixture
def _rerank_on(monkeypatch):
    monkeypatch.setattr(_ea_settings, "EXPANSION_LLM_RERANK_ENABLED", True)


# 1. Flag off: every candidate gets default metadata, order unchanged.
def test_integration_flag_off_preserves_order_and_attaches_metadata(monkeypatch):
    monkeypatch.setattr(_ea_settings, "EXPANSION_LLM_RERANK_ENABLED", False)
    cands = _build_candidates(20)
    original_ids = [c["parcel_id"] for c in cands]
    with patch(
        "app.services.expansion_advisor.generate_rerank"
    ) as mock_gen:
        out = _apply_rerank_to_candidates(cands, {"category": "QSR"})
    # generate_rerank is called (the real impl short-circuits on flag off,
    # but the integration layer always calls through so all the metadata
    # statuses are driven by generate_rerank's return value).
    assert mock_gen.called
    assert [c["parcel_id"] for c in out] == original_ids
    for i, c in enumerate(out, start=1):
        assert c["deterministic_rank"] == i
        assert c["final_rank"] == i
        assert c["rerank_applied"] is False
        assert c["rerank_reason"] is None
        assert c["rerank_delta"] == 0


# 2. Flag on, no LLM moves: every candidate tagged "unchanged".
def test_integration_flag_on_no_moves_tags_unchanged(_rerank_on):
    cands = _build_candidates(10)
    unchanged_decisions = [
        {"parcel_id": f"p{i}", "original_rank": i, "new_rank": i,
         "rerank_reason": None}
        for i in range(1, 11)
    ]
    with patch(
        "app.services.expansion_advisor.generate_rerank",
        return_value=unchanged_decisions,
    ):
        out = _apply_rerank_to_candidates(cands, {})
    for i, c in enumerate(out, start=1):
        assert c["final_rank"] == c["deterministic_rank"] == i
        assert c["rerank_applied"] is False
        assert c["rerank_status"] == "unchanged"


# 3. Flag on, p3 <-> p5 swap: candidate list sorted by final_rank.
def test_integration_flag_on_swap_reorders_by_final_rank(_rerank_on):
    cands = _build_candidates(10)
    decisions = []
    for i in range(1, 11):
        if i == 3:
            decisions.append({
                "parcel_id": "p3", "original_rank": 3, "new_rank": 5,
                "rerank_reason": _ok_reason(),
            })
        elif i == 5:
            decisions.append({
                "parcel_id": "p5", "original_rank": 5, "new_rank": 3,
                "rerank_reason": _ok_reason(),
            })
        else:
            decisions.append({
                "parcel_id": f"p{i}", "original_rank": i, "new_rank": i,
                "rerank_reason": None,
            })
    with patch(
        "app.services.expansion_advisor.generate_rerank",
        return_value=decisions,
    ):
        out = _apply_rerank_to_candidates(cands, {})
    # p5 moves to rank 3, p3 moves to rank 5.
    assert [c["parcel_id"] for c in out] == [
        "p1", "p2", "p5", "p4", "p3", "p6", "p7", "p8", "p9", "p10"
    ]
    by_pid = {c["parcel_id"]: c for c in out}
    assert by_pid["p3"]["deterministic_rank"] == 3
    assert by_pid["p3"]["final_rank"] == 5
    assert by_pid["p3"]["rerank_delta"] == 2
    assert by_pid["p3"]["rerank_applied"] is True
    assert by_pid["p3"]["rerank_status"] == "applied"
    assert isinstance(by_pid["p3"]["rerank_reason"], dict)
    assert by_pid["p5"]["final_rank"] == 3
    assert by_pid["p5"]["rerank_delta"] == -2


# 4. Flag on, 50 candidates with cap 30: top 30 reviewed, bottom 20 outside.
def test_integration_cap_boundary_tags_outside_rerank_cap(_rerank_on):
    cands = _build_candidates(50)
    unchanged_decisions = [
        {"parcel_id": f"p{i}", "original_rank": i, "new_rank": i,
         "rerank_reason": None}
        for i in range(1, 31)
    ]
    with patch(
        "app.services.expansion_advisor.generate_rerank",
        return_value=unchanged_decisions,
    ) as mock_gen:
        out = _apply_rerank_to_candidates(cands, {})
    # generate_rerank called with exactly 30 candidates.
    call_args = mock_gen.call_args
    passed_shortlist = call_args[0][0]
    assert len(passed_shortlist) == 30
    # Top 30 tagged "unchanged" (LLM reviewed, no move).
    for c in out[:30]:
        assert c["rerank_status"] == "unchanged"
    # Bottom 20 tagged "outside_rerank_cap", rank unchanged.
    for c in out[30:]:
        assert c["rerank_status"] == "outside_rerank_cap"
        assert c["final_rank"] == c["deterministic_rank"]


# 5. Flag on, 2 candidates below min 3: generate_rerank NOT called.
def test_integration_below_min_skips_llm_entirely(_rerank_on):
    cands = _build_candidates(2)
    # We patch generate_rerank to verify it DOES get called (per the
    # integration-layer contract — generate_rerank itself returns None
    # when below min), but the caller must tag the candidates
    # "shortlist_below_minimum", not "llm_failed".
    with patch(
        "app.services.expansion_advisor.generate_rerank",
        return_value=None,
    ):
        out = _apply_rerank_to_candidates(cands, {})
    for c in out:
        assert c["rerank_status"] == "shortlist_below_minimum"
        assert c["final_rank"] == c["deterministic_rank"]


# 6. Flag on, LLM returns None (failure): all "llm_failed", order preserved.
def test_integration_llm_failure_preserves_order(_rerank_on):
    cands = _build_candidates(10)
    original_ids = [c["parcel_id"] for c in cands]
    with patch(
        "app.services.expansion_advisor.generate_rerank",
        return_value=None,
    ):
        out = _apply_rerank_to_candidates(cands, {})
    assert [c["parcel_id"] for c in out] == original_ids
    for c in out:
        assert c["rerank_status"] == "llm_failed"
        assert c["final_rank"] == c["deterministic_rank"]
        assert c["rerank_applied"] is False


# 7. Four canonical regression searches with flag off produce byte-for-byte
#    identical candidate order. No fixture infrastructure exists in
#    tests/test_expansion_advisor_regression.py for full canonical brand-
#    profile search runs (the regression file tests helper functions, not
#    end-to-end searches), so per the spec's fallback guidance we prove
#    the safety property directly on the only new pipeline stage
#    (_apply_rerank_to_candidates): with the flag off, four differently-
#    shaped candidate lists — representing the four canonical brand/
#    district combinations (QSR burger Al Olaya, delivery shawarma
#    citywide, dine-in Indian Al Nakheel, cafe Al Yasmin) — pass through
#    with identical parcel_id order and identical final_rank per position.
def test_integration_four_canonical_searches_flag_off_unchanged(monkeypatch):
    monkeypatch.setattr(_ea_settings, "EXPANSION_LLM_RERANK_ENABLED", False)

    canonical_searches = [
        # QSR burger Al Olaya — medium shortlist with dense scoring.
        ("qsr_burger_al_olaya",
         [{"parcel_id": f"olaya_q{i}", "final_score": 0.85 - i * 0.004,
           "district": "Al Olaya"} for i in range(1, 16)]),
        # Delivery shawarma citywide — large shortlist, cross-district.
        ("delivery_shawarma_citywide",
         [{"parcel_id": f"citywide_d{i}", "final_score": 0.78 - i * 0.002,
           "district": ["Al Olaya", "Al Yasmin", "Al Malqa", "Al Nakheel"][i % 4]}
          for i in range(1, 51)]),
        # Dine-in Indian Al Nakheel — small shortlist, premium category.
        ("dinein_indian_al_nakheel",
         [{"parcel_id": f"nakheel_di{i}", "final_score": 0.80 - i * 0.006,
           "district": "Al Nakheel"} for i in range(1, 11)]),
        # Cafe Al Yasmin — minimum-sized shortlist.
        ("cafe_al_yasmin",
         [{"parcel_id": f"yasmin_c{i}", "final_score": 0.76 - i * 0.005,
           "district": "Al Yasmin"} for i in range(1, 9)]),
    ]

    for search_label, cands in canonical_searches:
        original_ids = [c["parcel_id"] for c in cands]
        original_count = len(cands)
        # Copy so we can run the deterministic pipeline independently of
        # the mutation done by _apply_rerank_to_candidates.
        cands_copy = [dict(c) for c in cands]
        out = _apply_rerank_to_candidates(cands_copy, {})
        # Byte-for-byte same IDs in the same order — the load-bearing
        # safety property of Phase 2 in flag-off mode.
        assert [c["parcel_id"] for c in out] == original_ids, search_label
        assert len(out) == original_count, search_label
        # Every candidate has rerank metadata with non-moving defaults.
        for i, c in enumerate(out, start=1):
            assert c["deterministic_rank"] == i, search_label
            assert c["final_rank"] == i, search_label
            assert c["rerank_applied"] is False, search_label
            assert c["rerank_reason"] is None, search_label
            assert c["rerank_delta"] == 0, search_label


def test_brand_presence_aggregation_shape():
    """Verify the brand_presence aggregation produces the expected shape:
    top 5 by branch count, with unique brand count and total branch summary."""
    # Simulate raw rows from the per-candidate UNION ALL query:
    raw_rows = [
        {"candidate_pid": "cand1", "canonical_brand_id": "starbucks",
         "display_name_en": "Starbucks", "display_name_ar": "ستاربكس",
         "branch_count": 8, "nearest_distance_m": 120.0},
        {"candidate_pid": "cand1", "canonical_brand_id": "kfc",
         "display_name_en": "KFC", "display_name_ar": "كنتاكي",
         "branch_count": 3, "nearest_distance_m": 240.0},
        {"candidate_pid": "cand1", "canonical_brand_id": "burger_king",
         "display_name_en": "Burger King", "display_name_ar": "بيرجر كنج",
         "branch_count": 2, "nearest_distance_m": 310.0},
    ]

    # Mirror the in-service grouping/sort logic
    per_candidate: dict[str, list[dict]] = {}
    for r in raw_rows:
        per_candidate.setdefault(str(r["candidate_pid"]), []).append({
            "canonical_brand_id": r["canonical_brand_id"],
            "display_name_en": r.get("display_name_en"),
            "display_name_ar": r.get("display_name_ar"),
            "branch_count": int(r["branch_count"]),
            "nearest_distance_m": float(r.get("nearest_distance_m") or 0.0),
        })
    for brands in per_candidate.values():
        brands.sort(key=lambda b: (
            -b["branch_count"], b["nearest_distance_m"],
            b["canonical_brand_id"] or "",
        ))

    assert "cand1" in per_candidate
    chains = per_candidate["cand1"][:5]
    assert chains[0]["canonical_brand_id"] == "starbucks"
    assert chains[0]["branch_count"] == 8
    assert chains[1]["canonical_brand_id"] == "kfc"
    assert chains[2]["canonical_brand_id"] == "burger_king"

    # Top-level wrapper shape
    presence = {
        "radius_m": 500,
        "unique_brands": len(chains),
        "total_branches": sum(c["branch_count"] for c in chains),
        "top_chains": chains[:5],
    }
    assert presence["unique_brands"] == 3
    assert presence["total_branches"] == 13
    assert len(presence["top_chains"]) == 3


def test_brand_presence_name_fallback_sort_and_telemetry():
    """Patch 02: rows from the canonical sub-select and from the
    name-deduped non-canonical sub-select must coexist in the per-candidate
    list. Canonical entries sort first; the wrapper carries the new
    unique_brands_canonical / unique_brands_total telemetry fields and
    unique_brands stays as the union total (the value the gate reads)."""
    raw_rows = [
        # canonical rows
        {"candidate_pid": "p", "canonical_brand_id": "starbucks",
         "norm_name_key": None, "display_name_en": "Starbucks",
         "display_name_ar": "ستاربكس", "branch_count": 5,
         "nearest_distance_m": 120.0},
        {"candidate_pid": "p", "canonical_brand_id": "kfc",
         "norm_name_key": None, "display_name_en": "KFC",
         "display_name_ar": "كنتاكي", "branch_count": 2,
         "nearest_distance_m": 200.0},
        # non-canonical rows (alias-deduped, denylist-filtered upstream)
        {"candidate_pid": "p", "canonical_brand_id": None,
         "norm_name_key": "abu sufyan", "display_name_en": "Abu Sufyan",
         "display_name_ar": None, "branch_count": 9,
         "nearest_distance_m": 80.0},
        {"candidate_pid": "p", "canonical_brand_id": None,
         "norm_name_key": "al baik", "display_name_en": "Al Baik",
         "display_name_ar": None, "branch_count": 1,
         "nearest_distance_m": 400.0},
    ]

    per_candidate: dict[str, list[dict]] = {}
    for r in raw_rows:
        per_candidate.setdefault(str(r["candidate_pid"]), []).append({
            "canonical_brand_id": r["canonical_brand_id"],
            "norm_name_key": r.get("norm_name_key"),
            "display_name_en": r.get("display_name_en"),
            "display_name_ar": r.get("display_name_ar"),
            "branch_count": int(r["branch_count"]),
            "nearest_distance_m": float(r.get("nearest_distance_m") or 0.0),
        })
    for brands in per_candidate.values():
        brands.sort(key=lambda b: (
            b.get("canonical_brand_id") is None,
            -b["branch_count"],
            b.get("nearest_distance_m") or 0.0,
            b.get("canonical_brand_id") or b.get("norm_name_key") or "",
        ))

    chains = per_candidate["p"]
    # Canonical rows come first regardless of branch_count
    assert chains[0]["canonical_brand_id"] == "starbucks"
    assert chains[1]["canonical_brand_id"] == "kfc"
    # Then non-canonical, sorted by branch_count desc
    assert chains[2]["canonical_brand_id"] is None
    assert chains[2]["norm_name_key"] == "abu sufyan"
    assert chains[3]["norm_name_key"] == "al baik"

    canonical_count = sum(1 for c in chains if c.get("canonical_brand_id") is not None)
    presence = {
        "radius_m": 500,
        "unique_brands": len(chains),
        "unique_brands_canonical": canonical_count,
        "unique_brands_total": len(chains),
        "total_branches": sum(c["branch_count"] for c in chains),
        "top_chains": chains[:5],
    }
    assert presence["unique_brands"] == 4              # gate input is the union
    assert presence["unique_brands_canonical"] == 2    # pre-patch number
    assert presence["unique_brands_total"] == 4        # diagnostic mirror
    assert presence["total_branches"] == 17


# ---------------------------------------------------------------------------
# value_score chip — geometric mean of revenue_index and rent_burden_score.
# ---------------------------------------------------------------------------

import math

from app.services.expansion_advisor import (
    _value_score,
    _classify_value_band,
    _value_band_is_low_confidence,
    _value_band_score_delta,
    _VALUE_BAND_BEST_VALUE_MIN,
    _VALUE_BAND_ABOVE_MARKET_MAX,
)
from app.core.config import settings as _ea_settings

# Score-delta refactor: _apply_value_band_pass, _apply_llm_fuzzy_tiebreak,
# _FUZZY_TIE_WINDOW, _VALUE_{UP,DOWN}RANK_MAX_POSITIONS were removed; the
# value-band signal now contributes a fixed score delta (+4 / -6) folded
# into final_score by run_expansion_search rather than a positional nudge.
# See _value_band_score_delta and the bonus_detail tests below.


def test_value_score_geometric_mean_basic():
    # Symmetric extremes
    assert _value_score(0, 0) == _value_score(0.0, 0.0)
    # Both inputs at the eps floor → sqrt(1*1) = 1.0
    assert abs(_value_score(0, 0) - 1.0) < 1e-9
    # Both 100 → 100
    assert abs(_value_score(100, 100) - 100.0) < 1e-9
    # Symmetric mid-points
    assert abs(_value_score(50, 50) - 50.0) < 1e-9
    # Geometric mean property: sqrt(x*y)
    assert abs(_value_score(80, 20) - math.sqrt(80 * 20)) < 1e-9
    assert abs(_value_score(20, 80) - math.sqrt(80 * 20)) < 1e-9


def test_value_score_dead_corner_pulled_low():
    # Cheap dead corner: weak revenue, very cheap rent. Geometric mean
    # punishes this directly — a candidate near zero on either axis
    # cannot be "best value" by construction.
    cheap_dead = _value_score(20, 95)  # ≈ 43.6
    strong_pricey = _value_score(85, 78)  # ≈ 81.4
    assert cheap_dead < _VALUE_BAND_BEST_VALUE_MIN
    assert strong_pricey >= _VALUE_BAND_BEST_VALUE_MIN


def test_value_score_clamped_to_unit_interval():
    # NaN / negative / oversized inputs must clamp into [0, 100].
    assert 0.0 <= _value_score(-50, 50) <= 100.0
    assert 0.0 <= _value_score(200, 50) <= 100.0
    assert 0.0 <= _value_score(float("nan"), 50) <= 100.0


def test_classify_value_band_cutoffs():
    # Band cutoffs (per Faisal's directive):
    #   >= 75 → "best_value"
    #   25 <= x < 75 → "neutral"
    #   < 25 → "above_market"
    assert _classify_value_band(None) is None
    assert _classify_value_band(0) == "above_market"
    assert _classify_value_band(24.99) == "above_market"
    assert _classify_value_band(25.0) == "neutral"
    assert _classify_value_band(50) == "neutral"
    assert _classify_value_band(74.99) == "neutral"
    assert _classify_value_band(75.0) == "best_value"
    assert _classify_value_band(100) == "best_value"


def test_value_band_low_confidence_gating():
    # Citywide pools → low confidence regardless of N.
    assert _value_band_is_low_confidence("city_band_type", 12) is True
    assert _value_band_is_low_confidence("city", 20) is True
    assert _value_band_is_low_confidence("city_band_type", 200) is True
    # District-scoped pools → high confidence.
    assert _value_band_is_low_confidence("district_band_type", 8) is False
    assert _value_band_is_low_confidence("district_type", 8) is False
    assert _value_band_is_low_confidence("district", 8) is False
    # Unknown / envelope / None → not low-confidence (preserves no-badge).
    assert _value_band_is_low_confidence(None, 0) is False
    assert _value_band_is_low_confidence("listing_above_envelope", 0) is False


def _make_candidate(*, id_, final_score, value_band=None, low_conf=False):
    return {
        "id": id_,
        "parcel_id": id_,
        "final_score": final_score,
        "value_band": value_band,
        "value_band_low_confidence": low_conf,
    }


def test_value_band_score_delta_high_conf_best_value_uprank():
    c = _make_candidate(id_="c", final_score=70.0, value_band="best_value", low_conf=False)
    assert _value_band_score_delta(c) == 4.0


def test_value_band_score_delta_high_conf_above_market_downrank():
    c = _make_candidate(id_="c", final_score=70.0, value_band="above_market", low_conf=False)
    assert _value_band_score_delta(c) == -6.0


def test_value_band_score_delta_low_conf_is_inert():
    # Low-confidence pools (citywide) preserve the skip semantics from the
    # deleted positional pass: zero delta even when the band is set.
    c_best = _make_candidate(id_="c", final_score=70.0, value_band="best_value", low_conf=True)
    c_above = _make_candidate(id_="c", final_score=70.0, value_band="above_market", low_conf=True)
    assert _value_band_score_delta(c_best) == 0.0
    assert _value_band_score_delta(c_above) == 0.0


def test_value_band_score_delta_neutral_or_missing_is_inert():
    assert _value_band_score_delta(_make_candidate(id_="c", final_score=70.0, value_band="neutral")) == 0.0
    assert _value_band_score_delta(_make_candidate(id_="c", final_score=70.0, value_band=None)) == 0.0


def test_value_band_score_delta_reads_economics_detail_first():
    # Production candidates carry the band inside score_breakdown_json
    # rather than at the top level (top-level is set by
    # _normalize_candidate_payload, which runs after this delta is folded).
    c = {
        "id": "c",
        "parcel_id": "c",
        "score_breakdown_json": {
            "economics_detail": {
                "value_band": "best_value",
                "value_band_low_confidence": False,
            }
        },
    }
    assert _value_band_score_delta(c) == 4.0


# ---------------------------------------------------------------------------
# CEO directive #1 (2-of-3): market viability conjunction pass.
# Tests the rent_pct + population_reach soft positional demotion.
# ---------------------------------------------------------------------------

from app.services.expansion_advisor import _apply_market_viability_pass


def _make_viability_candidate(
    *,
    id_: str,
    final_score: float,
    rent_pct: float | None = None,
    rent_scope: str | None = "district_band_type",
    pop_reach: float | None = None,
    value_band: str | None = None,
    low_conf: bool = False,
    realized_demand_30d: float | None = None,
    realized_demand_branches: int | None = None,
) -> dict:
    sb: dict = {}
    if rent_pct is not None:
        sb["economics_detail"] = {
            "rent_burden": {
                "percentile": rent_pct,
                "source_label": rent_scope,
            }
        }
    if value_band is not None:
        sb.setdefault("economics_detail", {})["value_band"] = value_band
        sb["economics_detail"]["value_band_low_confidence"] = low_conf
    fs: dict = {}
    if pop_reach is not None:
        fs["population_reach"] = pop_reach
    if realized_demand_30d is not None:
        fs["realized_demand_30d"] = realized_demand_30d
    if realized_demand_branches is not None:
        fs["realized_demand_branches"] = realized_demand_branches
    return {
        "id": id_,
        "parcel_id": id_,
        "final_score": final_score,
        "score_breakdown_json": sb,
        "feature_snapshot_json": fs,
    }


def _viability_cohort(target_pop_reach: float, target_rent_pct: float = 0.85,
                      target_rent_scope: str = "district_band_type") -> list[dict]:
    """Build a cohort with a stable p25 around 7000 plus one target row."""
    pops = [5000, 6000, 7000, 8000, 50000, 60000, 70000, 80000]
    cohort = [
        _make_viability_candidate(
            id_=f"bg{i}",
            final_score=80.0 - i,
            rent_pct=0.40,
            rent_scope="district_band_type",
            pop_reach=p,
        )
        for i, p in enumerate(pops)
    ]
    target = _make_viability_candidate(
        id_="target",
        final_score=78.5,
        rent_pct=target_rent_pct,
        rent_scope=target_rent_scope,
        pop_reach=target_pop_reach,
    )
    cohort.insert(2, target)
    return cohort


def test_viability_pass_fires_when_pop_below_p25(disable_market_viability_floors):
    # Explicit cohort: pops [5k,6k,7k,8k,50k,60k,70k,80k] + target pop=4500
    # p25 of {4500,5k,6k,7k,8k,50k,60k,70k,80k} sits between 5k-6k; 4500<thr.
    cohort = _viability_cohort(target_pop_reach=4500.0, target_rent_pct=0.85)
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    # Score-delta refactor: viability no longer reorders the list. Each fired
    # leg contributes -10 to viability_delta. This cohort fires both the pop
    # leg (target pop=4500 < p25) and the rent leg (rent_pct=0.85 confident).
    assert "population_below_quartile" in target["viability_legs_fired"]
    assert target["viability_delta"] <= -10.0
    assert target["viability_delta"] == -10.0 * len(target["viability_legs_fired"])


def test_viability_pass_high_rent_high_pop_fires_rent_leg(disable_market_viability_floors):
    # Decoupled rent leg (clause 2): high rent_pct on a confident scope is
    # demoted on its own merit, even when population is high. Pre-decouple,
    # this case was NOT demoted because the 3-of-3 conjunction required
    # pop_low. Now the rent leg fires alone with reason="rent_high".
    cohort = _viability_cohort(target_pop_reach=80000.0, target_rent_pct=0.85)
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    assert flag["rent_demote"] is True
    assert flag["population_demote"] is False
    assert flag["reason"] == "rent_high"


def test_viability_pass_skips_low_confidence_rent_scope(disable_market_viability_floors):
    # rent_scope = "city_band_type" → citywide fallback, not confident, so
    # the rent leg does NOT fire. But pop=4500 is below p25, so the pop leg
    # fires alone. Pre-decouple this case was not demoted; now the pop leg
    # fires with reason="population_below_quartile".
    cohort = _viability_cohort(
        target_pop_reach=4500.0,
        target_rent_pct=0.85,
        target_rent_scope="city_band_type",
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    assert flag["rent_demote"] is False
    assert flag["population_demote"] is True
    assert flag["reason"] == "population_below_quartile"


def test_viability_pass_pop_missing_still_fires_rent_leg(disable_market_viability_floors):
    # Missing population_reach disables the pop leg defensively, but the
    # rent leg is decoupled and still fires when rent_pct is high on a
    # confident scope. Pre-decouple this case was not demoted at all; now
    # rent leg fires alone.
    cohort = _viability_cohort(target_pop_reach=4500.0, target_rent_pct=0.85)
    target_in = next(c for c in cohort if c["id"] == "target")
    target_in["feature_snapshot_json"].pop("population_reach", None)
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target_out = next(c for c in out if c["id"] == "target")
    flag = target_out["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    assert flag["rent_demote"] is True
    assert flag["population_demote"] is False
    assert flag["reason"] == "rent_high"


def test_viability_pass_demotion_capped_at_end(disable_market_viability_floors):
    # Background has confident rent_scope but low rent_pct, plus the last
    # row is the flagged target with target_pop_reach=4500.
    pops = [5000, 6000, 7000, 8000, 50000, 60000, 70000, 80000]
    cohort = [
        _make_viability_candidate(
            id_=f"bg{i}",
            final_score=80.0 - i,
            rent_pct=0.40,
            pop_reach=p,
        )
        for i, p in enumerate(pops)
    ]
    cohort.append(
        _make_viability_candidate(
            id_="last",
            final_score=10.0,
            rent_pct=0.90,
            pop_reach=4500.0,
        )
    )
    n = len(cohort)
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    assert len(out) == n
    last = next(c for c in out if c["id"] == "last")
    flag = last["score_breakdown_json"]["market_viability_flag"]
    assert flag["demoted"] is True
    # Score-delta refactor: caller (run_expansion_search) folds viability_delta
    # into final_score before sorting; the function itself no longer reorders.
    assert last["viability_delta"] <= -10.0


def test_viability_pass_cohort_too_small():
    cohort = [
        _make_viability_candidate(
            id_=f"c{i}",
            final_score=80.0 - i,
            rent_pct=0.85,
            pop_reach=4500.0,
        )
        for i in range(3)
    ]
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    for c in out:
        assert "market_viability_flag" not in c["score_breakdown_json"]


def test_viability_pass_p25_correctness(disable_market_viability_floors):
    # Cohort: pops [5k, 6k, 7k, 8k, 50k, 60k, 70k, 80k]. With this exact
    # set, statistics.quantiles inclusive p25 lands near 6750. Use low rent
    # (0.40) on every row to isolate the pop leg — under the decoupled rent
    # leg, a high rent_pct on every row would flag every row regardless of
    # pop. With low rent, only candidates whose pop_reach < ~6750 are flagged.
    pops = [5000, 6000, 7000, 8000, 50000, 60000, 70000, 80000]
    cohort = [
        _make_viability_candidate(
            id_=f"c{i}",
            final_score=80.0 - i,
            rent_pct=0.40,
            pop_reach=p,
        )
        for i, p in enumerate(pops)
    ]
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    flagged = {c["id"]: ("market_viability_flag" in c["score_breakdown_json"])
               for c in out}
    # 5k and 6k must be flagged (below ~6750 threshold).
    assert flagged["c0"] is True
    assert flagged["c1"] is True
    # 8k and above must NOT be flagged.
    assert flagged["c3"] is False
    assert flagged["c4"] is False
    assert flagged["c7"] is False


def test_viability_pass_stacks_with_value_band_delta(disable_market_viability_floors):
    # Score-delta refactor: a candidate that is BOTH above_market (value_band)
    # and high-rent + low-pop accumulates both deltas. The viability pass only
    # writes the viability_delta side; the value_band delta is computed by
    # _value_band_score_delta in the run_expansion_search main flow. We assert
    # both signals are individually correct so the caller's sum is correct.
    pops = [5000, 6000, 7000, 8000, 50000, 60000, 70000, 80000]
    cohort = [
        _make_viability_candidate(
            id_=f"bg{i}",
            final_score=80.0 - i,
            rent_pct=0.40,
            pop_reach=p,
        )
        for i, p in enumerate(pops)
    ]
    target = _make_viability_candidate(
        id_="dual",
        final_score=78.0,
        rent_pct=0.90,
        rent_scope="district_band_type",
        pop_reach=4500.0,
        value_band="above_market",
    )
    cohort.insert(2, target)

    # Value-band delta is independent of the viability pass.
    assert _value_band_score_delta(target) == -6.0

    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target_out = next(c for c in out if c["id"] == "dual")
    flag = target_out["score_breakdown_json"]["market_viability_flag"]
    assert flag["demoted"] is True
    # Both pop and rent legs fire on this candidate; -10 each, no swap.
    assert sorted(target_out["viability_legs_fired"]) == [
        "population_below_quartile", "rent_high",
    ]
    assert target_out["viability_delta"] == -20.0


# ---------------------------------------------------------------------------
# CEO directive #1 (3-of-3): Black Marble VNP46A3 third (growth) leg.
# Tests that confident positive YoY radiance growth rescues a candidate that
# would otherwise be flagged 2-of-2 (high rent + low population).
# ---------------------------------------------------------------------------


def _make_viability_candidate_with_radiance(
    *,
    id_: str,
    final_score: float,
    rent_pct: float,
    pop_reach: float,
    radiance_confident: bool,
    radiance_yoy_pct: float | None,
    rent_scope: str = "district_band_type",
) -> dict:
    c = _make_viability_candidate(
        id_=id_,
        final_score=final_score,
        rent_pct=rent_pct,
        rent_scope=rent_scope,
        pop_reach=pop_reach,
    )
    c["feature_snapshot_json"]["radiance_growth"] = {
        "value_yoy_pct": radiance_yoy_pct,
        "source_label": "blackmarble_district_yoy_rolling6",
        "confident": radiance_confident,
        "pixel_count": 132 if radiance_confident else 5,
        "year_month": "2026-03",
    }
    return c


def _viability_cohort_with_radiance_target(
    target_radiance_confident: bool,
    target_radiance_yoy_pct: float | None,
) -> list[dict]:
    """Build a cohort like _viability_cohort but the target carries a radiance signal."""
    pops = [5000, 6000, 7000, 8000, 50000, 60000, 70000, 80000]
    cohort = [
        _make_viability_candidate(
            id_=f"bg{i}",
            final_score=80.0 - i,
            rent_pct=0.40,
            rent_scope="district_band_type",
            pop_reach=p,
        )
        for i, p in enumerate(pops)
    ]
    target = _make_viability_candidate_with_radiance(
        id_="target",
        final_score=78.5,
        rent_pct=0.85,
        pop_reach=4500.0,
        radiance_confident=target_radiance_confident,
        radiance_yoy_pct=target_radiance_yoy_pct,
    )
    cohort.insert(2, target)
    return cohort


def test_market_viability_third_leg_rescue(disable_market_viability_floors):
    # Confident, positive growth >= threshold (default 0.0) → not flagged.
    cohort = _viability_cohort_with_radiance_target(
        target_radiance_confident=True,
        target_radiance_yoy_pct=4.2,
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    assert "market_viability_flag" not in target["score_breakdown_json"]


def test_market_viability_third_leg_no_rescue_when_not_confident(disable_market_viability_floors):
    # Below pixel-count floor → confident=False → leg falls through, still flagged.
    cohort = _viability_cohort_with_radiance_target(
        target_radiance_confident=False,
        target_radiance_yoy_pct=8.0,  # ignored when not confident
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    assert flag["radiance_confident"] is False
    assert flag["radiance_pixel_count"] == 5
    assert flag["radiance_year_month"] == "2026-03"


def test_market_viability_third_leg_no_rescue_when_growth_below_threshold(disable_market_viability_floors):
    # Confident but YoY < threshold → still flagged. Use threshold=5.0 with
    # actual yoy=2.0 to exercise the comparison.
    cohort = _viability_cohort_with_radiance_target(
        target_radiance_confident=True,
        target_radiance_yoy_pct=2.0,
    )
    out = _apply_market_viability_pass(
        list(cohort), search_id="t", radiance_yoy_threshold=5.0
    )
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    assert flag["radiance_confident"] is True
    assert flag["radiance_growth_pct"] == 2.0


# ---------------------------------------------------------------------------
# Decoupled three-leg market-viability pass (Faisal directive). Each leg —
# population (clause 1), rent (clause 2), economics (clause 3) — is an
# independent soft demote. The economics leg has no growth rescue; the pop
# and rent legs do. Reasons concatenate with "_and_" in the order pop, rent,
# economics. See _apply_market_viability_pass docstring.
# ---------------------------------------------------------------------------


def _viability_cohort_with_econ_target(
    *,
    target_economics: float | None,
    target_pop_reach: float = 80000.0,
    target_rent_pct: float = 0.40,
    target_rent_scope: str = "district_band_type",
) -> list[dict]:
    """Like _viability_cohort but lets the target carry an economics_score.

    Background rows leave economics_score unset (econ leg cannot fire on them).
    Background rent is low (0.40) and pop spans both sides of p25.
    """
    pops = [5000, 6000, 7000, 8000, 50000, 60000, 70000, 80000]
    cohort = [
        _make_viability_candidate(
            id_=f"bg{i}",
            final_score=80.0 - i,
            rent_pct=0.40,
            rent_scope="district_band_type",
            pop_reach=p,
        )
        for i, p in enumerate(pops)
    ]
    target = _make_viability_candidate(
        id_="target",
        final_score=78.5,
        rent_pct=target_rent_pct,
        rent_scope=target_rent_scope,
        pop_reach=target_pop_reach,
    )
    if target_economics is not None:
        target["economics_score"] = target_economics
    cohort.insert(2, target)
    return cohort


def test_viability_pop_only_leg_fires(disable_market_viability_floors):
    # Pop leg alone: rent low, pop below p25, no growth, economics healthy.
    # Expected: demote with reason="population_below_quartile".
    cohort = _viability_cohort_with_econ_target(
        target_economics=80.0,
        target_pop_reach=4500.0,
        target_rent_pct=0.40,
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    assert flag["population_demote"] is True
    assert flag["rent_demote"] is False
    assert flag["economics_demote"] is False
    assert flag["reason"] == "population_below_quartile"


def test_viability_rent_only_leg_fires(disable_market_viability_floors):
    # Rent leg alone: high rent on confident scope, pop above p25, no growth,
    # economics healthy. Pre-decouple this row was NOT demoted (3-of-3 needed
    # pop_low). Now rent leg fires alone with reason="rent_high".
    cohort = _viability_cohort_with_econ_target(
        target_economics=80.0,
        target_pop_reach=80000.0,
        target_rent_pct=0.85,
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    assert flag["population_demote"] is False
    assert flag["rent_demote"] is True
    assert flag["economics_demote"] is False
    assert flag["reason"] == "rent_high"


def test_viability_economics_only_leg_fires(disable_market_viability_floors):
    # Economics leg alone: rent low, pop above p25, no growth, but
    # economics_score=60 < 65 default threshold. Pre-decouple this row was
    # not demoted at all. Now econ leg fires with reason="economics_below_threshold".
    cohort = _viability_cohort_with_econ_target(
        target_economics=60.0,
        target_pop_reach=80000.0,
        target_rent_pct=0.40,
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    assert flag["population_demote"] is False
    assert flag["rent_demote"] is False
    assert flag["economics_demote"] is True
    assert flag["economics_score"] == 60.0
    assert flag["reason"] == "economics_below_threshold"


def test_viability_all_three_legs_fire_with_compound_annotation(disable_market_viability_floors):
    # Every leg fires: pop below p25, rent high on confident scope, no growth,
    # economics_score=60 < 65. Reason concatenates in stable order:
    # population, rent, economics.
    cohort = _viability_cohort_with_econ_target(
        target_economics=60.0,
        target_pop_reach=4500.0,
        target_rent_pct=0.85,
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    assert flag["population_demote"] is True
    assert flag["rent_demote"] is True
    assert flag["economics_demote"] is True
    assert flag["reason"] == (
        "population_below_quartile_and_rent_high_and_economics_below_threshold"
    )


def test_viability_growth_rescue_saves_pop_and_rent_legs_not_econ(disable_market_viability_floors):
    # Confident positive radiance growth rescues the pop and rent legs.
    # economics_score=80 keeps econ leg quiet, so the candidate is NOT demoted.
    cohort = _viability_cohort_with_radiance_target(
        target_radiance_confident=True,
        target_radiance_yoy_pct=4.2,
    )
    target_in = next(c for c in cohort if c["id"] == "target")
    target_in["economics_score"] = 80.0
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    assert "market_viability_flag" not in target["score_breakdown_json"]


def test_viability_growth_rescue_does_not_save_economics_leg(disable_market_viability_floors):
    # Growth rescue applies to pop+rent only. With low pop & high rent
    # rescued by growth but economics_score=60, the econ leg STILL fires
    # alone. reason="economics_below_threshold".
    cohort = _viability_cohort_with_radiance_target(
        target_radiance_confident=True,
        target_radiance_yoy_pct=4.2,
    )
    target_in = next(c for c in cohort if c["id"] == "target")
    target_in["economics_score"] = 60.0
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    assert flag["population_demote"] is False, "growth should rescue pop leg"
    assert flag["rent_demote"] is False, "growth should rescue rent leg"
    assert flag["economics_demote"] is True
    assert flag["reason"] == "economics_below_threshold"


# ---------------------------------------------------------------------------
# Realized-demand soft-demote leg (B3, "strong potential for sales"). Mirrors
# the pop/rent/economics leg-isolation pattern. The leg fires on confident
# bottom-quartile realized_demand_30d, gated by a minimum branch count, and
# has no growth_rescue (mirrors the economics leg).
# ---------------------------------------------------------------------------


def _viability_cohort_with_demand_target(
    *,
    target_demand: float | None,
    target_demand_branches: int | None = 5,
    target_pop_reach: float = 80000.0,
    target_rent_pct: float = 0.40,
    target_rent_scope: str = "district_band_type",
    target_economics: float | None = 80.0,
) -> list[dict]:
    """Background carries confident realized_demand_30d well above any p25.

    Background demand spans 600..2200 (8 rows) so p25 lands near 750 with
    ``method="inclusive"``. Background pop_reach also spans both sides of
    its own p25 to keep the pop leg quiet on the target. Background rent
    is low (0.40) so the rent leg stays quiet. Background economics is
    unset so the econ leg cannot fire on background rows.
    """
    pops = [5000, 6000, 7000, 8000, 50000, 60000, 70000, 80000]
    demands = [600.0, 800.0, 1000.0, 1200.0, 1500.0, 1800.0, 2000.0, 2200.0]
    cohort = [
        _make_viability_candidate(
            id_=f"bg{i}",
            final_score=80.0 - i,
            rent_pct=0.40,
            rent_scope="district_band_type",
            pop_reach=p,
            realized_demand_30d=d,
            realized_demand_branches=5,
        )
        for i, (p, d) in enumerate(zip(pops, demands))
    ]
    target = _make_viability_candidate(
        id_="target",
        final_score=78.5,
        rent_pct=target_rent_pct,
        rent_scope=target_rent_scope,
        pop_reach=target_pop_reach,
        realized_demand_30d=target_demand,
        realized_demand_branches=target_demand_branches,
    )
    if target_economics is not None:
        target["economics_score"] = target_economics
    cohort.insert(2, target)
    return cohort


def test_viability_demand_only_leg_fires(disable_market_viability_floors):
    # Demand leg alone: rent low, pop above p25, economics healthy, no growth.
    # target realized_demand_30d=400 sits in the bottom quartile of the
    # cohort (p25 ≈ 750). target carries 5 branches (>= min 3).
    # Expected: demote with reason="demand_low".
    cohort = _viability_cohort_with_demand_target(
        target_demand=400.0,
        target_demand_branches=5,
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    assert flag["population_demote"] is False
    assert flag["rent_demote"] is False
    assert flag["economics_demote"] is False
    assert flag["demand_demote"] is True
    assert flag["realized_demand_30d"] == 400.0
    assert flag["realized_demand_branches"] == 5
    assert flag["realized_demand_threshold"] is not None
    assert flag["reason"] == "demand_low"
    # Score-delta refactor: each leg contributes -10; no positional swap.
    assert target["viability_legs_fired"] == ["demand_low"]
    assert target["viability_delta"] == -10.0


def test_viability_demand_leg_skipped_when_branches_below_min(
    disable_market_viability_floors,
):
    # Bottom-quartile realized_demand_30d but only 2 contributing branches
    # → confidence gate fails → leg does NOT fire on the target. Position
    # is not asserted: the cohort's bg0 / bg1 pop_reach (5k, 6k) sit below
    # the pop-leg p25, so they shift around the target legitimately. The
    # absence of market_viability_flag on the target alone proves the
    # demand leg did not fire here.
    cohort = _viability_cohort_with_demand_target(
        target_demand=400.0,
        target_demand_branches=2,
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is None, "demand leg must not fire when branches < min"


def test_viability_demand_leg_skipped_when_field_absent(
    disable_market_viability_floors,
):
    # Both realized_demand_30d AND realized_demand_branches absent on the
    # target (the flag-OFF / history_unavailable shape from the snapshot
    # writer). Background still carries demand so the cohort cutoff is
    # well-defined. The target's leg must NOT fire.
    cohort = _viability_cohort_with_demand_target(
        target_demand=None,
        target_demand_branches=None,
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is None, "demand leg must not fire when fields absent"


def test_viability_demand_leg_no_growth_rescue(disable_market_viability_floors):
    # Confidently low realized demand AND confidently positive radiance YoY
    # at/above the default threshold. Mirrors
    # test_viability_growth_rescue_does_not_save_economics_leg: the demand
    # leg STILL fires because sales potential is a present-state signal,
    # not a forward-state signal that growth can redeem.
    cohort = _viability_cohort_with_demand_target(
        target_demand=400.0,
        target_demand_branches=5,
    )
    target_in = next(c for c in cohort if c["id"] == "target")
    target_in["feature_snapshot_json"]["radiance_growth"] = {
        "value_yoy_pct": 4.2,
        "source_label": "blackmarble_district_yoy_rolling6",
        "confident": True,
        "pixel_count": 132,
        "year_month": "2026-03",
    }
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    assert flag["demand_demote"] is True
    assert "demand_low" in flag["reason"]
    assert flag["radiance_confident"] is True
    assert flag["radiance_growth_pct"] == 4.2


def test_viability_demand_leg_skipped_when_cohort_too_small(
    disable_market_viability_floors,
):
    # Fewer than 4 candidates have realized_demand_30d → demand_threshold
    # is None and the leg is silent regardless of value. Use a cohort big
    # enough to clear the outer pop-cohort guard (>=4 confident pop_reach
    # values), but where only 2 rows carry realized_demand_30d.
    pops = [5000, 6000, 7000, 8000, 50000, 60000, 70000, 80000]
    cohort = [
        _make_viability_candidate(
            id_=f"bg{i}",
            final_score=80.0 - i,
            rent_pct=0.40,
            rent_scope="district_band_type",
            pop_reach=p,
        )
        for i, p in enumerate(pops)
    ]
    # Only 2 rows have realized_demand_30d; one of them is in the bottom of
    # that 2-element set but the leg should still be silent.
    cohort[0]["feature_snapshot_json"]["realized_demand_30d"] = 100.0
    cohort[0]["feature_snapshot_json"]["realized_demand_branches"] = 5
    cohort[1]["feature_snapshot_json"]["realized_demand_30d"] = 5000.0
    cohort[1]["feature_snapshot_json"]["realized_demand_branches"] = 5
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    for c in out:
        flag = c["score_breakdown_json"].get("market_viability_flag")
        if flag is not None:
            assert flag.get("demand_demote") is False
            assert flag.get("realized_demand_threshold") is None


def test_viability_demand_leg_skipped_when_kill_switch_off(
    disable_market_viability_floors, monkeypatch,
):
    # EXPANSION_VIABILITY_DEMAND_LEG_ENABLED=False → leg silent even with
    # otherwise-firing inputs. Patch every live ``settings`` reference the
    # same way ``disable_market_viability_floors`` does, so this test is
    # robust to test-order dependence on importlib.reload.
    import sys

    import app.core.config as config

    seen_ids: set[int] = set()
    for module in list(sys.modules.values()):
        if module is None:
            continue
        candidate = getattr(module, "settings", None)
        if candidate is None or id(candidate) in seen_ids:
            continue
        if not hasattr(candidate, "EXPANSION_VIABILITY_DEMAND_LEG_ENABLED"):
            continue
        seen_ids.add(id(candidate))
        monkeypatch.setattr(
            candidate, "EXPANSION_VIABILITY_DEMAND_LEG_ENABLED", False
        )
    if id(config.settings) not in seen_ids:
        monkeypatch.setattr(
            config.settings, "EXPANSION_VIABILITY_DEMAND_LEG_ENABLED", False
        )

    cohort = _viability_cohort_with_demand_target(
        target_demand=400.0,
        target_demand_branches=5,
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is None, "kill switch must suppress the demand_demote decision"
    # Snapshot fields remain on feature_snapshot_json — pipeline untouched.
    fs = target["feature_snapshot_json"]
    assert fs.get("realized_demand_30d") == 400.0
    assert fs.get("realized_demand_branches") == 5


def test_viability_all_four_legs_fire_with_compound_annotation(
    disable_market_viability_floors,
):
    # Four-leg variant of test_viability_all_three_legs_fire_with_compound_annotation:
    # pop below p25, rent high on confident scope, economics_score=60 < 65,
    # realized_demand_30d=400 in the bottom quartile with 5 branches, no
    # growth. Reason concatenates in stable order: pop, rent, econ, demand.
    cohort = _viability_cohort_with_demand_target(
        target_demand=400.0,
        target_demand_branches=5,
        target_pop_reach=4500.0,
        target_rent_pct=0.85,
        target_economics=60.0,
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    assert flag["population_demote"] is True
    assert flag["rent_demote"] is True
    assert flag["economics_demote"] is True
    assert flag["demand_demote"] is True
    assert flag["reason"] == (
        "population_below_quartile_and_rent_high_and_economics_below_threshold"
        "_and_demand_low"
    )


# ---------------------------------------------------------------------------
# Radiance-growth soft-demote leg (B1+B2, "strong potential for business
# growth"). Mirrors the demand-leg isolation pattern. The leg fires on
# confident YoY radiance growth strictly below threshold, with NO growth
# rescue (mirrors the economics and demand legs).
# ---------------------------------------------------------------------------


def _viability_cohort_with_radiance_growth_target(
    *,
    target_yoy_pct: float | None,
    target_confident: bool = True,
    target_pop_reach: float = 80000.0,
    target_rent_pct: float = 0.40,
    target_rent_scope: str = "district_band_type",
    target_economics: float | None = 80.0,
    include_radiance_block: bool = True,
) -> list[dict]:
    """Background carries no radiance_growth so the leg cannot fire on bg rows.

    Background pop/rent/demand/economics are set so only the radiance leg
    (or the explicitly-tested leg) can fire on the target.
    """
    pops = [5000, 6000, 7000, 8000, 50000, 60000, 70000, 80000]
    demands = [600.0, 800.0, 1000.0, 1200.0, 1500.0, 1800.0, 2000.0, 2200.0]
    cohort = [
        _make_viability_candidate(
            id_=f"bg{i}",
            final_score=80.0 - i,
            rent_pct=0.40,
            rent_scope="district_band_type",
            pop_reach=p,
            realized_demand_30d=d,
            realized_demand_branches=5,
        )
        for i, (p, d) in enumerate(zip(pops, demands))
    ]
    target = _make_viability_candidate(
        id_="target",
        final_score=78.5,
        rent_pct=target_rent_pct,
        rent_scope=target_rent_scope,
        pop_reach=target_pop_reach,
        realized_demand_30d=1500.0,
        realized_demand_branches=5,
    )
    if target_economics is not None:
        target["economics_score"] = target_economics
    if include_radiance_block:
        target["feature_snapshot_json"]["radiance_growth"] = {
            "value_yoy_pct": target_yoy_pct,
            "source_label": "blackmarble_district_yoy_rolling6",
            "confident": target_confident,
            "pixel_count": 132 if target_confident else 5,
            "year_month": "2026-03",
        }
    cohort.insert(2, target)
    return cohort


def test_viability_radiance_growth_only_leg_fires(disable_market_viability_floors):
    # Radiance leg alone: pop above p25, rent low, economics healthy, demand
    # mid-cohort. Confident negative YoY (-2.0%) below the calibrated 0.0
    # demote threshold ⇒ leg fires alone with reason="radiance_growth_low".
    cohort = _viability_cohort_with_radiance_growth_target(
        target_yoy_pct=-2.0,
        target_confident=True,
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    assert flag["population_demote"] is False
    assert flag["rent_demote"] is False
    assert flag["economics_demote"] is False
    assert flag["demand_demote"] is False
    assert flag["radiance_growth_demote"] is True
    assert flag["radiance_growth_pct"] == -2.0
    assert flag["radiance_confident"] is True
    assert flag["radiance_yoy_demote_threshold"] == 0.0
    assert flag["reason"] == "radiance_growth_low"
    # Score-delta refactor: -10 per fired leg, no swap.
    assert target["viability_legs_fired"] == ["radiance_growth_low"]
    assert target["viability_delta"] == -10.0


def test_viability_radiance_growth_leg_skipped_when_not_confident(
    disable_market_viability_floors,
):
    # Confident=False → confidence gate fails → leg silent regardless of
    # value. Mirrors the demand-leg branches-below-min precedent: low-
    # confidence signals are not allowed to drive demote decisions.
    cohort = _viability_cohort_with_radiance_growth_target(
        target_yoy_pct=-5.0,
        target_confident=False,
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is None, "radiance leg must not fire when not confident"


def test_viability_radiance_growth_leg_skipped_when_field_absent(
    disable_market_viability_floors,
):
    # No radiance_growth key in feature_snapshot_json (history_unavailable
    # shape from the snapshot writer). The target's leg must NOT fire.
    cohort = _viability_cohort_with_radiance_growth_target(
        target_yoy_pct=None,
        include_radiance_block=False,
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is None, "radiance leg must not fire when field absent"


def test_viability_radiance_growth_leg_no_growth_rescue(
    disable_market_viability_floors,
):
    # Confident negative YoY drives the radiance leg, AND the same row
    # carries the same negative YoY (which can't self-rescue anyway —
    # rescue requires yoy >= radiance_yoy_threshold, default 2.0). Mirrors
    # the economics/demand precedent: the leg whose own forward-looking
    # signal triggered the demote cannot self-rescue.
    cohort = _viability_cohort_with_radiance_growth_target(
        target_yoy_pct=-3.0,
        target_confident=True,
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    assert flag["radiance_growth_demote"] is True
    assert "radiance_growth_low" in flag["reason"]
    assert flag["radiance_confident"] is True


def test_viability_radiance_growth_leg_skipped_when_kill_switch_off(
    disable_market_viability_floors, monkeypatch,
):
    # EXPANSION_VIABILITY_RADIANCE_GROWTH_LEG_ENABLED=False → leg silent
    # even when conditions otherwise met. Patch every live ``settings``
    # reference (mirrors the demand-leg kill-switch test).
    import sys

    import app.core.config as config

    seen_ids: set[int] = set()
    for module in list(sys.modules.values()):
        if module is None:
            continue
        candidate = getattr(module, "settings", None)
        if candidate is None or id(candidate) in seen_ids:
            continue
        if not hasattr(
            candidate, "EXPANSION_VIABILITY_RADIANCE_GROWTH_LEG_ENABLED"
        ):
            continue
        seen_ids.add(id(candidate))
        monkeypatch.setattr(
            candidate, "EXPANSION_VIABILITY_RADIANCE_GROWTH_LEG_ENABLED", False
        )
    if id(config.settings) not in seen_ids:
        monkeypatch.setattr(
            config.settings,
            "EXPANSION_VIABILITY_RADIANCE_GROWTH_LEG_ENABLED",
            False,
        )

    cohort = _viability_cohort_with_radiance_growth_target(
        target_yoy_pct=-2.0,
        target_confident=True,
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is None, "kill switch must suppress the radiance_growth_demote decision"
    # Snapshot field remains on feature_snapshot_json — pipeline untouched.
    fs = target["feature_snapshot_json"]
    assert fs.get("radiance_growth", {}).get("value_yoy_pct") == -2.0
    assert fs.get("radiance_growth", {}).get("confident") is True


def test_viability_radiance_growth_leg_threshold_boundary(
    disable_market_viability_floors,
):
    # Operator is strict ``<``: at value_yoy_pct == threshold exactly, the
    # leg must NOT fire. Tests against whatever the env default is so
    # threshold recalibrations don't break this assertion.
    from app.core.config import settings
    threshold = float(settings.EXPANSION_VIABILITY_RADIANCE_YOY_DEMOTE_THRESHOLD)
    cohort = _viability_cohort_with_radiance_growth_target(
        target_yoy_pct=threshold,
        target_confident=True,
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is None, (
        "radiance leg must not fire when yoy == threshold (operator is strict <)"
    )


def test_viability_radiance_growth_neutral_zone_neither_rescue_nor_demote(
    disable_market_viability_floors,
):
    # Post-calibration (2026-05-10) the rescue threshold is 2.0 and the
    # demote threshold is 0.0, opening a neutral zone in 0..2% YoY where
    # the radiance signal is confident but neither strong enough to
    # rescue the pop/rent legs nor weak enough to fire the radiance
    # demote leg. Per §7a of radiance_yoy_distribution.sql, ~42% of
    # confident candidates sit in this band — they should be evaluated
    # on the other legs as-is, with no growth-side intervention.
    #
    # Setup: target has low pop + high rent (which would normally fire
    # pop_demote and rent_demote), confident YoY=1.0 in the neutral
    # zone. Assertions: (a) radiance_growth_demote is False, and (b)
    # pop_demote / rent_demote fire — proving the rescue branch did NOT
    # mask them for this leg.
    cohort = _viability_cohort_with_radiance_target(
        target_radiance_confident=True,
        target_radiance_yoy_pct=1.0,
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    assert flag["radiance_growth_demote"] is False, (
        "YoY 1.0 is above the 0.0 demote threshold; leg must not fire"
    )
    assert flag["radiance_confident"] is True
    assert flag["radiance_growth_pct"] == 1.0
    # The rescue branch (operator >=, threshold 2.0) must NOT mask pop/rent
    # when YoY sits in the neutral zone. Both legs fire as if no radiance
    # signal had been considered.
    assert flag["population_demote"] is True, (
        "growth rescue must not apply in the neutral zone (1.0 < 2.0)"
    )
    assert flag["rent_demote"] is True, (
        "growth rescue must not apply in the neutral zone (1.0 < 2.0)"
    )


def test_viability_radiance_growth_negative_fires_demote_under_new_defaults(
    disable_market_viability_floors,
):
    # Post-calibration the demote threshold is 0.0 (operator strict <).
    # Confident YoY=-1.0 sits in the "confidently shrinking" tier (§7b,
    # ~7.1% of confident candidates) and must fire the radiance demote
    # leg alone — pop/rent/econ/demand are neutral in this cohort, so
    # the only firing leg is radiance_growth_low.
    cohort = _viability_cohort_with_radiance_growth_target(
        target_yoy_pct=-1.0,
        target_confident=True,
    )
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    assert flag["radiance_growth_demote"] is True
    assert flag["radiance_growth_pct"] == -1.0
    assert flag["radiance_confident"] is True
    assert flag["radiance_yoy_demote_threshold"] == 0.0
    assert flag["population_demote"] is False
    assert flag["rent_demote"] is False
    assert flag["economics_demote"] is False
    assert flag["demand_demote"] is False
    assert flag["reason"] == "radiance_growth_low"


def test_viability_all_five_legs_fire_with_compound_annotation(
    disable_market_viability_floors,
):
    # Five-leg variant of test_viability_all_four_legs_fire_with_compound_annotation:
    # pop below p25, rent high on confident scope, economics_score=60 < 65,
    # realized_demand_30d=400 in the bottom quartile with 5 branches,
    # confident negative radiance YoY (-1.5%) below the 0.0 demote threshold.
    # Reason concatenates in stable order: pop, rent, econ, demand, radiance.
    cohort = _viability_cohort_with_demand_target(
        target_demand=400.0,
        target_demand_branches=5,
        target_pop_reach=4500.0,
        target_rent_pct=0.85,
        target_economics=60.0,
    )
    target_in = next(c for c in cohort if c["id"] == "target")
    target_in["feature_snapshot_json"]["radiance_growth"] = {
        "value_yoy_pct": -1.5,
        "source_label": "blackmarble_district_yoy_rolling6",
        "confident": True,
        "pixel_count": 132,
        "year_month": "2026-03",
    }
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target = next(c for c in out if c["id"] == "target")
    flag = target["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None and flag["demoted"] is True
    assert flag["population_demote"] is True
    assert flag["rent_demote"] is True
    assert flag["economics_demote"] is True
    assert flag["demand_demote"] is True
    assert flag["radiance_growth_demote"] is True
    assert flag["reason"] == (
        "population_below_quartile_and_rent_high_and_economics_below_threshold"
        "_and_demand_low_and_radiance_growth_low"
    )


def test_viability_diagnostics_demote_legs_block_written(
    disable_market_viability_floors,
):
    # When ``diagnostics`` is passed and at least one leg fires, the
    # ``demote_legs`` block is populated with all five drop counters and
    # all expected threshold keys. Mirrors the existing
    # test_viability_pass_diagnostics_records_hard_floor_drops_per_leg
    # but for the soft-demote pass.
    cohort = _viability_cohort_with_radiance_growth_target(
        target_yoy_pct=-2.0,
        target_confident=True,
    )
    diagnostics: dict = {}
    _apply_market_viability_pass(
        list(cohort), search_id="t", diagnostics=diagnostics
    )
    assert "demote_legs" in diagnostics
    drops = diagnostics["demote_legs"]["drops"]
    expected_drop_keys = {
        "dropped_population",
        "dropped_rent",
        "dropped_economics",
        "dropped_demand",
        "dropped_radiance_growth",
        "dropped_rent_per_capita",
    }
    assert set(drops.keys()) == expected_drop_keys
    for key in expected_drop_keys:
        assert isinstance(drops[key], int)
        assert drops[key] >= 0
    # The radiance leg must have caught at least the target.
    assert drops["dropped_radiance_growth"] >= 1

    thresholds = diagnostics["demote_legs"]["thresholds"]
    expected_threshold_keys = {
        "rent_pct_threshold",
        "pop_percentile",
        "pop_threshold",
        "economics_min",
        "demand_percentile",
        "demand_threshold",
        "demand_min_branches",
        "radiance_yoy_demote_threshold",
        "rpc_percentile",
        "rpc_threshold",
        "rpc_min_cohort",
        "rpc_cohort_n",
    }
    assert set(thresholds.keys()) == expected_threshold_keys
    assert thresholds["radiance_yoy_demote_threshold"] == 0.0

    leg_enabled = diagnostics["demote_legs"]["leg_enabled"]
    assert leg_enabled["demand"] is True
    assert leg_enabled["radiance_growth"] is True
    # rpc leg is inactive in this cohort (no estimated_annual_rent_sar
    # supplied by the helper) so leg_enabled["rent_per_capita"] is False.
    assert leg_enabled["rent_per_capita"] is False


# ---------------------------------------------------------------------------
# rent_per_capita demote leg — catches the CEO "low-pop + high-rent"
# anti-pattern via cohort percentile on rent / population_reach.
# ---------------------------------------------------------------------------


def _rpc_candidate(
    *, id_: str, final_score: float, rent_sar: float | None,
    pop_reach: float | None,
) -> dict:
    fs: dict = {}
    if rent_sar is not None:
        fs["estimated_annual_rent_sar"] = rent_sar
    if pop_reach is not None:
        fs["population_reach"] = pop_reach
    return {
        "id": id_,
        "parcel_id": id_,
        "final_score": final_score,
        "score_breakdown_json": {},
        "feature_snapshot_json": fs,
    }


def test_viability_rpc_leg_demotes_top_quartile(disable_market_viability_floors):
    # Cohort of 12 candidates: rpc values 1, 2, ..., 11 SAR/person plus an
    # outlier "target" with rpc = 1000 (200K rent / 200 pop). Default
    # percentile is 0.75; the target sits at the top of the cohort and
    # must be demoted by the rpc leg, with telemetry written.
    cohort = [
        _rpc_candidate(
            id_=f"c{i}", final_score=80.0 - i,
            rent_sar=float(i * 1000), pop_reach=1000.0,
        )
        for i in range(1, 12)
    ]
    target = _rpc_candidate(
        id_="target", final_score=78.5,
        rent_sar=200_000.0, pop_reach=200.0,  # rpc = 1000
    )
    cohort.insert(2, target)
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    target_out = next(c for c in out if c["id"] == "target")
    flag = target_out["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None
    assert flag["rent_per_capita_demote"] is True
    assert flag["rent_per_capita_sar"] == 1000.0
    assert flag["rent_per_capita_pct"] == 1.0
    assert flag["demoted"] is True
    assert "rent_per_capita_high" in flag["reason"].split("_and_")
    # Score-delta refactor: viability_delta carries -10 per fired leg; the
    # function no longer reorders the candidate list.
    assert "rent_per_capita_high" in target_out["viability_legs_fired"]
    assert target_out["viability_delta"] <= -10.0


def test_viability_rpc_leg_skipped_below_min_cohort(
    disable_market_viability_floors,
):
    # Only 6 candidates have valid rpc inputs (< default min cohort 10);
    # rpc leg is silent, no flag writes, no demotions.
    cohort = [
        _rpc_candidate(
            id_=f"c{i}", final_score=80.0 - i,
            rent_sar=float(i * 1000), pop_reach=1000.0,
        )
        for i in range(1, 7)
    ]
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    for c in out:
        assert "market_viability_flag" not in c["score_breakdown_json"]


def test_viability_rpc_leg_writes_null_for_missing_inputs(
    disable_market_viability_floors,
):
    # Cohort exceeds min_cohort with valid rpc, but one candidate is
    # missing population_reach. That candidate gets null telemetry; the
    # leg is otherwise active.
    cohort = [
        _rpc_candidate(
            id_=f"c{i}", final_score=80.0 - i,
            rent_sar=float(i * 1000), pop_reach=1000.0,
        )
        for i in range(1, 12)
    ]
    missing = _rpc_candidate(
        id_="missing", final_score=78.5,
        rent_sar=144_000.0, pop_reach=None,
    )
    cohort.insert(2, missing)
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    miss_out = next(c for c in out if c["id"] == "missing")
    flag = miss_out["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None
    assert flag["rent_per_capita_sar"] is None
    assert flag["rent_per_capita_pct"] is None
    assert flag["rent_per_capita_demote"] is None


def test_viability_rpc_leg_handles_zero_population_safely(
    disable_market_viability_floors,
):
    # population_reach == 0 must not raise; treated as missing (null
    # telemetry) and no demotion.
    cohort = [
        _rpc_candidate(
            id_=f"c{i}", final_score=80.0 - i,
            rent_sar=float(i * 1000), pop_reach=1000.0,
        )
        for i in range(1, 12)
    ]
    zero_pop = _rpc_candidate(
        id_="zero", final_score=78.5,
        rent_sar=200_000.0, pop_reach=0.0,
    )
    cohort.insert(2, zero_pop)
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    zero_out = next(c for c in out if c["id"] == "zero")
    flag = zero_out["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None
    assert flag["rent_per_capita_sar"] is None
    assert flag["rent_per_capita_demote"] is None


def test_viability_rpc_leg_writes_telemetry_on_non_demoted(
    disable_market_viability_floors,
):
    # Below-threshold candidates still get the three rpc keys written
    # (telemetry, not just demote events).
    cohort = [
        _rpc_candidate(
            id_=f"c{i}", final_score=80.0 - i,
            rent_sar=float(i * 1000), pop_reach=1000.0,
        )
        for i in range(1, 12)
    ]
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    low = next(c for c in out if c["id"] == "c1")  # rpc = 1.0, lowest
    flag = low["score_breakdown_json"].get("market_viability_flag")
    assert flag is not None
    assert flag["rent_per_capita_sar"] == 1.0
    assert flag["rent_per_capita_demote"] is False
    assert 0.0 < flag["rent_per_capita_pct"] <= 1.0


# ---------------------------------------------------------------------------
# Hard-floor diagnostics: surface per-leg drop counts to the caller so the
# API meta can explain unsaturated-limit responses.
# ---------------------------------------------------------------------------


def test_viability_pass_diagnostics_records_hard_floor_drops_per_leg(monkeypatch):
    """Regression: when the directive's hard-floor pre-pass drops candidates,
    the optional ``diagnostics`` dict captures per-leg drop counts and the
    thresholds in effect. Without this, operators receive
    ``pool_size: N, rows_returned: M`` with no signal as to which gate
    filtered which candidates — they would have to consult kubectl logs to
    interpret the gap.
    """
    # Production thresholds, set explicitly so the test is independent of
    # whatever defaults are configured at the moment.
    import app.services.expansion_advisor as svc

    monkeypatch.setattr(svc.settings, "EXPANSION_VIABILITY_POPULATION_HARD_FLOOR", 20000)
    monkeypatch.setattr(svc.settings, "EXPANSION_VIABILITY_BRAND_PRESENCE_HARD_FLOOR", 1)
    monkeypatch.setattr(svc.settings, "EXPANSION_VIABILITY_CONSTRUCTION_BUFFER_M", 75.0)

    # Synthetic cohort spanning all four hard-floor outcomes:
    #   * one survivor that clears every floor
    #   * one drop on population (below 20k)
    #   * one drop on commercial floor (zero unique brands)
    #   * one drop on construction proximity (polygon within buffer)
    cohort = [
        {
            "id": "ok",
            "parcel_id": "ok",
            "final_score": 80.0,
            "score_breakdown_json": {},
            "feature_snapshot_json": {
                "population_reach": 50000.0,
                "brand_presence": {"unique_brands": 5},
                "construction_proximity": {"polygon_count": 0},
            },
        },
        {
            "id": "drop_pop",
            "parcel_id": "drop_pop",
            "final_score": 79.0,
            "score_breakdown_json": {},
            "feature_snapshot_json": {
                "population_reach": 5000.0,
                "brand_presence": {"unique_brands": 5},
                "construction_proximity": {"polygon_count": 0},
            },
        },
        {
            "id": "drop_brand",
            "parcel_id": "drop_brand",
            "final_score": 78.0,
            "score_breakdown_json": {},
            "feature_snapshot_json": {
                "population_reach": 50000.0,
                "brand_presence": {"unique_brands": 0},
                "construction_proximity": {"polygon_count": 0},
            },
        },
        {
            "id": "drop_constr",
            "parcel_id": "drop_constr",
            "final_score": 77.0,
            "score_breakdown_json": {},
            "feature_snapshot_json": {
                "population_reach": 50000.0,
                "brand_presence": {"unique_brands": 5},
                "construction_proximity": {"polygon_count": 3},
            },
        },
    ]

    diagnostics: dict = {}
    out = _apply_market_viability_pass(
        list(cohort), search_id="t", diagnostics=diagnostics
    )

    survivor_ids = {c["id"] for c in out}
    assert survivor_ids == {"ok"}

    assert "hard_floors" in diagnostics
    drops = diagnostics["hard_floors"]["drops"]
    assert drops == {
        "dropped_population": 1,
        "dropped_commercial": 1,
        "dropped_construction": 1,
        "remaining": 1,
    }

    thresholds = diagnostics["hard_floors"]["thresholds"]
    assert thresholds == {
        "hard_floor_pop_threshold": 20000,
        "hard_floor_brand_threshold": 1,
        "hard_floor_construction_buffer_m": 75.0,
    }


def test_viability_pass_diagnostics_unchanged_when_caller_omits_kwarg(
    disable_market_viability_floors,
):
    """Backwards-compat: existing callers that don't pass ``diagnostics`` must
    see no behavior change. Return type stays a list of candidates only."""
    cohort = _viability_cohort(target_pop_reach=4500.0, target_rent_pct=0.85)
    out = _apply_market_viability_pass(list(cohort), search_id="t")
    assert isinstance(out, list)
    assert all(isinstance(c, dict) for c in out)


# ---------------------------------------------------------------------------
# Bug A & Bug B regression coverage.
# ---------------------------------------------------------------------------

def test_compare_candidates_lowest_rent_burden_remains_smallest_absolute_rent():
    """Path 3: lowest_rent_burden_candidate_id keeps its existing semantics
    (smallest absolute annual rent across the compared set) and is
    INDEPENDENT of best_value_candidate_id. Both fields are populated."""
    rows = [
        {
            "id": "small-cheap-weak",
            "parcel_id": "p1",
            "district": "Olaya",
            "area_m2": 80.0,
            "final_score": 50.0,
            "demand_score": 40.0,
            "whitespace_score": 40.0,
            "fit_score": 40.0,
            "estimated_annual_rent_sar": 60_000.0,
            "estimated_revenue_index": 30.0,
            "economics_score": 50.0,
            "brand_fit_score": 50.0,
            "score_breakdown_json": {
                "economics_detail": {
                    "value_score": 35.0,
                    "value_band": "neutral",
                    "value_band_low_confidence": False,
                },
            },
            "gate_status_json": {"overall_pass": True},
            "confidence_grade": "B",
            "confidence_score": 60.0,
        },
        {
            "id": "large-fair-strong",
            "parcel_id": "p2",
            "district": "Olaya",
            "area_m2": 240.0,
            "final_score": 80.0,
            "demand_score": 80.0,
            "whitespace_score": 70.0,
            "fit_score": 80.0,
            "estimated_annual_rent_sar": 480_000.0,
            "estimated_revenue_index": 85.0,
            "economics_score": 75.0,
            "brand_fit_score": 80.0,
            "score_breakdown_json": {
                "economics_detail": {
                    "value_score": 82.0,
                    "value_band": "best_value",
                    "value_band_low_confidence": False,
                },
            },
            "gate_status_json": {"overall_pass": True},
            "confidence_grade": "A",
            "confidence_score": 85.0,
        },
    ]
    db = FakeDB(compare_rows=rows)
    result = compare_candidates(db, "search-1", ["small-cheap-weak", "large-fair-strong"])
    summary = result["summary"]
    # Lowest rent burden = literally smallest absolute rent. No change to
    # this field's meaning.
    assert summary["lowest_rent_burden_candidate_id"] == "small-cheap-weak"
    # Best value = highest published value_score.
    assert summary["best_value_candidate_id"] == "large-fair-strong"


def test_compare_candidates_summary_contract_includes_best_value():
    """Empty-list path must surface every key in _COMPARE_SUMMARY_KEYS,
    including the new best_value_candidate_id."""
    db = FakeDB(compare_rows=[])
    result = compare_candidates(db, "search-1", [])
    summary = result["summary"]
    assert "best_value_candidate_id" in summary
    assert summary["best_value_candidate_id"] is None
    # Legacy field still present and unchanged.
    assert "lowest_rent_burden_candidate_id" in summary
    assert summary["lowest_rent_burden_candidate_id"] is None


def test_get_recommendation_report_populates_dimension_winner_ids(monkeypatch):
    """Bug B: the frontend ExpansionReportPanel.tsx reads five
    *_candidate_id keys off rec.* that the backend never populated. After
    this PR they are populated, plus a new best_value_candidate_id."""
    candidates = [
        {
            "id": "cand-demand",
            "parcel_id": "p1",
            "rank_position": 2,
            "final_score": 70.0,
            "demand_score": 95.0,    # winner here
            "economics_score": 60.0,
            "brand_fit_score": 60.0,
            "provider_whitespace_score": 50.0,
            "confidence_grade": "B",
            "confidence_score": 60.0,
            "value_score": 55.0,
            "value_band": "neutral",
            "gate_status_json": {"overall_pass": False},
            "gate_reasons_json": {"blocking_failures": [{"k": "v"}]},
            "feature_snapshot_json": {},
            "score_breakdown_json": {"economics_detail": {"value_score": 55.0, "value_band": "neutral"}},
        },
        {
            "id": "cand-value",
            "parcel_id": "p2",
            "rank_position": 1,
            "final_score": 80.0,
            "demand_score": 60.0,
            "economics_score": 75.0,  # winner here
            "brand_fit_score": 60.0,
            "provider_whitespace_score": 50.0,
            "confidence_grade": "A",   # winner here
            "confidence_score": 90.0,
            "value_score": 88.0,       # winner here
            "value_band": "best_value",
            "gate_status_json": {"overall_pass": True},
            "gate_reasons_json": {},
            "feature_snapshot_json": {},
            "score_breakdown_json": {"economics_detail": {"value_score": 88.0, "value_band": "best_value"}},
        },
        {
            "id": "cand-brand",
            "parcel_id": "p3",
            "rank_position": 3,
            "final_score": 65.0,
            "demand_score": 50.0,
            "economics_score": 55.0,
            "brand_fit_score": 92.0,  # winner here
            "provider_whitespace_score": 88.0,  # winner here
            "confidence_grade": "C",
            "confidence_score": 50.0,
            "value_score": 40.0,
            "value_band": "neutral",
            "gate_status_json": {"overall_pass": True},
            "gate_reasons_json": {},
            "feature_snapshot_json": {},
            "score_breakdown_json": {"economics_detail": {"value_score": 40.0, "value_band": "neutral"}},
        },
    ]
    monkeypatch.setattr(expansion_service, "get_search", lambda *_a, **_kw: {"id": "search-1", "brand_profile": {}})
    monkeypatch.setattr(expansion_service, "get_candidates", lambda *_a, **_kw: candidates)

    report = get_recommendation_report(FakeDB(), "search-1")
    assert report is not None
    rec = report["recommendation"]
    assert rec["highest_demand_candidate_id"] == "cand-demand"
    assert rec["best_economics_candidate_id"] == "cand-value"
    assert rec["best_brand_fit_candidate_id"] == "cand-brand"
    assert rec["strongest_whitespace_candidate_id"] == "cand-brand"
    assert rec["most_confident_candidate_id"] == "cand-value"
    assert rec["best_value_candidate_id"] == "cand-value"


def test_get_recommendation_report_best_value_none_when_no_value_score(monkeypatch):
    candidates = [
        {
            "id": "cand-1",
            "parcel_id": "p1",
            "rank_position": 1,
            "final_score": 60.0,
            "demand_score": 50.0,
            "economics_score": 50.0,
            "brand_fit_score": 50.0,
            "provider_whitespace_score": 50.0,
            "confidence_grade": "C",
            "confidence_score": 50.0,
            "value_score": None,   # absolute_legacy / fallback row
            "value_band": None,
            "gate_status_json": {"overall_pass": True},
            "gate_reasons_json": {},
            "feature_snapshot_json": {},
            "score_breakdown_json": {},
        },
    ]
    monkeypatch.setattr(expansion_service, "get_search", lambda *_a, **_kw: {"id": "search-1", "brand_profile": {}})
    monkeypatch.setattr(expansion_service, "get_candidates", lambda *_a, **_kw: candidates)

    report = get_recommendation_report(FakeDB(), "search-1")
    assert report is not None
    assert report["recommendation"]["best_value_candidate_id"] is None


def test_value_band_score_delta_reads_band_from_score_breakdown_json():
    """Regression for the production bug where value_uprank_applied was always
    False because _apply_value_band_pass read value_band only from the top
    level. After the score-delta refactor, _value_band_score_delta consults
    the nested ``score_breakdown_json["economics_detail"]`` location first,
    so high-confidence best_value bands earn the +4 delta."""
    candidate = {
        "id": "c5-best-value",
        "parcel_id": "c5-best-value",
        "final_score": 75.98,
        "score_breakdown_json": {
            "economics_detail": {
                "value_score": 80.0,
                "value_band": "best_value",
                "value_band_low_confidence": False,
            },
        },
    }
    assert _value_band_score_delta(candidate) == 4.0


def test_recommendation_report_top_payload_preserves_economics_detail(monkeypatch):
    """Regression for Bug 2: top_candidates[0].score_breakdown_json
    .economics_detail was empty because get_recommendation_report's
    top_payload projection forgot to copy economics_detail from the source
    candidate's score_breakdown_json."""
    economics_detail = {
        "rent_burden": {
            "mode": "percentile",
            "percentile_rank": 35.0,
            "n_comparable": 24,
            "source_label": "district_band_type",
        },
        "value_score": 82.5,
        "value_band": "best_value",
        "value_band_low_confidence": False,
    }
    candidates = [
        {
            "id": "cand-1",
            "parcel_id": "p1",
            "rank_position": 1,
            "final_score": 80.0,
            "demand_score": 70.0,
            "economics_score": 75.0,
            "brand_fit_score": 70.0,
            "provider_whitespace_score": 60.0,
            "confidence_grade": "A",
            "confidence_score": 90.0,
            "value_score": 82.5,
            "value_band": "best_value",
            "gate_status_json": {"overall_pass": True},
            "gate_reasons_json": {},
            "feature_snapshot_json": {},
            "score_breakdown_json": {
                "weights": {"demand": 0.3},
                "inputs": {},
                "weighted_components": {},
                "display": {},
                "final_score": 80.0,
                "economics_detail": economics_detail,
            },
        },
    ]
    monkeypatch.setattr(expansion_service, "get_search", lambda *_a, **_kw: {"id": "search-1", "brand_profile": {}})
    monkeypatch.setattr(expansion_service, "get_candidates", lambda *_a, **_kw: candidates)

    report = get_recommendation_report(FakeDB(), "search-1")
    assert report is not None
    top = report["top_candidates"]
    assert len(top) == 1
    sb = top[0]["score_breakdown_json"]
    assert sb.get("economics_detail") == economics_detail
    # Sanity: rent_burden / value_score / value_band specifically must round-trip.
    assert sb["economics_detail"]["rent_burden"]["mode"] == "percentile"
    assert sb["economics_detail"]["value_score"] == 82.5
    assert sb["economics_detail"]["value_band"] == "best_value"


# ---------------------------------------------------------------------------
# _district_momentum_score: now joins external_feature_polygons_mat (PR fix).
# These tests mock the DB at the SQL boundary so we exercise the Python-side
# normalization (percentile composite, normalize_district_key, return shape)
# without needing a live PostGIS instance.
# ---------------------------------------------------------------------------


def test_district_momentum_score_normalizes_polygon_join_results():
    """After the matview rewrite, the function joins
    external_feature_polygons_mat instead of parsing JSONB on every call.
    Verify the Python-side aggregation works against rows shaped like the
    new SQL output (district_label + activity_30d + active_in_district +
    percentile_raw + percentile_absolute, mapped per row).
    """
    from unittest.mock import MagicMock

    from app.services.expansion_advisor import _district_momentum_score
    from app.services.expansion_advisor import normalize_district_key

    fake_rows = [
        {
            "district_label": "حي العليا",
            "activity_30d": 80,
            "active_in_district": 100,
            "percentile_raw": 1.0,
            "percentile_absolute": 1.0,
        },
        {
            "district_label": "الملقا",
            "activity_30d": 20,
            "active_in_district": 100,
            "percentile_raw": 0.5,
            "percentile_absolute": 0.5,
        },
        {
            "district_label": "السليمانية",
            "activity_30d": 5,
            "active_in_district": 100,
            "percentile_raw": 0.0,
            "percentile_absolute": 0.0,
        },
    ]

    mappings_proxy = MagicMock()
    mappings_proxy.all.return_value = fake_rows
    exec_proxy = MagicMock()
    exec_proxy.mappings.return_value = mappings_proxy
    db = MagicMock()
    db.execute.return_value = exec_proxy

    out = _district_momentum_score(db)

    assert isinstance(out, dict)
    assert len(out) == 3

    olaya_key = normalize_district_key("حي العليا")
    assert olaya_key in out
    olaya = out[olaya_key]
    # composite = 0.5*1.0 + 0.5*1.0 = 1.0 → 100.0
    assert olaya["momentum_score"] == 100.0
    assert olaya["activity_30d"] == 80
    assert olaya["active_in_district"] == 100
    assert olaya["percentile_raw"] == 1.0
    assert olaya["percentile_absolute"] == 1.0
    assert olaya["percentile_composite"] == 1.0
    assert olaya["district_label"] == "حي العليا"
    assert olaya["sample_floor_applied"] is False

    bottom_key = normalize_district_key("السليمانية")
    assert out[bottom_key]["momentum_score"] == 0.0


def test_district_momentum_score_returns_empty_on_db_error():
    """The try/except envelope must keep returning {} on failure so callers
    can apply the neutral 50.0 fallback without the request blowing up."""
    from unittest.mock import MagicMock

    from app.services.expansion_advisor import _district_momentum_score

    db = MagicMock()
    db.execute.side_effect = RuntimeError("simulated postgres error")

    out = _district_momentum_score(db)
    assert out == {}


# ===========================================================================
# Score-delta refactor: integrated final_score arithmetic + bonus_detail
# persistence + deterministic sort. The 12 tests below exercise the
# _apply_score_deltas_and_sort helper that the run_expansion_search main
# flow uses to fold value_band, viability, freshness, and momentum signals
# into a single final_score, the result of which drives ORDER BY.
# ===========================================================================

from app.services.expansion_advisor import (
    _apply_score_deltas_and_sort,
    _apply_market_viability_pass as _mv_pass,
    _LISTING_FRESHNESS_DAYS,
    _MOMENTUM_DISPLAY_THRESHOLD,
)


def _sd_candidate(
    *,
    parcel_id: str,
    base_final_score: float,
    value_band: str | None = None,
    value_band_low_conf: bool = False,
    viability_legs: list[str] | None = None,
    created_days: int | None = None,
    updated_days: int | None = None,
    momentum_score: float | None = None,
    sample_floor_applied: bool = False,
    cannibalization_score: float | None = None,
) -> dict:
    """Build a minimal candidate carrying just enough state to drive the
    score-delta pipeline. ``viability_legs`` is the list the viability pass
    would have attached as ``viability_legs_fired`` (each leg contributes
    -10 to ``viability_delta``); pass [] for "no leg fired"."""
    sb: dict = {}
    if value_band is not None:
        sb["economics_detail"] = {
            "value_band": value_band,
            "value_band_low_confidence": value_band_low_conf,
        }
    fs: dict = {}
    if created_days is not None or updated_days is not None:
        fs["listing_age"] = {
            "created_days": created_days,
            "updated_days": updated_days,
        }
    if momentum_score is not None:
        fs["district_momentum"] = {
            "momentum_score": momentum_score,
            "sample_floor_applied": sample_floor_applied,
        }
    cand: dict = {
        "id": parcel_id,
        "parcel_id": parcel_id,
        "final_score": base_final_score,
        "score_breakdown_json": sb,
        "feature_snapshot_json": fs,
    }
    if viability_legs is not None:
        cand["viability_legs_fired"] = list(viability_legs)
        cand["viability_delta"] = -10.0 * len(viability_legs)
    if cannibalization_score is not None:
        cand["cannibalization_score"] = cannibalization_score
    return cand


def test_score_delta_empty_branches():
    # Brief with no existing branches, candidate with viable economics:
    # base_deterministic stays as-is when no signals fire, and the bonus_detail
    # block is still written with zeroed-out fields so the persisted shape is
    # stable for the saved-study UI.
    c = _sd_candidate(parcel_id="p1", base_final_score=72.5, viability_legs=[])
    out = _apply_score_deltas_and_sort([c])
    bd = out[0]["score_breakdown_json"]["bonus_detail"]
    assert bd["base_deterministic"] == 72.5
    assert bd["value_band_delta"] == 0.0
    assert bd["viability_delta"] == 0.0
    assert bd["viability_legs_fired"] == []
    assert bd["freshness_bonus"] == 0.0
    assert bd["freshness_label"] is None
    assert bd["momentum_bonus"] == 0.0
    assert bd["total_delta"] == 0.0
    assert bd["final_score_clamped"] is False
    assert out[0]["final_score"] == 72.5
    # rank ordering matches final_score sort (single-row trivial case).
    assert [c["parcel_id"] for c in out] == ["p1"]


def test_score_delta_best_value_high_conf_uprank():
    c = _sd_candidate(
        parcel_id="p1",
        base_final_score=70.0,
        value_band="best_value",
        value_band_low_conf=False,
        viability_legs=[],
    )
    out = _apply_score_deltas_and_sort([c])
    bd = out[0]["score_breakdown_json"]["bonus_detail"]
    assert bd["value_band_delta"] == 4.0
    assert out[0]["final_score"] == 74.0
    # Legacy back-compat keys must still write on uprank.
    assert out[0]["value_uprank_applied"] is True
    assert out[0]["value_uprank_delta"] == 4


def test_score_delta_above_market_low_conf_no_penalty():
    c = _sd_candidate(
        parcel_id="p1",
        base_final_score=70.0,
        value_band="above_market",
        value_band_low_conf=True,  # citywide pool — skip
        viability_legs=[],
    )
    out = _apply_score_deltas_and_sort([c])
    bd = out[0]["score_breakdown_json"]["bonus_detail"]
    assert bd["value_band_delta"] == 0.0
    assert out[0]["final_score"] == 70.0
    # Low-confidence skip means no legacy downrank marker.
    assert out[0].get("value_downrank_applied") is not True


def test_score_delta_viability_stacks():
    # Three legs fire on a single candidate — each contributes -10, summed.
    c = _sd_candidate(
        parcel_id="p1",
        base_final_score=80.0,
        viability_legs=[
            "population_below_quartile",
            "rent_high",
            "economics_below_threshold",
        ],
    )
    out = _apply_score_deltas_and_sort([c])
    bd = out[0]["score_breakdown_json"]["bonus_detail"]
    assert bd["viability_delta"] == -30.0
    assert len(bd["viability_legs_fired"]) == 3
    assert out[0]["final_score"] == 50.0


def test_score_delta_freshness_mutual_exclusion():
    # created_days=2 (fresh) AND updated_days=1 (also recent) → "new" wins,
    # bonus is +2, NOT +3.
    c = _sd_candidate(
        parcel_id="p1",
        base_final_score=70.0,
        created_days=2,
        updated_days=1,
        viability_legs=[],
    )
    out = _apply_score_deltas_and_sort([c])
    bd = out[0]["score_breakdown_json"]["bonus_detail"]
    assert bd["freshness_label"] == "new"
    assert bd["freshness_bonus"] == 2.0
    assert out[0]["final_score"] == 72.0


def test_score_delta_top_tier_market_gated_by_sample_floor():
    # momentum_score=80 is well above the 70 cliff but sample_floor_applied
    # forces the neutral fallback shape — momentum bonus must NOT fire.
    c = _sd_candidate(
        parcel_id="p1",
        base_final_score=70.0,
        momentum_score=80.0,
        sample_floor_applied=True,
        viability_legs=[],
    )
    out = _apply_score_deltas_and_sort([c])
    bd = out[0]["score_breakdown_json"]["bonus_detail"]
    assert bd["momentum_bonus"] == 0.0
    # Sanity: with sample_floor_applied=False, the bonus does fire.
    c2 = _sd_candidate(
        parcel_id="p1",
        base_final_score=70.0,
        momentum_score=80.0,
        sample_floor_applied=False,
        viability_legs=[],
    )
    out2 = _apply_score_deltas_and_sort([c2])
    assert out2[0]["score_breakdown_json"]["bonus_detail"]["momentum_bonus"] == 2.0


def test_score_delta_clamping_at_100():
    # Base 95 + best_value (+4) + new (+2) + top-tier momentum (+2) = 103 → 100.
    c = _sd_candidate(
        parcel_id="p1",
        base_final_score=95.0,
        value_band="best_value",
        value_band_low_conf=False,
        created_days=1,
        momentum_score=80.0,
        sample_floor_applied=False,
        viability_legs=[],
    )
    out = _apply_score_deltas_and_sort([c])
    bd = out[0]["score_breakdown_json"]["bonus_detail"]
    assert bd["total_delta"] == 8.0
    assert out[0]["final_score"] == 100.0
    assert bd["final_score_clamped"] is True


def test_score_delta_clamping_at_0():
    # Base 8 + 3-leg viability stack (-30) = -22 → 0.
    c = _sd_candidate(
        parcel_id="p1",
        base_final_score=8.0,
        viability_legs=[
            "population_below_quartile",
            "rent_high",
            "economics_below_threshold",
        ],
    )
    out = _apply_score_deltas_and_sort([c])
    bd = out[0]["score_breakdown_json"]["bonus_detail"]
    assert bd["viability_delta"] == -30.0
    assert out[0]["final_score"] == 0.0
    assert bd["final_score_clamped"] is True


def test_sort_determinism_parcel_id_tiebreak():
    # Two candidates with identical final_score: lexicographically smaller
    # parcel_id ranks first. Two consecutive calls produce identical orderings.
    c_b = _sd_candidate(parcel_id="bbb", base_final_score=70.0, viability_legs=[])
    c_a = _sd_candidate(parcel_id="aaa", base_final_score=70.0, viability_legs=[])
    out_first = _apply_score_deltas_and_sort([c_b, c_a])
    assert [c["parcel_id"] for c in out_first] == ["aaa", "bbb"]
    # Re-run on a fresh copy — must yield byte-identical ordering.
    c_b2 = _sd_candidate(parcel_id="bbb", base_final_score=70.0, viability_legs=[])
    c_a2 = _sd_candidate(parcel_id="aaa", base_final_score=70.0, viability_legs=[])
    out_second = _apply_score_deltas_and_sort([c_b2, c_a2])
    assert [c["parcel_id"] for c in out_second] == [c["parcel_id"] for c in out_first]


def test_no_fuzzy_tiebreak():
    # The deleted _apply_llm_fuzzy_tiebreak symbol must not exist on the
    # module: re-runs of a search must produce identical orderings, no LLM
    # call. Two candidates with similar-but-not-identical scores rank
    # strictly by score — no within-window LLM-driven swap.
    assert not hasattr(expansion_service, "_apply_llm_fuzzy_tiebreak")
    assert not hasattr(expansion_service, "_FUZZY_TIE_WINDOW")
    c_high = _sd_candidate(parcel_id="hi", base_final_score=80.0, viability_legs=[])
    c_low = _sd_candidate(parcel_id="lo", base_final_score=79.5, viability_legs=[])
    out = _apply_score_deltas_and_sort([c_low, c_high])
    assert [c["parcel_id"] for c in out] == ["hi", "lo"]


def test_legacy_value_pass_keys_preserved_for_back_compat():
    # When the value-band delta fires, the deprecated value_pass.* keys
    # (and their top-level mirrors) must still get written so any existing
    # saved-study consumer that hasn't migrated to bonus_detail keeps working.
    c_up = _sd_candidate(
        parcel_id="p1", base_final_score=70.0, value_band="best_value",
        viability_legs=[],
    )
    out = _apply_score_deltas_and_sort([c_up])
    sb = out[0]["score_breakdown_json"]
    assert sb["value_pass"]["value_uprank_applied"] is True
    assert sb["value_pass"]["value_uprank_delta"] == 4
    assert out[0]["value_uprank_applied"] is True
    assert out[0]["value_uprank_delta"] == 4
    # And on the downrank side.
    c_down = _sd_candidate(
        parcel_id="p2", base_final_score=70.0, value_band="above_market",
        viability_legs=[],
    )
    out2 = _apply_score_deltas_and_sort([c_down])
    sb2 = out2[0]["score_breakdown_json"]
    assert sb2["value_pass"]["value_downrank_applied"] is True
    assert sb2["value_pass"]["value_downrank_delta"] == 6
    assert out2[0]["value_downrank_applied"] is True
    assert out2[0]["value_downrank_delta"] == 6


def test_existing_branches_present_no_score_change():
    # Regression guard: a brief with existing branches and viable
    # cannibalization_score must not have its final_score changed by the
    # refactor beyond what the legitimate viability legs contribute.
    # Sanity: the cannibalization_score field is still consumed by
    # _economics_score (its parameter name is unchanged), so a synthetic
    # candidate that carries it through the score-delta pipeline keeps the
    # field intact for downstream consumers.
    from app.services.expansion_advisor import _economics_score
    import inspect

    # The economics_score function still consumes cannibalization_score —
    # parameter name and semantics unchanged.
    sig = inspect.signature(_economics_score)
    assert "cannibalization_score" in sig.parameters

    c = _sd_candidate(
        parcel_id="p1", base_final_score=72.0,
        cannibalization_score=35.0,
        viability_legs=[],
    )
    out = _apply_score_deltas_and_sort([c])
    # No legitimate leg fired → final_score unchanged, cannibalization_score
    # untouched on the candidate dict (the score-delta pass must not strip it).
    assert out[0]["final_score"] == 72.0
    assert out[0]["cannibalization_score"] == 35.0


class _DiagnosticsFakeDB(FakeDB):
    """FakeDB variant that routes the rent-comparable percentile query and
    the listing-district momentum CTE to empty results so listing-backed
    candidates don't accidentally match the generic "FROM commercial_unit"
    branch and feed the candidate row back in as a rent comparable."""

    def execute(self, stmt, params=None):
        sql = stmt.text if hasattr(stmt, "text") else str(stmt)
        if "PERCENTILE_CONT" in sql:
            return _Result([])
        return super().execute(stmt, params)


def test_feature_snapshot_includes_listing_quality_signals_and_chain_provenance(
    disable_market_viability_floors,
):
    """Score Contributions diagnostics: per-input candidate values that the
    score functions read but the snapshot did not previously persist must
    flow into ``feature_snapshot_json`` as a purely additive surface."""
    db = _DiagnosticsFakeDB(
        candidate_rows=[
            {
                "parcel_id": "p1",
                "commercial_unit_id": "cu-1",
                "landuse_label": "Commercial",
                "landuse_code": "C",
                "area_m2": 180,
                "lon": 46.7,
                "lat": 24.7,
                "district": "حي العليا",
                "population_reach": 15000,
                "competitor_count": 4,
                "delivery_listing_count": 12,
                # Listing quality input scalars (consumed by _listing_quality_score)
                "unit_llm_suitability_score": 72.5,
                "unit_llm_listing_quality_score": 64.0,
                "unit_is_furnished": True,
                "unit_has_drive_thru": False,
                "unit_restaurant_score": 81.0,
                # Confidence inputs (consumed by _confidence_score)
                "area_confidence": "actual",
                # Chain-strength provenance from bulk competitor enrichment.
                # The bulk enricher returns {} against FakeDB (no POI rows),
                # so we inject these directly on the row to simulate the
                # post-enrichment state.
                "max_chain_strength": 78.0,
                "top_chain_strength_name": "Test Chain Brand",
            }
        ]
    )

    items = run_expansion_search(
        db,
        search_id="search-diag",
        brand_name="Brand X",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=300,
        target_area_m2=180,
        limit=10,
    )

    assert len(items) == 1
    snapshot = items[0]["feature_snapshot_json"]

    # Group A — listing_quality_signals (5 keys, listing-backed candidate).
    lqs = snapshot["listing_quality_signals"]
    assert lqs["llm_suitability_score"] == 72.5
    assert lqs["llm_listing_quality_score"] == 64.0
    assert lqs["is_furnished"] is True
    assert lqs["has_drive_thru"] is False
    assert lqs["unit_restaurant_score"] == 81.0

    # Group B — confidence inputs surfaced top-level.
    assert snapshot["area_confidence"] == "actual"
    assert snapshot["is_listing"] is True

    # Group C — chain_strength provenance under brand_presence.
    assert snapshot["brand_presence"]["top_chain_strength_name"] == "Test Chain Brand"


def test_feature_snapshot_parcel_candidate_omits_listing_quality_signals(
    disable_market_viability_floors,
):
    """Parcel-only (non-listing) candidate: is_listing must be False and the
    listing_quality_signals block must be empty (consumers treat missing
    keys as not-applicable). The unit_* columns are absent on parcel rows
    and the snapshot assembly must not raise."""
    db = FakeDB(
        candidate_rows=[
            {
                "parcel_id": "p1",
                # No commercial_unit_id → _is_listing is False.
                "landuse_label": "Commercial",
                "landuse_code": "C",
                "area_m2": 180,
                "lon": 46.7,
                "lat": 24.7,
                "district": "حي العليا",
                "population_reach": 15000,
                "competitor_count": 4,
                "delivery_listing_count": 12,
            }
        ]
    )

    items = run_expansion_search(
        db,
        search_id="search-parcel-diag",
        brand_name="Brand X",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=300,
        target_area_m2=180,
        limit=10,
    )

    assert len(items) == 1
    snapshot = items[0]["feature_snapshot_json"]
    assert snapshot["is_listing"] is False
    assert snapshot["listing_quality_signals"] == {}
    # area_confidence is absent on the parcel row → key present, value None.
    assert snapshot["area_confidence"] is None
    # brand_presence still has the chain_strength provenance key, but with
    # a None value because the bulk enricher returned no chain_strength rows.
    assert snapshot["brand_presence"]["top_chain_strength_name"] is None


# ---------------------------------------------------------------------------
# PR #4d §1 — _recommended_use_case Arabic localization
# ---------------------------------------------------------------------------

# Snapshot of every English phrase the pre-PR-4d function could return,
# keyed by (service_model, area_m2). Used to prove the EN branch is
# byte-identical after adding the ``lang`` parameter.
_USE_CASE_EN_SNAPSHOT = {
    ("dine_in", 300.0): "flagship dine-in",
    ("dine_in", 200.0): "neighborhood dine-in",
    ("delivery_first", 150.0): "delivery-led branch",
    ("cafe", 150.0): "compact cafe",
    ("cafe", 220.0): "destination cafe",
    ("qsr", 200.0): "neighborhood qsr",
}

_USE_CASE_AR_SNAPSHOT = {
    ("dine_in", 300.0): "مطعم رئيسي للتناول في الموقع",
    ("dine_in", 200.0): "مطعم تناول في الموقع للحي",
    ("delivery_first", 150.0): "فرع يعتمد على التوصيل",
    ("cafe", 150.0): "مقهى صغير",
    ("cafe", 220.0): "مقهى وجهة",
    ("qsr", 200.0): "مطعم خدمة سريعة للحي",
}


def test_recommended_use_case_english_is_byte_identical():
    """The default (lang='en') branch must not drift from the pre-PR-4d output."""
    for (service_model, area_m2), expected in _USE_CASE_EN_SNAPSHOT.items():
        # Implicit default and explicit lang="en" both return the EN phrase.
        assert (
            expansion_service._recommended_use_case(service_model, area_m2) == expected
        )
        assert (
            expansion_service._recommended_use_case(service_model, area_m2, lang="en")
            == expected
        )


def test_recommended_use_case_arabic_phrases():
    """lang='ar' returns the six pre-approved Arabic phrases."""
    for (service_model, area_m2), expected in _USE_CASE_AR_SNAPSHOT.items():
        assert (
            expansion_service._recommended_use_case(service_model, area_m2, lang="ar")
            == expected
        )


def test_recommended_use_case_unknown_locale_falls_back_to_english():
    """An unexpected lang token is treated as English (no crash, no key leak)."""
    assert (
        expansion_service._recommended_use_case("qsr", 200.0, lang="fr")
        == "neighborhood qsr"
    )


# ---------------------------------------------------------------------------
# PR #4d follow-up §1 — get_recommendation_report.best_format honors lang
# ---------------------------------------------------------------------------

def _install_recommendation_report_fixture(
    monkeypatch, *, service_model: str, area_m2: float
) -> None:
    """Stub get_search and get_candidates for a single-candidate report fixture."""
    monkeypatch.setattr(
        expansion_service,
        "get_search",
        lambda *_a, **_kw: {
            "id": "search-1",
            "service_model": service_model,
            "brand_profile": {"expansion_goal": "balanced"},
        },
    )
    monkeypatch.setattr(
        expansion_service,
        "get_candidates",
        lambda *_a, **_kw: [
            {
                "id": "c1", "final_score": 80, "brand_fit_score": 70, "economics_score": 65,
                "area_m2": area_m2, "district": "Olaya", "key_risks_json": ["risk"],
                "gate_status_json": {"overall_pass": True},
                "confidence_grade": "B", "confidence_score": 70,
                "rank_position": 1, "score_breakdown_json": {"final_score": 80},
                "top_positives_json": [], "top_risks_json": ["risk"],
                "feature_snapshot_json": {"parcel_area_m2": area_m2, "data_completeness_score": 70},
            }
        ],
    )


def test_get_recommendation_report_best_format_english_byte_identical(monkeypatch):
    """best_format is byte-identical to the pre-PR EN output when lang is
    omitted, lang='en', or an unknown locale (e.g. 'fr' → EN fallback)."""
    for (service_model, area_m2), expected in _USE_CASE_EN_SNAPSHOT.items():
        _install_recommendation_report_fixture(
            monkeypatch, service_model=service_model, area_m2=area_m2
        )
        # Implicit default.
        report = get_recommendation_report(FakeDB(), "search-1")
        assert report["recommendation"]["best_format"] == expected, (service_model, area_m2)
        # Explicit lang="en".
        report = get_recommendation_report(FakeDB(), "search-1", lang="en")
        assert report["recommendation"]["best_format"] == expected, (service_model, area_m2)
        # Unknown locale → English fallback.
        report = get_recommendation_report(FakeDB(), "search-1", lang="fr")
        assert report["recommendation"]["best_format"] == expected, (service_model, area_m2)


def test_get_recommendation_report_best_format_arabic(monkeypatch):
    """best_format returns the matching AR phrase when lang='ar'."""
    for (service_model, area_m2), expected in _USE_CASE_AR_SNAPSHOT.items():
        _install_recommendation_report_fixture(
            monkeypatch, service_model=service_model, area_m2=area_m2
        )
        report = get_recommendation_report(FakeDB(), "search-1", lang="ar")
        assert report["recommendation"]["best_format"] == expected, (service_model, area_m2)
