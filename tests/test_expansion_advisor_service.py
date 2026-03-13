from __future__ import annotations

from app.services import expansion_advisor as expansion_service
from app.services.expansion_advisor import (
    _brand_fit_score,
    _candidate_gate_status,
    _comparable_competitors,
    _confidence_grade,
    _payback_band,
    compare_candidates,
    get_candidate_memo,
    get_recommendation_report,
    get_search,
    run_expansion_search,
)


class _Result:
    def __init__(self, rows):
        self._rows = rows

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


def test_district_filtering_narrows_results_and_sets_economics_fields():
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
    assert items[0]["estimated_payback_months"] is not None
    assert items[0]["payback_band"] in {"strong", "promising", "borderline", "weak"}
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
                "estimated_payback_months": 24,
                "payback_band": "promising",
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
                "estimated_payback_months": 24,
                "payback_band": "promising",
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
                "estimated_payback_months": 27,
                "payback_band": "promising",
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
    assert result["items"][0]["gate_reasons_json"]["failed"] == ["frontage_access_pass"]
    assert result["items"][0]["gate_reasons_json"]["unknown"] == []
    assert result["items"][0]["gate_reasons_json"]["thresholds"] == {}
    assert result["items"][0]["cost_thesis"] == "Cost is manageable"
    assert result["items"][0]["comparable_competitors_json"] == [{"id": "r1"}]
    assert result["summary"]["best_gate_pass_candidate_id"] == "c2"


def test_payback_band_assignment_logic():
    assert _payback_band(12) == "strong"
    assert _payback_band(24) == "promising"
    assert _payback_band(35) == "borderline"
    assert _payback_band(52) == "weak"


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
            "estimated_payback_months": 22,
            "payback_band": "promising",
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
    assert memo["candidate"]["gate_reasons"]["passed"] == ["zoning_fit_pass"]
    assert memo["candidate"]["gate_reasons"]["unknown"] == []
    assert memo["candidate"]["score_breakdown_json"]["weights"] == {}
    assert memo["candidate"]["feature_snapshot"]["touches_road"] is True
    assert memo["candidate"]["comparable_competitors"][0]["id"] == "r1"


def test_run_expansion_search_caches_rent_resolution_by_district(monkeypatch):
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


def test_report_happy_path_returns_best_and_runner_up():
    db = FakeDB(candidate_rows=[], brand_profile_row={"price_tier": "mid", "preferred_districts_json": [], "excluded_districts_json": []})
    import app.services.expansion_advisor as svc
    svc.get_search = lambda _db, _sid: {"id": "search-1", "service_model": "qsr", "brand_profile": {"expansion_goal": "balanced"}}
    svc.get_candidates = lambda _db, _sid, district_lookup=None: [
        {"id": "c1", "final_score": 90, "brand_fit_score": 82, "economics_score": 70, "area_m2": 170, "district": "Olaya", "key_risks_json": ["risk"]},
        {"id": "c2", "final_score": 86, "brand_fit_score": 79, "economics_score": 68, "area_m2": 180, "district": "Malqa", "key_risks_json": ["risk2"]},
    ]
    report = get_recommendation_report(db, "search-1")
    assert report is not None
    assert report["recommendation"]["best_candidate_id"] == "c1"
    assert report["meta"]["version"] == "expansion_advisor_v6.1"


def test_brand_provider_scores_bounded():
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
        payback_band="promising",
        brand_profile={"primary_channel": "delivery", "excluded_districts": ["Malqa"], "cannibalization_tolerance_m": 1800},
        road_context_available=True,
        parking_context_available=True,
    )
    assert gates["overall_pass"] is True
    assert gates["district_pass"] is True
    assert reasons["failed"] == []


def test_confidence_grade_bounds():
    assert _confidence_grade(confidence_score=88, district="Olaya", provider_platform_count=2, multi_platform_presence_score=50, rent_source="aqar_city") == "A"
    assert _confidence_grade(confidence_score=70, district=None, provider_platform_count=None, multi_platform_presence_score=None, rent_source="conservative_default") in {"B", "C"}
    assert _confidence_grade(confidence_score=30, district=None, provider_platform_count=None, multi_platform_presence_score=None, rent_source="conservative_default") == "D"


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


def test_report_includes_new_decision_outputs():
    db = FakeDB(candidate_rows=[])
    import app.services.expansion_advisor as svc
    svc.get_search = lambda _db, _sid: {"id": "search-1", "service_model": "qsr", "brand_profile": {"expansion_goal": "balanced"}}
    svc.get_candidates = lambda _db, _sid, district_lookup=None: [
        {"id": "c1", "final_score": 90, "brand_fit_score": 82, "economics_score": 70, "area_m2": 170, "district": "Olaya", "key_risks_json": ["risk"], "confidence_grade": "A", "confidence_score": 85, "gate_status_json": {"overall_pass": True}, "demand_thesis": "d", "cost_thesis": "c", "comparable_competitors_json": [{"id": "x"}], "zoning_fit_score": 88, "frontage_score": 65, "access_score": 67, "parking_score": 62, "access_visibility_score": 66, "feature_snapshot_json": {"parcel_area_m2": 170, "data_completeness_score": 90}, "rank_position": 1, "score_breakdown_json": {"final_score": 90}, "top_positives_json": ["pos"], "top_risks_json": ["risk"]},
        {"id": "c2", "final_score": 86, "brand_fit_score": 79, "economics_score": 68, "area_m2": 180, "district": "Malqa", "key_risks_json": ["risk2"], "confidence_grade": "B", "confidence_score": 72, "gate_status_json": {"overall_pass": False}, "demand_thesis": "d2", "cost_thesis": "c2", "comparable_competitors_json": [], "zoning_fit_score": 78, "frontage_score": 61, "access_score": 60, "parking_score": 58, "access_visibility_score": 61, "feature_snapshot_json": {"parcel_area_m2": 180, "data_completeness_score": 80}, "rank_position": 2, "score_breakdown_json": {"final_score": 86}, "top_positives_json": ["pos2"], "top_risks_json": ["risk2"]},
    ]
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
        payback_band="promising",
        brand_profile={"excluded_districts": [], "cannibalization_tolerance_m": 1800},
        road_context_available=True,
        parking_context_available=True,
    )
    assert gates["overall_pass"] is False
    assert "zoning_fit_pass" in reasons["failed"]
    assert "frontage_access_pass" in reasons["failed"]
    assert "parking_pass" in reasons["failed"]


def test_missing_road_context_uses_neutral_scores_and_unknown_gate(monkeypatch):
    db = FakeDB(candidate_rows=[{
        "parcel_id": "p1", "landuse_label": "Commercial", "landuse_code": "C", "area_m2": 180, "lon": 46.7, "lat": 24.7, "district": "Olaya",
        "population_reach": 15000, "competitor_count": 5, "delivery_listing_count": 12
    }])

    monkeypatch.setattr(expansion_service, "_table_available", lambda _db, _table: False)

    items = run_expansion_search(db, search_id="s", brand_name="b", category="burger", service_model="qsr", min_area_m2=100, max_area_m2=300, target_area_m2=180, limit=3)
    item = items[0]
    assert item["frontage_score"] == 55.0
    assert item["access_score"] == 55.0
    assert "frontage_access_pass" in item["gate_reasons_json"]["unknown"]
    # With both road tables missing, frontage/parking gates are unknown (None),
    # so overall_pass is None (indeterminate), not True.
    assert item["gate_status_json"]["overall_pass"] is None


def test_missing_parking_context_uses_neutral_score_and_unknown_gate(monkeypatch):
    db = FakeDB(candidate_rows=[{
        "parcel_id": "p1", "landuse_label": "Commercial", "landuse_code": "C", "area_m2": 180, "lon": 46.7, "lat": 24.7, "district": "Olaya",
        "population_reach": 15000, "competitor_count": 5, "delivery_listing_count": 12
    }])

    monkeypatch.setattr(expansion_service, "_table_available", lambda _db, table: table == "public.planet_osm_line")

    items = run_expansion_search(db, search_id="s", brand_name="b", category="burger", service_model="qsr", min_area_m2=100, max_area_m2=300, target_area_m2=180, limit=3)
    item = items[0]
    assert 0.0 <= item["parking_score"] <= 100.0
    assert "parking_pass" in item["gate_reasons_json"]["unknown"]


def test_score_breakdown_matches_final_score():
    breakdown = expansion_service._score_breakdown(
        demand_score=80,
        whitespace_score=70,
        brand_fit_score=75,
        economics_score=60,
        provider_intelligence_composite=65,
        access_visibility_score=55,
        confidence_score=50,
    )
    weighted_sum = sum((breakdown.get("weighted_components") or {}).values())
    assert abs(weighted_sum - breakdown["final_score"]) < 0.01
    assert 0.0 <= breakdown["final_score"] <= 100.0


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
                "multi_platform_presence_score": 60, "delivery_competition_score": 50, "estimated_payback_months": 24,
                "payback_band": "promising", "competitor_count": 3, "delivery_listing_count": 12, "population_reach": 14000,
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
                "multi_platform_presence_score": 58, "delivery_competition_score": 52, "estimated_payback_months": 27,
                "payback_band": "promising", "competitor_count": 4, "delivery_listing_count": 11, "population_reach": 13200,
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


def test_search_caches_context_table_checks_and_limits_snapshot_work(monkeypatch):
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
                "district": "Olaya",
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
            "district": "Olaya",
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
            "estimated_rent_sar_m2_year": 900,
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

    monkeypatch.setattr(expansion_service, "_table_available", _fake_table_available)
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
    assert snapshot_calls == 50


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
    assert payload["meta"]["version"] == "expansion_advisor_v6.1"


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
        "fastest_payback_candidate_id",
        "most_confident_candidate_id",
        "best_gate_pass_candidate_id",
    }
    assert all(value is None for value in result["summary"].values())


def test_get_recommendation_report_empty_state_is_deterministic(monkeypatch):
    monkeypatch.setattr(expansion_service, "get_search", lambda *_args, **_kwargs: {"id": "search-1", "brand_profile": {}})
    monkeypatch.setattr(expansion_service, "get_candidates", lambda *_args, **_kwargs: [])

    report = get_recommendation_report(FakeDB(), "search-1")

    assert report is not None
    assert report["meta"]["version"] == "expansion_advisor_v6.1"
    assert report["top_candidates"] == []
    assert set(report["recommendation"].keys()) == {
        "best_candidate_id",
        "runner_up_candidate_id",
        "best_pass_candidate_id",
        "best_confidence_candidate_id",
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

def test_run_expansion_search_with_brand_profile_and_branches():
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
    assert item["estimated_payback_months"] is not None
    assert item["payback_band"] in {"strong", "promising", "borderline", "weak"}
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


def test_run_expansion_search_empty_existing_branches():
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
        limit=25,
        existing_branches=[],
        brand_profile={
            "preferred_districts": ["Alolaya"],
        },
    )

    assert len(items) == 1
    item = items[0]
    assert item["distance_to_nearest_branch_m"] is None
    assert item["cannibalization_score"] == 25.0
    assert 0.0 <= item["final_score"] <= 100.0
    assert item["economics_score"] is not None
    assert item["estimated_payback_months"] is not None


def test_run_expansion_search_preferred_districts_typo_no_crash():
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
        limit=25,
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
        limit=25,
        target_districts=["Nonexistent District"],
        existing_branches=[],
    )

    assert items == []


def test_run_expansion_search_exact_production_payload():
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
        limit=25,
        target_districts=["Al Olaya", "Al Malqa", "Al Nakheel"],
        existing_branches=[],
        brand_profile=brand_profile,
    )

    # "Al Olaya" in candidate matches "Al Olaya" in target_districts
    assert len(items) == 1
    item = items[0]
    assert item["distance_to_nearest_branch_m"] is None
    assert item["cannibalization_score"] == 25.0
    assert 0.0 <= item["final_score"] <= 100.0
    assert item["payback_band"] in {"strong", "promising", "borderline", "weak"}
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


def test_snapshot_db_failure_does_not_poison_session(monkeypatch):
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
    assert items[0]["frontage_score"] == 55.0
    assert items[0]["access_score"] == 55.0


def test_candidate_insert_failure_skips_candidate_gracefully(monkeypatch):
    """When one candidate INSERT fails, remaining candidates are still returned."""
    insert_count = 0

    class InsertFailDB(FakeDB):
        def execute(self, stmt, params=None):
            nonlocal insert_count
            sql = stmt.text if hasattr(stmt, "text") else str(stmt)
            if "INSERT INTO expansion_candidate" in sql:
                insert_count += 1
                if insert_count == 1:
                    raise RuntimeError("Simulated insert failure")
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
    # First candidate fails to INSERT but second succeeds
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


def test_rent_lookup_failure_falls_back_to_default(monkeypatch):
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
    assert items[0]["estimated_rent_sar_m2_year"] == 900.0


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
        limit=25,
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
        assert item["payback_band"] in {"strong", "promising", "borderline", "weak"}
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
    limit=25,
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
        limit=25,
        bbox=None,
        target_districts=["Al Olaya", "Al Malqa", "Al Nakheel"],
        existing_branches=[],
        brand_profile={
            "strategy": "balanced",
            "price_tier": "mid",
            "target_customer": "Families",
            "visibility_sensitivity": "35",
            "cannibalization_tolerance_m": 1500,
        },
    )
    assert isinstance(items, list)
    for item in items:
        assert 0.0 <= item["final_score"] <= 100.0
        assert item["payback_band"] in {"strong", "promising", "borderline", "weak"}


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
    frontage_score=70,
    access_score=66,
    parking_score=55,
    district="Olaya",
    distance_to_nearest_branch_m=2200,
    provider_density_score=50,
    multi_platform_presence_score=40,
    economics_score=65,
    payback_band="promising",
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
        **_GATE_BASE,
        area_fit_score=20.0,  # outside range (score < 55 threshold)
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
        **_GATE_BASE,
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


def test_report_best_pass_candidate_id_null_when_no_pass():
    """get_recommendation_report() must set best_pass_candidate_id = None when
    no candidates pass all gates."""
    import app.services.expansion_advisor as svc

    db = FakeDB(candidate_rows=[], brand_profile_row={
        "price_tier": "mid",
        "preferred_districts_json": [],
        "excluded_districts_json": [],
    })
    svc.get_search = lambda _db, _sid: {
        "id": "search-1",
        "service_model": "qsr",
        "brand_profile": {"expansion_goal": "balanced"},
    }
    # Both candidates have overall_pass=False
    svc.get_candidates = lambda _db, _sid, district_lookup=None: [
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
    ]

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

    positives, risks = _top_positives_and_risks(
        candidate=candidate, gate_reasons=gate_reasons,
    )

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
    )

    assert "display" in breakdown
    assert "demand_potential" in breakdown["display"]

    dp = breakdown["display"]["demand_potential"]
    assert "raw_input_score" in dp
    assert "weight_percent" in dp
    assert "weighted_points" in dp
    assert dp["raw_input_score"] == 80.0
    assert dp["weight_percent"] == 25
    assert dp["weighted_points"] == round(80.0 * 0.25, 2)

    # Verify weighted_points != weight_percent (they are NOT the same thing)
    for name, entry in breakdown["display"].items():
        assert entry["weighted_points"] != entry["weight_percent"] or entry["raw_input_score"] == 100.0, \
            f"{name}: weighted_points should differ from weight_percent unless input is 100"


def test_report_gate_verdict_uses_tristate():
    """top_candidates[].gate_verdict in reports must use tri-state mapping,
    not bool()."""
    import app.services.expansion_advisor as svc

    db = FakeDB(candidate_rows=[], brand_profile_row={
        "price_tier": "mid",
        "preferred_districts_json": [],
        "excluded_districts_json": [],
    })
    svc.get_search = lambda _db, _sid: {
        "id": "search-1",
        "service_model": "qsr",
        "brand_profile": {"expansion_goal": "balanced"},
    }
    svc.get_candidates = lambda _db, _sid, district_lookup=None: [
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
    ]

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
            "estimated_payback_months": 28,
            "payback_band": "promising",
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


def test_report_compatible_with_legacy_two_arg_get_candidates():
    """get_recommendation_report must work when get_candidates is a 2-arg callable (legacy monkeypatch)."""
    db = FakeDB(candidate_rows=[])
    import app.services.expansion_advisor as svc
    svc.get_search = lambda _db, _sid: {
        "id": "search-1",
        "service_model": "qsr",
        "brand_profile": {"expansion_goal": "balanced"},
    }
    # Legacy 2-arg callable — must not raise TypeError
    svc.get_candidates = lambda _db, _sid: [
        {"id": "c1", "final_score": 80, "brand_fit_score": 70, "economics_score": 60,
         "area_m2": 150, "district": "Olaya", "key_risks_json": []},
    ]
    report = get_recommendation_report(db, "search-1")
    assert report is not None
    assert report["recommendation"]["best_candidate_id"] == "c1"
