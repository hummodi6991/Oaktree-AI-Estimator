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


class FakeDB:
    def __init__(self, candidate_rows=None, compare_rows=None, has_search=True, memo_row=None, brand_profile_row=None):
        self.candidate_rows = candidate_rows or []
        self.compare_rows = compare_rows or []
        self.has_search = has_search
        self.memo_row = memo_row
        self.inserted = []
        self.brand_profile_row = brand_profile_row

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
    svc.get_candidates = lambda _db, _sid: [
        {"id": "c1", "final_score": 90, "brand_fit_score": 82, "economics_score": 70, "area_m2": 170, "district": "Olaya", "key_risks_json": ["risk"]},
        {"id": "c2", "final_score": 86, "brand_fit_score": 79, "economics_score": 68, "area_m2": 180, "district": "Malqa", "key_risks_json": ["risk2"]},
    ]
    report = get_recommendation_report(db, "search-1")
    assert report is not None
    assert report["recommendation"]["best_candidate_id"] == "c1"


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
        zoning_fit_score=80,
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
    svc.get_candidates = lambda _db, _sid: [
        {"id": "c1", "final_score": 90, "brand_fit_score": 82, "economics_score": 70, "area_m2": 170, "district": "Olaya", "key_risks_json": ["risk"], "confidence_grade": "A", "confidence_score": 85, "gate_status_json": {"overall_pass": True}, "demand_thesis": "d", "cost_thesis": "c", "comparable_competitors_json": [{"id": "x"}], "zoning_fit_score": 88, "frontage_score": 65, "access_score": 67, "parking_score": 62, "access_visibility_score": 66, "feature_snapshot_json": {"parcel_area_m2": 170}},
        {"id": "c2", "final_score": 86, "brand_fit_score": 79, "economics_score": 68, "area_m2": 180, "district": "Malqa", "key_risks_json": ["risk2"], "confidence_grade": "B", "confidence_score": 72, "gate_status_json": {"overall_pass": False}, "demand_thesis": "d2", "cost_thesis": "c2", "comparable_competitors_json": [], "zoning_fit_score": 78, "frontage_score": 61, "access_score": 60, "parking_score": 58, "access_visibility_score": 61, "feature_snapshot_json": {"parcel_area_m2": 180}},
    ]
    report = get_recommendation_report(db, "search-1")
    assert report["recommendation"]["best_pass_candidate_id"] == "c1"
    assert report["recommendation"]["best_confidence_candidate_id"] == "c1"
    assert "demand_thesis" in report["top_candidates"][0]
    assert "zoning_fit_score" in report["top_candidates"][0]
    assert "feature_snapshot_json" in report["top_candidates"][0]


def test_v6_feature_scores_are_bounded():
    assert 0 <= expansion_service._zoning_fit_score("commercial", "C") <= 100
    assert 0 <= expansion_service._frontage_score(parcel_perimeter_m=240, touches_road=True, nearby_road_count=5, nearest_major_road_m=120) <= 100
    assert 0 <= expansion_service._access_score(touches_road=False, nearest_major_road_m=350, nearby_road_count=2) <= 100
    assert 0 <= expansion_service._parking_score(area_m2=180, service_model="qsr", nearby_parking_count=3, access_score=65) <= 100


def test_gate_status_uses_v6_scores_for_failure():
    gates, reasons = _candidate_gate_status(
        fit_score=75,
        zoning_fit_score=40,
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
    )
    assert gates["overall_pass"] is False
    assert "zoning_fit_pass" in reasons["failed"]
    assert "frontage_access_pass" in reasons["failed"]
    assert "parking_pass" in reasons["failed"]
