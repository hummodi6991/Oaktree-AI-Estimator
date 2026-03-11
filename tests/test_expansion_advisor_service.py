from __future__ import annotations

from app.services import expansion_advisor as expansion_service
from app.services.expansion_advisor import (
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
        }
    )

    memo = get_candidate_memo(db, "c1")

    assert memo is not None
    assert memo["candidate_id"] == "c1"
    assert memo["recommendation"]["verdict"] in {"go", "consider", "caution"}
    assert memo["candidate"]["key_strengths"]


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
