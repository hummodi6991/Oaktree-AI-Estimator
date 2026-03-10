from __future__ import annotations

from app.services.expansion_advisor import compare_candidates, run_expansion_search


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
    def __init__(self, candidate_rows=None, compare_rows=None, has_search=True):
        self.candidate_rows = candidate_rows or []
        self.compare_rows = compare_rows or []
        self.has_search = has_search
        self.inserted = []

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
        return _Result([])


def test_district_filtering_narrows_results_and_sets_cannibalization_fields():
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
