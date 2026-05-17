from fastapi.testclient import TestClient

from app.db.deps import get_db
from app.main import app


class DummyDB:
    def __init__(self) -> None:
        self.executed: list[tuple[str, dict]] = []
        self.committed = False
        self.rolled_back = False

    def execute(self, stmt, params=None):
        sql_text = stmt.text if hasattr(stmt, "text") else str(stmt)
        self.executed.append((sql_text, params or {}))

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


def _client_with_db(db: DummyDB) -> TestClient:
    def override_get_db():
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    return TestClient(app, raise_server_exceptions=False)


def test_post_expansion_search_with_existing_branches(monkeypatch):
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(expansion_api, "persist_existing_branches", lambda _db, _search_id, _branches: None)
    monkeypatch.setattr(expansion_api, "persist_brand_profile", lambda _db, _search_id, _profile: None)
    monkeypatch.setattr(
        expansion_api,
        "run_expansion_search",
        lambda **kwargs: [
            {
                "id": "candidate-1",
                "search_id": kwargs["search_id"],
                "parcel_id": "parcel-123",
                "district": "Olaya",
                "lat": 24.7,
                "lon": 46.7,
                "cannibalization_score": 55.0,
                "distance_to_nearest_branch_m": 1400.0,
                "compare_rank": 1,
                "final_score": 86.6,
                "explanation": {"summary": "ok", "positives": [], "risks": [], "inputs": {}},
            }
        ],
    )

    client = _client_with_db(db)
    try:
        payload = {
            "brand_name": "Brand X",
            "category": "burger",
            "service_model": "qsr",
            "min_area_m2": 100,
            "max_area_m2": 350,
            "existing_branches": [
                {"name": "HQ", "lat": 24.71, "lon": 46.68, "district": "Olaya"}
            ],
            "target_districts": ["Olaya"],
            "bbox": {"min_lon": 46.5, "min_lat": 24.5, "max_lon": 46.9, "max_lat": 24.9},
            "limit": 10,
            "brand_profile": {
                "price_tier": "premium",
                "primary_channel": "delivery",
                "expansion_goal": "delivery_led",
                "preferred_districts": ["Olaya"],
                "excluded_districts": ["Malqa"],
            },
        }
        response = client.post("/v1/expansion-advisor/searches", json=payload)
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    body = response.json()
    assert body["brand_profile"]["existing_branches"][0]["name"] == "HQ"
    assert body["items"][0]["district"] == "Olaya"
    assert body["items"][0]["cannibalization_score"] == 55.0
    assert body["meta"]["version"] == "expansion_advisor_v7"
    assert body["items"][0]["score_breakdown_json"]["weights"] == {}
    assert db.committed is True




def test_get_expansion_search_detail_includes_versioned_meta(monkeypatch):
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(
        expansion_api,
        "get_search",
        lambda _db, _search_id, **_kw: {
            "id": "search-1",
            "created_at": "2026-01-01T00:00:00Z",
            "brand_name": "Brand X",
            "category": "burger",
            "service_model": "qsr",
            "target_districts": ["Olaya"],
            "min_area_m2": 100,
            "max_area_m2": 300,
            "target_area_m2": 180,
            "bbox": None,
            "request_json": {"brand_name": "Brand X"},
            "notes": {"version": "expansion_advisor_v7"},
            "existing_branches": [],
            "brand_profile": None,
            "meta": {"version": "expansion_advisor_v7", "parcel_source": "listings_only", "excluded_sources": ["arcgis_parcels", "hungerstation_poi", "suhail", "inferred_parcels"]},
        },
    )

    client = _client_with_db(db)
    try:
        response = client.get("/v1/expansion-advisor/searches/search-1")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "search-1"
    assert body["meta"]["version"] == "expansion_advisor_v7"

def test_get_expansion_search_candidates_shape(monkeypatch):
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(expansion_api, "get_search", lambda _db, _search_id, **_kw: {"id": "search-1"})
    monkeypatch.setattr(
        expansion_api,
        "get_candidates",
        lambda _db, _search_id, **_kw: [
            {
                "id": "candidate-1",
                "search_id": "search-1",
                "parcel_id": "parcel-123",
                "district": "Olaya",
                "cannibalization_score": 40.0,
                "distance_to_nearest_branch_m": 3200.0,
                "compare_rank": 1,
                "rank_position": 1,
                "estimated_rent_sar_m2_year": 980.0,
                "estimated_annual_rent_sar": 176400.0,
                "estimated_fitout_cost_sar": 468000.0,
                "estimated_revenue_index": 73.0,
                "economics_score": 69.0,
                "decision_summary": "summary",
                "key_risks_json": ["risk"],
                "key_strengths_json": ["strength"],
                "confidence_grade": "A",
                "gate_status_json": {"overall_pass": True},
                "gate_reasons_json": {"passed": ["zoning_fit_pass"], "failed": [], "unknown": [], "thresholds": {}, "explanations": {}},
                "feature_snapshot_json": {"parcel_area_m2": 180, "context_sources": {"road_context_available": True}, "missing_context": [], "data_completeness_score": 90},
                "score_breakdown_json": {"weights": {}, "inputs": {}, "weighted_components": {}, "final_score": 88.1},
                "demand_thesis": "Demand is strong",
                "cost_thesis": "Cost is manageable",
                "top_positives_json": ["Demand potential is strong for this district."],
                "top_risks_json": ["Delivery competition intensity is high."],
                "comparable_competitors_json": [{"id": "r1", "name": "Comp"}],
                "final_score": 88.1,
                "explanation": {"summary": "candidate explanation"},
            }
        ],
    )

    client = _client_with_db(db)
    try:
        response = client.get("/v1/expansion-advisor/searches/search-1/candidates")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    body = response.json()
    assert body["items"][0]["district"] == "Olaya"
    assert body["items"][0]["compare_rank"] == 1
    assert body["items"][0]["economics_score"] == 69.0
    assert "payback_band" not in body["items"][0]
    assert body["items"][0]["confidence_grade"] == "A"
    assert body["items"][0]["gate_status_json"]["overall_pass"] is True
    assert body["items"][0]["rank_position"] == 1
    assert "score_breakdown_json" in body["items"][0]
    assert "top_positives_json" in body["items"][0]
    assert body["meta"]["version"] == "expansion_advisor_v7"


def test_compare_endpoint_happy_path(monkeypatch):
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(
        expansion_api,
        "compare_candidates",
        lambda _db, _search_id, _candidate_ids, **_kw: {
            "items": [
                {"candidate_id": "c1", "economics_score": 70.0, "zoning_fit_score": 82, "frontage_score": 65, "access_score": 66, "parking_score": 61, "access_visibility_score": 64},
                {"candidate_id": "c2", "economics_score": 62.0},
            ],
            "summary": {
                "best_overall_candidate_id": "c1",
                "lowest_cannibalization_candidate_id": "c2",
                "highest_demand_candidate_id": "c1",
                "best_fit_candidate_id": "c1",
                "best_economics_candidate_id": "c1",
                "best_brand_fit_candidate_id": "c1",
                "strongest_delivery_market_candidate_id": "c2",
                "strongest_whitespace_candidate_id": "c2",
                "lowest_rent_burden_candidate_id": "c2",
                "most_confident_candidate_id": "c2",
                "best_gate_pass_candidate_id": "c1",
            },
        },
    )

    client = _client_with_db(db)
    try:
        response = client.post(
            "/v1/expansion-advisor/candidates/compare",
            json={"search_id": "search-1", "candidate_ids": ["c1", "c2"]},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    body = response.json()
    assert body["items"][0]["candidate_id"] == "c1"
    assert body["items"][0]["economics_score"] == 70.0
    assert body["items"][0]["zoning_fit_score"] == 82
    assert body["summary"]["best_economics_candidate_id"] == "c1"
    assert set(body["summary"].keys()) == {
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


def test_compare_endpoint_rejects_foreign_candidate_ids(monkeypatch):
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    def _raise_not_found(_db, _search_id, _candidate_ids, **_kw):
        raise ValueError("not_found")

    monkeypatch.setattr(expansion_api, "compare_candidates", _raise_not_found)

    client = _client_with_db(db)
    try:
        response = client.post(
            "/v1/expansion-advisor/candidates/compare",
            json={"search_id": "search-1", "candidate_ids": ["c1", "c-foreign"]},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 404


def test_post_expansion_search_rolls_back_when_scoring_fails(monkeypatch):
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(expansion_api, "persist_existing_branches", lambda *_args, **_kwargs: None)

    def _boom(**_kwargs):
        raise RuntimeError("scoring failed")

    monkeypatch.setattr(expansion_api, "run_expansion_search", _boom)

    client = _client_with_db(db)
    try:
        payload = {
            "brand_name": "Brand X",
            "category": "burger",
            "service_model": "qsr",
            "min_area_m2": 100,
            "max_area_m2": 350,
        }
        response = client.post("/v1/expansion-advisor/searches", json=payload)
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 500
    assert db.committed is False
    assert db.rolled_back is True


def test_candidate_memo_endpoint_happy_path(monkeypatch):
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(
        expansion_api,
        "get_candidate_memo",
        lambda _db, _candidate_id, **_kw: {
            "candidate_id": "c1",
            "search_id": "search-1",
            "brand_profile": {"brand_name": "Brand X", "category": "burger", "service_model": "qsr"},
            "candidate": {
                "parcel_id": "p1",
                "district": "Olaya",
                "area_m2": 180,
                "landuse_label": "Commercial",
                "final_score": 81,
                "economics_score": 72,
                "demand_score": 79,
                "whitespace_score": 68,
                "fit_score": 76,
                "confidence_score": 84,
                "confidence_grade": "A",
                "gate_status": {"overall_pass": True},
                "gate_reasons": {"passed": ["zoning_fit_pass"], "failed": [], "unknown": [], "thresholds": {}, "explanations": {}},
                "feature_snapshot": {"parcel_area_m2": 180, "touches_road": True, "context_sources": {}, "missing_context": [], "data_completeness_score": 90},
                "score_breakdown_json": {"weights": {}, "inputs": {}, "weighted_components": {}, "final_score": 81},
                "demand_thesis": "Demand is strong",
                "cost_thesis": "Costs are manageable",
                "top_positives_json": ["Demand potential is strong for this district."],
                "top_risks_json": ["Delivery competition intensity is high."],
                "comparable_competitors": [{"id": "r1", "name": "Comp"}],
                "cannibalization_score": 33,
                "distance_to_nearest_branch_m": 2600,
                "estimated_rent_sar_m2_year": 980,
                "estimated_annual_rent_sar": 176400,
                "estimated_fitout_cost_sar": 468000,
                "estimated_revenue_index": 75,
                "key_strengths": ["Strong demand"],
                "key_risks": ["Competition"],
                "decision_summary": "summary",
            },
            "recommendation": {
                "headline": "GO",
                "verdict": "go",
                "best_use_case": "neighborhood qsr",
                "main_watchout": "Competition",
                "gate_verdict": "pass",
            },
            "market_research": {
                "delivery_market_summary": "x",
                "competitive_context": "y",
                "district_fit_summary": "z",
            },
        },
    )

    client = _client_with_db(db)
    try:
        response = client.get("/v1/expansion-advisor/candidates/c1/memo")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    body = response.json()
    assert body["candidate_id"] == "c1"
    assert body["recommendation"]["verdict"] == "go"
    assert body["recommendation"]["gate_verdict"] == "pass"
    assert body["candidate"]["comparable_competitors"][0]["id"] == "r1"
    assert body["candidate"]["feature_snapshot"]["touches_road"] is True
    assert "score_breakdown_json" in body["candidate"]


def test_candidate_memo_endpoint_404(monkeypatch):
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(expansion_api, "get_candidate_memo", lambda _db, _candidate_id, **_kw: None)

    client = _client_with_db(db)
    try:
        response = client.get("/v1/expansion-advisor/candidates/missing/memo")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 404


def test_report_endpoint_happy_path(monkeypatch):
    db = DummyDB()
    from app.api import expansion_advisor as expansion_api
    monkeypatch.setattr(expansion_api, "get_recommendation_report", lambda _db, _search_id, **_kw: {"search_id": "search-1", "meta": {"version": "expansion_advisor_v7"}, "recommendation": {"best_candidate_id": "c1", "runner_up_candidate_id": "c2", "best_pass_candidate_id": "c1", "best_confidence_candidate_id": "c2", "why_best": "", "main_risk": "", "best_format": "", "summary": "", "report_summary": ""}, "assumptions": {}, "top_candidates": [{"id": "c1", "final_score": 91, "rank_position": 1, "confidence_grade": "A", "gate_verdict": "pass", "top_positives_json": [], "top_risks_json": [], "feature_snapshot_json": {}, "score_breakdown_json": {"weights": {}, "inputs": {}, "weighted_components": {}, "final_score": 91}}]})
    client = _client_with_db(db)
    try:
        response = client.get("/v1/expansion-advisor/searches/search-1/report")
    finally:
        app.dependency_overrides.pop(get_db, None)
    assert response.status_code == 200
    assert response.json()["recommendation"]["best_candidate_id"] == "c1"
    assert response.json()["recommendation"]["best_pass_candidate_id"] == "c1"
    assert response.json()["recommendation"]["best_confidence_candidate_id"] == "c2"
    assert response.json()["meta"]["version"] == "expansion_advisor_v7"


def test_report_endpoint_404(monkeypatch):
    db = DummyDB()
    from app.api import expansion_advisor as expansion_api
    monkeypatch.setattr(expansion_api, "get_recommendation_report", lambda _db, _search_id, **_kw: None)
    client = _client_with_db(db)
    try:
        response = client.get("/v1/expansion-advisor/searches/missing/report")
    finally:
        app.dependency_overrides.pop(get_db, None)
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# Regression: full payload matching the exact shape that triggered the 500
# ---------------------------------------------------------------------------

def test_post_expansion_search_full_payload_with_brand_profile(monkeypatch):
    """Regression: the complete payload (brand_profile + existing_branches +
    target_districts + bbox) must return 200 without hitting an unhandled
    exception.
    """
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(expansion_api, "persist_existing_branches", lambda _db, _search_id, _branches: None)
    monkeypatch.setattr(expansion_api, "persist_brand_profile", lambda _db, _search_id, _profile: None)
    monkeypatch.setattr(
        expansion_api,
        "run_expansion_search",
        lambda **kwargs: [
            {
                "id": "c-regr-1",
                "search_id": kwargs["search_id"],
                "parcel_id": "parcel-regr",
                "district": "حي العليا",
                "lat": 24.7,
                "lon": 46.7,
                "cannibalization_score": 42.0,
                "distance_to_nearest_branch_m": 1800.0,
                "compare_rank": 1,
                "final_score": 78.3,
                "explanation": {"summary": "regression candidate", "positives": [], "risks": [], "inputs": {}},
            }
        ],
    )

    client = _client_with_db(db)
    try:
        payload = {
            "brand_name": "Test Burger",
            "category": "burger",
            "service_model": "qsr",
            "min_area_m2": 100,
            "max_area_m2": 350,
            "existing_branches": [
                {"name": "HQ", "lat": 24.71, "lon": 46.68, "district": "Olaya"},
                {"name": "Branch 2", "lat": 24.75, "lon": 46.72, "district": "Malqa"},
            ],
            "target_districts": ["Olaya", "Al Mohammadiyah"],
            "bbox": {"min_lon": 46.5, "min_lat": 24.5, "max_lon": 46.9, "max_lat": 24.9},
            "limit": 20,
            "brand_profile": {
                "price_tier": "premium",
                "primary_channel": "delivery",
                "expansion_goal": "delivery_led",
                "preferred_districts": ["Olaya"],
                "excluded_districts": ["Malqa"],
            },
        }
        response = client.post("/v1/expansion-advisor/searches", json=payload)
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    body = response.json()
    assert body["search_id"] is not None
    assert len(body["items"]) == 1
    assert body["items"][0]["district"] == "حي العليا"
    assert body["brand_profile"]["existing_branches"][0]["name"] == "HQ"
    assert body["brand_profile"]["target_districts"] == ["Olaya", "Al Mohammadiyah"]
    assert db.committed is True


def test_post_expansion_search_logs_on_failure(monkeypatch, caplog):
    """Verify the endpoint logs exception details when scoring fails."""
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(expansion_api, "persist_existing_branches", lambda *_a, **_kw: None)

    def _boom(**_kwargs):
        raise RuntimeError("simulated DB failure")

    monkeypatch.setattr(expansion_api, "run_expansion_search", _boom)

    import logging
    with caplog.at_level(logging.ERROR, logger="app.api.expansion_advisor"):
        client = _client_with_db(db)
        try:
            payload = {
                "brand_name": "Test Brand",
                "category": "coffee",
                "service_model": "dine_in",
                "min_area_m2": 80,
                "max_area_m2": 200,
            }
            response = client.post("/v1/expansion-advisor/searches", json=payload)
        finally:
            app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 500
    assert db.rolled_back is True
    assert any("Expansion search failed" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Regression: empty existing_branches payload (production 500 trigger)
# ---------------------------------------------------------------------------


def test_post_expansion_search_empty_existing_branches(monkeypatch):
    """Regression: empty existing_branches list must not crash the endpoint."""
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(expansion_api, "persist_existing_branches", lambda _db, _sid, _b: None)
    monkeypatch.setattr(expansion_api, "persist_brand_profile", lambda _db, _sid, _p: None)
    monkeypatch.setattr(
        expansion_api,
        "run_expansion_search",
        lambda **kwargs: [
            {
                "id": "c-empty-br",
                "search_id": kwargs["search_id"],
                "parcel_id": "parcel-e1",
                "district": "Al Olaya",
                "lat": 24.7,
                "lon": 46.7,
                "cannibalization_score": 25.0,
                "distance_to_nearest_branch_m": None,
                "compare_rank": 1,
                "final_score": 72.0,
                "explanation": {"summary": "ok", "positives": [], "risks": [], "inputs": {}},
            }
        ],
    )

    client = _client_with_db(db)
    try:
        payload = {
            "brand_name": "Test",
            "category": "Burger",
            "service_model": "qsr",
            "min_area_m2": 100,
            "max_area_m2": 500,
            "target_area_m2": 200,
            "target_districts": ["Al Olaya", "Al Malqa", "Al Nakheel"],
            "existing_branches": [],
            "brand_profile": {
                "preferred_districts": ["Alolaya"],
            },
        }
        response = client.post("/v1/expansion-advisor/searches", json=payload)
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    body = response.json()
    assert body["brand_profile"]["existing_branches"] == []
    assert body["items"][0]["distance_to_nearest_branch_m"] is None
    assert body["items"][0]["cannibalization_score"] == 25.0
    assert db.committed is True


def test_post_expansion_search_unmatched_preferred_districts(monkeypatch):
    """Regression: misspelled preferred_districts must not crash."""
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(expansion_api, "persist_existing_branches", lambda _db, _sid, _b: None)
    monkeypatch.setattr(expansion_api, "persist_brand_profile", lambda _db, _sid, _p: None)
    monkeypatch.setattr(
        expansion_api,
        "run_expansion_search",
        lambda **kwargs: [],
    )

    client = _client_with_db(db)
    try:
        payload = {
            "brand_name": "Test",
            "category": "Burger",
            "service_model": "qsr",
            "min_area_m2": 100,
            "max_area_m2": 500,
            "existing_branches": [],
            "brand_profile": {
                "preferred_districts": ["Alolaya", "NonexistentDistrict"],
            },
        }
        response = client.post("/v1/expansion-advisor/searches", json=payload)
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    body = response.json()
    assert body["items"] == []
    assert db.committed is True


def test_post_expansion_search_failure_returns_clean_500(monkeypatch):
    """When run_expansion_search raises, the API returns a structured 500 with search_id."""
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(expansion_api, "persist_existing_branches", lambda *_a, **_kw: None)

    def _boom(**_kwargs):
        raise RuntimeError("simulated scoring crash")

    monkeypatch.setattr(expansion_api, "run_expansion_search", _boom)

    client = _client_with_db(db)
    try:
        payload = {
            "brand_name": "Test",
            "category": "Burger",
            "service_model": "qsr",
            "min_area_m2": 100,
            "max_area_m2": 500,
            "existing_branches": [],
        }
        response = client.post("/v1/expansion-advisor/searches", json=payload)
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 500
    body = response.json()
    assert "search_id" in body["detail"]
    assert db.rolled_back is True


def test_post_expansion_search_with_one_branch(monkeypatch):
    """Valid payload with exactly one existing branch must succeed."""
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(expansion_api, "persist_existing_branches", lambda _db, _sid, _b: None)
    monkeypatch.setattr(expansion_api, "persist_brand_profile", lambda _db, _sid, _p: None)
    monkeypatch.setattr(
        expansion_api,
        "run_expansion_search",
        lambda **kwargs: [
            {
                "id": "c-1br",
                "search_id": kwargs["search_id"],
                "parcel_id": "parcel-1br",
                "district": "Olaya",
                "lat": 24.7,
                "lon": 46.7,
                "cannibalization_score": 55.0,
                "distance_to_nearest_branch_m": 1400.0,
                "compare_rank": 1,
                "final_score": 80.0,
                "explanation": {"summary": "ok", "positives": [], "risks": [], "inputs": {}},
            }
        ],
    )

    client = _client_with_db(db)
    try:
        payload = {
            "brand_name": "Test",
            "category": "Burger",
            "service_model": "qsr",
            "min_area_m2": 100,
            "max_area_m2": 500,
            "existing_branches": [
                {"name": "Main Branch", "lat": 24.71, "lon": 46.68, "district": "Olaya"}
            ],
            "brand_profile": {
                "preferred_districts": ["Olaya"],
            },
        }
        response = client.post("/v1/expansion-advisor/searches", json=payload)
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    body = response.json()
    assert len(body["brand_profile"]["existing_branches"]) == 1
    assert body["items"][0]["distance_to_nearest_branch_m"] == 1400.0
    assert db.committed is True


# ---------------------------------------------------------------------------
# Phase 4 — feature_snapshot_json.listing_age and .district_momentum must
# survive the Pydantic response layer and reach the frontend. The response
# model CandidateFeatureSnapshotResponse extends FlexibleResponseModel
# (extra="allow"), so extra keys should pass through — this test pins it.
# ---------------------------------------------------------------------------


def test_feature_snapshot_surfaces_listing_age_and_district_momentum(monkeypatch):
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(expansion_api, "get_search", lambda _db, _search_id, **_kw: {"id": "search-1"})
    monkeypatch.setattr(
        expansion_api,
        "get_candidates",
        lambda _db, _search_id, **_kw: [
            {
                "id": "candidate-1",
                "search_id": "search-1",
                "parcel_id": "parcel-123",
                "district": "Olaya",
                "cannibalization_score": 40.0,
                "distance_to_nearest_branch_m": 3200.0,
                "compare_rank": 1,
                "rank_position": 1,
                "estimated_rent_sar_m2_year": 980.0,
                "estimated_annual_rent_sar": 176400.0,
                "estimated_fitout_cost_sar": 468000.0,
                "estimated_revenue_index": 73.0,
                "economics_score": 69.0,
                "decision_summary": "summary",
                "key_risks_json": [],
                "key_strengths_json": [],
                "confidence_grade": "A",
                "gate_status_json": {"overall_pass": True},
                "gate_reasons_json": {"passed": [], "failed": [], "unknown": [], "thresholds": {}, "explanations": {}},
                "feature_snapshot_json": {
                    "parcel_area_m2": 180,
                    "context_sources": {},
                    "missing_context": [],
                    "data_completeness_score": 90,
                    # Phase 4 keys must round-trip unchanged.
                    "listing_age": {
                        "effective_age_days": 3,
                        "source": "aqar_created",
                        "created_days": 3,
                        "updated_days": 3,
                    },
                    "district_momentum": {
                        "momentum_score": 82.5,
                        "activity_30d": 120,
                        "active_in_district": 340,
                        "percentile_raw": 0.9,
                        "percentile_absolute": 0.75,
                        "percentile_composite": 0.825,
                        "district_label": "العليا",
                        "sample_floor_applied": False,
                    },
                },
                "score_breakdown_json": {"weights": {}, "inputs": {}, "weighted_components": {}, "final_score": 88.1},
                "demand_thesis": "",
                "cost_thesis": "",
                "top_positives_json": [],
                "top_risks_json": [],
                "comparable_competitors_json": [],
                "final_score": 88.1,
                "explanation": {},
            }
        ],
    )

    client = _client_with_db(db)
    try:
        response = client.get("/v1/expansion-advisor/searches/search-1/candidates")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    body = response.json()
    snap = body["items"][0]["feature_snapshot_json"]
    assert "listing_age" in snap
    assert snap["listing_age"]["effective_age_days"] == 3
    assert snap["listing_age"]["source"] == "aqar_created"
    # Phase 4.1: created_days and updated_days round-trip through
    # FlexibleResponseModel(extra="allow") alongside the legacy fields.
    assert snap["listing_age"]["created_days"] == 3
    assert snap["listing_age"]["updated_days"] == 3
    assert "district_momentum" in snap
    assert snap["district_momentum"]["momentum_score"] == 82.5
    assert snap["district_momentum"]["sample_floor_applied"] is False
    # The original context_sources / data_completeness_score contract stays.
    assert snap["data_completeness_score"] == 90


# ---------------------------------------------------------------------------
# P0 hotfix for PR #1178 — score_breakdown_json carries a JSONB blob whose
# shape is intentionally loose. _record_value_pass_marker writes a
# ``value_pass`` key into it; production was 500'ing because the response
# model rejected the unknown key. The fix is extra="allow" via
# FlexibleResponseModel. These tests pin the contract so a future tightening
# of the model is caught at test time, not in production.
# ---------------------------------------------------------------------------
def test_score_breakdown_response_allows_value_pass_and_unknown_keys():
    from app.api.expansion_advisor import (
        CandidateScoreBreakdownResponse,
        ExpansionCandidateResponse,
    )

    breakdown = CandidateScoreBreakdownResponse.model_validate(
        {
            "weights": {"demand": 0.4},
            "inputs": {"demand": 70},
            "weighted_components": {"demand": 28.0},
            "display": {},
            "final_score": 82.5,
            "value_pass": {"value_uprank_applied": True, "value_uprank_delta": 3},
            "some_future_key": {"nested": [1, 2, 3]},
        }
    )

    dumped = breakdown.model_dump()
    assert dumped["value_pass"] == {
        "value_uprank_applied": True,
        "value_uprank_delta": 3,
    }
    assert dumped["some_future_key"] == {"nested": [1, 2, 3]}

    # And the same blob round-trips through the candidate-level response
    # model (which is what /searches actually serializes).
    candidate = ExpansionCandidateResponse.model_validate(
        {
            "id": "cand-12",
            "rank_position": 12,
            "score_breakdown_json": {
                "final_score": 73.4,
                "value_pass": {
                    "value_uprank_applied": True,
                    "value_uprank_delta": 3,
                },
            },
            "value_uprank_applied": True,
            "value_uprank_delta": 3,
        }
    )
    cand_dump = candidate.model_dump()
    assert cand_dump["score_breakdown_json"]["value_pass"]["value_uprank_delta"] == 3


# ---------------------------------------------------------------------------
# Soft-demote leg diagnostics: surface the `demote_legs` block written by
# `_apply_market_viability_pass` through the API meta. Mirrors the existing
# `hard_floor_*` flat-key surfacing pattern.
# ---------------------------------------------------------------------------


def _post_search_with_demote_legs_notes(monkeypatch, demote_legs_block):
    """Helper: stub `run_expansion_search` to return a dict whose
    ``notes.viability.demote_legs`` mirrors the service-layer diagnostics
    contract, then issue a single POST and return the parsed body."""
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(
        expansion_api, "persist_existing_branches", lambda *_a, **_kw: None
    )
    monkeypatch.setattr(
        expansion_api, "persist_brand_profile", lambda *_a, **_kw: None
    )
    monkeypatch.setattr(
        expansion_api,
        "run_expansion_search",
        lambda **kwargs: {
            "items": [
                {
                    "id": "candidate-1",
                    "search_id": kwargs["search_id"],
                    "parcel_id": "parcel-123",
                    "district": "Olaya",
                    "lat": 24.7,
                    "lon": 46.7,
                    "cannibalization_score": 55.0,
                    "distance_to_nearest_branch_m": 1400.0,
                    "compare_rank": 1,
                    "final_score": 86.6,
                    "explanation": {
                        "summary": "ok",
                        "positives": [],
                        "risks": [],
                        "inputs": {},
                    },
                }
            ],
            "notes": {
                "viability": {"demote_legs": demote_legs_block},
            },
        },
    )

    client = _client_with_db(db)
    try:
        payload = {
            "brand_name": "Brand X",
            "category": "burger",
            "service_model": "qsr",
            "min_area_m2": 100,
            "max_area_m2": 350,
        }
        response = client.post("/v1/expansion-advisor/searches", json=payload)
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    return response.json()


def _full_demote_legs_block():
    """Realistic demote_legs diagnostics shape, matching the contract verified
    in tests/test_expansion_advisor_service.py::
    test_viability_diagnostics_demote_legs_block_written.
    """
    return {
        "drops": {
            "dropped_population": 2,
            "dropped_rent": 1,
            "dropped_economics": 0,
            "dropped_demand": 3,
            "dropped_radiance_growth": 1,
            "dropped_rent_per_capita": 2,
        },
        "thresholds": {
            "rent_pct_threshold": 0.85,
            "pop_percentile": 0.25,
            "pop_threshold": 5000.0,
            "economics_min": 35.0,
            "demand_percentile": 0.25,
            "demand_threshold": None,
            "demand_min_branches": 5,
            "radiance_yoy_demote_threshold": 2.0,
            "rpc_percentile": 0.75,
            "rpc_threshold": 1500.0,
            "rpc_min_cohort": 10,
            "rpc_cohort_n": 12,
        },
        "leg_enabled": {
            "demand": True,
            "radiance_growth": True,
            "rent_per_capita": True,
        },
    }


def test_meta_includes_demote_leg_drops(monkeypatch):
    body = _post_search_with_demote_legs_notes(
        monkeypatch, _full_demote_legs_block()
    )
    drops = body["meta"]["demote_leg_drops"]
    assert set(drops.keys()) == {
        "dropped_population",
        "dropped_rent",
        "dropped_economics",
        "dropped_demand",
        "dropped_radiance_growth",
        "dropped_rent_per_capita",
    }
    for value in drops.values():
        assert isinstance(value, int)
        assert value >= 0


def test_meta_includes_demote_leg_thresholds(monkeypatch):
    body = _post_search_with_demote_legs_notes(
        monkeypatch, _full_demote_legs_block()
    )
    thresholds = body["meta"]["demote_leg_thresholds"]
    assert set(thresholds.keys()) == {
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
    assert isinstance(thresholds["rent_pct_threshold"], float)
    assert isinstance(thresholds["pop_percentile"], float)
    assert isinstance(thresholds["pop_threshold"], float)
    assert isinstance(thresholds["economics_min"], float)
    assert isinstance(thresholds["demand_percentile"], float)
    assert thresholds["demand_threshold"] is None
    assert isinstance(thresholds["demand_min_branches"], int)
    assert isinstance(thresholds["radiance_yoy_demote_threshold"], float)
    assert isinstance(thresholds["rpc_percentile"], float)
    assert isinstance(thresholds["rpc_threshold"], float)
    assert isinstance(thresholds["rpc_min_cohort"], int)
    assert isinstance(thresholds["rpc_cohort_n"], int)


def test_meta_includes_demote_leg_enabled(monkeypatch):
    body = _post_search_with_demote_legs_notes(
        monkeypatch, _full_demote_legs_block()
    )
    leg_enabled = body["meta"]["demote_leg_enabled"]
    assert "demand" in leg_enabled
    assert "radiance_growth" in leg_enabled
    assert "rent_per_capita" in leg_enabled
    assert isinstance(leg_enabled["demand"], bool)
    assert isinstance(leg_enabled["radiance_growth"], bool)
    assert isinstance(leg_enabled["rent_per_capita"], bool)


def test_meta_demote_leg_fields_default_none_when_block_absent(monkeypatch):
    """When the viability pass writes no demote_legs diagnostics (e.g. an
    empty cohort), the three new meta fields must be absent / None — matching
    the `hard_floor_drops` precedent so the response is unchanged for
    searches where no soft-demote leg fired."""
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(
        expansion_api, "persist_existing_branches", lambda *_a, **_kw: None
    )
    monkeypatch.setattr(
        expansion_api, "persist_brand_profile", lambda *_a, **_kw: None
    )
    monkeypatch.setattr(
        expansion_api,
        "run_expansion_search",
        lambda **kwargs: {"items": [], "notes": {}},
    )

    client = _client_with_db(db)
    try:
        payload = {
            "brand_name": "Brand X",
            "category": "burger",
            "service_model": "qsr",
            "min_area_m2": 100,
            "max_area_m2": 350,
        }
        response = client.post("/v1/expansion-advisor/searches", json=payload)
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    meta = response.json()["meta"]
    assert meta["demote_leg_drops"] is None
    assert meta["demote_leg_thresholds"] is None
    assert meta["demote_leg_enabled"] is None


# ---------------------------------------------------------------------------
# PR #1: `lang` parameter threading — plumbing only.
#
# Each updated endpoint must accept `lang` (body field for POST/PATCH, query
# param for GET), default to "en", coerce any invalid value to "en" (always
# 200, never 422), and produce output identical to omitting `lang` — because
# this PR wires the parameter but does not consume it yet.
# ---------------------------------------------------------------------------


def _setup_searches_mocks(monkeypatch):
    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(expansion_api, "persist_existing_branches", lambda *_a, **_k: None)
    monkeypatch.setattr(expansion_api, "persist_brand_profile", lambda *_a, **_k: None)
    # Constant candidate payload so `lang` is the only variable under test:
    # the real run_expansion_search stamps the fresh per-call search_id onto
    # each candidate, which would otherwise mask the comparison.
    monkeypatch.setattr(
        expansion_api,
        "run_expansion_search",
        lambda **kwargs: [
            {
                "id": "candidate-1",
                "search_id": "search-fixed",
                "parcel_id": "parcel-123",
                "district": "Olaya",
                "final_score": 80.0,
            }
        ],
    )


def _post_search(client, **body_overrides):
    payload = {"brand_name": "Brand X", "category": "burger"}
    payload.update(body_overrides)
    return client.post("/v1/expansion-advisor/searches", json=payload)


def test_post_searches_lang_accepted_defaulted_and_coerced(monkeypatch):
    """POST /searches accepts lang en/ar, defaults to en when omitted, and
    coerces invalid values to en (200, not 422). Response is identical to
    omitting lang (search_id masked — it is a fresh UUID per call)."""
    _setup_searches_mocks(monkeypatch)
    client = _client_with_db(DummyDB())
    try:
        omitted = _post_search(client)
        en = _post_search(client, lang="en")
        ar = _post_search(client, lang="ar")
        invalid = _post_search(client, lang="fr")
    finally:
        app.dependency_overrides.pop(get_db, None)

    for resp in (omitted, en, ar, invalid):
        assert resp.status_code == 200

    def _masked(resp):
        body = resp.json()
        body.pop("search_id", None)
        return body

    base = _masked(omitted)
    assert _masked(en) == base
    assert _masked(ar) == base
    assert _masked(invalid) == base


def test_post_searches_lang_not_persisted_in_request_json(monkeypatch):
    """R1: the persisted expansion_search.request_json blob must NOT gain a
    `lang` key — otherwise rows written before/after this PR drift."""
    _setup_searches_mocks(monkeypatch)
    db = DummyDB()
    client = _client_with_db(db)
    try:
        response = _post_search(client, lang="ar")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    insert_rows = [
        params for sql, params in db.executed
        if "INSERT INTO expansion_search" in sql
    ]
    assert insert_rows, "expected an INSERT INTO expansion_search"
    request_json = insert_rows[0]["request_json"]
    assert "lang" not in request_json
    assert '"lang"' not in request_json


def test_get_search_detail_accepts_lang(monkeypatch):
    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(
        expansion_api,
        "get_search",
        lambda _db, _search_id, **_kw: {
            "id": "search-1",
            "created_at": "2026-01-01T00:00:00Z",
            "brand_name": "Brand X",
            "category": "burger",
            "service_model": "qsr",
            "target_districts": [],
            "min_area_m2": 100,
            "max_area_m2": 300,
            "target_area_m2": 180,
            "bbox": None,
            "request_json": {},
            "notes": {"version": "expansion_advisor_v7"},
            "existing_branches": [],
            "brand_profile": None,
            "meta": {"version": "expansion_advisor_v7"},
        },
    )
    client = _client_with_db(DummyDB())
    try:
        omitted = client.get("/v1/expansion-advisor/searches/search-1")
        en = client.get("/v1/expansion-advisor/searches/search-1?lang=en")
        ar = client.get("/v1/expansion-advisor/searches/search-1?lang=ar")
        invalid = client.get("/v1/expansion-advisor/searches/search-1?lang=fr")
    finally:
        app.dependency_overrides.pop(get_db, None)

    for resp in (omitted, en, ar, invalid):
        assert resp.status_code == 200
        assert resp.json() == omitted.json()


def test_get_search_candidates_accepts_lang(monkeypatch):
    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(expansion_api, "get_search", lambda _db, _search_id, **_kw: {"id": "search-1"})
    monkeypatch.setattr(
        expansion_api,
        "get_candidates",
        lambda _db, _search_id, **_kw: [
            {"id": "candidate-1", "search_id": "search-1", "district": "Olaya"}
        ],
    )
    client = _client_with_db(DummyDB())
    try:
        omitted = client.get("/v1/expansion-advisor/searches/search-1/candidates")
        en = client.get("/v1/expansion-advisor/searches/search-1/candidates?lang=en")
        ar = client.get("/v1/expansion-advisor/searches/search-1/candidates?lang=ar")
        invalid = client.get("/v1/expansion-advisor/searches/search-1/candidates?lang=EN")
    finally:
        app.dependency_overrides.pop(get_db, None)

    for resp in (omitted, en, ar, invalid):
        assert resp.status_code == 200
        assert resp.json() == omitted.json()


def test_get_search_report_accepts_lang(monkeypatch):
    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(
        expansion_api,
        "get_recommendation_report",
        lambda _db, _search_id, **_kw: {
            "search_id": "search-1",
            "meta": {"version": "expansion_advisor_v7"},
            "recommendation": {
                "best_candidate_id": "c1",
                "why_best": "",
                "main_risk": "",
                "best_format": "",
                "summary": "",
                "report_summary": "",
            },
            "assumptions": {},
            "top_candidates": [],
        },
    )
    client = _client_with_db(DummyDB())
    try:
        omitted = client.get("/v1/expansion-advisor/searches/search-1/report")
        en = client.get("/v1/expansion-advisor/searches/search-1/report?lang=en")
        ar = client.get("/v1/expansion-advisor/searches/search-1/report?lang=ar")
        invalid = client.get("/v1/expansion-advisor/searches/search-1/report?lang=")
    finally:
        app.dependency_overrides.pop(get_db, None)

    for resp in (omitted, en, ar, invalid):
        assert resp.status_code == 200
        assert resp.json() == omitted.json()


def test_compare_endpoint_accepts_lang(monkeypatch):
    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(
        expansion_api,
        "compare_candidates",
        lambda _db, _search_id, _candidate_ids, **_kw: {
            "items": [{"candidate_id": "c1"}, {"candidate_id": "c2"}],
            "summary": {"best_overall_candidate_id": "c1"},
        },
    )
    client = _client_with_db(DummyDB())

    def _compare(**extra):
        body = {"search_id": "search-1", "candidate_ids": ["c1", "c2"]}
        body.update(extra)
        return client.post("/v1/expansion-advisor/candidates/compare", json=body)

    try:
        omitted = _compare()
        en = _compare(lang="en")
        ar = _compare(lang="ar")
        invalid = _compare(lang="fr")
    finally:
        app.dependency_overrides.pop(get_db, None)

    for resp in (omitted, en, ar, invalid):
        assert resp.status_code == 200
        assert resp.json() == omitted.json()


def test_candidate_memo_endpoint_accepts_lang(monkeypatch):
    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(
        expansion_api,
        "get_candidate_memo",
        lambda _db, _candidate_id, **_kw: {
            "candidate_id": "c1",
            "search_id": "search-1",
            "brand_profile": {},
            "candidate": {},
            "recommendation": {},
            "market_research": {},
        },
    )
    client = _client_with_db(DummyDB())
    try:
        omitted = client.get("/v1/expansion-advisor/candidates/c1/memo")
        en = client.get("/v1/expansion-advisor/candidates/c1/memo?lang=en")
        ar = client.get("/v1/expansion-advisor/candidates/c1/memo?lang=ar")
        invalid = client.get("/v1/expansion-advisor/candidates/c1/memo?lang=fr")
    finally:
        app.dependency_overrides.pop(get_db, None)

    for resp in (omitted, en, ar, invalid):
        assert resp.status_code == 200
        assert resp.json() == omitted.json()
