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
    assert body["meta"]["version"] == "expansion_advisor_v6.1"
    assert body["items"][0]["score_breakdown_json"]["weights"] == {}
    assert db.committed is True




def test_get_expansion_search_detail_includes_versioned_meta(monkeypatch):
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(
        expansion_api,
        "get_search",
        lambda _db, _search_id: {
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
            "notes": {"version": "expansion_advisor_v6.1"},
            "existing_branches": [],
            "brand_profile": None,
            "meta": {"version": "expansion_advisor_v6.1", "parcel_source": "arcgis_only", "excluded_sources": ["suhail", "inferred_parcels"]},
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
    assert body["meta"]["version"] == "expansion_advisor_v6.1"

def test_get_expansion_search_candidates_shape(monkeypatch):
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(expansion_api, "get_search", lambda _db, _search_id: {"id": "search-1"})
    monkeypatch.setattr(
        expansion_api,
        "get_candidates",
        lambda _db, _search_id: [
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
                "estimated_payback_months": 24.0,
                "payback_band": "promising",
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
    assert body["items"][0]["payback_band"] == "promising"
    assert body["items"][0]["confidence_grade"] == "A"
    assert body["items"][0]["gate_status_json"]["overall_pass"] is True
    assert body["items"][0]["rank_position"] == 1
    assert "score_breakdown_json" in body["items"][0]
    assert "top_positives_json" in body["items"][0]
    assert body["meta"]["version"] == "expansion_advisor_v6.1"


def test_compare_endpoint_happy_path(monkeypatch):
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(
        expansion_api,
        "compare_candidates",
        lambda _db, _search_id, _candidate_ids: {
            "items": [
                {"candidate_id": "c1", "economics_score": 70.0, "estimated_payback_months": 20.0, "zoning_fit_score": 82, "frontage_score": 65, "access_score": 66, "parking_score": 61, "access_visibility_score": 64},
                {"candidate_id": "c2", "economics_score": 62.0, "estimated_payback_months": 28.0},
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
                "fastest_payback_candidate_id": "c1",
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
        "fastest_payback_candidate_id",
        "most_confident_candidate_id",
        "best_gate_pass_candidate_id",
    }


def test_compare_endpoint_rejects_foreign_candidate_ids(monkeypatch):
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    def _raise_not_found(_db, _search_id, _candidate_ids):
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
        lambda _db, _candidate_id: {
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
                "estimated_payback_months": 21,
                "payback_band": "promising",
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

    monkeypatch.setattr(expansion_api, "get_candidate_memo", lambda _db, _candidate_id: None)

    client = _client_with_db(db)
    try:
        response = client.get("/v1/expansion-advisor/candidates/missing/memo")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 404


def test_report_endpoint_happy_path(monkeypatch):
    db = DummyDB()
    from app.api import expansion_advisor as expansion_api
    monkeypatch.setattr(expansion_api, "get_recommendation_report", lambda _db, _search_id: {"search_id": "search-1", "meta": {"version": "expansion_advisor_v6.1"}, "recommendation": {"best_candidate_id": "c1", "runner_up_candidate_id": "c2", "best_pass_candidate_id": "c1", "best_confidence_candidate_id": "c2", "why_best": "", "main_risk": "", "best_format": "", "summary": "", "report_summary": ""}, "assumptions": {}, "top_candidates": [{"id": "c1", "final_score": 91, "rank_position": 1, "confidence_grade": "A", "gate_verdict": "pass", "top_positives_json": [], "top_risks_json": [], "feature_snapshot_json": {}, "score_breakdown_json": {"weights": {}, "inputs": {}, "weighted_components": {}, "final_score": 91}}]})
    client = _client_with_db(db)
    try:
        response = client.get("/v1/expansion-advisor/searches/search-1/report")
    finally:
        app.dependency_overrides.pop(get_db, None)
    assert response.status_code == 200
    assert response.json()["recommendation"]["best_candidate_id"] == "c1"
    assert response.json()["recommendation"]["best_pass_candidate_id"] == "c1"
    assert response.json()["recommendation"]["best_confidence_candidate_id"] == "c2"
    assert response.json()["meta"]["version"] == "expansion_advisor_v6.1"


def test_report_endpoint_404(monkeypatch):
    db = DummyDB()
    from app.api import expansion_advisor as expansion_api
    monkeypatch.setattr(expansion_api, "get_recommendation_report", lambda _db, _search_id: None)
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
