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
    assert body["meta"]["version"] == "expansion_advisor_v2"
    assert db.committed is True


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


def test_compare_endpoint_happy_path(monkeypatch):
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(
        expansion_api,
        "compare_candidates",
        lambda _db, _search_id, _candidate_ids: {
            "items": [
                {"candidate_id": "c1", "economics_score": 70.0, "estimated_payback_months": 20.0},
                {"candidate_id": "c2", "economics_score": 62.0, "estimated_payback_months": 28.0},
            ],
            "summary": {
                "best_overall_candidate_id": "c1",
                "best_economics_candidate_id": "c1",
                "fastest_payback_candidate_id": "c1",
                "lowest_rent_burden_candidate_id": "c2",
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
    assert body["summary"]["best_economics_candidate_id"] == "c1"


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
    monkeypatch.setattr(expansion_api, "get_recommendation_report", lambda _db, _search_id: {"search_id": "search-1", "recommendation": {"best_candidate_id": "c1"}})
    client = _client_with_db(db)
    try:
        response = client.get("/v1/expansion-advisor/searches/search-1/report")
    finally:
        app.dependency_overrides.pop(get_db, None)
    assert response.status_code == 200
    assert response.json()["recommendation"]["best_candidate_id"] == "c1"


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
