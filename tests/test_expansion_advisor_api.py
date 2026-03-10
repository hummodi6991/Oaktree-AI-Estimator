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

    monkeypatch.setattr(
        expansion_api,
        "persist_existing_branches",
        lambda _db, _search_id, _branches: None,
    )
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
        }
        response = client.post("/v1/expansion-advisor/searches", json=payload)
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    body = response.json()
    assert body["brand_profile"]["existing_branches"][0]["name"] == "HQ"
    assert body["items"][0]["district"] == "Olaya"
    assert body["items"][0]["cannibalization_score"] == 55.0
    assert body["meta"]["version"] == "expansion_advisor_v1"
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


def test_compare_endpoint_happy_path(monkeypatch):
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(
        expansion_api,
        "compare_candidates",
        lambda _db, _search_id, _candidate_ids: {
            "items": [{"candidate_id": "c1"}, {"candidate_id": "c2"}],
            "summary": {"best_overall_candidate_id": "c1"},
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
