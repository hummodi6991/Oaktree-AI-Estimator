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


def test_post_expansion_search_returns_expected_shape(monkeypatch):
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(
        expansion_api,
        "run_expansion_search",
        lambda **kwargs: [
            {
                "id": "candidate-1",
                "search_id": kwargs["search_id"],
                "parcel_id": "parcel-123",
                "lat": 24.7,
                "lon": 46.7,
                "area_m2": 210.0,
                "landuse_label": "Commercial",
                "landuse_code": "C",
                "population_reach": 15000.0,
                "competitor_count": 2,
                "delivery_listing_count": 12,
                "demand_score": 83.5,
                "whitespace_score": 82.0,
                "fit_score": 89.0,
                "confidence_score": 100.0,
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
            "target_districts": ["Olaya"],
            "bbox": {"min_lon": 46.5, "min_lat": 24.5, "max_lon": 46.9, "max_lat": 24.9},
            "limit": 10,
        }
        response = client.post("/v1/expansion-advisor/searches", json=payload)
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    body = response.json()
    assert "search_id" in body
    assert body["brand_profile"]["brand_name"] == "Brand X"
    assert body["brand_profile"]["target_districts"] == ["Olaya"]
    assert isinstance(body["items"], list)
    assert body["items"][0]["parcel_id"] == "parcel-123"
    assert body["meta"]["parcel_source"] == "arcgis_only"
    assert body["meta"]["excluded_sources"] == ["suhail", "inferred_parcels"]
    assert db.committed is True


def test_get_expansion_search_by_id_shape(monkeypatch):
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

    monkeypatch.setattr(
        expansion_api,
        "get_search",
        lambda _db, search_id: {
            "id": search_id,
            "brand_name": "Brand X",
            "category": "burger",
            "service_model": "qsr",
            "request_json": {"brand_name": "Brand X"},
            "notes": {"version": "expansion_advisor_v0"},
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
    assert body["brand_name"] == "Brand X"
    assert body["request_json"]["brand_name"] == "Brand X"


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
    assert "items" in body
    assert len(body["items"]) == 1
    assert body["items"][0]["parcel_id"] == "parcel-123"
    assert body["items"][0]["explanation"]["summary"] == "candidate explanation"


def test_post_expansion_search_rolls_back_when_scoring_fails(monkeypatch):
    db = DummyDB()

    from app.api import expansion_advisor as expansion_api

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
