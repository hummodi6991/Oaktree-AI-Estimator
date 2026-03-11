from fastapi.testclient import TestClient

from app.db.deps import get_db
from app.main import app


class DummyDB:
    def __init__(self):
        self.committed = False
        self.rolled_back = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


def _client(db: DummyDB) -> TestClient:
    def override_get_db():
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    return TestClient(app, raise_server_exceptions=False)


def test_saved_search_crud_endpoints(monkeypatch):
    db = DummyDB()
    from app.api import expansion_advisor as api

    monkeypatch.setattr(api, "get_search", lambda *_: {"id": "search-1"})
    monkeypatch.setattr(
        api,
        "create_saved_search",
        lambda *_args, **_kwargs: {"id": "saved-1", "search_id": "search-1", "title": "Study A", "status": "draft"},
    )
    monkeypatch.setattr(
        api,
        "list_saved_searches",
        lambda *_args, **_kwargs: [{"id": "saved-1", "title": "Study A", "status": "draft"}],
    )
    monkeypatch.setattr(
        api,
        "get_saved_search",
        lambda *_args, **_kwargs: {"id": "saved-1", "search_id": "search-1", "search": {"id": "search-1", "target_districts": [], "existing_branches": [], "meta": {"version": "expansion_advisor_v6.1", "excluded_sources": []}}, "candidates": [{"id": "c1"}]},
    )
    monkeypatch.setattr(api, "update_saved_search", lambda *_args, **_kwargs: {"id": "saved-1", "title": "Renamed"})
    monkeypatch.setattr(api, "delete_saved_search", lambda *_args, **_kwargs: True)

    client = _client(db)
    try:
      created = client.post("/v1/expansion-advisor/saved-searches", json={"search_id": "search-1", "title": "Study A", "status": "draft"})
      listed = client.get("/v1/expansion-advisor/saved-searches")
      fetched = client.get("/v1/expansion-advisor/saved-searches/saved-1")
      patched = client.patch("/v1/expansion-advisor/saved-searches/saved-1", json={"title": "Renamed"})
      deleted = client.delete("/v1/expansion-advisor/saved-searches/saved-1")
    finally:
      app.dependency_overrides.pop(get_db, None)

    assert created.status_code == 200
    assert listed.json()["items"][0]["id"] == "saved-1"
    assert fetched.json()["candidates"][0]["id"] == "c1"
    assert patched.json()["title"] == "Renamed"
    assert deleted.json()["deleted"] is True


def test_saved_search_404_paths(monkeypatch):
    db = DummyDB()
    from app.api import expansion_advisor as api

    monkeypatch.setattr(api, "get_search", lambda *_: None)
    monkeypatch.setattr(api, "get_saved_search", lambda *_: None)
    monkeypatch.setattr(api, "update_saved_search", lambda *_: None)
    monkeypatch.setattr(api, "delete_saved_search", lambda *_: False)

    client = _client(db)
    try:
      create_res = client.post("/v1/expansion-advisor/saved-searches", json={"search_id": "missing", "title": "Study A", "status": "draft"})
      get_res = client.get("/v1/expansion-advisor/saved-searches/missing")
      patch_res = client.patch("/v1/expansion-advisor/saved-searches/missing", json={"title": "Renamed"})
      delete_res = client.delete("/v1/expansion-advisor/saved-searches/missing")
    finally:
      app.dependency_overrides.pop(get_db, None)

    assert create_res.status_code == 404
    assert get_res.status_code == 404
    assert patch_res.status_code == 404
    assert delete_res.status_code == 404
