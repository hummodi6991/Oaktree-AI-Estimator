from fastapi.testclient import TestClient
from sqlalchemy.exc import ProgrammingError

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
        lambda *_args, **_kwargs: {
            "id": "saved-1",
            "search_id": "search-1",
            "title": "Study A",
            "status": "draft",
            "selected_candidate_ids": [],
            "filters_json": {},
            "ui_state_json": {},
            "description": None,
            "search": None,
            "candidates": [],
        },
    )
    monkeypatch.setattr(
        api,
        "list_saved_searches",
        lambda *_args, **_kwargs: [{
            "id": "saved-1",
            "search_id": "search-1",
            "title": "Study A",
            "status": "draft",
            "selected_candidate_ids": [],
            "filters_json": {},
            "ui_state_json": {},
            "description": None,
            "search": {
                "id": "search-1",
                "target_districts": [],
                "existing_branches": [],
                "request_json": {},
                "notes": {},
                "meta": {"version": "expansion_advisor_v7", "parcel_source": "listings_only", "excluded_sources": ["arcgis_parcels", "hungerstation_poi", "suhail", "inferred_parcels"]},
            },
            "candidates": [{"id": "c1", "gate_reasons_json": {"passed": [], "failed": [], "unknown": [], "thresholds": {}, "explanations": {}}, "feature_snapshot_json": {"context_sources": {}, "missing_context": [], "data_completeness_score": 0}, "score_breakdown_json": {"weights": {}, "inputs": {}, "weighted_components": {}, "final_score": 0}}],
        }],
    )
    monkeypatch.setattr(
        api,
        "get_saved_search",
        lambda *_args, **_kwargs: {
            "id": "saved-1",
            "search_id": "search-1",
            "title": "Study A",
            "status": "draft",
            "selected_candidate_ids": [],
            "filters_json": {},
            "ui_state_json": {},
            "description": None,
            "search": {"id": "search-1", "target_districts": [], "existing_branches": [], "request_json": {}, "notes": {}, "meta": {"version": "expansion_advisor_v7", "parcel_source": "listings_only", "excluded_sources": ["arcgis_parcels", "hungerstation_poi", "suhail", "inferred_parcels"]}},
            "candidates": [{"id": "c1", "gate_reasons_json": {"passed": [], "failed": [], "unknown": [], "thresholds": {}, "explanations": {}}, "feature_snapshot_json": {"context_sources": {}, "missing_context": [], "data_completeness_score": 0}, "score_breakdown_json": {"weights": {}, "inputs": {}, "weighted_components": {}, "final_score": 0}}],
        },
    )
    monkeypatch.setattr(api, "update_saved_search", lambda *_args, **_kwargs: {"id": "saved-1", "search_id": "search-1", "title": "Renamed", "status": "draft", "selected_candidate_ids": [], "filters_json": {}, "ui_state_json": {}, "description": None, "search": None, "candidates": []})
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
    assert listed.json()["items"][0]["search"]["meta"]["version"] == "expansion_advisor_v7"
    assert fetched.json()["candidates"][0]["id"] == "c1"
    assert fetched.json()["selected_candidate_ids"] == []
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


def test_list_saved_searches_table_missing_returns_empty(monkeypatch):
    """When the saved-searches table doesn't exist (migration pending),
    the endpoint should return 200 with an empty list, not 500."""
    db = DummyDB()
    from app.api import expansion_advisor as api

    def _raise_programming_error(*_args, **_kwargs):
        raise ProgrammingError(
            "SELECT",
            {},
            Exception('relation "expansion_saved_search" does not exist'),
        )

    monkeypatch.setattr(api, "list_saved_searches", _raise_programming_error)

    client = _client(db)
    try:
        res = client.get("/v1/expansion-advisor/saved-searches")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert res.status_code == 200
    assert res.json() == {"items": []}
    assert db.rolled_back


def test_list_saved_searches_empty_returns_200(monkeypatch):
    """A successful query returning zero rows should give 200 {items: []}."""
    db = DummyDB()
    from app.api import expansion_advisor as api

    monkeypatch.setattr(api, "list_saved_searches", lambda *_args, **_kwargs: [])

    client = _client(db)
    try:
        res = client.get("/v1/expansion-advisor/saved-searches")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert res.status_code == 200
    assert res.json() == {"items": []}


def test_list_saved_searches_generic_error_propagates(monkeypatch):
    """Non-ProgrammingError exceptions should still surface as 500."""
    db = DummyDB()
    from app.api import expansion_advisor as api

    monkeypatch.setattr(api, "list_saved_searches", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("db connection lost")))

    client = _client(db)
    try:
        res = client.get("/v1/expansion-advisor/saved-searches")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert res.status_code == 500


# ---------------------------------------------------------------------------
# PR #1: `lang` parameter threading — plumbing only.
#
# The candidate-shaped saved-search endpoints (create / list / get / patch)
# must accept `lang`, default to "en", coerce invalid values to "en" (200,
# not 422), and behave identically to omitting it. DELETE is out of scope.
# ---------------------------------------------------------------------------

_SAVED_ROW = {
    "id": "saved-1",
    "search_id": "search-1",
    "title": "Study A",
    "status": "draft",
    "selected_candidate_ids": [],
    "filters_json": {},
    "ui_state_json": {},
    "description": None,
    "search": None,
    "candidates": [],
}


def test_create_saved_search_accepts_lang(monkeypatch):
    from app.api import expansion_advisor as api

    monkeypatch.setattr(api, "get_search", lambda *_: {"id": "search-1"})
    monkeypatch.setattr(api, "create_saved_search", lambda *_a, **_k: dict(_SAVED_ROW))

    client = _client(DummyDB())

    def _create(**extra):
        body = {"search_id": "search-1", "title": "Study A", "status": "draft"}
        body.update(extra)
        return client.post("/v1/expansion-advisor/saved-searches", json=body)

    try:
        omitted = _create()
        en = _create(lang="en")
        ar = _create(lang="ar")
        invalid = _create(lang="fr")
    finally:
        app.dependency_overrides.pop(get_db, None)

    for resp in (omitted, en, ar, invalid):
        assert resp.status_code == 200
        assert resp.json() == omitted.json()


def test_list_saved_searches_accepts_lang(monkeypatch):
    from app.api import expansion_advisor as api

    monkeypatch.setattr(api, "list_saved_searches", lambda *_a, **_k: [dict(_SAVED_ROW)])

    client = _client(DummyDB())
    try:
        omitted = client.get("/v1/expansion-advisor/saved-searches")
        en = client.get("/v1/expansion-advisor/saved-searches?lang=en")
        ar = client.get("/v1/expansion-advisor/saved-searches?lang=ar")
        invalid = client.get("/v1/expansion-advisor/saved-searches?lang=fr")
    finally:
        app.dependency_overrides.pop(get_db, None)

    for resp in (omitted, en, ar, invalid):
        assert resp.status_code == 200
        assert resp.json() == omitted.json()


def test_get_saved_search_accepts_lang(monkeypatch):
    from app.api import expansion_advisor as api

    monkeypatch.setattr(api, "get_saved_search", lambda *_a, **_k: dict(_SAVED_ROW))

    client = _client(DummyDB())
    try:
        omitted = client.get("/v1/expansion-advisor/saved-searches/saved-1")
        en = client.get("/v1/expansion-advisor/saved-searches/saved-1?lang=en")
        ar = client.get("/v1/expansion-advisor/saved-searches/saved-1?lang=ar")
        invalid = client.get("/v1/expansion-advisor/saved-searches/saved-1?lang=fr")
    finally:
        app.dependency_overrides.pop(get_db, None)

    for resp in (omitted, en, ar, invalid):
        assert resp.status_code == 200
        assert resp.json() == omitted.json()


def test_patch_saved_search_accepts_lang(monkeypatch):
    from app.api import expansion_advisor as api

    monkeypatch.setattr(api, "update_saved_search", lambda *_a, **_k: dict(_SAVED_ROW))

    client = _client(DummyDB())
    try:
        omitted = client.patch("/v1/expansion-advisor/saved-searches/saved-1", json={"title": "Renamed"})
        en = client.patch("/v1/expansion-advisor/saved-searches/saved-1", json={"title": "Renamed", "lang": "en"})
        ar = client.patch("/v1/expansion-advisor/saved-searches/saved-1", json={"title": "Renamed", "lang": "ar"})
        invalid = client.patch("/v1/expansion-advisor/saved-searches/saved-1", json={"title": "Renamed", "lang": "fr"})
    finally:
        app.dependency_overrides.pop(get_db, None)

    for resp in (omitted, en, ar, invalid):
        assert resp.status_code == 200
        assert resp.json() == omitted.json()


def test_patch_saved_search_does_not_pass_lang_to_update(monkeypatch):
    """R3: `lang` must be popped from the payload before it reaches
    update_saved_search — it is not a persisted saved-search column."""
    from app.api import expansion_advisor as api

    captured = {}

    def _spy_update(_db, _saved_id, payload):
        captured["payload"] = payload
        return dict(_SAVED_ROW)

    monkeypatch.setattr(api, "update_saved_search", _spy_update)

    client = _client(DummyDB())
    try:
        res = client.patch(
            "/v1/expansion-advisor/saved-searches/saved-1",
            json={"title": "Renamed", "lang": "ar"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert res.status_code == 200
    assert "payload" in captured
    assert "lang" not in captured["payload"]
    assert captured["payload"]["title"] == "Renamed"
