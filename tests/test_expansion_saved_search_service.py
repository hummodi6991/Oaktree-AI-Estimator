from app.services.expansion_advisor import (
    create_saved_search,
    delete_saved_search,
    get_saved_search,
    list_saved_searches,
    update_saved_search,
)


class _Res:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return self._rows


class FakeDB:
    def execute(self, stmt, params=None):
        sql = stmt.text if hasattr(stmt, "text") else str(stmt)
        if "INSERT INTO expansion_saved_search" in sql:
            return _Res([{"id": "saved-1", "search_id": params["search_id"], "title": params["title"], "status": params["status"]}])
        if "FROM expansion_saved_search" in sql and "ORDER BY updated_at DESC" in sql:
            return _Res([{"id": "saved-1", "title": "Study A", "status": "draft"}])
        if "FROM expansion_saved_search" in sql and "WHERE id = :saved_id" in sql:
            return _Res([{"id": "saved-1", "search_id": "search-1", "title": "Study A", "status": "draft"}])
        if "UPDATE expansion_saved_search" in sql:
            return _Res([{"id": "saved-1", "title": params.get("title", "Study A"), "status": "final"}])
        if "DELETE FROM expansion_saved_search" in sql:
            return _Res([{"id": "saved-1"}])
        if "FROM expansion_search" in sql:
            return _Res([{"id": "search-1"}])
        if "FROM expansion_candidate" in sql:
            return _Res([{"id": "c1", "search_id": "search-1", "parcel_id": "p1", "lat": 24.7, "lon": 46.7}])
        return _Res([])


def test_saved_search_service_helpers():
    db = FakeDB()
    created = create_saved_search(db, search_id="search-1", title="Study A", description=None, status="draft", selected_candidate_ids=["c1"], filters_json={"a": 1}, ui_state_json={"tab": "results"})
    listed = list_saved_searches(db, status=None, limit=20)
    loaded = get_saved_search(db, "saved-1")
    patched = update_saved_search(db, "saved-1", {"title": "Study B", "status": "final"})
    deleted = delete_saved_search(db, "saved-1")

    assert created["id"] == "saved-1"
    assert listed[0]["id"] == "saved-1"
    assert loaded and loaded["search"]["id"] == "search-1"
    assert loaded and loaded["candidates"][0]["id"] == "c1"
    assert patched and patched["title"] == "Study B"
    assert deleted is True
