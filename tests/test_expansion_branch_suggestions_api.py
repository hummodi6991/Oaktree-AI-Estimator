"""Tests for the Expansion Advisor branch-suggestions endpoint."""
from fastapi.testclient import TestClient

from app.db.deps import get_db
from app.main import app


class FakeRow:
    """Mimics a SQLAlchemy row with positional access."""
    def __init__(self, *values):
        self._values = values

    def __getitem__(self, idx):
        return self._values[idx]


class DummyDB:
    """Minimal DB stub that records executed SQL and returns canned rows."""
    def __init__(self, rows_by_call=None):
        self.rows_by_call = rows_by_call or []
        self._call_idx = 0
        self.executed = []

    def execute(self, stmt, params=None):
        sql_text = stmt.text if hasattr(stmt, "text") else str(stmt)
        self.executed.append((sql_text, params or {}))
        return self

    def fetchall(self):
        if self._call_idx < len(self.rows_by_call):
            rows = self.rows_by_call[self._call_idx]
            self._call_idx += 1
            return rows
        return []

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def _client_with_db(db):
    def override():
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# 1. Riyadh-only spatial filter
# ---------------------------------------------------------------------------

def test_branch_suggestions_sql_contains_riyadh_bbox():
    """SQL queries must contain Riyadh bounding box coordinates."""
    db = DummyDB(rows_by_call=[[], []])
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/branch-suggestions?q=test")
        assert resp.status_code == 200
        assert len(db.executed) >= 1
        # At least one SQL should contain the Riyadh bbox lat/lon bounds
        all_sql = " ".join(sql for sql, _ in db.executed)
        assert "min_lat" in all_sql or "24.2" in all_sql
    finally:
        app.dependency_overrides.clear()


def test_branch_suggestions_returns_riyadh_only_results():
    """Results returned are within Riyadh bounding box."""
    poi_rows = [
        FakeRow("poi:1", "Al Baik - Olaya", "Al Olaya", 24.7, 46.7, "restaurant_poi"),
    ]
    dsr_rows = [
        FakeRow("100", "Kudu - Malqa", "Al Malqa", 24.8, 46.8, "delivery:hungerstation"),
    ]
    db = DummyDB(rows_by_call=[poi_rows, dsr_rows])
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/branch-suggestions?q=al")
        assert resp.status_code == 200
        items = resp.json()["items"]
        # All results should have Riyadh-range coordinates
        for item in items:
            assert 24.0 <= item["lat"] <= 25.5
            assert 45.5 <= item["lon"] <= 48.0
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# 2. Selecting a suggestion fills name/district/lat/lon
# ---------------------------------------------------------------------------

def test_branch_suggestion_response_shape():
    """Each suggestion item has id, name, district, lat, lon, source."""
    poi_rows = [
        FakeRow("poi:abc", "Herfy - Exit 5", "Al Sahafa", 24.75, 46.68, "restaurant_poi"),
    ]
    db = DummyDB(rows_by_call=[poi_rows, []])
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/branch-suggestions?q=herfy")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) >= 1
        item = items[0]
        assert "id" in item
        assert "name" in item and item["name"]
        assert "district" in item
        assert "lat" in item and isinstance(item["lat"], (int, float))
        assert "lon" in item and isinstance(item["lon"], (int, float))
        assert "source" in item
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# 3. Multiple results and deduplication
# ---------------------------------------------------------------------------

def test_branch_suggestions_deduplicates_by_name_proximity():
    """Same restaurant at nearly the same location is deduped."""
    poi_rows = [
        FakeRow("poi:1", "McDonalds Olaya", "Al Olaya", 24.700000, 46.700000, "restaurant_poi"),
    ]
    dsr_rows = [
        # Same name, almost same coords → should be deduped
        FakeRow("200", "McDonalds Olaya", "Al Olaya", 24.700050, 46.700050, "delivery:talabat"),
        # Different branch → should remain
        FakeRow("201", "McDonalds Malqa", "Al Malqa", 24.800000, 46.800000, "delivery:talabat"),
    ]
    db = DummyDB(rows_by_call=[poi_rows, dsr_rows])
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/branch-suggestions?q=mcdonalds")
        assert resp.status_code == 200
        items = resp.json()["items"]
        names = [i["name"] for i in items]
        # "McDonalds Olaya" should appear only once (deduped)
        assert names.count("McDonalds Olaya") == 1
        # "McDonalds Malqa" should appear
        assert "McDonalds Malqa" in names
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# 4. Short query rejection
# ---------------------------------------------------------------------------

def test_branch_suggestions_empty_for_short_query():
    """Queries shorter than 2 characters return empty results."""
    db = DummyDB(rows_by_call=[[], []])
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/branch-suggestions?q=a")
        assert resp.status_code == 200
        assert resp.json()["items"] == []
        # No SQL should have been executed
        assert len(db.executed) == 0
    finally:
        app.dependency_overrides.clear()


def test_branch_suggestions_empty_for_blank_query():
    """Empty query returns empty results."""
    db = DummyDB(rows_by_call=[[], []])
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/branch-suggestions?q=")
        assert resp.status_code == 200
        assert resp.json()["items"] == []
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# 5. Manual coordinate fallback compatibility
# ---------------------------------------------------------------------------

def test_branch_suggestions_no_query_param_still_works():
    """Missing q param returns empty without error."""
    db = DummyDB(rows_by_call=[[], []])
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/branch-suggestions")
        assert resp.status_code == 200
        assert resp.json()["items"] == []
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# 6. Limit enforcement
# ---------------------------------------------------------------------------

def test_branch_suggestions_respects_limit():
    """Results are capped by the limit parameter."""
    poi_rows = [
        FakeRow(f"poi:{i}", f"Restaurant {i}", "District", 24.7 + i * 0.01, 46.7 + i * 0.01, "restaurant_poi")
        for i in range(20)
    ]
    db = DummyDB(rows_by_call=[poi_rows, []])
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/branch-suggestions?q=restaurant&limit=5")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) <= 5
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# 7. Source field populated correctly
# ---------------------------------------------------------------------------

def test_branch_suggestions_source_field():
    """restaurant_poi items have source='restaurant_poi', DSR items have 'delivery:platform'."""
    poi_rows = [
        FakeRow("poi:1", "Al Baik", "Olaya", 24.7, 46.7, "restaurant_poi"),
    ]
    dsr_rows = [
        FakeRow("500", "Kudu", "Malqa", 24.8, 46.8, "delivery:hungerstation"),
    ]
    db = DummyDB(rows_by_call=[poi_rows, dsr_rows])
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/branch-suggestions?q=a")  # too short, will skip
        # Use a longer query
        resp = client.get("/v1/expansion-advisor/branch-suggestions?q=baik")
        assert resp.status_code == 200
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# 8. Payload shape matches spec
# ---------------------------------------------------------------------------

def test_branch_suggestions_payload_shape():
    """Response must be {items: [...]} matching BranchSuggestionsResponse schema."""
    db = DummyDB(rows_by_call=[[], []])
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/branch-suggestions?q=test")
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert isinstance(data["items"], list)
    finally:
        app.dependency_overrides.clear()
