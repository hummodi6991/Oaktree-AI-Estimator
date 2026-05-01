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
    def __init__(self, rows=None):
        self.rows = rows or []
        self.executed = []

    def execute(self, stmt, params=None):
        sql_text = stmt.text if hasattr(stmt, "text") else str(stmt)
        self.executed.append((sql_text, params or {}))
        return self

    def fetchall(self):
        return self.rows

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


def test_districts_endpoint_returns_deduped_sorted():
    rows = [
        FakeRow("حي العليا", "Al Olaya"),
        FakeRow("حي العليا", "Olaya"),
        FakeRow("حي الملقا", "Al Malqa"),
        FakeRow("حي النخيل", None),
    ]
    db = DummyDB(rows)
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/districts")
        assert resp.status_code == 200
        data = resp.json()
        items = data["items"]
        # Should be 3 unique districts (العليا deduped)
        values = [i["value"] for i in items]
        assert len(values) == len(set(values)), "Values must be unique"
        # Sorted by Arabic label
        labels = [i["label_ar"] for i in items]
        assert labels == sorted(labels)
    finally:
        app.dependency_overrides.clear()


def test_districts_endpoint_dedups_same_district():
    """When two rows share the same normalized key, exactly one item is
    emitted. The first-seen English label wins; later non-null labels do
    not overwrite a present one."""
    rows = [
        FakeRow("حي العليا", "Olaya District"),
        FakeRow("حي العليا", "Al Olaya"),
    ]
    db = DummyDB(rows)
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/districts")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) == 1
        assert items[0]["label_ar"] == "حي العليا"
        assert items[0]["label_en"] in ("Olaya District", "Al Olaya")
    finally:
        app.dependency_overrides.clear()


def test_districts_endpoint_excludes_blank():
    rows = [
        FakeRow("", None),
        FakeRow(None, None),
        FakeRow("حي الملقا", "Al Malqa"),
    ]
    db = DummyDB(rows)
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/districts")
        assert resp.status_code == 200
        items = resp.json()["items"]
        # Blank/null are excluded by SQL, but even if they leak, the Python code filters them
        assert len(items) == 1
        assert items[0]["label_ar"] == "حي الملقا"
    finally:
        app.dependency_overrides.clear()


def test_districts_endpoint_empty_table():
    db = DummyDB([])
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/districts")
        assert resp.status_code == 200
        assert resp.json()["items"] == []
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Layer scoping: SQL must filter to aqar_district_hulls only
# ---------------------------------------------------------------------------


def test_districts_sql_filters_to_aqar_only():
    """After osm_districts removal (2026-05-01), the SQL must filter to
    layer_name='aqar_district_hulls' and must not reference osm_districts."""
    db = DummyDB([])
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/districts")
        assert resp.status_code == 200
        assert len(db.executed) == 1
        sql_text = db.executed[0][0]
        assert "'aqar_district_hulls'" in sql_text
        assert "'osm_districts'" not in sql_text, (
            "OSM has been removed; SQL must not reference it."
        )
    finally:
        app.dependency_overrides.clear()


def test_districts_riyadh_rows_present():
    """Valid Riyadh district rows from Aqar are retained."""
    rows = [
        FakeRow("حي النرجس", "An Narjis"),
        FakeRow("حي العارض", None),
        FakeRow("حي الياسمين", "Al Yasmin"),
    ]
    db = DummyDB(rows)
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/districts")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) == 3
        values = [i["value"] for i in items]
        # All three unique districts present
        assert len(values) == len(set(values))
    finally:
        app.dependency_overrides.clear()


def test_districts_aliases_for_duplicate_arabic():
    """When two rows normalize to the same key but carry different
    Arabic spellings, the second spelling becomes an alias."""
    rows = [
        FakeRow("حي العليا", "Al Olaya"),
        FakeRow("العليا", None),
    ]
    db = DummyDB(rows)
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/districts")
        assert resp.status_code == 200
        items = resp.json()["items"]
        # Both normalize to "العليا" → 1 item
        assert len(items) == 1
        item = items[0]
        # First-seen label wins
        assert item["label_ar"] == "حي العليا"
        # The other variant appears in aliases
        assert "العليا" in item["aliases"]
    finally:
        app.dependency_overrides.clear()


def test_districts_output_deduped_and_sorted():
    """Output is deduplicated by normalized key and sorted by Arabic label."""
    rows = [
        FakeRow("حي النخيل", "Al Nakheel"),
        FakeRow("حي النخيل", None),
        FakeRow("حي العليا", "Al Olaya"),
        FakeRow("حي الملقا", "Al Malqa"),
        FakeRow("حي الياسمين", "Al Yasmin"),
    ]
    db = DummyDB(rows)
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/districts")
        assert resp.status_code == 200
        items = resp.json()["items"]
        # النخيل appears twice → 4 unique
        values = [i["value"] for i in items]
        assert len(values) == 4
        assert len(values) == len(set(values)), "Values must be unique"
        # Sorted by label_ar
        labels = [i["label_ar"] for i in items]
        assert labels == sorted(labels), "Items must be sorted by Arabic label"
    finally:
        app.dependency_overrides.clear()


def test_districts_payload_shape_unchanged():
    """Response payload shape must match the DistrictOptionsListResponse schema."""
    rows = [
        FakeRow("حي العليا", "Al Olaya"),
    ]
    db = DummyDB(rows)
    client = _client_with_db(db)
    try:
        resp = client.get("/v1/expansion-advisor/districts")
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        item = data["items"][0]
        # Verify all required fields present
        assert "value" in item
        assert "label" in item
        assert "label_ar" in item
        assert "label_en" in item
        assert "aliases" in item
        assert isinstance(item["aliases"], list)
        # label and label_ar should match
        assert item["label"] == item["label_ar"]
    finally:
        app.dependency_overrides.clear()
