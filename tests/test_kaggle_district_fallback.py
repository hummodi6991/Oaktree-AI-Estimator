from app.services.kaggle_district import infer_district_from_kaggle


class FakeDB:
    """
    Minimal stub for db.execute(text(sql), params).all()
    Simulates:
      - city-filtered query returns empty (common when city is English but DB stores Arabic or normalized)
      - unfiltered query returns evidence rows
    """

    def __init__(self):
        self.calls = 0

    def execute(self, stmt, params):
        self.calls += 1
        sql = str(stmt)
        # First call has city filter -> empty
        if "lower(city) = lower(:city)" in sql and self.calls == 1:
            return FakeResult([])
        # Second call (no city filter) returns evidence
        return FakeResult([
            ("العليا", "riyadh", 46.675, 24.713),
            ("العليا", "riyadh", 46.676, 24.713),
            ("الشفا", "riyadh", 46.50, 24.55),  # farther away
        ])


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


def test_infer_district_retries_without_city_filter():
    db = FakeDB()
    out = infer_district_from_kaggle(db, city="Riyadh", lon=46.675, lat=24.713)
    assert out["district_raw"] == "العليا"
    assert out["evidence_count"] > 0
    assert out["confidence"] >= 0.25
