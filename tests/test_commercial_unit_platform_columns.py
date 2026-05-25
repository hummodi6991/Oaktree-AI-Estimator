"""Regression tests for PR2 — ``platform`` and ``platform_listing_id``
on ``commercial_unit``.

PR2 widens the table additively so it can hold rows from multiple
portals (Aqar today, Bayut in PR4). These tests pin three invariants:

1. Every Aqar upsert — INSERT and UPDATE branches — sets
   ``platform='aqar'`` and ``platform_listing_id=aqar_id``.
2. The unique index ``ix_commercial_unit_platform_listing_id`` exists
   on the model with the correct shape so PR4's Bayut writer can rely
   on it as a conflict target.
3. The ORM exposes ``platform`` and ``platform_listing_id`` as
   non-nullable attributes.
"""

from __future__ import annotations

from typing import Any


class _FakeResult:
    def __init__(self, scalar_value: Any = None, rowcount: int = 0) -> None:
        self._scalar = scalar_value
        self.rowcount = rowcount

    def scalar(self) -> Any:
        return self._scalar

    def first(self) -> Any:
        return self._scalar


class _FakeDB:
    """Records ``execute`` calls and returns canned results in order."""

    def __init__(self, results: list[_FakeResult]) -> None:
        self._results = list(results)
        self.calls: list[tuple[str, dict]] = []

    def execute(self, stmt, params=None):
        self.calls.append((str(stmt), dict(params or {})))
        return self._results.pop(0)


_BASE_LISTING: dict[str, Any] = {
    "aqar_id": "1234567",
    "title": "Test store",
    "description": "Some description",
    "neighborhood": "olaya",
    "listing_url": "https://aqar.fm/x/1234567",
    "image_url": "https://aqar.fm/img.jpg",
    "price_sar_annual": 240000,
    "area_sqm": 200,
    "street_width_m": 12,
    "num_floors": 1,
    "has_mezzanine": False,
    "has_drive_thru": False,
    "facade_direction": "north",
    "contact_phone": "0500000000",
    "listing_type": "store",
    "property_type": "Commercial",
    "is_furnished": False,
    "apartments_count": None,
    "num_rooms": None,
    "lat": 24.7,
    "lon": 46.7,
    "restaurant_score": 80,
    "restaurant_suitable": True,
    "restaurant_signals": [],
}


class TestUpsertListingSetsPlatformColumns:
    """upsert_listing writes platform='aqar' and platform_listing_id=aqar_id."""

    def test_insert_branch_includes_platform_columns(self):
        from scripts.scrape_aqar import upsert_listing

        # First call: SELECT existing row → returns None (no match).
        # Second call: INSERT.
        select_result = _FakeResult(scalar_value=None)
        select_result.first = lambda: None  # type: ignore[assignment]
        db = _FakeDB([select_result, _FakeResult(rowcount=1)])

        action = upsert_listing(db, dict(_BASE_LISTING))

        assert action == "insert"
        assert len(db.calls) == 2
        insert_sql, insert_params = db.calls[1]
        assert "INSERT INTO commercial_unit" in insert_sql
        assert "platform" in insert_sql
        assert "platform_listing_id" in insert_sql
        assert ":platform" in insert_sql
        assert ":platform_listing_id" in insert_sql
        assert insert_params["platform"] == "aqar"
        assert insert_params["platform_listing_id"] == "1234567"
        assert insert_params["aqar_id"] == "1234567"

    def test_update_branch_includes_platform_columns(self):
        from scripts.scrape_aqar import upsert_listing

        # First call: SELECT existing row → returns a truthy row.
        select_result = _FakeResult(scalar_value=("1234567",))
        select_result.first = lambda: ("1234567",)  # type: ignore[assignment]
        db = _FakeDB([select_result, _FakeResult(rowcount=1)])

        action = upsert_listing(db, dict(_BASE_LISTING))

        assert action == "update"
        assert len(db.calls) == 2
        update_sql, update_params = db.calls[1]
        assert "UPDATE commercial_unit SET" in update_sql
        assert "platform = :platform" in update_sql
        assert "platform_listing_id = :platform_listing_id" in update_sql
        assert update_params["platform"] == "aqar"
        assert update_params["platform_listing_id"] == "1234567"

    def test_platform_listing_id_tracks_aqar_id(self):
        """platform_listing_id is sourced from aqar_id, not a separate field."""
        from scripts.scrape_aqar import _listing_params

        params = _listing_params({"aqar_id": "99999999"})
        assert params["platform"] == "aqar"
        assert params["platform_listing_id"] == "99999999"
        assert params["aqar_id"] == "99999999"


class TestCommercialUnitModelAttributes:
    """ORM exposes the new columns and the unique index."""

    def test_model_has_platform_and_platform_listing_id_columns(self):
        from app.models.tables import CommercialUnit

        cols = CommercialUnit.__table__.columns
        assert "platform" in cols
        assert "platform_listing_id" in cols
        assert cols["platform"].nullable is False
        assert cols["platform_listing_id"].nullable is False
        # VARCHAR length sanity — guards against accidental shrink.
        assert cols["platform"].type.length == 16
        assert cols["platform_listing_id"].type.length == 128

    def test_unique_index_on_platform_listing_id_pair(self):
        """PR4's Bayut writer will use this as a conflict target."""
        from app.models.tables import CommercialUnit

        indexes = {ix.name: ix for ix in CommercialUnit.__table__.indexes}
        ix = indexes.get("ix_commercial_unit_platform_listing_id")
        assert ix is not None, "missing unique index on (platform, platform_listing_id)"
        assert ix.unique is True
        col_names = [c.name for c in ix.columns]
        assert col_names == ["platform", "platform_listing_id"]

    def test_non_unique_index_on_platform_alone(self):
        from app.models.tables import CommercialUnit

        indexes = {ix.name: ix for ix in CommercialUnit.__table__.indexes}
        ix = indexes.get("ix_commercial_unit_platform")
        assert ix is not None
        assert ix.unique is False
        assert [c.name for c in ix.columns] == ["platform"]
