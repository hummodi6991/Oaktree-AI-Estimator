"""Regression test for the realized-demand broadcast bug.

Context
-------
The bulk realized-demand enrichment populates
``_bulk_delivery[parcel_id]["realized_demand_30d"]`` with per-catchment
values.  The *first* scoring pass (``for row in rows:``) correctly resets
``_realized_demand_30d`` / ``_realized_demand_branches`` at the top of each
iteration and uses them to compute the per-parcel delivery_score.

The *second* pass — the shortlist loop
(``for prepared_item in prepared[:shortlist_size]:``) — is where
``feature_snapshot_json["realized_demand_30d"]`` is written.  Previously
that loop read from the outer-scope variables left over from the first
pass, so every candidate in the shortlist received whatever value the
final iteration of the first loop produced.  Symptom in production:
a 20-candidate burger search across 13 distinct districts surfaced the
same ``realized_demand_30d`` (and ``realized_demand_branches``) for every
candidate.

This test mocks ``_bulk_delivery`` with three parcels carrying three
different realized-demand values and asserts that each candidate's
``feature_snapshot_json`` surfaces its own per-parcel value — not a single
broadcast figure.
"""

from __future__ import annotations

import pytest

from app.core.config import settings
from app.services import expansion_advisor as expansion_service


# ────────────────────────────────────────────────────────────────────────
#  Minimal FakeDB.  Only the query shapes exercised by run_expansion_search
#  along the path we care about (candidate pool → bulk-delivery enrichment
#  → bulk realized-demand enrichment → shortlist loop → INSERT) are
#  handled; everything else returns an empty result which the service
#  already tolerates via its own except-branches and "not populated" guards.
# ────────────────────────────────────────────────────────────────────────


class _FakeNestedTransaction:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False  # propagate


class _Result:
    def __init__(self, rows):
        self._rows = list(rows)

    def scalar(self):
        if not self._rows:
            return None
        row = self._rows[0]
        if isinstance(row, dict):
            return next(iter(row.values()), None)
        return row

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None


class FakeDB:
    def __init__(
        self,
        candidate_rows,
        bulk_delivery_rows,
        realized_demand_rows,
    ):
        self.candidate_rows = candidate_rows
        self.bulk_delivery_rows = bulk_delivery_rows
        self.realized_demand_rows = realized_demand_rows
        self.inserted = []

    def begin_nested(self):
        return _FakeNestedTransaction()

    def execute(self, stmt, params=None):
        sql = stmt.text if hasattr(stmt, "text") else str(stmt)

        # Candidate pool queries (both listings-first and parcel pools land
        # here).  Return the same three seeded rows for all pool paths.
        if "FROM candidate_base" in sql:
            return _Result(self.candidate_rows)
        if "FROM commercial_unit" in sql and "INSERT" not in sql:
            return _Result(self.candidate_rows)
        if "COUNT(*)" in sql and "candidate_location" in sql:
            return _Result([{"count": 0}])

        # Bulk realized-demand enrichment uses a `branch_delta` CTE — match
        # that first so it doesn't fall through to the bulk-delivery branch.
        if "branch_delta" in sql:
            return _Result(self.realized_demand_rows)

        # Bulk delivery enrichment query (listing_count / platform_count /
        # cat_count over a 1200m catchment).
        if "listing_count" in sql and "platform_count" in sql:
            return _Result(self.bulk_delivery_rows)

        if "INSERT INTO expansion_candidate" in sql:
            self.inserted.append(params)
            return _Result([])
        if "SELECT id FROM expansion_search" in sql:
            return _Result([{"id": "search-1"}])

        return _Result([])


# ────────────────────────────────────────────────────────────────────────
#  Fixtures
# ────────────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset_expansion_caches():
    """Clear the module-level table-availability cache between tests so
    the monkeypatched ``_cached_table_available`` takes effect cleanly."""
    expansion_service.clear_expansion_caches()
    yield
    expansion_service.clear_expansion_caches()


# ────────────────────────────────────────────────────────────────────────
#  Test
# ────────────────────────────────────────────────────────────────────────


def test_realized_demand_is_per_candidate_not_broadcast(monkeypatch):
    """Three geographically distinct parcels must each surface their own
    realized-demand value.  Before the fix the shortlist loop read from
    outer-scope variables set by the first scoring pass, so every
    candidate's feature_snapshot_json inherited whichever parcel was
    processed last in that first loop.
    """
    # Three parcels in three different districts, with three different
    # realized-demand deltas mirroring the CEO-regression diagnostic:
    #   الورود cluster → 33 Δ / 9 branches
    #   العليا         → 252 Δ / 61 branches
    #   الشهداء        → 291 Δ / 25 branches
    candidate_rows = [
        {
            "parcel_id": "p-alwurud",
            "landuse_label": "Commercial",
            "landuse_code": "C",
            "area_m2": 180,
            "lon": 46.60,
            "lat": 24.75,
            "district": "حي الورود",
            "population_reach": 12000,
            "competitor_count": 3,
            "delivery_listing_count": 9,
            "provider_listing_count": 9,
            "provider_platform_count": 3,
            "delivery_competition_count": 9,
        },
        {
            "parcel_id": "p-olaya",
            "landuse_label": "Commercial",
            "landuse_code": "C",
            "area_m2": 180,
            "lon": 46.69,
            "lat": 24.70,
            "district": "حي العليا",
            "population_reach": 18000,
            "competitor_count": 6,
            "delivery_listing_count": 61,
            "provider_listing_count": 61,
            "provider_platform_count": 5,
            "delivery_competition_count": 61,
        },
        {
            "parcel_id": "p-shuhada",
            "landuse_label": "Commercial",
            "landuse_code": "C",
            "area_m2": 180,
            "lon": 46.82,
            "lat": 24.65,
            "district": "حي الشهداء",
            "population_reach": 14000,
            "competitor_count": 4,
            "delivery_listing_count": 25,
            "provider_listing_count": 25,
            "provider_platform_count": 4,
            "delivery_competition_count": 25,
        },
    ]
    bulk_delivery_rows = [
        {"parcel_id": "p-alwurud", "listing_count": 9, "platform_count": 3, "cat_count": 9},
        {"parcel_id": "p-olaya", "listing_count": 61, "platform_count": 5, "cat_count": 61},
        {"parcel_id": "p-shuhada", "listing_count": 25, "platform_count": 4, "cat_count": 25},
    ]
    realized_demand_rows = [
        {"parcel_id": "p-alwurud", "realized_demand": 33, "contributing_branches": 9},
        {"parcel_id": "p-olaya", "realized_demand": 252, "contributing_branches": 61},
        {"parcel_id": "p-shuhada", "realized_demand": 291, "contributing_branches": 25},
    ]

    db = FakeDB(candidate_rows, bulk_delivery_rows, realized_demand_rows)

    # Enable the realized-demand path.  Make the EA delivery and rating
    # history tables look populated so the enrichment branches fire.
    monkeypatch.setattr(settings, "EXPANSION_REALIZED_DEMAND_ENABLED", True)
    monkeypatch.setattr(
        expansion_service, "_cached_ea_table_has_rows", lambda _db, _t: True
    )
    monkeypatch.setattr(
        expansion_service, "_cached_table_available", lambda _db, _t: True
    )
    # Stable rent resolution so we don't pull on any unrelated DB paths.
    monkeypatch.setattr(
        expansion_service,
        "_estimate_rent_sar_m2_year",
        lambda _db, _d: (900.0, "test"),
    )

    result = expansion_service.run_expansion_search(
        db,
        search_id="search-1",
        brand_name="Burger X",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=400,
        target_area_m2=180,
        limit=10,
    )
    items = result["items"] if isinstance(result, dict) else result

    per_pid = {c["parcel_id"]: c for c in items}
    # All three parcels must survive to the shortlist.
    assert set(per_pid) == {"p-alwurud", "p-olaya", "p-shuhada"}, items

    snap = lambda pid: per_pid[pid]["feature_snapshot_json"]

    # The bug: every candidate was seeing the same realized_demand_30d
    # (the last value set in the first scoring pass).  Per-parcel values
    # must be preserved.
    assert snap("p-alwurud").get("realized_demand_30d") == 33.0
    assert snap("p-olaya").get("realized_demand_30d") == 252.0
    assert snap("p-shuhada").get("realized_demand_30d") == 291.0

    # Branch counts are written alongside the Δ value and must also be
    # per-parcel — not the leaked last-iteration count.
    assert snap("p-alwurud").get("realized_demand_branches") == 9
    assert snap("p-olaya").get("realized_demand_branches") == 61
    assert snap("p-shuhada").get("realized_demand_branches") == 25

    # Sanity: the three values are distinct (guards against a future
    # refactor that accidentally sets them all to the same constant).
    rd_values = {snap(pid).get("realized_demand_30d") for pid in per_pid}
    assert len(rd_values) == 3, rd_values
