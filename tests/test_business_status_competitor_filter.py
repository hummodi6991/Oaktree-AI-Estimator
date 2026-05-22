"""Tests for the typed ``business_status`` column and the closed-venue
filter applied to competitor counts.

Covers:
  * the 20260522 migration upgrade/downgrade roundtrip + backfill
  * the RestaurantPOI model carrying the new column
  * the Expansion Advisor competitor-count query excluding closed venues
  * the ECQ chain_counts CTE excluding closed venues
  * the Place Details ``fields`` parameter requesting business_status

PostGIS is not available in the test environment, so the spatial query
shape is exercised via a simplified SQLite query that preserves the
exact ``business_status`` filter clause. The production query strings
are additionally asserted against directly to guard regressions.
"""
from __future__ import annotations

import importlib.util
import pathlib
from unittest.mock import patch

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    REPO_ROOT
    / "alembic"
    / "versions"
    / "20260522_restaurant_poi_business_status.py"
)


def _has_closed_filter(src: str, prefix: str = "") -> bool:
    """True if src contains the OPERATIONAL-or-NULL filter, ignoring the
    whitespace the SQL is wrapped across. ``prefix`` is the optional
    table alias (e.g. ``rp.``)."""
    collapsed = " ".join(src.split())
    needle = (
        f"WHERE ({prefix}business_status IS NULL "
        f"OR {prefix}business_status = 'OPERATIONAL')"
    )
    alt = (
        f"AND ({prefix}business_status IS NULL "
        f"OR {prefix}business_status = 'OPERATIONAL')"
    )
    return needle in collapsed or alt in collapsed


def _load_migration():
    spec = importlib.util.spec_from_file_location("mig_20260522", MIGRATION_PATH)
    mig = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mig)
    return mig


def _seed_poi_table(conn):
    conn.execute(
        text(
            "CREATE TABLE restaurant_poi ("
            "id TEXT PRIMARY KEY, name TEXT, raw TEXT)"
        )
    )
    conn.execute(
        text(
            "INSERT INTO restaurant_poi (id, name, raw) VALUES "
            "('g:1', 'Open Diner', '{\"business_status\": \"OPERATIONAL\"}'),"
            "('g:2', 'Gone Grill', '{\"business_status\": \"CLOSED_PERMANENTLY\"}'),"
            "('g:3', 'Paused Pizza', '{\"business_status\": \"CLOSED_TEMPORARILY\"}'),"
            "('osm:4', 'No Status Cafe', '{}')"
        )
    )


# ---------------------------------------------------------------------------
# 1. Migration upgrade/downgrade roundtrip + backfill
# ---------------------------------------------------------------------------


def test_migration_upgrade_downgrade_roundtrip():
    """upgrade() adds the column + index and backfills; downgrade() drops
    the column cleanly, leaving the table at its original shape."""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        _seed_poi_table(conn)

    conn = engine.connect()
    op = Operations(MigrationContext.configure(conn))
    mig = _load_migration()
    mig.op = op  # bind the alembic op proxy used inside the migration

    mig.upgrade()
    cols = {c["name"] for c in inspect(conn).get_columns("restaurant_poi")}
    assert "business_status" in cols
    indexes = {i["name"] for i in inspect(conn).get_indexes("restaurant_poi")}
    assert "ix_restaurant_poi_business_status" in indexes

    mig.downgrade()
    cols_after = {c["name"] for c in inspect(conn).get_columns("restaurant_poi")}
    assert "business_status" not in cols_after
    indexes_after = {i["name"] for i in inspect(conn).get_indexes("restaurant_poi")}
    assert "ix_restaurant_poi_business_status" not in indexes_after
    conn.close()


def test_migration_backfills_business_status_from_raw():
    """The backfill copies raw->>'business_status' for rows that have it
    and leaves rows without it NULL."""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        _seed_poi_table(conn)

    conn = engine.connect()
    op = Operations(MigrationContext.configure(conn))
    mig = _load_migration()
    mig.op = op
    mig.upgrade()

    rows = dict(
        conn.execute(
            text("SELECT id, business_status FROM restaurant_poi")
        ).all()
    )
    assert rows == {
        "g:1": "OPERATIONAL",
        "g:2": "CLOSED_PERMANENTLY",
        "g:3": "CLOSED_TEMPORARILY",
        "osm:4": None,
    }
    conn.close()


# ---------------------------------------------------------------------------
# 2. Model carries the new column
# ---------------------------------------------------------------------------


def test_restaurant_poi_model_has_business_status_column():
    from app.models.tables import RestaurantPOI

    col = RestaurantPOI.__table__.columns.get("business_status")
    assert col is not None, "RestaurantPOI is missing business_status column"
    assert col.nullable is True
    assert col.type.length == 32  # String(32)
    index_cols = {
        tuple(c.name for c in idx.columns)
        for idx in RestaurantPOI.__table__.indexes
    }
    assert ("business_status",) in index_cols


# ---------------------------------------------------------------------------
# 3. Competitor count excludes closed venues
# ---------------------------------------------------------------------------


def _competitor_count_session():
    """SQLite restaurant_poi with the typed business_status column."""
    engine = create_engine("sqlite:///:memory:")
    db = sessionmaker(bind=engine)()
    db.execute(
        text(
            "CREATE TABLE restaurant_poi ("
            "id TEXT PRIMARY KEY, name TEXT, category TEXT, "
            "business_status TEXT)"
        )
    )
    db.execute(
        text(
            "INSERT INTO restaurant_poi "
            "(id, name, category, business_status) VALUES "
            "('g:1', 'Open Burger', 'burger', 'OPERATIONAL'),"
            "('g:2', 'Closed Burger', 'burger', 'CLOSED_PERMANENTLY'),"
            "('g:3', 'Paused Burger', 'burger', 'CLOSED_TEMPORARILY'),"
            "('osm:4', 'Legacy Burger', 'burger', NULL)"
        )
    )
    db.commit()
    return db


def test_competitor_count_excludes_closed_venues():
    """Mirrors the Source-1 restaurant_poi subquery filter: closed venues
    are excluded, OPERATIONAL and NULL rows are counted."""
    db = _competitor_count_session()
    count = db.execute(
        text(
            "SELECT COUNT(*) FROM restaurant_poi "
            "WHERE (business_status IS NULL "
            "       OR business_status = 'OPERATIONAL')"
        )
    ).scalar()
    # g:1 (OPERATIONAL) + osm:4 (NULL) counted; g:2, g:3 excluded.
    assert count == 2


def test_competitor_query_source_carries_closed_filter():
    """Guard: the production competitor-count query keeps the filter."""
    src = (REPO_ROOT / "app" / "services" / "expansion_advisor.py").read_text()
    assert _has_closed_filter(src, prefix="rp."), (
        "competitor-count query lost the OPERATIONAL-or-NULL filter"
    )
    # The unsafe single-sided form must not be used.
    assert "business_status != 'CLOSED_PERMANENTLY'" not in src


# ---------------------------------------------------------------------------
# 4. ECQ chain_counts CTE excludes closed venues
# ---------------------------------------------------------------------------


def test_ecq_chain_counts_excludes_closed_venues():
    """Closed venues do not inflate chain sizes."""
    engine = create_engine("sqlite:///:memory:")
    db = sessionmaker(bind=engine)()
    db.execute(
        text("CREATE TABLE restaurant_poi (id TEXT PRIMARY KEY, "
             "name TEXT, business_status TEXT)")
    )
    # Four "Burger King" branches: two open, one closed permanently,
    # one closed temporarily. Only the two open ones count.
    db.execute(
        text(
            "INSERT INTO restaurant_poi (id, name, business_status) VALUES "
            "('g:1', 'Burger King', 'OPERATIONAL'),"
            "('g:2', 'Burger King', 'OPERATIONAL'),"
            "('g:3', 'Burger King', 'CLOSED_PERMANENTLY'),"
            "('g:4', 'Burger King', 'CLOSED_TEMPORARILY')"
        )
    )
    db.commit()
    chain_size = db.execute(
        text(
            "SELECT COUNT(*) FROM restaurant_poi "
            "WHERE name IS NOT NULL AND name != '' "
            "  AND (business_status IS NULL "
            "       OR business_status = 'OPERATIONAL')"
        )
    ).scalar()
    assert chain_size == 2


def test_ecq_chain_counts_source_carries_closed_filter():
    """Guard: the chain_counts CTE keeps the OPERATIONAL-or-NULL filter."""
    src = (
        REPO_ROOT / "app" / "ingest" / "expansion_advisor_competitors.py"
    ).read_text()
    assert _has_closed_filter(src), (
        "chain_counts CTE lost the OPERATIONAL-or-NULL filter"
    )
    assert "business_status != 'CLOSED_PERMANENTLY'" not in src


# ---------------------------------------------------------------------------
# 5. Place Details fields parameter requests business_status
# ---------------------------------------------------------------------------


def test_place_details_fields_includes_business_status_sync():
    from app.connectors import google_places

    captured = {}

    def fake_request(url, params):
        captured["url"] = url
        captured["params"] = params
        return {
            "status": "OK",
            "result": {
                "place_id": "p1",
                "name": "Test Venue",
                "business_status": "CLOSED_PERMANENTLY",
            },
        }

    google_places._details_cache.clear()
    with patch.object(google_places, "_get_api_key", return_value="k"), patch.object(
        google_places, "_request_with_retry", side_effect=fake_request
    ):
        details = google_places.get_place_details("p1")

    assert "business_status" in captured["params"]["fields"]
    assert details["business_status"] == "CLOSED_PERMANENTLY"
    google_places._details_cache.clear()


def test_place_details_fields_includes_business_status_async():
    """The async connector requests the same Basic-tier field."""
    src = (
        REPO_ROOT / "app" / "connectors" / "google_places_async.py"
    ).read_text()
    # The async get_place_details fields string must request the field.
    assert "formatted_address,business_status" in src
