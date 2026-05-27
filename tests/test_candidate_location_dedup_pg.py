"""End-to-end behaviour tests for PR5 cross-portal dedup activation.

These tests insert real rows into ephemeral ``commercial_unit`` and
``candidate_location`` tables, run ``_ingest_tier1_aqar`` and
``_run_deduplication``, and assert on the resulting state.

Test-infrastructure requirement: a running PostgreSQL instance with the
PostGIS extension installed, reachable via DATABASE_URL (or the default
psycopg2 localhost connection). If unreachable or PostGIS is missing,
the entire module is skipped. This mirrors the pattern used by
tests/test_expansion_advisor_radiance.py.

What these tests cover that the SQL-shape tests in
``test_candidate_location_dedup.py`` cannot:

* That ``cu.platform`` actually lands in ``candidate_location.source_type``
  at row level (not just that the SELECT projects it).
* That a same-license (Aqar, Bayut) pair collapses to one primary with
  the Aqar row surviving.
* That two same-license Bayut rows collapse to one primary.
"""
from __future__ import annotations

import os
import uuid

import pytest

_PG_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres@localhost:5432/postgres",
)


def _pg_with_postgis_available() -> bool:
    try:
        from sqlalchemy import create_engine, text as _t

        engine = create_engine(_PG_URL, connect_args={"connect_timeout": 3})
        with engine.connect() as conn:
            conn.execute(_t("SELECT 1"))
            # Probe for PostGIS: ST_ClusterDBSCAN + ST_MakePoint are used
            # by _run_deduplication and the candidate_location geom trigger.
            conn.execute(_t("SELECT PostGIS_Version()"))
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _pg_with_postgis_available(),
    reason=(
        "Cross-portal dedup behaviour tests require a live PostgreSQL "
        "instance with PostGIS installed. Set DATABASE_URL or run a "
        "local Postgres with PostGIS to enable these tests."
    ),
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def pg_session():
    """Yield a SQLAlchemy Session wrapped in a rolled-back transaction.

    Creates the minimum schema ``_ingest_tier1_aqar`` and
    ``_run_deduplication`` need: ``commercial_unit`` (with the platform
    columns added in PR4) and ``candidate_location`` (with the
    ``rega_advertisement_license`` column from PR3 and the geom trigger
    that auto-populates ``geom`` from lat/lon).
    """
    from sqlalchemy import create_engine, text as _t
    from sqlalchemy.orm import Session

    engine = create_engine(_PG_URL)
    with engine.connect() as conn:
        conn.execute(_t("BEGIN"))
        # commercial_unit — only the columns _ingest_tier1_aqar reads.
        conn.execute(_t("""
            CREATE TEMP TABLE commercial_unit (
                aqar_id                       VARCHAR(64) PRIMARY KEY,
                neighborhood                  TEXT,
                listing_url                   TEXT,
                image_url                     TEXT,
                description                   TEXT,
                price_sar_annual              NUMERIC(14,2),
                area_sqm                      NUMERIC(10,2),
                street_width_m                NUMERIC(8,2),
                has_drive_thru                BOOLEAN,
                listing_type                  VARCHAR(32),
                property_type                 VARCHAR(64),
                is_furnished                  BOOLEAN,
                apartments_count              INTEGER,
                num_rooms                     INTEGER,
                lat                           NUMERIC(10,7),
                lon                           NUMERIC(10,7),
                restaurant_suitable           BOOLEAN,
                status                        VARCHAR(16) NOT NULL DEFAULT 'active',
                last_seen_at                  TIMESTAMP DEFAULT now(),
                aqar_advertisement_license    TEXT,
                platform                      VARCHAR(16) NOT NULL,
                platform_listing_id           VARCHAR(128) NOT NULL
            ) ON COMMIT DROP
        """))
        # candidate_location — destination of the INSERT, plus the geom
        # column and trigger so dedup's ST_DWithin works.
        conn.execute(_t("""
            CREATE TEMP TABLE candidate_location (
                id                            SERIAL PRIMARY KEY,
                source_tier                   SMALLINT NOT NULL,
                source_type                   VARCHAR(32) NOT NULL,
                source_id                     VARCHAR(256),
                lat                           NUMERIC(10,7) NOT NULL,
                lon                           NUMERIC(10,7) NOT NULL,
                geom                          geometry(Point, 4326),
                district_ar                   VARCHAR(256),
                district_en                   VARCHAR(256),
                neighborhood_raw              VARCHAR(256),
                area_sqm                      NUMERIC(10,2),
                rent_sar_annual               NUMERIC(14,2),
                rent_sar_m2_month             NUMERIC(12,2),
                rent_confidence               VARCHAR(24),
                area_confidence               VARCHAR(24),
                listing_url                   TEXT,
                listing_type                  VARCHAR(32),
                image_url                     TEXT,
                is_vacant                     BOOLEAN,
                street_width_m                NUMERIC(8,2),
                has_drive_thru                BOOLEAN,
                cluster_id                    INTEGER,
                is_cluster_primary            BOOLEAN DEFAULT TRUE,
                rega_advertisement_license    VARCHAR(64),
                landuse_code                  INTEGER,
                landuse_label                 VARCHAR(64),
                current_tenant                VARCHAR(512),
                current_category              VARCHAR(64),
                avg_rating                    NUMERIC(3,2),
                total_rating_count            INTEGER,
                platform_count                SMALLINT,
                population_run_id             VARCHAR(64)
            ) ON COMMIT DROP
        """))
        conn.execute(_t("""
            CREATE OR REPLACE FUNCTION pg_temp.trg_candidate_location_geom()
            RETURNS TRIGGER AS $$
            BEGIN
                IF NEW.lat IS NOT NULL AND NEW.lon IS NOT NULL THEN
                    NEW.geom := ST_SetSRID(ST_MakePoint(
                        NEW.lon::double precision,
                        NEW.lat::double precision
                    ), 4326);
                ELSE
                    NEW.geom := NULL;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql
        """))
        conn.execute(_t("""
            CREATE TRIGGER trg_cl_geom_sync
            BEFORE INSERT OR UPDATE OF lat, lon ON candidate_location
            FOR EACH ROW EXECUTE FUNCTION pg_temp.trg_candidate_location_geom()
        """))
        session = Session(bind=conn)
        try:
            yield session
        finally:
            session.close()
            conn.execute(_t("ROLLBACK"))


def _insert_commercial_unit(session, **overrides) -> str:
    """Insert a commercial_unit row with restaurant-suitable defaults that
    pass the ``_ingest_tier1_aqar`` WHERE clause. Returns the aqar_id.
    """
    from sqlalchemy import text as _t

    defaults = {
        "aqar_id": str(uuid.uuid4())[:12],
        "neighborhood": "حي النخيل",
        "listing_url": "https://example.invalid/listing",
        "image_url": None,
        "description": "Showroom for rent",
        "price_sar_annual": 120000,
        "area_sqm": 150,
        "street_width_m": 20,
        "has_drive_thru": False,
        "listing_type": "showroom",
        "property_type": "Commercial",
        "is_furnished": False,
        "apartments_count": 0,
        "num_rooms": 0,
        "lat": 24.7600,
        "lon": 46.7400,
        "restaurant_suitable": True,
        "status": "active",
        "last_seen_at": "2026-05-01 00:00:00",
        "aqar_advertisement_license": None,
        "platform": "aqar",
        "platform_listing_id": None,
    }
    defaults.update(overrides)
    if defaults["platform_listing_id"] is None:
        defaults["platform_listing_id"] = defaults["aqar_id"]
    session.execute(
        _t("""
            INSERT INTO commercial_unit (
                aqar_id, neighborhood, listing_url, image_url, description,
                price_sar_annual, area_sqm, street_width_m, has_drive_thru,
                listing_type, property_type, is_furnished, apartments_count,
                num_rooms, lat, lon, restaurant_suitable, status,
                last_seen_at, aqar_advertisement_license,
                platform, platform_listing_id
            ) VALUES (
                :aqar_id, :neighborhood, :listing_url, :image_url, :description,
                :price_sar_annual, :area_sqm, :street_width_m, :has_drive_thru,
                :listing_type, :property_type, :is_furnished, :apartments_count,
                :num_rooms, :lat, :lon, :restaurant_suitable, :status,
                :last_seen_at, :aqar_advertisement_license,
                :platform, :platform_listing_id
            )
        """),
        defaults,
    )
    return defaults["aqar_id"]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_bayut_row_gets_source_type_bayut(pg_session):
    """PR5: a commercial_unit row with platform='bayut' must yield a
    candidate_location row with source_type='bayut' (not the pre-PR5
    hardcoded 'aqar')."""
    from sqlalchemy import text as _t
    from app.ingest.candidate_locations import _ingest_tier1_aqar

    _insert_commercial_unit(
        pg_session,
        aqar_id="bayut:42",
        platform="bayut",
        platform_listing_id="42",
        aqar_advertisement_license="REGA-A",
    )
    pg_session.commit()

    _ingest_tier1_aqar(pg_session, run_id="run-bayut")

    rows = pg_session.execute(
        _t("""
            SELECT source_type, source_id
              FROM candidate_location
             WHERE population_run_id = :rid
        """),
        {"rid": "run-bayut"},
    ).mappings().all()
    assert len(rows) == 1
    assert rows[0]["source_type"] == "bayut"
    assert rows[0]["source_id"] == "bayut:42"


def test_aqar_row_gets_source_type_aqar(pg_session):
    """PR5: a commercial_unit row with platform='aqar' must still yield a
    candidate_location row with source_type='aqar' — no regression."""
    from sqlalchemy import text as _t
    from app.ingest.candidate_locations import _ingest_tier1_aqar

    _insert_commercial_unit(
        pg_session,
        aqar_id="aqar-1",
        platform="aqar",
        platform_listing_id="aqar-1",
        aqar_advertisement_license="REGA-B",
    )
    pg_session.commit()

    _ingest_tier1_aqar(pg_session, run_id="run-aqar")

    rows = pg_session.execute(
        _t("""
            SELECT source_type, source_id
              FROM candidate_location
             WHERE population_run_id = :rid
        """),
        {"rid": "run-aqar"},
    ).mappings().all()
    assert len(rows) == 1
    assert rows[0]["source_type"] == "aqar"
    assert rows[0]["source_id"] == "aqar-1"


def test_cross_portal_dedup_collapses_same_license(pg_session):
    """PR5: an Aqar and a Bayut row sharing the same REGA license collapse
    to a single primary; the Aqar row wins the tier-break."""
    from sqlalchemy import text as _t
    from app.ingest.candidate_locations import (
        _ingest_tier1_aqar,
        _run_deduplication,
    )

    aqar_id = _insert_commercial_unit(
        pg_session,
        aqar_id="aqar-x",
        platform="aqar",
        platform_listing_id="aqar-x",
        aqar_advertisement_license="REGA-SAME",
        last_seen_at="2026-05-01 00:00:00",
    )
    bayut_id = _insert_commercial_unit(
        pg_session,
        aqar_id="bayut:x",
        platform="bayut",
        platform_listing_id="x",
        aqar_advertisement_license="REGA-SAME",
        # Identical coordinates — same physical unit
        lat=24.7600,
        lon=46.7400,
        last_seen_at="2026-05-15 00:00:00",  # more recent than aqar
    )
    pg_session.commit()

    _ingest_tier1_aqar(pg_session, run_id="run-xport")
    _run_deduplication(pg_session, run_id="run-xport")

    rows = pg_session.execute(
        _t("""
            SELECT source_type, source_id, is_cluster_primary
              FROM candidate_location
             WHERE population_run_id = :rid
             ORDER BY id
        """),
        {"rid": "run-xport"},
    ).mappings().all()
    assert len(rows) == 2
    primaries = [r for r in rows if r["is_cluster_primary"]]
    # The cross-portal pair must collapse to exactly one primary…
    assert len(primaries) == 1
    # …and the Aqar row must win (per (source_type='aqar') DESC tie-break,
    # which beats Bayut's more recent last_seen_at).
    assert primaries[0]["source_type"] == "aqar"
    assert primaries[0]["source_id"] == aqar_id
    # The Bayut row was inserted with aqar_id="bayut:x" — sanity-check
    # the unused identifier ties out so the fixture intent is clear.
    assert bayut_id == "bayut:x"


def test_within_portal_bayut_dedup_collapses_same_license(pg_session):
    """PR5: two Bayut rows sharing a REGA license collapse to one primary
    via the ``cu.last_seen_at DESC NULLS LAST`` tier-break.

    To prove the last_seen_at tier-break is the decider (and not the
    final-fallback ``cl.id ASC``), the row with the MORE RECENT
    last_seen_at is inserted SECOND — it therefore gets the HIGHER
    cl.id. Under cl.id ASC it would lose; under last_seen_at DESC it
    wins. Asserting that it wins proves the tier-break is live, which
    requires the LEFT JOIN to actually match Bayut rows.
    """
    from sqlalchemy import text as _t
    from app.ingest.candidate_locations import (
        _ingest_tier1_aqar,
        _run_deduplication,
    )

    # Inserted first (lower cl.id) with the OLDER last_seen_at.
    _insert_commercial_unit(
        pg_session,
        aqar_id="bayut:stale",
        platform="bayut",
        platform_listing_id="stale",
        aqar_advertisement_license="REGA-DUP",
        last_seen_at="2026-04-01 00:00:00",
    )
    # Inserted second (higher cl.id) with the MORE RECENT last_seen_at.
    _insert_commercial_unit(
        pg_session,
        aqar_id="bayut:recent",
        platform="bayut",
        platform_listing_id="recent",
        aqar_advertisement_license="REGA-DUP",
        last_seen_at="2026-05-20 00:00:00",
    )
    pg_session.commit()

    _ingest_tier1_aqar(pg_session, run_id="run-bdup")
    _run_deduplication(pg_session, run_id="run-bdup")

    rows = pg_session.execute(
        _t("""
            SELECT source_type, source_id, is_cluster_primary
              FROM candidate_location
             WHERE population_run_id = :rid
             ORDER BY id
        """),
        {"rid": "run-bdup"},
    ).mappings().all()
    assert len(rows) == 2
    primaries = [r for r in rows if r["is_cluster_primary"]]
    assert len(primaries) == 1
    # The recent-seen row wins. If the join were prefix-asymmetric and
    # cu.last_seen_at joined NULL for both, the tier-break would fall
    # through to cl.id ASC and the "stale" (newer-inserted, higher id)
    # row would lose — but for the wrong reason. Asserting that
    # "recent" wins proves the last_seen_at tier-break is live.
    assert primaries[0]["source_type"] == "bayut"
    assert primaries[0]["source_id"] == "bayut:recent"
