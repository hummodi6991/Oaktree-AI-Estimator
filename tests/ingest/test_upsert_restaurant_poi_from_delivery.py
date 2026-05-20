"""Tests for ``upsert_restaurant_poi_from_delivery``.

The function is the post-scrape Stage 2 that the weekly ``ingest-restaurant-pois``
workflow now calls instead of re-running the delivery scrape. It must read the
``delivery_source_record`` rows already produced by the SCCC daily K8s job and
upsert them into ``restaurant_poi``.

These tests use a SQLite in-memory engine with the real ORM tables
(``Base.metadata.create_all``) — no SQL-layer mocks. JSONB columns are compiled
to JSON on SQLite via ``@compiles`` (same pattern as ``tests/test_revenue_paths.py``).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.delivery.models import DeliveryIngestRun, DeliverySourceRecord
from app.ingest.restaurant_pois import upsert_restaurant_poi_from_delivery
from app.models.base import Base
from app.models.tables import RestaurantPOI


@compiles(JSONB, "sqlite")
def _compile_jsonb(element, compiler, **kw):  # noqa: ARG001
    return "JSON"


@pytest.fixture
def session_factory():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(
        bind=engine,
        tables=[
            DeliveryIngestRun.__table__,
            DeliverySourceRecord.__table__,
            RestaurantPOI.__table__,
        ],
    )
    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    yield Session
    engine.dispose()


@pytest.fixture
def db(session_factory):
    s = session_factory()
    try:
        yield s
    finally:
        s.close()


def _make_run(
    db,
    *,
    platform: str,
    status: str = "completed",
    finished_at: datetime | None = None,
) -> DeliveryIngestRun:
    started = (finished_at or datetime.now(timezone.utc)) - timedelta(minutes=10)
    run = DeliveryIngestRun(
        platform=platform,
        started_at=started,
        finished_at=finished_at or datetime.now(timezone.utc),
        status=status,
    )
    db.add(run)
    db.flush()
    return run


def _make_record(
    db,
    *,
    run_id: int,
    platform: str = "hungerstation",
    source_listing_id: str | None = "lst-1",
    lat: float | None = 24.7136,
    lon: float | None = 46.6753,
    location_confidence: float | None = 0.9,
    geocode_method: str = "platform_payload",
    name: str = "Burger King - Olaya",
    cuisine_raw: str | None = "burger",
    brand_raw: str | None = "Burger King",
    district_text: str | None = "Al Olaya",
    rating: float | None = None,
    rating_count: int | None = None,
) -> DeliverySourceRecord:
    row = DeliverySourceRecord(
        platform=platform,
        source_listing_id=source_listing_id,
        source_url=f"https://{platform}.example/{source_listing_id}",
        scraped_at=datetime.now(timezone.utc),
        city="riyadh",
        lat=lat,
        lon=lon,
        geocode_method=geocode_method,
        location_confidence=location_confidence,
        restaurant_name_raw=name,
        restaurant_name_normalized=name,
        brand_raw=brand_raw,
        cuisine_raw=cuisine_raw,
        district_text=district_text,
        rating=rating,
        rating_count=rating_count,
        ingest_run_id=run_id,
    )
    db.add(row)
    db.flush()
    return row


def _poi_count(db) -> int:
    return db.query(RestaurantPOI).count()


# ---------------------------------------------------------------------------
# 1. Empty case
# ---------------------------------------------------------------------------

class TestEmpty:
    def test_no_runs_returns_zero(self, db):
        n = upsert_restaurant_poi_from_delivery(db)
        assert n == 0
        assert _poi_count(db) == 0

    def test_runs_but_no_records_returns_zero(self, db):
        _make_run(db, platform="hungerstation")
        db.commit()
        n = upsert_restaurant_poi_from_delivery(db)
        assert n == 0
        assert _poi_count(db) == 0


# ---------------------------------------------------------------------------
# 2. Row-level filter correctness
# ---------------------------------------------------------------------------

class TestRowFilter:
    def test_filters_by_geocode_method_and_confidence(self, db):
        run = _make_run(db, platform="hungerstation")

        # Should pass: platform_payload + 0.9 confidence
        _make_record(
            db, run_id=run.id, source_listing_id="ok-1",
            geocode_method="platform_payload", location_confidence=0.9,
        )
        # Should pass: json_ld + 0.7 confidence
        _make_record(
            db, run_id=run.id, source_listing_id="ok-2",
            geocode_method="json_ld", location_confidence=0.7,
        )
        # Should pass: address_geocode + 0.8 confidence
        _make_record(
            db, run_id=run.id, source_listing_id="ok-3",
            geocode_method="address_geocode", location_confidence=0.8,
        )
        # Should reject: geocode_method='none'
        _make_record(
            db, run_id=run.id, source_listing_id="bad-none",
            geocode_method="none", location_confidence=0.9,
        )
        # Should reject: confidence below 0.7
        _make_record(
            db, run_id=run.id, source_listing_id="bad-conf",
            geocode_method="platform_payload", location_confidence=0.6,
        )
        # Should reject: missing lat
        _make_record(
            db, run_id=run.id, source_listing_id="bad-lat",
            geocode_method="platform_payload", location_confidence=0.9,
            lat=None,
        )
        # Should reject: missing lon
        _make_record(
            db, run_id=run.id, source_listing_id="bad-lon",
            geocode_method="platform_payload", location_confidence=0.9,
            lon=None,
        )
        db.commit()

        n = upsert_restaurant_poi_from_delivery(db)
        assert n == 3
        ids = {p.id for p in db.query(RestaurantPOI).all()}
        assert ids == {
            "hungerstation:ok-1",
            "hungerstation:ok-2",
            "hungerstation:ok-3",
        }


# ---------------------------------------------------------------------------
# 3. Most-recent-per-platform semantics (since=None)
# ---------------------------------------------------------------------------

class TestMostRecentPerPlatform:
    def test_only_latest_run_per_platform_is_used(self, db):
        now = datetime.now(timezone.utc)
        old_run = _make_run(
            db, platform="hungerstation", finished_at=now - timedelta(days=7),
        )
        new_run = _make_run(
            db, platform="hungerstation", finished_at=now - timedelta(hours=1),
        )

        _make_record(
            db, run_id=old_run.id, source_listing_id="old-listing",
            name="Stale Burger",
        )
        _make_record(
            db, run_id=new_run.id, source_listing_id="new-listing",
            name="Fresh Burger",
        )
        db.commit()

        n = upsert_restaurant_poi_from_delivery(db)
        assert n == 1
        ids = {p.id for p in db.query(RestaurantPOI).all()}
        assert ids == {"hungerstation:new-listing"}

    def test_picks_latest_per_platform_independently(self, db):
        now = datetime.now(timezone.utc)
        hs_old = _make_run(
            db, platform="hungerstation", finished_at=now - timedelta(days=2),
        )
        hs_new = _make_run(
            db, platform="hungerstation", finished_at=now - timedelta(hours=2),
        )
        jz_old = _make_run(
            db, platform="jahez", finished_at=now - timedelta(days=2),
        )
        jz_new = _make_run(
            db, platform="jahez", finished_at=now - timedelta(hours=3),
        )

        _make_record(
            db, run_id=hs_old.id, platform="hungerstation",
            source_listing_id="hs-old", name="HS Old",
        )
        _make_record(
            db, run_id=hs_new.id, platform="hungerstation",
            source_listing_id="hs-new", name="HS New",
        )
        _make_record(
            db, run_id=jz_old.id, platform="jahez",
            source_listing_id="jz-old", name="JZ Old",
        )
        _make_record(
            db, run_id=jz_new.id, platform="jahez",
            source_listing_id="jz-new", name="JZ New",
        )
        db.commit()

        n = upsert_restaurant_poi_from_delivery(db)
        assert n == 2
        ids = {p.id for p in db.query(RestaurantPOI).all()}
        assert ids == {"hungerstation:hs-new", "jahez:jz-new"}


# ---------------------------------------------------------------------------
# 4. ``since`` filter
# ---------------------------------------------------------------------------

class TestSinceFilter:
    def test_since_excludes_older_runs(self, db):
        now = datetime.now(timezone.utc)
        old_run = _make_run(
            db, platform="hungerstation", finished_at=now - timedelta(days=7),
        )
        new_run = _make_run(
            db, platform="hungerstation", finished_at=now - timedelta(hours=2),
        )

        _make_record(
            db, run_id=old_run.id, source_listing_id="should-be-skipped",
        )
        _make_record(
            db, run_id=new_run.id, source_listing_id="should-be-included",
        )
        db.commit()

        n = upsert_restaurant_poi_from_delivery(
            db, since=now - timedelta(days=1),
        )
        assert n == 1
        ids = {p.id for p in db.query(RestaurantPOI).all()}
        assert ids == {"hungerstation:should-be-included"}

    def test_since_keeps_multiple_runs_in_window(self, db):
        # With since= provided, ALL runs in window are used, not just the latest.
        now = datetime.now(timezone.utc)
        run_a = _make_run(
            db, platform="hungerstation", finished_at=now - timedelta(hours=5),
        )
        run_b = _make_run(
            db, platform="hungerstation", finished_at=now - timedelta(hours=2),
        )

        _make_record(db, run_id=run_a.id, source_listing_id="a")
        _make_record(db, run_id=run_b.id, source_listing_id="b")
        db.commit()

        n = upsert_restaurant_poi_from_delivery(
            db, since=now - timedelta(days=1),
        )
        assert n == 2


# ---------------------------------------------------------------------------
# 5. ``platforms`` filter
# ---------------------------------------------------------------------------

class TestPlatformsFilter:
    def test_platforms_excludes_other_platform_runs(self, db):
        _hs_run = _make_run(db, platform="hungerstation")
        _jz_run = _make_run(db, platform="jahez")

        _make_record(
            db, run_id=_hs_run.id, platform="hungerstation",
            source_listing_id="hs-1",
        )
        _make_record(
            db, run_id=_jz_run.id, platform="jahez",
            source_listing_id="jz-1",
        )
        db.commit()

        n = upsert_restaurant_poi_from_delivery(db, platforms=["hungerstation"])
        assert n == 1
        ids = {p.id for p in db.query(RestaurantPOI).all()}
        assert ids == {"hungerstation:hs-1"}


# ---------------------------------------------------------------------------
# 6. Status filter
# ---------------------------------------------------------------------------

class TestStatusFilter:
    def test_failed_runs_excluded_by_default(self, db):
        run = _make_run(db, platform="hungerstation", status="failed")
        _make_record(db, run_id=run.id, source_listing_id="from-failed")
        db.commit()

        n = upsert_restaurant_poi_from_delivery(db)
        assert n == 0
        assert _poi_count(db) == 0

    def test_failed_runs_included_when_requested(self, db):
        run = _make_run(db, platform="hungerstation", status="failed")
        _make_record(db, run_id=run.id, source_listing_id="from-failed")
        db.commit()

        n = upsert_restaurant_poi_from_delivery(
            db, statuses=("failed",),
        )
        assert n == 1

    def test_completed_with_errors_included_by_default(self, db):
        run = _make_run(db, platform="hungerstation", status="completed_with_errors")
        _make_record(db, run_id=run.id, source_listing_id="from-cwe")
        db.commit()

        n = upsert_restaurant_poi_from_delivery(db)
        assert n == 1


# ---------------------------------------------------------------------------
# 7. Upsert behavior — existing row updated, not duplicated
# ---------------------------------------------------------------------------

class TestUpsertBehavior:
    def test_existing_row_is_updated_not_duplicated(self, db):
        # Pre-existing POI with the deterministic id this function would emit.
        existing = RestaurantPOI(
            id="hungerstation:dupe-listing",
            name="Stale Name",
            category="international",
            source="hungerstation",
            lat=24.0,
            lon=46.0,
            rating=2.0,
            review_count=10,
            observed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        db.add(existing)
        db.commit()
        assert _poi_count(db) == 1

        run = _make_run(db, platform="hungerstation")
        _make_record(
            db,
            run_id=run.id,
            source_listing_id="dupe-listing",
            name="Fresh Name",
            cuisine_raw="burger",
            rating=4.5,
            rating_count=200,
        )
        db.commit()

        n = upsert_restaurant_poi_from_delivery(db)
        # Function counts both inserts and updates as upserts.
        assert n == 1
        # No duplicate row.
        assert _poi_count(db) == 1

        refreshed = db.query(RestaurantPOI).filter_by(
            id="hungerstation:dupe-listing"
        ).one()
        assert refreshed.name == "Fresh Name"
        # _upsert_poi update path refreshes rating / review_count / observed_at.
        assert float(refreshed.rating) == pytest.approx(4.5)
        assert refreshed.review_count == 200
        # _upsert_poi update path does NOT change lat/lon — preserved from original.
        assert float(refreshed.lat) == pytest.approx(24.0)
        assert float(refreshed.lon) == pytest.approx(46.0)
