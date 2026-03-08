"""
SQLAlchemy models for the delivery data pipeline.

These tables live alongside the existing ``restaurant_poi`` table but store
raw per-platform records independently.  A record in ``delivery_source_record``
is never required to have coordinates — it is valuable even with only district,
brand, cuisine, or rating data.
"""

from __future__ import annotations

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

from app.models.base import Base


class DeliverySourceRecord(Base):
    """Raw per-platform listing.  One row per scraped record per run."""

    __tablename__ = "delivery_source_record"

    id = Column(Integer, primary_key=True, autoincrement=True)
    platform = Column(String(32), nullable=False)
    source_listing_id = Column(String(256))
    source_url = Column(Text)
    scraped_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )
    city = Column(String(64), nullable=False, server_default=text("'riyadh'"))

    # Location
    district_text = Column(String(256))
    area_text = Column(String(256))
    address_raw = Column(Text)
    lat = Column(Numeric(10, 7))
    lon = Column(Numeric(10, 7))
    geocode_method = Column(String(32), server_default=text("'none'"))
    location_confidence = Column(Float, server_default=text("0.0"))

    # Restaurant identity
    restaurant_name_raw = Column(String(512))
    restaurant_name_normalized = Column(String(512))
    brand_raw = Column(String(256))
    branch_raw = Column(String(256))

    # Category / cuisine
    cuisine_raw = Column(String(256))
    category_raw = Column(String(256))
    category_confidence = Column(Float, server_default=text("0.0"))

    # Pricing & ratings
    price_band_raw = Column(String(32))
    rating = Column(Numeric(3, 2))
    rating_count = Column(Integer)

    # Delivery details
    delivery_time_min = Column(Integer)
    delivery_fee = Column(Numeric(8, 2))
    minimum_order = Column(Numeric(8, 2))

    # Status & availability
    promo_text = Column(Text)
    availability_text = Column(String(256))
    is_open_now_raw = Column(Boolean)

    # Contact
    phone_raw = Column(String(64))
    website_raw = Column(Text)
    menu_url = Column(Text)

    # Raw payload
    raw_payload = Column(JSONB)

    # Ingest tracking
    ingest_run_id = Column(Integer)
    parse_confidence = Column(Float, server_default=text("0.0"))

    # Entity resolution
    entity_resolution_status = Column(
        String(16), server_default=text("'pending'")
    )
    matched_restaurant_poi_id = Column(String(128))
    matched_entity_confidence = Column(Float)

    __table_args__ = (
        Index("ix_dsr_platform", "platform"),
        Index("ix_dsr_platform_scraped", "platform", "scraped_at"),
        Index("ix_dsr_name_normalized", "restaurant_name_normalized"),
        Index("ix_dsr_district", "district_text"),
        Index("ix_dsr_resolution_status", "entity_resolution_status"),
        Index("ix_dsr_matched_poi", "matched_restaurant_poi_id"),
        Index("ix_dsr_ingest_run", "ingest_run_id"),
        Index("ix_dsr_brand", "brand_raw"),
    )


class DeliveryIngestRun(Base):
    """Tracks each scraper execution."""

    __tablename__ = "delivery_ingest_run"

    id = Column(Integer, primary_key=True, autoincrement=True)
    platform = Column(String(32), nullable=False)
    started_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )
    finished_at = Column(DateTime(timezone=True))
    status = Column(String(16), nullable=False, server_default=text("'running'"))
    rows_scraped = Column(Integer, server_default=text("0"))
    rows_parsed = Column(Integer, server_default=text("0"))
    rows_inserted = Column(Integer, server_default=text("0"))
    rows_updated = Column(Integer, server_default=text("0"))
    rows_skipped = Column(Integer, server_default=text("0"))
    rows_matched = Column(Integer, server_default=text("0"))
    error_summary = Column(JSONB)

    __table_args__ = (
        Index("ix_dir_platform_started", "platform", "started_at"),
    )
