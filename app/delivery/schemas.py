"""
Pydantic schemas for the delivery data pipeline.

Every scraper must produce ``DeliveryRecord`` instances instead of ad-hoc
dicts.  This enforces a consistent structure and makes downstream processing
(raw storage, resolution, feature extraction) reliable.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class Platform(str, Enum):
    HUNGERSTATION = "hungerstation"
    TALABAT = "talabat"
    MRSOOL = "mrsool"
    JAHEZ = "jahez"
    TOYOU = "toyou"
    KEETA = "keeta"
    THECHEFZ = "thechefz"
    LUGMETY = "lugmety"
    SHGARDI = "shgardi"
    NINJA = "ninja"
    NANA = "nana"
    DAILYMEALZ = "dailymealz"
    CAREEMFOOD = "careemfood"
    DELIVEROO = "deliveroo"


class GeocodeMethod(str, Enum):
    """How coordinates were obtained."""
    PLATFORM_PAYLOAD = "platform_payload"
    JSON_LD = "json_ld"
    ADDRESS_GEOCODE = "address_geocode"
    DISTRICT_CENTROID = "district_centroid"
    POI_MATCH = "poi_match"
    NONE = "none"


class EntityResolutionStatus(str, Enum):
    PENDING = "pending"
    MATCHED = "matched"
    UNMATCHED = "unmatched"
    AMBIGUOUS = "ambiguous"


class DeliveryRecord(BaseModel):
    """Structured output from a platform scraper/parser."""

    model_config = {"use_enum_values": True}

    platform: Platform
    source_listing_id: Optional[str] = None
    source_url: Optional[str] = None
    scraped_at: datetime = Field(default_factory=lambda: datetime.utcnow())
    city: str = "riyadh"

    # Location
    district_text: Optional[str] = None
    area_text: Optional[str] = None
    address_raw: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    geocode_method: GeocodeMethod = GeocodeMethod.NONE
    location_confidence: float = 0.0  # 0.0 - 1.0

    # Restaurant identity
    restaurant_name_raw: Optional[str] = None
    restaurant_name_normalized: Optional[str] = None
    brand_raw: Optional[str] = None
    branch_raw: Optional[str] = None

    # Category & cuisine
    cuisine_raw: Optional[str] = None
    category_raw: Optional[str] = None
    category_confidence: float = 0.0

    # Pricing & ratings
    price_band_raw: Optional[str] = None
    rating: Optional[float] = None
    rating_count: Optional[int] = None

    # Delivery details
    delivery_time_min: Optional[int] = None
    delivery_fee: Optional[float] = None
    minimum_order: Optional[float] = None

    # Status & availability
    promo_text: Optional[str] = None
    availability_text: Optional[str] = None
    is_open_now_raw: Optional[bool] = None

    # Contact
    phone_raw: Optional[str] = None
    website_raw: Optional[str] = None
    menu_url: Optional[str] = None

    # Raw payload for debugging
    raw_payload: Optional[dict[str, Any]] = None

    # Confidence
    parse_confidence: float = 0.0  # 0.0 - 1.0


class IngestRunStats(BaseModel):
    """Statistics for a single ingest run."""
    platform: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    status: str = "running"
    rows_scraped: int = 0
    rows_parsed: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_skipped: int = 0
    rows_matched: int = 0
    error_summary: Optional[dict[str, Any]] = None
