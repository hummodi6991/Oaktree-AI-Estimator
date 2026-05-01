from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Index,
    Integer,
    JSON,
    Numeric,
    SmallInteger,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

from app.models.base import Base


class Rate(Base):
    __tablename__ = "rates"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    tenor = Column(String(16), nullable=False)
    rate_type = Column(String(32), nullable=False)
    value = Column(Numeric(6, 3), nullable=False)
    source_url = Column(String(512))

    __table_args__ = (Index("ix_rate_date_type_tenor", "date", "rate_type", "tenor"),)


class SaleComp(Base):
    __tablename__ = "sale_comp"

    id = Column(String(64), primary_key=True)
    date = Column(Date, nullable=False)
    city = Column(String(64), nullable=False)
    district = Column(String(128))
    asset_type = Column(String(32), nullable=False)
    net_area_m2 = Column(Numeric(14, 2))
    price_total = Column(Numeric(14, 2))
    price_per_m2 = Column(Numeric(12, 2))
    source = Column(String(64))
    source_url = Column(String(512))
    asof_date = Column(Date)


class RentComp(Base):
    __tablename__ = "rent_comp"

    id = Column(String(64), primary_key=True)
    date = Column(Date, nullable=False)
    city = Column(String(64), nullable=False)
    district = Column(String(128))
    asset_type = Column(String(32), nullable=False)
    unit_type = Column(String(32))
    lease_term_months = Column(Integer)
    rent_per_unit = Column(Numeric(12, 2))
    rent_per_m2 = Column(Numeric(12, 2))
    source = Column(String(64))
    source_url = Column(String(512))
    asof_date = Column(Date)


class Parcel(Base):
    __tablename__ = "parcel"

    id = Column(String(64), primary_key=True)
    gis_polygon = Column(JSONB)
    municipality = Column(String(64))
    district = Column(String(128))
    zoning = Column(String(64))
    far = Column(Numeric(6, 3))
    frontage_m = Column(Numeric(10, 2))
    road_class = Column(String(32))
    setbacks = Column(JSONB)
    source_url = Column(String(512))
    asof_date = Column(Date)


class AssumptionLedger(Base):
    __tablename__ = "assumption_ledger"

    id = Column(Integer, primary_key=True)
    estimate_id = Column(String(64), nullable=False)
    line_id = Column(String(64))
    source_type = Column(String(64), nullable=False)
    source_ref = Column(String(128))
    url = Column(String(512))
    value = Column(Numeric(18, 4))
    unit = Column(String(16))
    owner = Column(String(64))
    created_at = Column(DateTime)


class BoqItem(Base):
    __tablename__ = "boq_item"

    code = Column(String(32), primary_key=True)
    description = Column(String(256), nullable=False)
    uom = Column(String(16), nullable=False, default="m2")
    quantity_per_m2 = Column(Numeric(12, 4), nullable=False, default=1.0)
    baseline_unit_cost = Column(Numeric(12, 2), nullable=False)
    city_factor = Column(Numeric(6, 3), nullable=False, default=1.000)
    volatility_tag = Column(String(32))
    source_url = Column(String(512))


class MarketIndicator(Base):
    __tablename__ = "market_indicator"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    city = Column(String(64), nullable=False)
    asset_type = Column(String(32), nullable=False)
    indicator_type = Column(String(32), nullable=False)
    value = Column(Numeric(12, 2), nullable=False)
    unit = Column(String(16), nullable=False)
    source_url = Column(String(512))
    asof_date = Column(Date)


class LandUseStat(Base):
    __tablename__ = "land_use_stat"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    city = Column(String(64), nullable=False)
    sub_municipality = Column(String(128))
    category = Column(String(128))
    metric = Column(String(64))
    unit = Column(String(32))
    value = Column(Numeric(18, 4))
    source_url = Column(String(512))


class LandUseResidentialShare(Base):
    __tablename__ = "land_use_residential_share"

    city = Column(String(64), primary_key=True)
    sub_municipality = Column(String(128), primary_key=True)
    residential_share = Column(Numeric(18, 6))


class SuhailLandMetric(Base):
    __tablename__ = "suhail_land_metrics"

    id = Column(Integer, primary_key=True)
    as_of_date = Column(Date, nullable=False)
    observed_at = Column(
        DateTime,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )
    region_id = Column(Integer, nullable=False)
    province_id = Column(Integer)
    province_name = Column(String(128))
    neighborhood_id = Column(Integer, nullable=False)
    neighborhood_name = Column(String(256), nullable=False)
    district_norm = Column(String(256))
    land_use_group = Column(String(128), nullable=False)
    median_ppm2 = Column(Numeric(12, 2))
    last_price_ppm2 = Column(Numeric(12, 2))
    last_txn_date = Column(Date)
    raw = Column(JSONB)

    __table_args__ = (
        Index(
            "ux_suhail_land_metrics_as_of_region_neighborhood_land_use",
            "as_of_date",
            "region_id",
            "neighborhood_id",
            "land_use_group",
            unique=True,
        ),
        Index(
            "ix_suhail_land_metrics_district_norm",
            "district_norm",
        ),
    )


class EstimateHeader(Base):
    __tablename__ = "estimate_header"

    id = Column(String(36), primary_key=True)
    created_at = Column(DateTime)
    owner = Column(String(64))
    strategy = Column(String(32), nullable=False)
    input_json = Column(Text, nullable=False)
    totals_json = Column(Text, nullable=False)
    notes_json = Column(Text)


class EstimateLine(Base):
    __tablename__ = "estimate_line"

    id = Column(Integer, primary_key=True)
    estimate_id = Column(String(36), nullable=False)
    category = Column(String(32), nullable=False)
    key = Column(String(64), nullable=False)
    value = Column(Numeric(18, 4))
    unit = Column(String(16))
    source_type = Column(String(64))
    url = Column(String(512))
    model_version = Column(String(64))
    owner = Column(String(64))
    created_at = Column(DateTime)


class ExternalFeature(Base):
    __tablename__ = "external_feature"

    id = Column(Integer, primary_key=True)
    layer_name = Column(String(128), nullable=False)
    feature_type = Column(String(16), nullable=False)
    geometry = Column(JSONB, nullable=False)
    properties = Column(JSONB)
    source = Column(String(256))


class FarRule(Base):
    __tablename__ = "far_rule"

    id = Column(Integer, primary_key=True)
    city = Column(String(64), nullable=False)
    district = Column(String(128), nullable=False)
    zoning = Column(String(64))
    road_class = Column(String(32))
    frontage_min_m = Column(Numeric(10, 2))
    far_max = Column(Numeric(6, 3), nullable=False)
    asof_date = Column(Date)
    source_url = Column(String(512))


class UsageEvent(Base):
    __tablename__ = "usage_event"

    id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    ts = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    user_id = Column(String(128))
    is_admin = Column(Boolean, nullable=False, server_default=text("false"))
    event_name = Column(String(128))
    method = Column(String(16), nullable=False)
    path = Column(String(512), nullable=False)
    status_code = Column(Integer, nullable=False)
    duration_ms = Column(Integer, nullable=False)
    estimate_id = Column(String(64))
    meta = Column(JSONB().with_variant(JSON, "sqlite"))

    __table_args__ = (
        Index("ix_usage_event_ts", "ts"),
        Index("ix_usage_event_user_ts", "user_id", "ts"),
        Index("ix_usage_event_event_ts", "event_name", "ts"),
        Index("ix_usage_event_path_ts", "path", "ts"),
    )


class PriceQuote(Base):
    __tablename__ = "price_quote"

    id = Column(Integer, primary_key=True)
    provider = Column(String(32), nullable=False)
    city = Column(String(64), nullable=False)
    district = Column(String(128))
    parcel_id = Column(String(64))
    sar_per_m2 = Column(Numeric(12, 2), nullable=False)
    observed_at = Column(DateTime)
    method = Column(String(64))
    source_url = Column(String(512))


class TaxRule(Base):
    """
    Generic tax rules (starting with Saudi RETT).
    Ingested from CSV via /v1/ingest/tax_rules.
    """

    __tablename__ = "tax_rule"

    id = Column(Integer, primary_key=True, index=True)
    rule_id = Column(Integer, nullable=False)
    tax_type = Column(String(32), nullable=False)  # e.g. 'RETT'
    rate = Column(Numeric(6, 4), nullable=False)  # 0.0500 = 5%
    base_type = Column(String(128), nullable=True)  # e.g. 'max(sale_price,fair_market_value)'
    payer_default = Column(String(32), nullable=True)
    exemptions = Column(Text, nullable=True)
    notes = Column(Text, nullable=True)

    __table_args__ = (
        Index("ix_tax_rule_type_rule_id", "tax_type", "rule_id", unique=True),
    )


# ---------------------------------------------------------------------------
# Restaurant Location Finder tables
# ---------------------------------------------------------------------------


class RestaurantPOI(Base):
    """Restaurant point of interest aggregated from multiple data sources."""

    __tablename__ = "restaurant_poi"

    id = Column(String(128), primary_key=True)  # source:external_id
    name = Column(String(256), nullable=False)
    name_ar = Column(String(256))
    category = Column(String(64), nullable=False)  # burger, pizza, traditional, etc.
    subcategory = Column(String(64))
    source = Column(String(32), nullable=False)  # overture, osm, hungerstation, talabat, mrsool
    lat = Column(Numeric(10, 7), nullable=False)
    lon = Column(Numeric(10, 7), nullable=False)
    # PostGIS geometry(Point, 4326) — auto-populated by DB trigger from lat/lon.
    # Managed via raw SQL in the migration; not mapped by GeoAlchemy2.
    rating = Column(Numeric(3, 2))
    review_count = Column(Integer)
    price_level = Column(Integer)  # 1-4
    chain_name = Column(String(128))
    district = Column(String(128))
    raw = Column(JSONB)
    observed_at = Column(DateTime)
    google_place_id = Column(Text)
    google_fetched_at = Column(DateTime(timezone=True))
    google_confidence = Column(Numeric)

    __table_args__ = (
        Index("ix_restaurant_poi_category", "category"),
        Index("ix_restaurant_poi_source", "source"),
        Index("ix_restaurant_poi_district", "district"),
        Index("ix_restaurant_poi_chain_name", "chain_name"),
        Index("ix_restaurant_poi_google_place_id", "google_place_id"),
    )


class PopulationDensity(Base):
    """Population density per H3 hex cell."""

    __tablename__ = "population_density"

    id = Column(Integer, primary_key=True)
    h3_index = Column(String(16), unique=True, nullable=False)
    lat = Column(Numeric(10, 7))
    lon = Column(Numeric(10, 7))
    population = Column(Numeric(10, 1))
    source = Column(String(32))
    observed_at = Column(DateTime)


class DistrictRadianceMonthly(Base):
    """Monthly NASA Black Marble VNP46A3 radiance aggregates per district."""

    __tablename__ = "district_radiance_monthly"

    district_key = Column(Text, primary_key=True, nullable=False)
    year_month = Column(Date, primary_key=True, nullable=False)
    source = Column(String(64), primary_key=True, nullable=False)
    radiance_mean = Column(Numeric(12, 4))
    radiance_median = Column(Numeric(12, 4))
    radiance_sum = Column(Numeric(14, 4))
    radiance_p90 = Column(Numeric(12, 4))
    pixel_count_total = Column(Integer, nullable=False)
    pixel_count_valid = Column(Integer, nullable=False)
    quality_filter = Column(String(32), nullable=False)
    tile = Column(String(16), nullable=False)
    ingested_at = Column(DateTime(timezone=True), server_default=text("NOW()"), nullable=False)


class RestaurantHeatmapCache(Base):
    """Cached city-wide opportunity heatmap payload per category + radius."""

    __tablename__ = "restaurant_heatmap_cache"

    category = Column(Text, primary_key=True)
    radius_m = Column(Integer, primary_key=True)
    computed_at = Column(DateTime(timezone=True), nullable=False)
    payload = Column(JSONB, nullable=False)


class GeocodeCache(Base):
    """Cached Google Maps geocoding results keyed by query string."""

    __tablename__ = "geocode_cache"

    query = Column(String(512), primary_key=True)
    lat = Column(Numeric(10, 7))
    lon = Column(Numeric(10, 7))
    formatted_address = Column(Text)
    raw = Column(JSONB)
    created_at = Column(DateTime, server_default=text("now()"))


class CommercialUnit(Base):
    """Commercial unit listing scraped from Aqar.fm."""

    __tablename__ = "commercial_unit"

    aqar_id = Column(String(64), primary_key=True)
    title = Column(Text)
    description = Column(Text)
    neighborhood = Column(Text)
    listing_url = Column(Text)
    image_url = Column(Text)
    price_sar_annual = Column(Numeric(14, 2))
    price_per_sqm = Column(Numeric(12, 2))
    area_sqm = Column(Numeric(10, 2))
    street_width_m = Column(Numeric(8, 2))
    num_floors = Column(Integer)
    has_mezzanine = Column(Boolean)
    has_drive_thru = Column(Boolean)
    facade_direction = Column(String(32))
    contact_phone = Column(Text)
    listing_type = Column(String(32))  # 'store', 'showroom', 'warehouse', 'building'
    property_type = Column(String(64))  # 'Residential', 'Commercial', etc. — from Aqar's structured field
    is_furnished = Column(Boolean)  # True if Aqar's Features list includes 'Furnished'
    apartments_count = Column(Integer)  # From Aqar's Apartments field; >=2 = residential building
    num_rooms = Column(Integer)  # From Aqar's Rooms field; >=6 on a building = residential/multi-room non-F&B
    lat = Column(Numeric(10, 7))
    lon = Column(Numeric(10, 7))
    restaurant_score = Column(Integer)
    restaurant_suitable = Column(Boolean)
    restaurant_signals = Column(JSONB)
    # LLM-based suitability classifier outputs (Patch 12)
    llm_suitability_verdict = Column(String(16))
    llm_suitability_score = Column(Integer)
    llm_listing_quality_score = Column(Integer)
    llm_landlord_signal_score = Column(Integer)
    llm_reasoning = Column(Text)
    llm_classified_at = Column(DateTime)
    llm_classifier_version = Column(String(32))
    status = Column(String(16), nullable=False, server_default=text("'active'"))
    first_seen_at = Column(DateTime, server_default=text("now()"))
    last_seen_at = Column(DateTime, server_default=text("now()"))
    # Phase 2 detail-page fields (Info block on Aqar listing detail pages).
    # Populated by the detail scraper — nullable because existing rows and
    # newly discovered list-page rows predate the detail-scrape step.
    aqar_created_at = Column(DateTime(timezone=True))
    aqar_updated_at = Column(DateTime(timezone=True))
    aqar_views = Column(Integer)
    aqar_advertisement_license = Column(Text)
    aqar_license_expiry = Column(Date)
    aqar_plan_parcel = Column(Text)
    aqar_area_deed = Column(Numeric(10, 2))
    aqar_listing_source = Column(Text)
    aqar_detail_scraped_at = Column(DateTime(timezone=True))

    __table_args__ = (
        Index("ix_commercial_unit_neighborhood", "neighborhood"),
        Index("ix_commercial_unit_status", "status"),
        Index("ix_commercial_unit_restaurant_suitable", "restaurant_suitable"),
        Index("ix_commercial_unit_property_type", "property_type"),
        Index("ix_commercial_unit_is_furnished", "is_furnished"),
        Index("ix_commercial_unit_apartments_count", "apartments_count"),
        Index("ix_commercial_unit_num_rooms", "num_rooms"),
        Index(
            "ix_commercial_unit_llm_suitability_verdict",
            "llm_suitability_verdict",
        ),
        Index(
            "ix_commercial_unit_llm_classified_at",
            "llm_classified_at",
        ),
        Index(
            "idx_commercial_unit_aqar_created_at",
            aqar_created_at.desc(),
        ),
        Index(
            "idx_commercial_unit_aqar_updated_at",
            aqar_updated_at.desc(),
        ),
        Index(
            "idx_commercial_unit_detail_unscraped",
            "aqar_id",
            postgresql_where=text("aqar_detail_scraped_at IS NULL"),
        ),
    )


class CandidateLocation(Base):
    """Unified candidate location for expansion advisor.

    Merges three tiers:
      Tier 1 (Aqar): Vacant commercial listings with actual rent/area
      Tier 2 (Delivery/POI): Proven restaurant locations (occupied)
      Tier 3 (ArcGIS): Commercial/mixed-use parcels (spatial fallback)
    """

    __tablename__ = "candidate_location"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Source tracking
    source_tier = Column(SmallInteger, nullable=False)
    source_type = Column(String(32), nullable=False)
    source_id = Column(String(256))

    # Location
    lat = Column(Numeric(10, 7), nullable=False)
    lon = Column(Numeric(10, 7), nullable=False)
    # geom is auto-populated by trigger

    # District
    district_ar = Column(String(256))
    district_en = Column(String(256))
    neighborhood_raw = Column(String(256))

    # Unit attributes
    area_sqm = Column(Numeric(10, 2))
    rent_sar_annual = Column(Numeric(14, 2))
    rent_sar_m2_month = Column(Numeric(12, 2))
    rent_confidence = Column(String(24))
    area_confidence = Column(String(24))

    # Listing info
    listing_url = Column(Text)
    listing_type = Column(String(32))
    image_url = Column(Text)

    # Occupancy
    is_vacant = Column(Boolean)
    current_tenant = Column(String(512))
    current_category = Column(String(64))

    # Quality signals
    street_width_m = Column(Numeric(8, 2))
    has_drive_thru = Column(Boolean)
    road_class = Column(String(32))
    landuse_code = Column(Integer)
    landuse_label = Column(String(64))

    # Delivery context
    platform_count = Column(SmallInteger)
    avg_rating = Column(Numeric(3, 2))
    total_rating_count = Column(Integer)
    supports_late_night = Column(Boolean)

    # Clustering
    cluster_id = Column(Integer)
    is_cluster_primary = Column(Boolean, server_default=text("TRUE"))

    # Profitability model
    profitability_score = Column(Numeric(5, 2))
    success_proxy = Column(Numeric(5, 2))
    model_features = Column(JSONB)
    model_version = Column(String(32))
    model_scored_at = Column(DateTime(timezone=True))

    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=text("now()"))
    updated_at = Column(DateTime(timezone=True), server_default=text("now()"))
    population_run_id = Column(String(64))

    __table_args__ = (
        Index("ix_cl_source_tier", "source_tier"),
        Index("ix_cl_source_type_id", "source_type", "source_id"),
        Index("ix_cl_district_ar", "district_ar"),
        Index("ix_cl_is_vacant", "is_vacant"),
        Index("ix_cl_cluster_primary", "is_cluster_primary"),
        Index("ix_cl_current_category", "current_category"),
        Index("ix_cl_rent_confidence", "rent_confidence"),
        Index("ix_cl_profitability", "profitability_score"),
    )


class LocationScore(Base):
    """Pre-computed restaurant location demand-potential score per H3 cell and category."""

    __tablename__ = "location_score"

    id = Column(Integer, primary_key=True)
    parcel_id = Column(String(64))
    h3_index = Column(String(16))
    category = Column(String(64), nullable=False)
    overall_score = Column(Numeric(5, 2))  # 0-100
    demand_score = Column(Numeric(5, 2))  # 0-100 demand-potential component
    cost_penalty = Column(Numeric(5, 2))  # 0-100 cost component (higher = cheaper = better)
    factors = Column(JSONB)  # {competition: 72, traffic: 85, ...}
    model_version = Column(String(32))
    computed_at = Column(DateTime)

    __table_args__ = (
        Index("ix_location_score_category_h3", "category", "h3_index"),
    )
