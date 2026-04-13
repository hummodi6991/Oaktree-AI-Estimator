import os

from dotenv import load_dotenv

# IMPORTANT:
# In CI, loading .env from the repo can override Settings defaults and break tests.
# GitHub Actions sets CI=true, so we skip dotenv there.
_CI = os.getenv("CI", "").strip().lower() in {"1", "true", "yes"}
if not _CI:
    load_dotenv()


class Settings:
    APP_ENV: str = os.getenv("APP_ENV", "local")
    APP_NAME: str = os.getenv("APP_NAME", "oaktree-estimator")
    DB_USER: str = os.getenv("POSTGRES_USER", "oaktree")
    DB_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "devpass")
    DB_NAME: str = os.getenv("POSTGRES_DB", "oaktree")
    DB_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    DB_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))

    # --- Parcel source tables (ArcGIS is the default) ---
    # These are used by:
    # - /v1/tiles/parcels (outlines)
    # - /v1/geo/identify (click selection)
    # ArcGIS proxy view exposes: id, geom(4326), area_m2, perimeter_m, landuse_* fields.
    PARCEL_TILE_TABLE: str = os.getenv(
        "PARCEL_TILE_TABLE", "public.riyadh_parcels_arcgis_proxy"
    )
    PARCEL_IDENTIFY_TABLE: str = os.getenv(
        "PARCEL_IDENTIFY_TABLE", "public.riyadh_parcels_arcgis_proxy"
    )
    PARCEL_IDENTIFY_GEOM_COLUMN: str = os.getenv("PARCEL_IDENTIFY_GEOM_COLUMN", "geom")

    # --- External data & APIs (env-driven) ---
    # ArcGIS (البوابة المكانية) parcels/zoning
    ARCGIS_BASE_URL: str | None = os.getenv("ARCGIS_BASE_URL")
    ARCGIS_PARCEL_LAYER: int | None = (
        int(os.getenv("ARCGIS_PARCEL_LAYER")) if os.getenv("ARCGIS_PARCEL_LAYER") else None
    )
    ARCGIS_TOKEN: str | None = os.getenv("ARCGIS_TOKEN")

    # SAMA rates (open-data JSON endpoint)
    SAMA_OPEN_JSON: str | None = os.getenv("SAMA_OPEN_JSON")

    # REGA / SREM indicators (one or more CSV URLs; comma-separated)
    REGA_CSV_URLS: list[str] = [
        u.strip() for u in os.getenv("REGA_CSV_URLS", "").split(",") if u.strip()
    ]

    # Suhail (licensed partner API)
    SUHAIL_API_URL: str | None = os.getenv("SUHAIL_API_URL")
    SUHAIL_API_KEY: str | None = os.getenv("SUHAIL_API_KEY")

    # Restaurant Location Finder — optional API keys for enrichment
    GOOGLE_PLACES_API_KEY: str | None = os.getenv("GOOGLE_PLACES_API_KEY")
    FOURSQUARE_API_KEY: str | None = os.getenv("FOURSQUARE_API_KEY")

    # Parcels identify service configuration
    PARCEL_TARGET_SRID: int = int(os.getenv("PARCEL_TARGET_SRID", "4326"))
    PARCEL_IDENTIFY_TOLERANCE_M: float = float(
        os.getenv("PARCEL_IDENTIFY_TOLERANCE_M", "25.0")
    )
    PARCEL_ENVELOPE_PAD_M: float = float(os.getenv("PARCEL_ENVELOPE_PAD_M", "5.0"))
    PARCEL_SIMPLIFY_TOLERANCE_M: float = float(
        os.getenv("PARCEL_SIMPLIFY_TOLERANCE_M", "1.0")
    )

    # --- Expansion Advisor normalized tables ---
    EXPANSION_ROADS_TABLE: str = os.getenv("EXPANSION_ROADS_TABLE", "expansion_road_context")
    EXPANSION_PARKING_TABLE: str = os.getenv("EXPANSION_PARKING_TABLE", "expansion_parking_asset")
    EXPANSION_DELIVERY_TABLE: str = os.getenv("EXPANSION_DELIVERY_TABLE", "expansion_delivery_market")
    EXPANSION_RENT_TABLE: str = os.getenv("EXPANSION_RENT_TABLE", "expansion_rent_comp")
    EXPANSION_COMPETITOR_TABLE: str = os.getenv(
        "EXPANSION_COMPETITOR_TABLE", "expansion_competitor_quality"
    )

    # --- Realized demand (rating_count Δ) signal ---
    # When enabled AND the ``expansion_delivery_rating_history`` table has
    # ≥2 snapshots for the candidate's catchment, the service layer blends a
    # realized-demand score (rating_count growth per category per radius over
    # the last N days) into the supply-based _delivery_score().  Default OFF
    # so behavior is unchanged until history has accumulated.
    EXPANSION_REALIZED_DEMAND_ENABLED: bool = (
        os.getenv("EXPANSION_REALIZED_DEMAND_ENABLED", "").strip().lower()
        in {"1", "true", "yes", "on"}
    )
    EXPANSION_REALIZED_DEMAND_WINDOW_DAYS: int = int(
        os.getenv("EXPANSION_REALIZED_DEMAND_WINDOW_DAYS", "30")
    )
    EXPANSION_REALIZED_DEMAND_RADIUS_M: int = int(
        os.getenv("EXPANSION_REALIZED_DEMAND_RADIUS_M", "1200")
    )
    # Weight given to realized-demand vs listing-count when both are available.
    # 0.5 = equal blend; 1.0 = realized-demand only; 0.0 = listing-count only.
    EXPANSION_REALIZED_DEMAND_BLEND: float = float(
        os.getenv("EXPANSION_REALIZED_DEMAND_BLEND", "0.5")
    )


settings = Settings()
