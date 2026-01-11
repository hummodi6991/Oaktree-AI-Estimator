import os

from dotenv import load_dotenv

load_dotenv()


class Settings:
    APP_ENV: str = os.getenv("APP_ENV", "local")
    APP_NAME: str = os.getenv("APP_NAME", "oaktree-estimator")
    DB_USER: str = os.getenv("POSTGRES_USER", "oaktree")
    DB_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "devpass")
    DB_NAME: str = os.getenv("POSTGRES_DB", "oaktree")
    DB_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    DB_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))

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

    # Parcels identify service configuration
    PARCEL_TARGET_SRID: int = int(os.getenv("PARCEL_TARGET_SRID", "4326"))
    PARCEL_IDENTIFY_TOLERANCE_M: float = float(
        os.getenv("PARCEL_IDENTIFY_TOLERANCE_M", "25.0")
    )
    PARCEL_IDENTIFY_TABLE: str = os.getenv("PARCEL_IDENTIFY_TABLE", "public.ms_buildings_raw")
    PARCEL_IDENTIFY_GEOM_COLUMN: str = os.getenv("PARCEL_IDENTIFY_GEOM_COLUMN", "geom")
    PARCEL_TILE_TABLE: str = os.getenv("PARCEL_TILE_TABLE", "public.ms_buildings_raw")


settings = Settings()
