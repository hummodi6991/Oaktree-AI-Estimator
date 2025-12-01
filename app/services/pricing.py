from __future__ import annotations

from typing import Any, Dict, Optional, Tuple
from datetime import datetime
import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.models.tables import PriceQuote
from app.services.comps import fetch_sale_comps, summarize_ppm2
from app.services.hedonic import land_price_per_m2
from app.services.kaggle_district import infer_district_from_kaggle

logger = logging.getLogger(__name__)


def price_from_srem(db: Session, city: str, district: Optional[str]) -> Optional[Tuple[float, str]]:
    """Return a SAR/m² estimate from SREM-fed comps."""

    comps = fetch_sale_comps(db, city=city, district=district, since=None, limit=200)
    ppm2 = summarize_ppm2(comps)
    if ppm2 is None:
        return None
    return float(ppm2), "SREM/REGA comps median"


def price_from_suhail(db: Session, city: str, district: Optional[str]) -> Optional[Tuple[float, str]]:
    """Placeholder for the Suhail provider until the API is wired."""

    return None


def price_from_aqar(db: Session, city: str | None, district: str | None):
    """
    Return (value, method) where value is SAR/m² and method describes the source.

    Uses ONLY the Kaggle aqar.fm dataset:
      1) District-level median for 'land' from aqar.mv_city_price_per_sqm
      2) City-level median from aqar.listings (land-only filter)
    """
    if not city:
        return None

    # 1) Try district-level median in the materialized view
    try:
        val = db.execute(
            text(
                """
                SELECT price_per_sqm
                FROM aqar.mv_city_price_per_sqm
                WHERE lower(city)=lower(:city)
                  AND property_type='land'
                  AND (:district IS NULL OR lower(district)=lower(:district))
                ORDER BY
                  CASE
                    WHEN :district IS NOT NULL AND lower(district)=lower(:district)
                    THEN 0 ELSE 1
                  END,
                  n DESC NULLS LAST
                LIMIT 1
                """
            ),
            {"city": city, "district": district},
        ).scalar()
    except SQLAlchemyError as exc:
        logger.warning("aqar.mv_city_price_per_sqm query failed: %s", exc)
        val = None

    if val is not None:
        return float(val), "aqar.mv_city_price_per_sqm"

    # 2) Fallback: direct city median from aqar.listings (still Kaggle data)
    try:
        val = db.execute(
            text(
                """
                SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY price_per_sqm)
                FROM aqar.listings
                WHERE lower(city)=lower(:city)
                  AND price_per_sqm IS NOT NULL
                  AND (
                    lower(property_type) ~ '\\m(أرض|ارض|land|plot)\\M'
                    OR lower(coalesce(title,'')) ~ '\\m(أرض|ارض|land|plot)\\M'
                    OR lower(coalesce(description,'')) ~ '\\m(أرض|ارض|land|plot)\\M'
                  )
                """
            ),
            {"city": city},
        ).scalar()
    except SQLAlchemyError as exc:
        logger.warning("aqar.listings median query failed: %s", exc)
        val = None

    if val is not None:
        return float(val), "aqar.listings_median_fallback"

    # Nothing usable from Kaggle
    return None


def price_from_kaggle_hedonic(
    db: Session,
    *,
    city: Optional[str],
    lon: Optional[float] = None,
    lat: Optional[float] = None,
    district: Optional[str] = None,
) -> Tuple[Optional[float], str, Dict[str, Any]]:
    inferred_district: Optional[str] = None
    inferred_distance: Optional[float] = None
    hedonic_meta: Optional[Dict[str, Any]] = None

    if not city:
        return None, "kaggle_hedonic_v0", {
            "source": "kaggle_hedonic_v0",
            "district": district,
            "inferred_district": inferred_district,
            "distance_m": inferred_distance,
            "hedonic_meta": hedonic_meta,
        }

    # Try to infer a district from Kaggle listings when we only have coords
    if district is None and lon is not None and lat is not None:
        try:
            inferred_district, inferred_distance = infer_district_from_kaggle(
                db, lon=lon, lat=lat, city=city
            )
            if inferred_district:
                district = inferred_district
        except Exception as exc:  # noqa: BLE001 - Keep the API robust
            # Keep the API robust: if inference fails, just fall back to city-only
            logger.warning("infer_district_from_kaggle failed: %s", exc)

    ppm2, hedonic_meta = land_price_per_m2(db, city=city, since=None, district=district)
    value = float(ppm2) if ppm2 is not None else None

    meta: Dict[str, Any] = {
        "source": "kaggle_hedonic_v0",
        "district": district,
        "inferred_district": inferred_district,
        "distance_m": inferred_distance,
        "hedonic_meta": hedonic_meta,
    }
    return value, "kaggle_hedonic_v0", meta


def store_quote(
    db: Session,
    provider: str,
    city: str,
    district: Optional[str],
    parcel_id: Optional[str],
    sar_per_m2: float,
    method: str,
    url: Optional[str] = None,
) -> None:
    """Persist a pricing quote for auditing purposes."""

    quote = PriceQuote(
        provider=provider,
        city=city,
        district=district,
        parcel_id=parcel_id,
        sar_per_m2=sar_per_m2,
        observed_at=datetime.utcnow(),
        method=method,
        source_url=url,
    )
    db.add(quote)
    db.commit()
