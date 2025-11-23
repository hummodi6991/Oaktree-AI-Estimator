from typing import Optional, Tuple
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models.tables import PriceQuote
from app.services.comps import fetch_sale_comps, summarize_ppm2


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
    Return (value, method) where value is SAR/m2 and method describes the source.
    Prefers district-level median for 'land' from aqar.mv_city_price_per_sqm.
    Falls back to city-level, then a direct median from aqar.listings.
    """
    if not city:
        return None

    # 1) Try district-level median in the materialized view
    val = db.execute(text("""
        SELECT price_per_sqm
        FROM aqar.mv_city_price_per_sqm
        WHERE lower(city)=lower(:city)
          AND property_type='land'
          AND (:district IS NULL OR lower(district)=lower(:district))
        ORDER BY (CASE WHEN lower(district)=lower(:district) THEN 0 ELSE 1 END), n DESC NULLS LAST
        LIMIT 1
    """), {"city": city, "district": district}).scalar()
    if val:
        return float(val), "aqar.mv_city_price_per_sqm"

    # 2) Fallback: direct city median from aqar.listings (no month)
    val = db.execute(text("""
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY price_per_sqm)
        FROM aqar.listings
        WHERE lower(city)=lower(:city)
          AND price_per_sqm IS NOT NULL
          AND (
            lower(property_type) ~ '\\m(أرض|ارض|land|plot)\\M'
            OR lower(coalesce(title,'')) ~ '\\m(أرض|ارض|land|plot)\\M'
            OR lower(coalesce(description,'')) ~ '\\m(أرض|ارض|land|plot)\\M'
          )
    """), {"city": city}).scalar()
    if val:
        return float(val), "aqar.listings_median_fallback"
    return None


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
