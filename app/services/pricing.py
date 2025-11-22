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


def price_from_aqar(db: Session, city: str, district: Optional[str]) -> Optional[Tuple[float, str]]:
    """
    Return SAR/m² from the Kaggle aqar.fm dataset aggregated in Postgres.
    Prefers district-level, falls back to city-level. Assumes the view:
      aqar.mv_city_month_price_per_sqm(month, city, district, property_type, price_per_sqm)
    and that land listings map to Arabic/English variants ('أرض','ارض','land').
    """
    # 1) District-level (if column exists in the view)
    if district:
        try:
            row = db.execute(
                text(
                    """
                    SELECT price_per_sqm
                    FROM aqar.mv_city_month_price_per_sqm
                    WHERE lower(city) = lower(:city)
                      AND lower(coalesce(district, '')) = lower(:district)
                      AND lower(property_type) IN ('أرض','ارض','land')
                    ORDER BY month DESC
                    LIMIT 1
                    """
                ),
                {"city": city, "district": district},
            ).first()
            if row and row[0] is not None:
                return float(row[0]), "aqar.mv_city_month_price_per_sqm (district)"
        except Exception:
            # If the view doesn't have district or any other SQL issue, silently fall through.
            pass

    # 2) City-level
    row = db.execute(
        text(
            """
            SELECT price_per_sqm
            FROM aqar.mv_city_month_price_per_sqm
            WHERE lower(city) = lower(:city)
              AND lower(property_type) IN ('أرض','ارض','land')
            ORDER BY month DESC
            LIMIT 1
        """
        ),
        {"city": city},
    ).first()
    if not row or row[0] is None:
        return None
    return float(row[0]), "aqar.mv_city_month_price_per_sqm"


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
