from typing import Optional, Tuple
from datetime import datetime

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
