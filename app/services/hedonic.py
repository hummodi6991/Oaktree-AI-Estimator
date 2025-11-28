from datetime import date
from typing import Any, Dict, Optional, Tuple

from sqlalchemy.orm import Session

from app.services.hedonic_model import predict_ppm2


def land_price_per_m2(
    db: Session,
    city: str = "Riyadh",
    *,
    since: Optional[date] = None,
    district: Optional[str] = None,
) -> Tuple[Optional[float], Dict[str, Any]]:
    """
    Land price per m² using the Kaggle hedonic model only.
    Other providers / comps are disabled for now.
    """

    ppm2, model_meta = predict_ppm2(city=city, district=district, on=since)

    meta: Dict[str, Any] = {
        "city": city,
        "district": district,
        "since": since.isoformat() if since else None,
        "method": "kaggle_hedonic_v0",
        "n_comps": 0,
        "median_ppm2": None,
        "model": model_meta,
    }

    return ppm2, meta
