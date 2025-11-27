from datetime import date
from typing import Any, Dict, Optional, Tuple

from sqlalchemy.orm import Session

from app.services.comps import fetch_sale_comps, summarize_ppm2
from app.services.hedonic_model import predict_ppm2


def land_price_per_m2(
    db: Session,
    city: str = "Riyadh",
    *,
    since: Optional[date] = None,
    district: Optional[str] = None,
) -> Tuple[Optional[float], Dict[str, Any]]:
    """
    Land price per m² using the Kaggle hedonic model as primary source,
    with Kaggle sale comps as fallback when the model is unavailable.
    """

    model_ppm2, model_meta = predict_ppm2(city=city, district=district, on=since)

    comps = fetch_sale_comps(
        db,
        city=city,
        district=district,
        since=since,
        source="kaggle_aqar",
        property_type="land",
    )
    median_ppm2 = summarize_ppm2(comps)

    method = "none"
    ppm2: Optional[float] = None

    # NEW LOGIC: prefer hedonic; use comps only as fallback
    if model_ppm2 is not None:
        ppm2 = float(model_ppm2)
        method = "kaggle_hedonic_v0"
    elif median_ppm2 is not None and len(comps) >= 5:
        ppm2 = float(median_ppm2)
        method = "kaggle_comps_median"
    elif median_ppm2 is not None:
        ppm2 = float(median_ppm2)
        method = "kaggle_comps_median_thin"

    meta = {
        "city": city,
        "district": district,
        "since": since.isoformat() if since else None,
        "n_comps": len(comps),
        "median_ppm2": float(median_ppm2) if median_ppm2 is not None else None,
        "method": method,
        "model": model_meta,
    }

    return ppm2, meta
