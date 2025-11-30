from typing import Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session


def infer_district_from_kaggle(
    db: Session,
    lon: float,
    lat: float,
    city: Optional[str] = None,
    max_radius_m: float = 2000.0,
) -> Tuple[Optional[str], Optional[float]]:
    """
    Find the nearest Kaggle listing to (lon, lat) and return its district.

    Returns:
        (district, distance_m) or (None, None) if nothing suitable found.
    """

    params = {"lon": lon, "lat": lat, "city": city}

    sql = """
    SELECT district,
           ST_DistanceSphere(
               ST_SetSRID(ST_MakePoint(:lon, :lat), 4326),
               ST_SetSRID(ST_MakePoint(lon, lat), 4326)
           ) AS dist_m
    FROM aqar.listings
    WHERE price_per_sqm IS NOT NULL
      AND lat IS NOT NULL
      AND lon IS NOT NULL
      AND (:city IS NULL OR lower(city) = lower(:city))
    ORDER BY dist_m
    LIMIT 1
    """

    row = db.execute(text(sql), params).first()
    if not row:
        return None, None

    district, dist_m = row[0], row[1]
    if max_radius_m is not None and dist_m is not None and dist_m > max_radius_m:
        # Too far away – treat as no match
        return None, dist_m

    return district, dist_m
