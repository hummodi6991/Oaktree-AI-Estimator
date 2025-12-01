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
    Look up the nearest Kaggle listing and return (district, distance_m).

    We first try to constrain by city; if that finds nothing we fall back
    to a global nearest-neighbour search (no city filter). If the closest
    listing is farther than `max_radius_m`, we return (None, distance_m).
    """

    base_sql = """
        SELECT district,
               ST_DistanceSphere(
                   ST_SetSRID(ST_MakePoint(:lon, :lat), 4326),
                   ST_SetSRID(ST_MakePoint(lon, lat), 4326)
               ) AS dist_m
        FROM aqar.listings
        WHERE price_per_sqm IS NOT NULL
          AND lat IS NOT NULL
          AND lon IS NOT NULL
    """
    order_clause = " ORDER BY dist_m ASC LIMIT 1"

    params = {"lon": lon, "lat": lat}

    row = None

    # 1) Try with city filter if we have a city
    if city:
        row = db.execute(
            text(base_sql + " AND lower(city) = lower(:city)" + order_clause),
            {**params, "city": city},
        ).first()

    # 2) Fall back to nearest listing globally if city-constrained search
    #    finds nothing (or if no city was supplied)
    if not row:
        row = db.execute(text(base_sql + order_clause), params).first()

    if not row or row.dist_m is None:
        return None, None

    dist_m = float(row.dist_m)

    # If the closest listing is too far away, still report the distance
    # but don't trust the district.
    if max_radius_m is not None and dist_m > max_radius_m:
        return None, dist_m

    return row.district, dist_m
