from __future__ import annotations

import argparse
import json
import statistics
from typing import Iterable, List, Sequence, Tuple

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.ml.name_normalization import norm_city, norm_district
from app.models.tables import ExternalFeature

LAYER_NAME = "aqar_district_hulls"
SOURCE = "aqar:listings_convex_hull"


def _matching_cities(db: Session, target_city_norm: str) -> List[str]:
    rows = db.execute(
        text("SELECT DISTINCT city FROM aqar.listings WHERE city IS NOT NULL")
    )
    return [city for (city,) in rows if norm_city(city) == target_city_norm]


def _fetch_hulls(
    db: Session, city_values: Sequence[str], min_points: int
) -> Iterable[dict]:
    if not city_values:
        return []

    hull_sql = text(
        """
        SELECT
            district AS district_raw,
            COUNT(*) AS n_points,
            ST_AsGeoJSON(ST_ConvexHull(ST_Collect(point))) AS geometry
        FROM (
            SELECT
                district,
                ST_SetSRID(ST_MakePoint(lon, lat), 4326) AS point
            FROM aqar.listings
            WHERE lon IS NOT NULL
              AND lat IS NOT NULL
              AND city IN :city_values
        ) pts
        WHERE district IS NOT NULL
          AND district <> ''
        GROUP BY district
        HAVING COUNT(*) >= :min_points
        """
    ).bindparams(
        bindparam("city_values", value=list(city_values), expanding=True),
        bindparam("min_points", value=min_points),
    )

    return db.execute(hull_sql).mappings()


def ingest_aqar_district_hulls(
    db: Session, city_name: str, min_points: int
) -> Tuple[int, int, List[int]]:
    target_city_norm = norm_city(city_name) or "riyadh"

    db.query(ExternalFeature).filter_by(layer_name=LAYER_NAME).delete()

    city_values = _matching_cities(db, target_city_norm)
    rows = _fetch_hulls(db, city_values, min_points)

    inserted = 0
    districts = set()
    point_counts: List[int] = []

    for row in rows:
        district_raw = str(row["district_raw"])
        n_points = int(row["n_points"])
        geometry = json.loads(row["geometry"])
        district_norm = norm_district(target_city_norm, district_raw)

        db.add(
            ExternalFeature(
                layer_name=LAYER_NAME,
                feature_type="polygon",
                geometry=geometry,
                properties={
                    "district": district_norm,
                    "district_raw": district_raw,
                    "n_points": n_points,
                },
                source=SOURCE,
            )
        )

        inserted += 1
        point_counts.append(n_points)
        if district_norm:
            districts.add(district_norm)

    db.commit()

    return inserted, len(districts), point_counts


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build convex hull polygons per district from aqar.listings"
    )
    parser.add_argument(
        "--city",
        default="Riyadh",
        help="City to ingest (matched via norm_city); defaults to Riyadh",
    )
    parser.add_argument(
        "--min-points",
        dest="min_points",
        type=int,
        default=50,
        help="Minimum points required per district",
    )
    args = parser.parse_args()

    with SessionLocal() as db:
        polygons, distinct, point_counts = ingest_aqar_district_hulls(
            db, args.city, args.min_points
        )

    if point_counts:
        median_points = statistics.median(point_counts)
        summary_stats = (
            f"min/median/max n_points: {min(point_counts)}/{median_points}/{max(point_counts)}"
        )
    else:
        summary_stats = "min/median/max n_points: n/a"

    print(
        f"Ingested {polygons} district hull polygons into external_feature "
        f"(distinct districts: {distinct}; {summary_stats})"
    )


if __name__ == "__main__":
    main()
