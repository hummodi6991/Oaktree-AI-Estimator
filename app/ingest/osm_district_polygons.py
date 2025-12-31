from __future__ import annotations

import json
from typing import Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.ml.name_normalization import norm_district
from app.models.tables import ExternalFeature


OSM_DISTRICT_SQL = text(
    """
    SELECT
        name,
        place,
        ST_AsGeoJSON(way) AS geometry
    FROM planet_osm_polygon
    WHERE place = 'neighbourhood'
      AND name IS NOT NULL
    """
)


def ingest_osm_district_polygons(db: Session) -> Tuple[int, int]:
    """
    Ingest Riyadh district polygons from OSM into external_feature.

    Returns:
        A tuple of (polygons_inserted, distinct_district_norm_count).
    """
    db.query(ExternalFeature).filter_by(layer_name="osm_districts").delete()

    rows = db.execute(OSM_DISTRICT_SQL).mappings()

    inserted = 0
    districts = set()

    for row in rows:
        district_raw = str(row["name"])
        district_norm = norm_district("riyadh", district_raw)
        place = str(row["place"]) if row.get("place") is not None else None

        geometry = json.loads(row["geometry"])

        db.add(
            ExternalFeature(
                layer_name="osm_districts",
                feature_type="polygon",
                geometry=geometry,
                properties={
                    "district": district_norm,
                    "district_raw": district_raw,
                    "place": place,
                },
                source="osm:planet_osm_polygon",
            )
        )

        inserted += 1
        if district_norm:
            districts.add(district_norm)

    db.commit()

    return inserted, len(districts)


def main() -> None:
    with SessionLocal() as db:
        polygons, distinct = ingest_osm_district_polygons(db)
        print(
            f"Ingested {polygons} OSM district polygons into external_feature "
            f"(distinct districts: {distinct})"
        )


if __name__ == "__main__":
    main()
