import json
import logging
from typing import Any, Dict, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

_OVERTURE_BUILDING_METRICS_SQL = text(
    """
    WITH input_geom AS (
      SELECT ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(:gj), 4326), 32638) AS geom
    ),
    buffered AS (
      SELECT
        CASE
          WHEN :buffer_m IS NOT NULL THEN ST_Buffer(geom, :buffer_m)
          ELSE geom
        END AS geom
      FROM input_geom
    ),
    site AS (
      SELECT geom FROM buffered
    ),
    buildings AS (
      SELECT
        ST_Area(ST_Intersection(o.geom, s.geom)) AS footprint_area_m2,
        o.num_floors::float AS num_floors,
        o.height::float AS height
      FROM overture_buildings o
      JOIN site s ON o.geom && s.geom AND ST_Intersects(o.geom, s.geom)
    ),
    floors AS (
      SELECT
        footprint_area_m2,
        CASE
          WHEN num_floors IS NOT NULL THEN num_floors
          WHEN height IS NOT NULL THEN GREATEST(1, LEAST(60, round(height / 3.2)))
          ELSE NULL
        END AS floors_proxy
      FROM buildings
    ),
    agg AS (
      SELECT
        COALESCE(SUM(footprint_area_m2), 0) AS footprint_area_m2,
        COUNT(*) AS building_count,
        COUNT(floors_proxy) AS floors_count,
        AVG(floors_proxy) FILTER (WHERE floors_proxy IS NOT NULL) AS floors_mean,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY floors_proxy) AS floors_median
      FROM floors
    ),
    bua AS (
      SELECT
        COALESCE(
          SUM(
            footprint_area_m2 * COALESCE(
              floors_proxy,
              (SELECT floors_median FROM agg),
              1
            )
          ),
          0
        ) AS existing_bua_m2
      FROM floors
    )
    SELECT
      ST_Area(site.geom) AS site_area_m2,
      agg.footprint_area_m2,
      CASE
        WHEN ST_Area(site.geom) > 0 THEN agg.footprint_area_m2 / ST_Area(site.geom)
        ELSE NULL
      END AS coverage_ratio,
      agg.floors_mean,
      agg.floors_median,
      bua.existing_bua_m2,
      CASE
        WHEN ST_Area(site.geom) > 0 THEN bua.existing_bua_m2 / ST_Area(site.geom)
        ELSE NULL
      END AS far_proxy_existing,
      CASE
        WHEN ST_Area(site.geom) > 0 THEN bua.existing_bua_m2 / (ST_Area(site.geom) / 10000.0)
        ELSE NULL
      END AS built_density_m2_per_ha,
      agg.building_count,
      CASE
        WHEN agg.building_count > 0 THEN agg.floors_count::float / agg.building_count
        ELSE 0.0
      END AS pct_buildings_with_floors_data
    FROM site
    CROSS JOIN agg
    CROSS JOIN bua;
    """
)


def floors_proxy(num_floors: Any, height: Any) -> Optional[int]:
    """
    Compute a floors proxy using the same logic as the SQL:
      - prefer num_floors
      - else height / 3.2, rounded, clamped to [1, 60]
      - else None
    """
    try:
        if num_floors is not None:
            val = float(num_floors)
            if val > 0:
                return int(round(val))
    except Exception:
        pass

    try:
        if height is not None:
            height_val = float(height)
            proxy = int(round(height_val / 3.2))
            return max(1, min(60, proxy))
    except Exception:
        pass
    return None


def compute_building_metrics(
    db, geometry: Dict[str, Any], buffer_m: float | None = None
) -> Dict[str, Any]:
    """
    Compute coverage, floors proxies, and built-up area inside the input geometry
    using Overture buildings stored in PostGIS (SRID 32638).
    """

    if not hasattr(db, "execute"):
        logger.debug("compute_building_metrics: db has no execute(); returning empty metrics")
        return {}

    try:
        row = (
            db.execute(_OVERTURE_BUILDING_METRICS_SQL, {"gj": json.dumps(geometry), "buffer_m": buffer_m})
            .mappings()
            .first()
        )
    except Exception as exc:
        logger.warning("Overture building metrics query failed: %s", exc)
        return {}

    if not row:
        return {}

    site_area_m2 = float(row.get("site_area_m2") or 0.0)
    footprint = float(row.get("footprint_area_m2") or 0.0)
    existing_bua = float(row.get("existing_bua_m2") or 0.0)
    far_proxy_existing = float(row.get("far_proxy_existing") or 0.0) if site_area_m2 > 0 else None
    built_density = (
        float(row.get("built_density_m2_per_ha") or 0.0) if site_area_m2 > 0 else None
    )

    return {
        "site_area_m2": site_area_m2,
        "footprint_area_m2": footprint,
        "coverage_ratio": float(row.get("coverage_ratio") or 0.0) if site_area_m2 > 0 else None,
        "floors_mean": float(row.get("floors_mean")) if row.get("floors_mean") is not None else None,
        "floors_median": float(row.get("floors_median")) if row.get("floors_median") is not None else None,
        "existing_bua_m2": existing_bua,
        "far_proxy_existing": far_proxy_existing,
        "built_density_m2_per_ha": built_density,
        "building_count": int(row.get("building_count") or 0),
        "pct_buildings_with_floors_data": float(row.get("pct_buildings_with_floors_data") or 0.0),
        "buffer_m": buffer_m,
    }
