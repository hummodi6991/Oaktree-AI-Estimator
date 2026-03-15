"""Expansion Advisor — Competitor Quality Materialization.

Builds expansion_competitor_quality from:
- restaurant_poi
- Google review enriched fields if present
- delivery_source_record / normalized delivery market table

Derives quality scores:
- chain_strength_score: based on how many locations the brand has
- review_score: normalized Google/platform rating
- delivery_presence_score: how many delivery platforms carry the brand
- multi_platform_score: breadth of delivery presence
- late_night_score: late-night availability signal
- overall_quality_score: weighted composite

Riyadh only.  If Google review enrichment data is missing,
degrades gracefully and still builds rows.
"""
from __future__ import annotations

import argparse
import logging
import sys

from sqlalchemy import text

from app.ingest.expansion_advisor_common import (
    RIYADH_BBOX,
    get_session,
    log_table_counts,
    table_exists,
    validate_db_env,
    write_stats,
)

logger = logging.getLogger("expansion_advisor.competitors")


def _has_google_review_columns(db) -> bool:
    """Check if restaurant_poi has Google review enrichment columns."""
    try:
        row = db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'restaurant_poi'
              AND column_name IN ('google_place_id', 'google_confidence')
        """)).fetchall()
        return len(row) >= 1
    except Exception:
        return False


def _build_competitor_quality(db, replace: bool) -> dict:
    """Build expansion_competitor_quality from restaurant_poi + delivery data."""
    if not table_exists(db, "restaurant_poi"):
        logger.error("restaurant_poi table not found")
        sys.exit(1)

    if replace:
        db.execute(text("DELETE FROM expansion_competitor_quality WHERE city = 'riyadh'"))
        db.commit()

    has_google = _has_google_review_columns(db)
    has_delivery_market = table_exists(db, "expansion_delivery_market")
    has_delivery_source = table_exists(db, "delivery_source_record")

    logger.info("Google review data available: %s", has_google)
    logger.info("expansion_delivery_market available: %s", has_delivery_market)
    logger.info("delivery_source_record available: %s", has_delivery_source)

    bbox = RIYADH_BBOX

    # Build delivery presence subquery
    if has_delivery_market:
        delivery_cte = """
            delivery_stats AS (
                SELECT
                    resolved_restaurant_poi_id AS poi_id,
                    COUNT(*) AS listing_count,
                    COUNT(DISTINCT platform) AS platform_count,
                    BOOL_OR(COALESCE(supports_late_night, FALSE)) AS has_late_night
                FROM expansion_delivery_market
                WHERE city = 'riyadh'
                  AND resolved_restaurant_poi_id IS NOT NULL
                GROUP BY resolved_restaurant_poi_id
            )
        """
    elif has_delivery_source:
        delivery_cte = """
            delivery_stats AS (
                SELECT
                    matched_restaurant_poi_id AS poi_id,
                    COUNT(*) AS listing_count,
                    COUNT(DISTINCT platform) AS platform_count,
                    FALSE AS has_late_night
                FROM delivery_source_record
                WHERE matched_restaurant_poi_id IS NOT NULL
                GROUP BY matched_restaurant_poi_id
            )
        """
    else:
        delivery_cte = """
            delivery_stats AS (
                SELECT NULL::varchar AS poi_id, 0 AS listing_count, 0 AS platform_count, FALSE AS has_late_night
                WHERE FALSE
            )
        """

    # Chain strength: count of POIs sharing the same chain_name
    chain_cte = """
        chain_counts AS (
            SELECT chain_name, COUNT(*) AS chain_size
            FROM restaurant_poi
            WHERE chain_name IS NOT NULL AND chain_name != ''
            GROUP BY chain_name
        )
    """

    insert_sql = text(f"""
        WITH {chain_cte},
        {delivery_cte}
        INSERT INTO expansion_competitor_quality (
            city, restaurant_poi_id, brand_name, category, district, geom,
            chain_strength_score, review_score, review_count,
            delivery_presence_score, multi_platform_score, late_night_score,
            price_tier, overall_quality_score, refreshed_at
        )
        SELECT
            'riyadh',
            rp.id,
            COALESCE(rp.chain_name, rp.name),
            rp.category,
            rp.district,
            COALESCE(
                rp.geom,
                CASE WHEN rp.lon IS NOT NULL AND rp.lat IS NOT NULL
                     THEN ST_SetSRID(ST_MakePoint(rp.lon, rp.lat), 4326)
                     ELSE NULL
                END
            ),
            -- chain_strength_score (0-100): more locations = stronger chain
            LEAST(100.0, COALESCE(cc.chain_size, 1) * 12.0),
            -- review_score (0-100): normalized from 1-5 star rating
            CASE
                WHEN rp.rating IS NOT NULL
                THEN LEAST(100.0, GREATEST(0.0, (rp.rating - 1.0) / 4.0 * 100.0))
                ELSE NULL
            END,
            rp.review_count,
            -- delivery_presence_score (0-100): based on listing count
            CASE
                WHEN ds.listing_count IS NOT NULL
                THEN LEAST(100.0, ds.listing_count * 15.0)
                ELSE 0.0
            END,
            -- multi_platform_score (0-100): platforms / 5 * 100
            CASE
                WHEN ds.platform_count IS NOT NULL
                THEN LEAST(100.0, ds.platform_count * 20.0)
                ELSE 0.0
            END,
            -- late_night_score (0 or 100)
            CASE WHEN COALESCE(ds.has_late_night, FALSE) THEN 100.0 ELSE 0.0 END,
            -- price_tier from price_level
            CASE
                WHEN rp.price_level = 1 THEN 'budget'
                WHEN rp.price_level = 2 THEN 'mid'
                WHEN rp.price_level = 3 THEN 'premium'
                WHEN rp.price_level = 4 THEN 'luxury'
                ELSE NULL
            END,
            -- overall_quality_score: weighted composite
            LEAST(100.0, GREATEST(0.0,
                COALESCE(LEAST(100.0, COALESCE(cc.chain_size, 1) * 12.0), 50.0) * 0.15
                + COALESCE(
                    CASE WHEN rp.rating IS NOT NULL
                         THEN LEAST(100.0, (rp.rating - 1.0) / 4.0 * 100.0)
                         ELSE 50.0
                    END, 50.0) * 0.35
                + COALESCE(
                    CASE WHEN ds.listing_count IS NOT NULL
                         THEN LEAST(100.0, ds.listing_count * 15.0)
                         ELSE 0.0
                    END, 0.0) * 0.25
                + COALESCE(
                    CASE WHEN ds.platform_count IS NOT NULL
                         THEN LEAST(100.0, ds.platform_count * 20.0)
                         ELSE 0.0
                    END, 0.0) * 0.15
                + COALESCE(
                    CASE WHEN ds.has_late_night THEN 100.0 ELSE 0.0 END, 0.0) * 0.10
            )),
            now()
        FROM restaurant_poi rp
        LEFT JOIN chain_counts cc ON cc.chain_name = rp.chain_name
        LEFT JOIN delivery_stats ds ON ds.poi_id = rp.id
        WHERE COALESCE(
                rp.geom,
                CASE WHEN rp.lon IS NOT NULL AND rp.lat IS NOT NULL
                     THEN ST_SetSRID(ST_MakePoint(rp.lon, rp.lat), 4326)
                     ELSE NULL
                END
              ) IS NOT NULL
          AND ST_Intersects(
                COALESCE(
                    rp.geom,
                    ST_SetSRID(ST_MakePoint(rp.lon, rp.lat), 4326)
                ),
                ST_MakeEnvelope(
                    {bbox['min_lon']}, {bbox['min_lat']},
                    {bbox['max_lon']}, {bbox['max_lat']}, 4326
                )
              )
    """)

    result = db.execute(insert_sql)
    db.commit()
    inserted = result.rowcount
    logger.info("Inserted %d competitor quality records", inserted)

    return {
        "inserted": inserted,
        "google_review_data_used": has_google,
        "delivery_source": "expansion_delivery_market" if has_delivery_market else (
            "delivery_source_record" if has_delivery_source else "none"
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Expansion Advisor — Competitor Quality ingest")
    parser.add_argument("--city", default="riyadh", help="City filter (default: riyadh)")
    parser.add_argument("--replace", type=lambda v: v.lower() in ("true", "1", "yes"), default=True,
                        help="Replace existing rows (default: true)")
    parser.add_argument("--write-stats", type=str, default=None, help="Write JSON stats to path")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    validate_db_env()

    db = get_session()
    try:
        stats = _build_competitor_quality(db, replace=args.replace)
        counts = log_table_counts(db, ["expansion_competitor_quality"])
        stats["row_counts"] = counts

        if args.write_stats:
            write_stats(args.write_stats, stats)
    finally:
        db.close()


if __name__ == "__main__":
    main()
