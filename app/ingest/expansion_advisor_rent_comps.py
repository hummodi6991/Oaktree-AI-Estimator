"""Expansion Advisor — Retail Rent & Lease Comps Ingestion.

Wraps existing app.ingest.aqar_rent_comps logic and normalizes
retail/commercial rent comps into expansion_rent_comp.

Supports download paths:
  a) Kaggle dataset via KAGGLE_USERNAME/KAGGLE_KEY
  b) direct CSV URL input
  c) local artifact path input

Enforces city=riyadh and prioritizes commercial/retail.
Computes rent_sar_m2_year consistently.
"""
from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import subprocess
import sys
import tempfile
from datetime import date
from typing import Any

from sqlalchemy import text

from app.ingest.expansion_advisor_common import (
    get_session,
    log_table_counts,
    validate_db_env,
    write_stats,
)

logger = logging.getLogger("expansion_advisor.rent_comps")


def _download_kaggle(dataset: str, dest_dir: str) -> str | None:
    """Download a Kaggle dataset CSV. Returns path to downloaded file or None."""
    if not os.getenv("KAGGLE_USERNAME") or not os.getenv("KAGGLE_KEY"):
        logger.warning("KAGGLE_USERNAME/KAGGLE_KEY not set, skipping Kaggle download")
        return None

    try:
        result = subprocess.run(
            ["kaggle", "datasets", "download", "-d", dataset, "-p", dest_dir, "--unzip"],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            logger.error("Kaggle download failed: %s", result.stderr)
            return None
        # Find a CSV in the dest dir
        for f in os.listdir(dest_dir):
            if f.endswith(".csv"):
                return os.path.join(dest_dir, f)
        logger.warning("No CSV found in Kaggle download dir")
        return None
    except Exception:
        logger.error("Kaggle download failed", exc_info=True)
        return None


def _download_csv_url(url: str, dest_dir: str) -> str | None:
    """Download a CSV from a URL."""
    import httpx

    try:
        resp = httpx.get(url, timeout=120, follow_redirects=True)
        resp.raise_for_status()
        path = os.path.join(dest_dir, "rent_comps.csv")
        with open(path, "wb") as f:
            f.write(resp.content)
        return path
    except Exception:
        logger.error("CSV download failed from %s", url, exc_info=True)
        return None


def _normalize_from_existing_rent_comp(db, replace: bool) -> dict:
    """Normalize from the existing rent_comp table (populated by aqar_rent_comps)."""
    if replace:
        db.execute(text("DELETE FROM expansion_rent_comp WHERE city = 'riyadh'"))
        db.commit()

    insert_sql = text("""
        INSERT INTO expansion_rent_comp (
            city, district, source, listing_id, asset_type, unit_type,
            area_m2, monthly_rent_sar, annual_rent_sar, rent_sar_m2_year,
            ingested_at
        )
        SELECT
            rc.city,
            rc.district,
            rc.source,
            rc.id,
            rc.asset_type,
            rc.unit_type,
            NULL,  -- area_m2 not in rent_comp directly
            rc.rent_per_unit,
            rc.rent_per_unit * 12.0,
            -- rent_sar_m2_year: use rent_per_m2 * 12
            CASE
                WHEN rc.rent_per_m2 IS NOT NULL AND rc.rent_per_m2 > 0
                THEN rc.rent_per_m2 * 12.0
                ELSE NULL
            END,
            now()
        FROM rent_comp rc
        WHERE lower(rc.city) = 'riyadh'
          AND lower(COALESCE(rc.asset_type, '')) IN ('commercial', '')
    """)

    result = db.execute(insert_sql)
    db.commit()
    inserted = result.rowcount
    logger.info("Inserted %d rent comps from existing rent_comp table", inserted)
    return {"inserted": inserted, "source": "rent_comp_table"}


def _normalize_from_csv(db, csv_path: str, replace: bool) -> dict:
    """Normalize rent comps from a CSV file into expansion_rent_comp."""
    if replace:
        db.execute(text("DELETE FROM expansion_rent_comp WHERE city = 'riyadh' AND source = 'csv_import'"))
        db.commit()

    inserted = 0
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=1):
            city = (row.get("city") or "riyadh").strip().lower()
            if city != "riyadh":
                continue

            area_raw = row.get("area_sqm") or row.get("area_m2") or row.get("area")
            price_raw = row.get("price_sar") or row.get("price") or row.get("rent")

            try:
                area = float(area_raw) if area_raw else None
                price = float(price_raw) if price_raw else None
            except (ValueError, TypeError):
                continue

            if not price or price <= 0:
                continue

            district = (row.get("district") or "").strip() or None
            asset_type = (row.get("asset_type") or row.get("property_type") or "commercial").strip().lower()

            # Prioritize commercial/retail
            if asset_type not in ("commercial", "retail", "office"):
                # Check text fields for commercial keywords
                text_blob = " ".join(str(row.get(c, "") or "") for c in ["property_type", "title", "category"]).lower()
                if not any(kw in text_blob for kw in ["محل", "تجاري", "shop", "retail", "مكتب", "office", "commercial"]):
                    continue
                asset_type = "commercial"

            # Normalize to annual
            monthly = price
            annual = price * 12.0
            rent_m2_year = (annual / area) if area and area > 0 else None

            db.execute(
                text("""
                    INSERT INTO expansion_rent_comp (
                        city, district, source, listing_id, asset_type, unit_type,
                        area_m2, monthly_rent_sar, annual_rent_sar, rent_sar_m2_year,
                        ingested_at
                    ) VALUES (
                        'riyadh', :district, 'csv_import', :listing_id, :asset_type, :unit_type,
                        :area_m2, :monthly, :annual, :rent_m2_year, now()
                    )
                """),
                {
                    "district": district,
                    "listing_id": f"csv_{idx}",
                    "asset_type": asset_type,
                    "unit_type": row.get("unit_type"),
                    "area_m2": area,
                    "monthly": monthly,
                    "annual": annual,
                    "rent_m2_year": rent_m2_year,
                },
            )
            inserted += 1

    db.commit()
    logger.info("Inserted %d rent comps from CSV", inserted)
    return {"inserted": inserted, "source": "csv_import", "csv_path": csv_path}


def _log_district_medians(db) -> dict[str, float]:
    """Log median rent by district and return as dict."""
    rows = db.execute(text("""
        SELECT district,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rent_sar_m2_year) AS median_rent
        FROM expansion_rent_comp
        WHERE city = 'riyadh'
          AND rent_sar_m2_year IS NOT NULL
          AND rent_sar_m2_year > 0
        GROUP BY district
        ORDER BY median_rent DESC
    """)).fetchall()

    medians: dict[str, float] = {}
    logger.info("District median rents (SAR/m²/year):")
    for row in rows:
        district = row[0] or "(unknown)"
        median = float(row[1])
        medians[district] = round(median, 2)
        logger.info("  %s: %.2f", district, median)
    return medians


def main() -> None:
    parser = argparse.ArgumentParser(description="Expansion Advisor — Rent Comps ingest")
    parser.add_argument("--city", default="riyadh", help="City filter (default: riyadh)")
    parser.add_argument("--replace", type=lambda v: v.lower() in ("true", "1", "yes"), default=True,
                        help="Replace existing rows (default: true)")
    parser.add_argument("--kaggle-dataset", default=None, help="Kaggle dataset slug")
    parser.add_argument("--csv-url", default=None, help="Direct CSV URL")
    parser.add_argument("--local-path", default=None, help="Local CSV file path")
    parser.add_argument("--write-stats", type=str, default=None, help="Write JSON stats to path")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    validate_db_env()

    db = get_session()
    try:
        stats: dict | None = None

        # Priority: local path → CSV URL → Kaggle → existing rent_comp table
        if args.local_path:
            if os.path.exists(args.local_path):
                stats = _normalize_from_csv(db, args.local_path, replace=args.replace)
            else:
                logger.error("Local path does not exist: %s", args.local_path)
                sys.exit(1)
        elif args.csv_url:
            with tempfile.TemporaryDirectory() as tmpdir:
                csv_path = _download_csv_url(args.csv_url, tmpdir)
                if csv_path and os.path.exists(csv_path):
                    stats = _normalize_from_csv(db, csv_path, replace=args.replace)
                else:
                    logger.error("CSV download failed from %s", args.csv_url)
                    sys.exit(1)
        elif args.kaggle_dataset:
            with tempfile.TemporaryDirectory() as tmpdir:
                csv_path = _download_kaggle(args.kaggle_dataset, tmpdir)
                if csv_path and os.path.exists(csv_path):
                    stats = _normalize_from_csv(db, csv_path, replace=args.replace)
                else:
                    logger.error("Kaggle download failed for %s", args.kaggle_dataset)
                    sys.exit(1)

        if stats is None:
            logger.info("No CSV source provided, normalizing from existing rent_comp table")
            stats = _normalize_from_existing_rent_comp(db, replace=args.replace)

        medians = _log_district_medians(db)
        stats["district_medians"] = medians

        counts = log_table_counts(db, ["expansion_rent_comp"])
        stats["row_counts"] = counts

        if args.write_stats:
            write_stats(args.write_stats, stats)
    finally:
        db.close()


if __name__ == "__main__":
    main()
