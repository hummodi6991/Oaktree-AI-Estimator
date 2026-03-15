"""Shared helpers for Expansion Advisor data-ingestion modules.

Provides:
- Riyadh bounding-box filter
- Source-table existence checks
- JSON stats writing
- Row-count logging
- DB environment validation
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger("expansion_advisor.common")

# Riyadh approximate bounding box (WGS-84)
RIYADH_BBOX = {
    "min_lon": 46.3,
    "max_lon": 47.1,
    "min_lat": 24.4,
    "max_lat": 25.0,
}


def validate_db_env() -> None:
    """Ensure DATABASE_URL or POSTGRES_* env vars are set."""
    if os.getenv("DATABASE_URL"):
        return
    required = ["POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_DB"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        logger.error("Missing DB env vars: %s", missing)
        sys.exit(1)


def table_exists(db: Session, table_name: str, schema: str = "public") -> bool:
    """Check if a table exists in the database."""
    try:
        row = db.execute(
            text("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = :schema AND table_name = :table
                ) AS available
            """),
            {"schema": schema, "table": table_name},
        ).scalar()
        return bool(row)
    except Exception:
        logger.debug("table_exists check failed for %s.%s", schema, table_name, exc_info=True)
        return False


def table_row_count(db: Session, table_name: str) -> int:
    """Return approximate row count for a table (safe on empty/missing tables)."""
    try:
        row = db.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
        return int(row or 0)
    except Exception:
        return 0


def detect_srid(db: Session, table_name: str, geom_col: str = "geom") -> int:
    """Detect the SRID of a geometry column. Returns 4326 as fallback."""
    try:
        srid = db.execute(
            text("""
                SELECT Find_SRID('public', :table, :col)
            """),
            {"table": table_name, "col": geom_col},
        ).scalar()
        if srid and srid > 0:
            return int(srid)
    except Exception:
        db.rollback()
    # Fallback: sample a row
    try:
        srid = db.execute(
            text(f"SELECT ST_SRID({geom_col}) FROM {table_name} WHERE {geom_col} IS NOT NULL LIMIT 1")
        ).scalar()
        if srid and srid > 0:
            return int(srid)
    except Exception:
        db.rollback()
    return 4326


def riyadh_bbox_filter_sql(
    geom_col: str, alias: str = "", source_srid: int = 4326
) -> str:
    """Return a SQL WHERE clause fragment bounding to Riyadh, handling SRID transforms.

    If source_srid != 4326, wraps geom_col in ST_Transform(..., 4326) so the
    comparison against the WGS-84 bounding box is valid.
    """
    prefix = f"{alias}." if alias else ""
    col_expr = f"{prefix}{geom_col}"
    if source_srid != 4326:
        col_expr = f"ST_Transform({col_expr}, 4326)"
    return (
        f"ST_Intersects({col_expr}, "
        f"ST_MakeEnvelope({RIYADH_BBOX['min_lon']}, {RIYADH_BBOX['min_lat']}, "
        f"{RIYADH_BBOX['max_lon']}, {RIYADH_BBOX['max_lat']}, 4326))"
    )


def riyadh_filter_sql(geom_col: str = "geom", alias: str = "") -> str:
    """Return a SQL WHERE clause fragment bounding to Riyadh (assumes SRID 4326)."""
    prefix = f"{alias}." if alias else ""
    return (
        f"ST_Intersects({prefix}{geom_col}, "
        f"ST_MakeEnvelope({RIYADH_BBOX['min_lon']}, {RIYADH_BBOX['min_lat']}, "
        f"{RIYADH_BBOX['max_lon']}, {RIYADH_BBOX['max_lat']}, 4326))"
    )


def write_stats(path: str, stats: dict[str, Any]) -> None:
    """Write JSON stats to a file."""
    stats["generated_at"] = datetime.now(timezone.utc).isoformat()
    with open(path, "w") as f:
        json.dump(stats, f, indent=2, default=str)
    logger.info("Stats written to %s", path)


def log_table_counts(db: Session, tables: list[str]) -> dict[str, int]:
    """Log and return row counts for a list of tables."""
    counts: dict[str, int] = {}
    for t in tables:
        c = table_row_count(db, t)
        counts[t] = c
        logger.info("  %s: %d rows", t, c)
    return counts


def get_session():
    """Create a new DB session using the app's standard session factory."""
    from app.db.session import SessionLocal
    return SessionLocal()
