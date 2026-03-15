"""Post-ingestion refresh for Expansion Advisor normalized tables.

Responsibilities:
- Run alembic upgrade head (idempotent)
- Refresh any materialized views
- Log row counts for each Expansion Advisor table
- Callable from workflows as a final step
- Idempotent and safe on repeated runs
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys

from app.ingest.expansion_advisor_common import (
    get_session,
    log_table_counts,
    validate_db_env,
    write_stats,
)

logger = logging.getLogger("expansion_advisor.refresh")

EXPANSION_TABLES = [
    "expansion_road_context",
    "expansion_parking_asset",
    "expansion_delivery_market",
    "expansion_rent_comp",
    "expansion_competitor_quality",
]


def run_alembic_upgrade() -> None:
    """Run alembic upgrade head. Idempotent."""
    logger.info("Running alembic upgrade head ...")
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.error("alembic upgrade failed: %s", result.stderr)
        raise RuntimeError(f"alembic upgrade failed: {result.stderr}")
    logger.info("alembic upgrade completed successfully")


def refresh_materialized_views(db) -> None:
    """Refresh any materialized views used by Expansion Advisor.

    Currently no materialized views are defined — the normalized tables
    are regular tables populated by the ingest modules.  This function
    is a placeholder that will refresh views when they are added.
    """
    # Future: REFRESH MATERIALIZED VIEW CONCURRENTLY expansion_*_mv;
    logger.info("No materialized views to refresh (tables are directly populated)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Expansion Advisor post-ingestion refresh")
    parser.add_argument("--skip-alembic", action="store_true", help="Skip alembic upgrade")
    parser.add_argument("--write-stats", type=str, default=None, help="Path to write JSON stats")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    validate_db_env()

    if not args.skip_alembic:
        run_alembic_upgrade()

    db = get_session()
    try:
        refresh_materialized_views(db)

        logger.info("Expansion Advisor table row counts:")
        counts = log_table_counts(db, EXPANSION_TABLES)

        if args.write_stats:
            write_stats(args.write_stats, {"table_row_counts": counts})

        logger.info("Refresh complete.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
