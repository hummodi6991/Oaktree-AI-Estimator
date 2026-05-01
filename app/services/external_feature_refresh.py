"""On-demand refresh helpers for materialized views derived from
external_feature.

Polygons in external_feature are reference data — populated by one-shot
ingest scripts and rarely changed. Materialized views derived from them
(currently external_feature_polygons_mat) are refreshed on demand, not
on cron, via the "Refresh external_feature_polygons_mat" GitHub Actions
workflow.
"""
from __future__ import annotations

import logging
import time

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def refresh_external_feature_polygons_mat(db: Session) -> dict[str, float | int]:
    """Refresh external_feature_polygons_mat concurrently.

    Returns a dict with timing and resulting row count, suitable for logging
    or workflow output capture.

    Concurrent refresh requires a unique index on the matview (which the
    creating migration provides). Concurrent refresh does NOT take an
    AccessExclusiveLock, so production reads of the matview continue
    uninterrupted during the refresh.
    """
    t0 = time.monotonic()
    db.execute(
        text("REFRESH MATERIALIZED VIEW CONCURRENTLY external_feature_polygons_mat")
    )
    db.commit()
    elapsed_s = time.monotonic() - t0

    n = db.execute(
        text("SELECT COUNT(*) FROM external_feature_polygons_mat")
    ).scalar_one()

    logger.info(
        "Refreshed external_feature_polygons_mat: rows=%d elapsed=%.2fs",
        n,
        elapsed_s,
    )
    return {"rows": int(n), "elapsed_s": round(elapsed_s, 2)}


if __name__ == "__main__":
    from app.db.session import SessionLocal

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    db = SessionLocal()
    try:
        result = refresh_external_feature_polygons_mat(db)
        print(f"Refreshed: {result}")
    finally:
        db.close()
