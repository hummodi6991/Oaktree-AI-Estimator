#!/usr/bin/env python3
"""One-time quarantine of legacy area-corrupted commercial_unit rows.

Identifies rows in the active pool whose area_sqm is implausible for
a store/showroom listing — almost certainly the result of pre-Patch-14
decimal-comma parser corruption — and flips their status from 'active'
to 'quarantined_area'. The Expansion Advisor JOIN clause already
filters status = 'active' so quarantined rows disappear from the
candidate pool automatically.

This script does NOT re-fetch any pages. It does NOT try to guess the
correct area. It only removes rows from the active pool so the LLM
stops reasoning about hallucinated values.

Quarantine criteria (store/showroom only):
  - area_sqm > 10000 (16 rows expected based on Q32 audit)
  - area_sqm < 5     (1 row expected based on Q32 audit)

Total expected: ~17 rows touched.

Reversible: re-running with --restore flips quarantined_area rows back
to active. Use this if a future investigation shows a quarantined row
was actually correct.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_HARD_ROW_CEILING = 100  # Safety guard — never quarantine more than this in one run

_QUARANTINE_STATUS = "quarantined_area"


def _build_engine():
    return create_engine(
        f"postgresql://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}"
        f"/{os.environ['POSTGRES_DB']}",
        connect_args={"sslmode": os.environ.get("POSTGRES_SSLMODE") or "require"},
    )


def _quarantine(session, dry_run: bool) -> int:
    rows = session.execute(text(
        """
        SELECT aqar_id, listing_type, neighborhood, area_sqm
        FROM commercial_unit
        WHERE status = 'active'
          AND listing_type IN ('store', 'showroom')
          AND (area_sqm > 10000 OR area_sqm < 5)
        ORDER BY area_sqm DESC NULLS LAST
        """
    )).mappings().all()

    if len(rows) > _HARD_ROW_CEILING:
        logger.error(
            "Refusing to quarantine %d rows (ceiling %d)", len(rows), _HARD_ROW_CEILING
        )
        return 1

    logger.info("Quarantining %d rows (dry_run=%s)", len(rows), dry_run)
    for row in rows:
        logger.info(
            "QUARANTINE aqar_id=%s area_sqm=%s listing_type=%s neighborhood=%s",
            row["aqar_id"], row["area_sqm"], row["listing_type"], row["neighborhood"],
        )

    if not dry_run:
        session.execute(text(
            f"""
            UPDATE commercial_unit
            SET status = '{_QUARANTINE_STATUS}',
                last_seen_at = now()
            WHERE status = 'active'
              AND listing_type IN ('store', 'showroom')
              AND (area_sqm > 10000 OR area_sqm < 5)
            """
        ))
        session.commit()
        logger.info("Committed %d quarantine flips", len(rows))
    return 0


def _restore(session, dry_run: bool) -> int:
    rows = session.execute(text(
        f"""
        SELECT aqar_id, listing_type, neighborhood, area_sqm
        FROM commercial_unit
        WHERE status = '{_QUARANTINE_STATUS}'
        """
    )).mappings().all()
    logger.info("Restoring %d quarantined rows (dry_run=%s)", len(rows), dry_run)
    for row in rows:
        logger.info(
            "RESTORE aqar_id=%s area_sqm=%s",
            row["aqar_id"], row["area_sqm"],
        )
    if not dry_run:
        session.execute(text(
            f"""
            UPDATE commercial_unit
            SET status = 'active', last_seen_at = now()
            WHERE status = '{_QUARANTINE_STATUS}'
            """
        ))
        session.commit()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="Print planned changes without writing (default)")
    parser.add_argument("--no-dry-run", dest="dry_run", action="store_false",
                        help="Actually write changes to the DB")
    parser.add_argument("--restore", action="store_true",
                        help="Reverse: flip quarantined_area back to active")
    args = parser.parse_args()

    engine = _build_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    if args.restore:
        return _restore(session, args.dry_run)
    return _quarantine(session, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
