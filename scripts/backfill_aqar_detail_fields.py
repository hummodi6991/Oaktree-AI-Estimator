"""One-time backfill: populate Aqar Info-block fields on existing rows.

After the Phase 2 migration lands, every ``commercial_unit`` row has
``aqar_detail_scraped_at IS NULL`` — the nine new columns are empty.
This script walks active listings newest-first (the ones users are
most likely to land on), fetches each detail page, and writes the
parsed Info block back to the row.

Designed to run as a K8s Job rather than locally — long-running DB
operations should not run in a Codespace (project memory:
``backfill-aqar-detail-fields.yaml`` follows the same conventions as
``backfill-llm-suitability.yaml``).

Usage (inside the container):

  python scripts/backfill_aqar_detail_fields.py            # full run
  python scripts/backfill_aqar_detail_fields.py --limit 50 # dry sample
  python scripts/backfill_aqar_detail_fields.py --dry-run  # no writes

Re-running is safe: the query only selects rows where
``aqar_detail_scraped_at IS NULL`` (or older than 24 hours with
``--reprocess``), so a partial run that hit network trouble can just
be restarted and it picks up from where it left off.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

import requests
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.ingest.aqar.detail_scraper import AqarDetailPayload, fetch_listing_detail

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)


# Politeness: 1.5 seconds between detail fetches. Matches the Phase 2
# spec baseline. Tune via ``--rate-limit`` on re-runs if Aqar tolerates
# faster (ease up on a fresh IP, tighten after a backoff event).
_DEFAULT_RATE_LIMIT_S = 1.5
_BATCH_COMMIT_SIZE = 50


def _build_engine():
    return create_engine(
        f"postgresql://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}"
        f"/{os.environ['POSTGRES_DB']}",
        connect_args={"sslmode": os.environ.get("POSTGRES_SSLMODE", "require")},
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and parse, but don't write to the DB",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after N rows (use for dry samples on a fresh deploy)",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=_DEFAULT_RATE_LIMIT_S,
        help="Seconds to sleep between detail fetches (default: 1.5)",
    )
    parser.add_argument(
        "--reprocess",
        action="store_true",
        help="Also re-fetch rows whose aqar_detail_scraped_at is older "
        "than 24 hours (default: only NULL)",
    )
    args = parser.parse_args()

    engine = _build_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    # Newest-first — users are most likely to hit recent listings, so
    # their rows should populate first.
    predicate = "aqar_detail_scraped_at IS NULL"
    if args.reprocess:
        predicate = (
            "(aqar_detail_scraped_at IS NULL "
            "OR aqar_detail_scraped_at < now() - interval '24 hours')"
        )

    rows = session.execute(
        text(
            f"""
            SELECT aqar_id, listing_url
            FROM commercial_unit
            WHERE status = 'active'
              AND listing_url IS NOT NULL
              AND {predicate}
            ORDER BY first_seen_at DESC
            """
        )
    ).mappings().all()

    if args.limit:
        rows = rows[: args.limit]

    total = len(rows)
    logger.info(
        "Backfilling Aqar detail fields for %d listings (dry_run=%s, "
        "rate_limit=%.2fs, reprocess=%s)",
        total,
        args.dry_run,
        args.rate_limit,
        args.reprocess,
    )

    success = 0
    skipped_404 = 0
    failures = 0
    http_session = requests.Session()

    for i, row in enumerate(rows, start=1):
        aqar_id = row["aqar_id"]
        listing_url = row["listing_url"]

        try:
            payload = fetch_listing_detail(aqar_id, listing_url, http_session)
        except Exception as exc:
            logger.warning(
                "Detail fetch raised for aqar_id=%s: %s", aqar_id, exc
            )
            failures += 1
            time.sleep(args.rate_limit)
            continue

        if payload is None:
            # fetch_listing_detail already logged WARNING with the
            # specific reason (404, 5xx exhausted, structure change).
            skipped_404 += 1
            time.sleep(args.rate_limit)
            continue

        print(
            f"[{i}/{total}] aqar_id={aqar_id} "
            f"created_at={payload.aqar_created_at} "
            f"last_update={payload.aqar_updated_at} "
            f"views={payload.aqar_views}"
        )

        if not args.dry_run:
            _update_row(session, aqar_id, payload)
            if i % _BATCH_COMMIT_SIZE == 0:
                session.commit()
                logger.info(
                    "Progress: %d/%d (success=%d skipped=%d failures=%d)",
                    i, total, success, skipped_404, failures,
                )

        success += 1
        time.sleep(args.rate_limit)

    if not args.dry_run:
        session.commit()

    logger.info(
        "Backfill complete. processed=%d success=%d skipped_or_removed=%d "
        "failures=%d",
        total, success, skipped_404, failures,
    )
    return 0


def _update_row(session, aqar_id: str, payload: AqarDetailPayload) -> None:
    session.execute(
        text(
            """
            UPDATE commercial_unit SET
              aqar_created_at           = :aqar_created_at,
              aqar_updated_at           = :aqar_updated_at,
              aqar_views                = :aqar_views,
              aqar_advertisement_license= :aqar_advertisement_license,
              aqar_license_expiry       = :aqar_license_expiry,
              aqar_plan_parcel          = :aqar_plan_parcel,
              aqar_area_deed            = :aqar_area_deed,
              aqar_listing_source       = :aqar_listing_source,
              aqar_detail_scraped_at    = :aqar_detail_scraped_at
            WHERE aqar_id = :aqar_id
            """
        ),
        {
            "aqar_id": aqar_id,
            "aqar_created_at": payload.aqar_created_at,
            "aqar_updated_at": payload.aqar_updated_at,
            "aqar_views": payload.aqar_views,
            "aqar_advertisement_license": payload.aqar_advertisement_license,
            "aqar_license_expiry": payload.aqar_license_expiry,
            "aqar_plan_parcel": payload.aqar_plan_parcel,
            "aqar_area_deed": payload.aqar_area_deed,
            "aqar_listing_source": payload.aqar_listing_source,
            "aqar_detail_scraped_at": payload.aqar_detail_scraped_at,
        },
    )


if __name__ == "__main__":
    sys.exit(main())
