"""Backfill LLM suitability scores for all active commercial_unit rows.

Resumable: skips rows that already have llm_classified_at set, so
re-running this script after a partial run will only process the
remaining unclassified rows.

Cost ceiling: aborts if more than 5,000 rows are processed in a
single invocation, as a guardrail against runaway billing if the
script is misconfigured.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.services.llm_suitability import classify_listing

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)

_HARD_ROW_CEILING = 5000  # safety limit per invocation


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
        "--dry-run", action="store_true", help="Don't write to DB"
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Process at most N rows"
    )
    parser.add_argument(
        "--reclassify",
        action="store_true",
        help="Reclassify rows that already have llm_classified_at",
    )
    args = parser.parse_args()

    engine = _build_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    where_clause = "WHERE status = 'active'"
    if not args.reclassify:
        where_clause += " AND llm_classified_at IS NULL"

    rows = (
        session.execute(
            text(
                f"""
            SELECT aqar_id, listing_type, property_type, neighborhood,
                   area_sqm, price_sar_annual, street_width_m,
                   is_furnished, has_drive_thru, description, image_url
            FROM commercial_unit
            {where_clause}
            ORDER BY first_seen_at DESC
        """
            )
        )
        .mappings()
        .all()
    )

    if args.limit:
        rows = rows[: args.limit]

    if len(rows) > _HARD_ROW_CEILING:
        logger.error(
            "Refusing to process %d rows (hard ceiling %d). Use --limit to override.",
            len(rows),
            _HARD_ROW_CEILING,
        )
        return 1

    logger.info(
        "Backfilling LLM classification for %d rows (dry_run=%s)",
        len(rows),
        args.dry_run,
    )

    success = 0
    failures = 0
    verdict_counts: dict[str, int] = {}

    for i, row in enumerate(rows, start=1):
        row_dict = dict(row)
        photo_urls = (
            [row_dict["image_url"]] if row_dict.get("image_url") else []
        )

        try:
            result = classify_listing(row_dict, photo_urls=photo_urls)
        except Exception as exc:
            logger.warning(
                "Classification raised for aqar_id=%s: %s",
                row_dict.get("aqar_id"),
                exc,
            )
            failures += 1
            continue

        verdict = result["llm_suitability_verdict"]
        verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1

        if not args.dry_run:
            session.execute(
                text(
                    """
                    UPDATE commercial_unit
                    SET llm_suitability_verdict = :verdict,
                        llm_suitability_score = :sscore,
                        llm_listing_quality_score = :lqscore,
                        llm_landlord_signal_score = :lsscore,
                        llm_reasoning = :reasoning,
                        llm_classified_at = :classified_at,
                        llm_classifier_version = :version
                    WHERE aqar_id = :aqar_id
                """
                ),
                {
                    "verdict": result["llm_suitability_verdict"],
                    "sscore": result["llm_suitability_score"],
                    "lqscore": result["llm_listing_quality_score"],
                    "lsscore": result["llm_landlord_signal_score"],
                    "reasoning": result["llm_reasoning"],
                    "classified_at": result["llm_classified_at"],
                    "version": result["llm_classifier_version"],
                    "aqar_id": row_dict["aqar_id"],
                },
            )
            if i % 25 == 0:
                session.commit()
                logger.info(
                    "Progress: %d/%d (suitable=%d unsuitable=%d uncertain=%d)",
                    i,
                    len(rows),
                    verdict_counts.get("suitable", 0),
                    verdict_counts.get("unsuitable", 0),
                    verdict_counts.get("uncertain", 0),
                )

        success += 1
        # Be polite to OpenAI's rate limits — 0.1s = 10 req/s max
        time.sleep(0.1)

    if not args.dry_run:
        session.commit()

    logger.info(
        "Backfill complete. success=%d failures=%d verdicts=%s",
        success,
        failures,
        verdict_counts,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
