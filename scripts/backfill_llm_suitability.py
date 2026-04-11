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

# Hard cost ceiling.  The script tracks estimated spend on every API
# call and aborts the moment it crosses this threshold.  This catches
# all cost-runaway scenarios — over-large batches, per-row token
# blowup, photo-retry storms, and internal SDK retry loops — because
# it tracks the actual variable that hurts (money spent), not a proxy.
COST_CEILING_USD = 8.00

# Approximate per-call cost.  gpt-4o-mini at $0.15/M input + $0.60/M
# output, ~600 input tokens + ~150 output tokens per classification.
# Photo retries roughly double the per-row cost; the in-loop tripwire
# accounts for this naturally because each call is counted separately.
EST_COST_PER_CALL_USD = 0.0003


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

    estimated_cost_usd = len(rows) * EST_COST_PER_CALL_USD
    logger.info(
        "Backfilling %d rows. Estimated cost: $%.4f. Hard ceiling: $%.2f.",
        len(rows),
        estimated_cost_usd,
        COST_CEILING_USD,
    )
    if estimated_cost_usd > COST_CEILING_USD:
        logger.error(
            "Estimated cost ($%.4f) exceeds ceiling ($%.2f). "
            "Use --limit to reduce the batch.",
            estimated_cost_usd,
            COST_CEILING_USD,
        )
        return 2

    logger.info(
        "Backfilling LLM classification for %d rows (dry_run=%s)",
        len(rows),
        args.dry_run,
    )

    success = 0
    failures = 0
    verdict_counts: dict[str, int] = {}
    actual_call_count = 0

    for i, row in enumerate(rows, start=1):
        # Each iteration is at least one API call.  Photo retries inside
        # classify_listing are accounted for by the worst-case ceiling
        # check upfront, but in-loop we still want to halt the moment
        # estimated spend crosses the threshold so a runaway can't
        # silently consume the entire budget.
        actual_call_count += 1
        spent_so_far_usd = actual_call_count * EST_COST_PER_CALL_USD
        if spent_so_far_usd > COST_CEILING_USD:
            logger.error(
                "Hit cost ceiling at row %d/%d (~$%.4f spent). Halting.",
                i,
                len(rows),
                spent_so_far_usd,
            )
            break

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
        "Backfill complete. success=%d failures=%d verdicts=%s "
        "estimated_spend=$%.4f",
        success,
        failures,
        verdict_counts,
        actual_call_count * EST_COST_PER_CALL_USD,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
