"""
CLI commands for the delivery data pipeline.

Usage:
    python -m app.delivery.cli scrape --platform hungerstation
    python -m app.delivery.cli scrape --all
    python -m app.delivery.cli resolve
    python -m app.delivery.cli stats
    python -m app.delivery.cli inspect --run-id 42
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


def cmd_scrape(args: argparse.Namespace) -> None:
    """Run scraper pipeline for one or all platforms."""
    from app.db.session import SessionLocal
    from app.delivery.pipeline import run_all_platforms, run_platform_scrape

    db = SessionLocal()
    try:
        if args.platform:
            result = run_platform_scrape(
                db,
                args.platform,
                max_pages=args.max_pages,
                run_resolver=not args.no_resolve,
            )
            print(json.dumps(result, indent=2, default=str))
        elif args.all:
            results = run_all_platforms(
                db,
                max_pages=args.max_pages,
                run_resolver=not args.no_resolve,
            )
            print(json.dumps(results, indent=2, default=str))
        else:
            print("Specify --platform <name> or --all")
            sys.exit(1)
    finally:
        db.close()


def cmd_resolve(args: argparse.Namespace) -> None:
    """Run entity resolver on pending records."""
    from app.db.session import SessionLocal
    from app.delivery.resolver import resolve_all_pending

    db = SessionLocal()
    try:
        matched = resolve_all_pending(db, limit=args.limit)
        db.commit()
        print(f"Matched {matched} records")
    finally:
        db.close()


def cmd_stats(args: argparse.Namespace) -> None:
    """Print data quality report."""
    from app.db.session import SessionLocal
    from app.delivery.stats import delivery_data_quality_report

    db = SessionLocal()
    try:
        report = delivery_data_quality_report(db)
        print(json.dumps(report, indent=2, default=str))
    finally:
        db.close()


def cmd_inspect(args: argparse.Namespace) -> None:
    """Inspect a specific ingest run."""
    from app.db.session import SessionLocal
    from app.delivery.models import DeliveryIngestRun, DeliverySourceRecord

    db = SessionLocal()
    try:
        run = db.query(DeliveryIngestRun).filter_by(id=args.run_id).first()
        if not run:
            print(f"Run {args.run_id} not found")
            sys.exit(1)

        print(f"Run #{run.id}")
        print(f"  Platform:  {run.platform}")
        print(f"  Status:    {run.status}")
        print(f"  Started:   {run.started_at}")
        print(f"  Finished:  {run.finished_at}")
        print(f"  Scraped:   {run.rows_scraped}")
        print(f"  Parsed:    {run.rows_parsed}")
        print(f"  Inserted:  {run.rows_inserted}")
        print(f"  Skipped:   {run.rows_skipped}")
        print(f"  Matched:   {run.rows_matched}")
        if run.error_summary:
            print(f"  Errors:    {json.dumps(run.error_summary, indent=4)}")

        # Sample records
        sample = (
            db.query(DeliverySourceRecord)
            .filter_by(ingest_run_id=args.run_id)
            .limit(5)
            .all()
        )
        if sample:
            print(f"\nSample records ({len(sample)} shown):")
            for rec in sample:
                print(
                    f"  - {rec.restaurant_name_raw or '?'} | "
                    f"{rec.platform} | "
                    f"district={rec.district_text} | "
                    f"loc_conf={rec.location_confidence} | "
                    f"status={rec.entity_resolution_status}"
                )
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delivery data pipeline CLI",
        prog="python -m app.delivery.cli",
    )
    sub = parser.add_subparsers(dest="command")

    # scrape
    p_scrape = sub.add_parser("scrape", help="Run scraper pipeline")
    p_scrape.add_argument("--platform", type=str, help="Platform key (e.g. hungerstation)")
    p_scrape.add_argument("--all", action="store_true", help="Run all platforms")
    p_scrape.add_argument("--max-pages", type=int, default=200)
    p_scrape.add_argument("--no-resolve", action="store_true", help="Skip entity resolution")
    p_scrape.set_defaults(func=cmd_scrape)

    # resolve
    p_resolve = sub.add_parser("resolve", help="Run entity resolver")
    p_resolve.add_argument("--limit", type=int, default=5000)
    p_resolve.set_defaults(func=cmd_resolve)

    # stats
    p_stats = sub.add_parser("stats", help="Print data quality report")
    p_stats.set_defaults(func=cmd_stats)

    # inspect
    p_inspect = sub.add_parser("inspect", help="Inspect an ingest run")
    p_inspect.add_argument("--run-id", type=int, required=True)
    p_inspect.set_defaults(func=cmd_inspect)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
