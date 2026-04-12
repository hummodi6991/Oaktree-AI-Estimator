#!/usr/bin/env python3
"""Repair commercial_unit rows whose area_sqm was corrupted by the
pre-Patch-14 Aqar area parser.

Selects active store/showroom rows with implausible area_sqm, re-fetches
each listing's HTML from the stored ``listing_url``, re-parses the area
with the new disambiguator, and updates ``area_sqm`` in place if the new
value is plausible AND different from the stored value.

Marks repaired rows for selective LLM re-classification by clearing
``llm_classified_at``, so the existing auto-backfill path picks them up
on the next deploy and re-scores them with correct area context.

Safety guardrails:

  - Hard ceiling of 100 rows per invocation (``_HARD_ROW_CEILING``).
  - 1-second delay between HTTP requests (``_REQUEST_DELAY_SEC``).
  - ``--dry-run`` prints the planned repairs without touching the DB.
  - Manual trigger only (no auto-fire on deploy) — this script re-fetches
    Aqar pages and must not become a recurring load on their servers.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from scripts.scrape_aqar import _AREA_RE, _parse_area_token

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

_HARD_ROW_CEILING = 100
_REQUEST_DELAY_SEC = 1.0  # be polite to Aqar

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 "
    "OaktreeAtlas-AreaRepair/1.0"
)


def _build_engine():
    return create_engine(
        f"postgresql://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}"
        f"/{os.environ['POSTGRES_DB']}",
        connect_args={"sslmode": os.environ.get("POSTGRES_SSLMODE") or "require"},
    )


def _fetch_listing_html(url: str) -> str | None:
    try:
        r = requests.get(url, timeout=15, headers={"User-Agent": _USER_AGENT})
        if r.status_code != 200:
            logger.warning("Non-200 (%s) for %s", r.status_code, url)
            return None
        return r.text
    except Exception as exc:  # pragma: no cover - network error path
        logger.warning("Fetch failed for %s: %s", url, exc)
        return None


def _extract_detail_page_area_text(html: str) -> str | None:
    """Pull a raw area token from an Aqar detail page.

    The Aqar card DOM (which the scraper's ``_parse_listing_from_card``
    already understands) reappears on the detail page in the Listing
    Details panel: an ``<img src=".../area.svg">`` followed by a sibling
    ``<span>`` containing a token like ``"120,205 m²"``.  We look for that
    first, since the "Area" label also appears in the Info panel where
    the value is rendered without the decimal separator (un-disambiguable).

    Returns the raw token with the ``m²`` unit still attached, or None
    if no area element was found.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Strategy 1: reuse the scraper's area.svg + sibling span convention.
    for img_el in soup.find_all("img", src=True):
        src = img_el.get("src", "")
        if "area.svg" not in src:
            continue
        sibling = img_el.find_next("span")
        if not sibling:
            continue
        span_text = sibling.get_text(strip=True)
        if _AREA_RE.search(span_text):
            return span_text

    # Strategy 2: regex-scan the full page text for an "m²" token that
    # looks like an area.  Last resort if the DOM structure shifted.
    page_text = soup.get_text(" ", strip=True)
    match = _AREA_RE.search(page_text)
    if match:
        return match.group(0)

    return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Repair commercial_unit rows with corrupted area_sqm values. "
            "Re-fetches each listing's HTML and re-parses the area token "
            "with the Patch-14 disambiguator."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned repairs without writing to the DB.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap the number of rows processed (still subject to hard ceiling).",
    )
    args = parser.parse_args()

    engine = _build_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    rows = session.execute(
        text(
            """
            SELECT aqar_id, listing_type, neighborhood, area_sqm, listing_url
            FROM commercial_unit
            WHERE status = 'active'
              AND listing_type IN ('store', 'showroom')
              AND (area_sqm > 1000 OR area_sqm < 5)
              AND listing_url IS NOT NULL
            ORDER BY area_sqm DESC NULLS LAST
            """
        )
    ).mappings().all()

    if args.limit is not None:
        rows = rows[: args.limit]

    if len(rows) > _HARD_ROW_CEILING:
        logger.error(
            "Refusing to process %d rows (ceiling %d). Use --limit to shrink the batch.",
            len(rows),
            _HARD_ROW_CEILING,
        )
        return 1

    logger.info("Repairing %d rows (dry_run=%s)", len(rows), args.dry_run)

    repaired = 0
    skipped = 0
    failed = 0

    for row in rows:
        aqar_id = row["aqar_id"]
        old_area = float(row["area_sqm"]) if row["area_sqm"] is not None else None
        url = row["listing_url"]
        listing_type = row["listing_type"]

        html = _fetch_listing_html(url)
        if not html:
            failed += 1
            time.sleep(_REQUEST_DELAY_SEC)
            continue

        area_text = _extract_detail_page_area_text(html)
        new_area = _parse_area_token(area_text, listing_type=listing_type)

        if new_area is None:
            logger.info(
                "aqar_id=%s: parse returned None for raw=%r — skipping",
                aqar_id,
                area_text,
            )
            skipped += 1
            time.sleep(_REQUEST_DELAY_SEC)
            continue

        if old_area is not None and abs(new_area - old_area) < 0.5:
            logger.info(
                "aqar_id=%s: new area %.3f matches existing — skipping",
                aqar_id,
                new_area,
            )
            skipped += 1
            time.sleep(_REQUEST_DELAY_SEC)
            continue

        logger.info(
            "aqar_id=%s: REPAIR area_sqm %.2f → %.3f (raw=%r, district=%s)",
            aqar_id,
            old_area if old_area is not None else 0.0,
            new_area,
            area_text,
            row["neighborhood"],
        )

        if not args.dry_run:
            session.execute(
                text(
                    """
                    UPDATE commercial_unit
                    SET area_sqm = :new_area,
                        llm_classified_at = NULL,
                        last_seen_at = now()
                    WHERE aqar_id = :aqar_id
                    """
                ),
                {"new_area": new_area, "aqar_id": aqar_id},
            )
            session.commit()

        repaired += 1
        time.sleep(_REQUEST_DELAY_SEC)

    logger.info(
        "Repair complete. repaired=%d skipped=%d failed=%d",
        repaired,
        skipped,
        failed,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
