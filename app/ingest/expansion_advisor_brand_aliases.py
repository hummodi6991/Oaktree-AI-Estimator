"""Brand alias seed loader.

Reads ``data/brand_aliases.csv`` and UPSERTs into the ``brand_alias`` table.
Idempotent: re-running on the same CSV updates display names + notes
without creating duplicates. Rows with empty ``canonical_brand_id`` are
skipped (these are non-chain rows in the source CSV — generic descriptors,
city names, cuisine types).
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path

from sqlalchemy import text

from app.db.session import SessionLocal


logger = logging.getLogger("expansion_advisor.brand_aliases")

# Standard data-path convention (per app/ingest/real_estate_indices.py:11)
DATA_PATH = (
    Path(__file__).resolve().parents[2] / "data" / "brand_aliases.csv"
)

# Required columns. The CSV has additional audit columns (total_pois,
# sample_raw_names) that are intentionally ignored.
_REQUIRED_COLUMNS = {
    "chain_key",
    "canonical_brand_id",
    "display_name_en",
    "display_name_ar",
    "notes",
}


def _load_csv(path: Path) -> list[dict[str, str]]:
    """Read CSV and return cleaned rows ready to upsert.

    Skips rows with empty canonical_brand_id (non-chain rows).
    """
    if not path.exists():
        raise FileNotFoundError(f"brand_aliases.csv not found at {path}")

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        missing = _REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"CSV missing required columns: {sorted(missing)}"
            )

        rows: list[dict[str, str]] = []
        for raw_row in reader:
            canonical = (raw_row.get("canonical_brand_id") or "").strip()
            if not canonical:
                continue  # non-chain row; skip
            chain_key = (raw_row.get("chain_key") or "").strip()
            if not chain_key:
                continue  # malformed, no key to upsert against
            rows.append({
                "alias_key": chain_key,
                "canonical_brand_id": canonical,
                "display_name_en": (raw_row.get("display_name_en") or "").strip() or None,
                "display_name_ar": (raw_row.get("display_name_ar") or "").strip() or None,
                "notes": (raw_row.get("notes") or "").strip() or None,
            })

    return rows


def load_brand_aliases(
    db,
    *,
    csv_path: Path | None = None,
) -> dict:
    """UPSERT brand aliases from CSV into the brand_alias table.

    Returns a stats dict: {"read": N, "upserted": M, "skipped": K, "csv_path": "..."}
    """
    path = csv_path or DATA_PATH
    rows = _load_csv(path)

    if not rows:
        logger.warning("No rows to load from %s", path)
        return {"read": 0, "upserted": 0, "skipped": 0, "csv_path": str(path)}

    # ON CONFLICT upsert — Postgres-specific. Updates display names + notes
    # + canonical_brand_id (in case the human pass corrected a mapping) but
    # leaves created_at intact and bumps updated_at.
    upsert_sql = text("""
        INSERT INTO brand_alias (
            alias_key, canonical_brand_id, display_name_en, display_name_ar, notes
        ) VALUES (
            :alias_key, :canonical_brand_id, :display_name_en, :display_name_ar, :notes
        )
        ON CONFLICT (alias_key) DO UPDATE SET
            canonical_brand_id = EXCLUDED.canonical_brand_id,
            display_name_en    = EXCLUDED.display_name_en,
            display_name_ar    = EXCLUDED.display_name_ar,
            notes              = EXCLUDED.notes,
            updated_at         = now()
    """)

    upserted = 0
    for row in rows:
        db.execute(upsert_sql, row)
        upserted += 1

    db.commit()

    logger.info(
        "Brand aliases loaded: %d rows from %s (canonical_brand_ids: %d distinct)",
        upserted, path, len({r["canonical_brand_id"] for r in rows}),
    )

    return {
        "read": upserted,
        "upserted": upserted,
        "skipped": 0,
        "csv_path": str(path),
        "distinct_canonical_brands": len({r["canonical_brand_id"] for r in rows}),
    }


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Expansion Advisor — Brand Alias seed loader"
    )
    parser.add_argument(
        "--csv-path",
        type=Path,
        default=None,
        help=f"CSV path (default: {DATA_PATH})",
    )
    parser.add_argument(
        "--write-stats",
        type=str,
        default=None,
        help="Write JSON stats to path",
    )
    args = parser.parse_args()

    db = SessionLocal()
    try:
        stats = load_brand_aliases(db, csv_path=args.csv_path)
    finally:
        db.close()

    if args.write_stats:
        Path(args.write_stats).write_text(json.dumps(stats, indent=2))
        logger.info("Stats written to %s", args.write_stats)

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
