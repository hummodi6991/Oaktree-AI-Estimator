#!/usr/bin/env python3
"""Read-only post-deploy verification for Phase 3 chunk 1.

Prints a summary table of the eight new fields on ``expansion_candidate``
(six rerank columns + ``decision_memo_present`` + ``decision_memo_json``
presence) for every candidate belonging to a given search.

Usage (Codespace against production):

    SEARCH_ID=303f4ba5-b61e-42dc-bd22-6ff04ef5b537 \\
    PGHOST=... PGPORT=... PGUSER=... PGPASSWORD=... PGDATABASE=... \\
      python scripts/diagnostics/verify_phase3_chunk1.py

This script is intentionally read-only: it executes a single SELECT and
closes the connection. It does NOT hardcode credentials; all connection
parameters come from standard libpq env vars.

Expected output with the rerank flag off (the default):

* Every row has ``deterministic_rank == final_rank == rank_position``.
* ``rerank_applied`` is False for every row.
* ``rerank_delta`` is 0 for every row.
* ``rerank_status`` is ``flag_off`` (or NULL if the row predates
  the migration; the pre-warm and UI do not depend on the status for
  correctness, only for explainability).
* ``decision_memo_present`` flips to True as the pre-warm batch runs
  (or as users open candidate memos via POST /decision-memo).
"""
from __future__ import annotations

import os
import sys
from collections import Counter

try:
    import psycopg  # psycopg3
    _DRIVER = "psycopg"
except ImportError:  # pragma: no cover — older envs
    try:
        import psycopg2 as psycopg  # type: ignore[no-redef]
        _DRIVER = "psycopg2"
    except ImportError:
        print(
            "ERROR: neither psycopg nor psycopg2 is installed in this env. "
            "Install one (or run from a venv that has them).",
            file=sys.stderr,
        )
        sys.exit(2)


QUERY = """
    SELECT
        id,
        parcel_id,
        rank_position,
        deterministic_rank,
        final_rank,
        rerank_applied,
        rerank_delta,
        rerank_status,
        (decision_memo IS NOT NULL)      AS has_memo_text,
        (decision_memo_json IS NOT NULL) AS has_memo_json
    FROM expansion_candidate
    WHERE search_id = %s
    ORDER BY rank_position ASC NULLS LAST, final_rank ASC NULLS LAST
"""


def _connect():
    kwargs = {
        "host": os.environ.get("PGHOST"),
        "port": os.environ.get("PGPORT"),
        "user": os.environ.get("PGUSER"),
        "password": os.environ.get("PGPASSWORD"),
        "dbname": os.environ.get("PGDATABASE"),
    }
    kwargs = {k: v for k, v in kwargs.items() if v}
    if _DRIVER == "psycopg":
        return psycopg.connect(**kwargs)
    return psycopg.connect(**kwargs)  # psycopg2 accepts the same kwargs


def main() -> int:
    search_id = os.environ.get("SEARCH_ID")
    if not search_id:
        print("ERROR: SEARCH_ID env var is required", file=sys.stderr)
        return 2

    try:
        conn = _connect()
    except Exception as exc:
        print(f"ERROR: cannot connect to database: {exc}", file=sys.stderr)
        return 2

    try:
        cur = conn.cursor()
        cur.execute(QUERY, (search_id,))
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        print(f"No candidates found for search_id={search_id}")
        return 1

    print(f"search_id={search_id}  candidates={len(rows)}")
    print()
    header = (
        f"{'rank':>4}  {'det':>4}  {'fin':>4}  {'app':>3}  "
        f"{'d':>3}  {'status':<24}  {'txt':>3}  {'jsn':>3}  parcel_id"
    )
    print(header)
    print("-" * len(header))

    statuses: Counter[str] = Counter()
    applied_any = False
    rank_mismatch = 0
    memo_text_count = 0
    memo_json_count = 0

    for row in rows:
        (
            _id, parcel_id, rank_pos, det_rank, fin_rank,
            applied, delta, status,
            has_txt, has_json,
        ) = row
        statuses[str(status)] += 1
        if applied:
            applied_any = True
        if has_txt:
            memo_text_count += 1
        if has_json:
            memo_json_count += 1
        if det_rank is not None and fin_rank is not None and rank_pos is not None:
            if not (det_rank == fin_rank == rank_pos):
                rank_mismatch += 1
        print(
            f"{_fmt(rank_pos):>4}  {_fmt(det_rank):>4}  {_fmt(fin_rank):>4}  "
            f"{('Y' if applied else 'n'):>3}  {_fmt(delta):>3}  "
            f"{(status or '-'):<24}  "
            f"{('Y' if has_txt else '-'):>3}  {('Y' if has_json else '-'):>3}  "
            f"{parcel_id}"
        )

    print()
    print("Summary:")
    for status, count in sorted(statuses.items()):
        print(f"  rerank_status={status:<24}  count={count}")
    print(f"  rerank_applied=True on any row: {applied_any}")
    print(f"  rows where det_rank == final_rank == rank_position: "
          f"{len(rows) - rank_mismatch}/{len(rows)}")
    print(f"  decision_memo (text): {memo_text_count}/{len(rows)}")
    print(f"  decision_memo_json:   {memo_json_count}/{len(rows)}")
    return 0


def _fmt(v) -> str:
    return "-" if v is None else str(v)


if __name__ == "__main__":
    sys.exit(main())
