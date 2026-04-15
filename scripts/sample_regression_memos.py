"""Sample regression memos — run the four standard Expansion-Advisor searches
end-to-end, generate a structured decision memo for each top-ranked candidate,
and dump the raw ``memo_json`` for eyeballing prompt/signal quality.

WHAT THIS IS
    A read-mostly CLI sanity script. For each of the four canonical searches
    (QSR burger in Al Olaya; delivery shawarma citywide; dine-in Indian in
    Al Nakheel; café in Al Yasmin) it:

      1. Calls ``run_expansion_search(db, …)`` directly against the service
         layer (NOT the HTTP API). This creates an ``expansion_search`` row
         and the usual ``expansion_candidate`` rows via the normal pipeline.
      2. Picks rank 1.
      3. Runs ``build_memo_context`` → ``generate_structured_memo`` directly.
         Does NOT hit the /decision-memo endpoint, does NOT write to the
         ``decision_memo`` / ``decision_memo_json`` columns. We never want
         this script to prime the decision-memo cache because future runs
         should regenerate cleanly against a new prompt or a new signal.

WHEN TO RUN IT
    - After tweaking the structured-memo prompt (``STRUCTURED_MEMO_SYSTEM_PROMPT``
      or ``render_structured_memo_prompt``) and wanting to eyeball the delta.
    - After calibrating a new signal (realized demand, delivery blend, rent
      comps, etc.) and wanting to see whether the memo surfaces the change.
    - Before any PR that touches memo wording or scoring components — use
      this as the human-judgment sanity pass that the pytest suite cannot
      substitute for.

COST AND PREREQS
    Requires ``OPENAI_API_KEY`` to be set in the environment. Requires a
    populated Riyadh database reachable via the app's normal DB settings
    (``DATABASE_URL`` or ``POSTGRES_*``) — the four searches rely on real
    parcel, competitor, delivery, and rent-comp data.

    One full run issues four ``chat.completions.create`` calls against
    ``settings.EXPANSION_MEMO_MODEL`` (default ``gpt-4o-mini``). Typical
    cost with ``gpt-4o-mini`` is ~$0.01–0.04 total across four searches.
    Not free — don't loop it in CI.

USAGE
    python scripts/sample_regression_memos.py
    python scripts/sample_regression_memos.py --search cafe_al_yasmin
    python scripts/sample_regression_memos.py --out /tmp/memos.json

OUTPUT
    A single JSON object keyed by search name; each value is either the
    raw structured ``memo_json`` produced by the LLM or
    ``{"error": "<reason>", "skipped": true}`` when the search or memo
    step couldn't run.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from typing import Any

logger = logging.getLogger("sample_regression_memos")


# ── Canonical regression briefs ─────────────────────────────────────
#
# Hardcoded on purpose: this is a one-off quality check, not a configurable
# tool. A generous Riyadh bbox covers every search so the district filter
# is the discriminator that matters.

_RIYADH_BBOX = {
    "min_lon": 46.40,
    "min_lat": 24.40,
    "max_lon": 47.00,
    "max_lat": 25.00,
}


REGRESSION_BRIEFS: dict[str, dict[str, Any]] = {
    "qsr_burger_al_olaya": {
        "brand_name": "Sample Burger Co",
        "category": "burger",
        "service_model": "qsr",
        "min_area_m2": 100,
        "target_area_m2": 160,
        "max_area_m2": 250,
        "limit": 12,
        "bbox": _RIYADH_BBOX,
        "target_districts": ["Al Olaya"],
        "existing_branches": [],
        "brand_profile": {
            "primary_channel": "dine_in",
            "price_tier": "mid",
        },
    },
    "delivery_shawarma_citywide": {
        "brand_name": "Sample Shawarma Co",
        "category": "shawarma",
        "service_model": "delivery",
        "min_area_m2": 50,
        "target_area_m2": 80,
        "max_area_m2": 120,
        "limit": 12,
        "bbox": _RIYADH_BBOX,
        "target_districts": [],
        "existing_branches": [],
        "brand_profile": {
            "primary_channel": "delivery",
            "price_tier": "value",
        },
    },
    "dine_in_indian_al_nakheel": {
        "brand_name": "Sample Indian Co",
        "category": "indian",
        "service_model": "dine_in",
        "min_area_m2": 150,
        "target_area_m2": 250,
        "max_area_m2": 400,
        "limit": 12,
        "bbox": _RIYADH_BBOX,
        "target_districts": ["Al Nakheel"],
        "existing_branches": [],
        "brand_profile": {
            "primary_channel": "dine_in",
            "price_tier": "premium",
        },
    },
    "cafe_al_yasmin": {
        "brand_name": "Sample Cafe Co",
        "category": "cafe",
        "service_model": "cafe",
        "min_area_m2": 60,
        "target_area_m2": 100,
        "max_area_m2": 150,
        "limit": 12,
        "bbox": _RIYADH_BBOX,
        "target_districts": ["Al Yasmin"],
        "existing_branches": [],
        "brand_profile": {
            "primary_channel": "dine_in",
            "price_tier": "mid",
        },
    },
}


_REQUIRED_CANDIDATE_FIELDS = (
    "parcel_id",
    "search_id",
    "feature_snapshot_json",
    "score_breakdown_json",
)


def _pick_rank_one(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Return the candidate at rank 1, falling back to the first item when
    ``rank_position`` isn't populated. Returns None for an empty list."""
    if not candidates:
        return None
    for c in candidates:
        if c.get("rank_position") == 1:
            return c
    return candidates[0]


def _skipped(reason: str) -> dict[str, Any]:
    return {"error": reason, "skipped": True}


def _run_one_search(
    db,
    name: str,
    brief: dict[str, Any],
) -> dict[str, Any]:
    """Run one search end-to-end and return the resulting ``memo_json``
    or a ``{"error", "skipped"}`` dict explaining why not."""
    # Imports are local so ``--help`` works without hitting DB / config.
    from app.services.expansion_advisor import run_expansion_search
    from app.services.llm_decision_memo import (
        build_memo_context,
        generate_structured_memo,
    )

    search_id = str(uuid.uuid4())
    try:
        candidates = run_expansion_search(
            db,
            search_id=search_id,
            brand_name=brief["brand_name"],
            category=brief["category"],
            service_model=brief["service_model"],
            min_area_m2=brief["min_area_m2"],
            max_area_m2=brief["max_area_m2"],
            target_area_m2=brief["target_area_m2"],
            limit=brief["limit"],
            bbox=brief.get("bbox"),
            target_districts=brief.get("target_districts"),
            existing_branches=brief.get("existing_branches"),
            brand_profile=brief.get("brand_profile"),
        )
    except Exception as exc:
        logger.warning("[%s] run_expansion_search raised: %s", name, exc)
        try:
            db.rollback()
        except Exception:
            pass
        return _skipped(f"run_expansion_search raised: {exc}")

    rank_one = _pick_rank_one(candidates)
    if rank_one is None:
        logger.warning("[%s] search returned 0 candidates", name)
        return _skipped("search returned 0 candidates")

    missing = [f for f in _REQUIRED_CANDIDATE_FIELDS if not rank_one.get(f)]
    if missing:
        logger.warning(
            "[%s] rank-1 candidate missing required fields: %s",
            name, missing,
        )
        return _skipped(f"rank-1 candidate missing fields: {missing}")

    try:
        ctx = build_memo_context(candidate=rank_one, brief=brief, lang="en")
    except Exception as exc:
        logger.warning("[%s] build_memo_context raised: %s", name, exc)
        return _skipped(f"build_memo_context raised: {exc}")

    memo_json = generate_structured_memo(ctx)
    if memo_json is None:
        # generate_structured_memo already logged the specific reason
        # (flag off / ceiling / api error / malformed / missing keys).
        logger.warning(
            "[%s] generate_structured_memo returned None — see prior WARNING",
            name,
        )
        return _skipped(
            "generate_structured_memo returned None "
            "(flag off, ceiling hit, LLM error, or malformed response — "
            "see preceding WARNING log lines)"
        )

    return memo_json


def main(
    argv: list[str] | None = None,
    *,
    db=None,
    briefs: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Entry point.

    Args:
        argv: CLI args (``None`` → ``sys.argv[1:]``).
        db: Injected DB session for tests. Production uses ``SessionLocal()``.
        briefs: Injected briefs dict for tests. Production uses
            ``REGRESSION_BRIEFS``.

    Returns the results dict (also written to stdout / ``--out``).
    """
    parser = argparse.ArgumentParser(
        description="Run the four standard expansion-advisor regression "
                    "searches and print structured memo_json for each."
    )
    parser.add_argument(
        "--search",
        choices=sorted(REGRESSION_BRIEFS.keys()),
        default=None,
        help="Run only this search (default: run all four).",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Write pretty-printed JSON to this path instead of stdout.",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable INFO/WARNING log output to stderr.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    briefs = briefs if briefs is not None else REGRESSION_BRIEFS
    selected = {args.search: briefs[args.search]} if args.search else briefs

    owns_db = db is None
    if owns_db:
        from app.db.session import SessionLocal
        db = SessionLocal()

    try:
        results: dict[str, Any] = {}
        for name, brief in selected.items():
            logger.info("=== running %s ===", name)
            results[name] = _run_one_search(db, name, brief)
    finally:
        if owns_db:
            try:
                db.close()
            except Exception:
                pass

    rendered = json.dumps(results, indent=2, ensure_ascii=False)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(rendered + "\n")
    else:
        print(rendered)

    return results


if __name__ == "__main__":
    main()
