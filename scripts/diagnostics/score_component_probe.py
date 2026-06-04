#!/usr/bin/env python3
"""Offline probe for the Expansion Advisor scoring components (READ-ONLY).

This is a throwaway diagnostic harness (intentionally uncommitted). It imports
the REAL scoring functions from ``app.services.expansion_advisor`` and:

  Part 1 — Component monotonicity sweep
    For each leg-producing function whose output is driven by a single
    numeric input, sweep that input across [0, 25, 50, 75, 100] plus the
    None / missing case and assert:
      * output is within [0, 100],
      * output is monotonic non-decreasing across the numeric sweep,
      * flag components whose output is FLAT across the sweep (degenerate).

  Part 2 — _score_breakdown invariants
    Build one synthetic candidate, call ``_score_breakdown`` and assert:
      * sum(weights) == 100 (+/- 1e-3),
      * each weighted_points == round(raw * weight / 100, 2),
      * final_score == round(sum(weighted_points), 2).

No DB access, no network, no writes. Run:

    python scripts/diagnostics/score_component_probe.py
"""
from __future__ import annotations

import math
from typing import Any, Callable

from app.core.config import settings
from app.services.expansion_advisor import (
    _score_breakdown,
    _chain_strength_score,
    _confidence_score,
    _landlord_signal_component,
    _competition_whitespace_score,
    _population_score,
    _delivery_score,
    _listing_quality_score,
)

SWEEP = [0, 25, 50, 75, 100]


def _fmt(v: Any) -> str:
    if v is None:
        return "None"
    if isinstance(v, float):
        return f"{v:.2f}"
    return str(v)


def _run_sweep(
    name: str,
    fn: Callable[[Any], float],
    *,
    none_expected: float | None,
    sweep_values: list[Any] | None = None,
    probe_none: bool = True,
) -> dict[str, Any]:
    """Sweep a single-input scoring function. Returns a result record."""
    values = sweep_values if sweep_values is not None else SWEEP
    outputs: list[float] = []
    notes: list[str] = []
    in_range = True
    monotonic = True

    for x in values:
        y = fn(x)
        outputs.append(y)
        if not (0.0 <= y <= 100.0 + 1e-9):
            in_range = False
            notes.append(f"OUT_OF_RANGE@{_fmt(x)}={_fmt(y)}")

    for a, b in zip(outputs, outputs[1:]):
        if b + 1e-9 < a:
            monotonic = False
            notes.append("NON_MONOTONIC")
            break

    flat = len(set(round(o, 6) for o in outputs)) == 1

    none_out: float | None = None
    none_ok = True
    if probe_none:
        none_out = fn(None)
        if none_expected is not None:
            none_ok = abs(none_out - none_expected) < 1e-6
            if not none_ok:
                notes.append(
                    f"NONE_DEFAULT_MISMATCH expected={_fmt(none_expected)} "
                    f"got={_fmt(none_out)}"
                )
        if not (0.0 <= none_out <= 100.0 + 1e-9):
            in_range = False
            notes.append(f"NONE_OUT_OF_RANGE={_fmt(none_out)}")

    return {
        "name": name,
        "values": values,
        "outputs": outputs,
        "none_out": none_out,
        "in_range": in_range,
        "monotonic": monotonic,
        "flat": flat,
        "none_ok": none_ok,
        "notes": notes,
    }


def part1_component_sweeps() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    # 1. chain_strength: single float input, None -> 50.0 neutral.
    results.append(
        _run_sweep(
            "chain_strength (_chain_strength_score)",
            lambda x: _chain_strength_score(x),
            none_expected=50.0,
        )
    )

    # 2. landlord_signal: single int/float input, None -> 50.0 neutral.
    results.append(
        _run_sweep(
            "landlord_signal (_landlord_signal_component)",
            lambda x: _landlord_signal_component(x),
            none_expected=50.0,
        )
    )

    # 3. competition_whitespace: input is competitor_count (lower => higher
    #    score), so we sweep on a DESCENDING axis to assert non-decreasing
    #    output. confident=True so count=0 earns the wide-open 100.
    #    None handled separately below (count is an int leg, not Optional).
    whitespace_counts = [20, 12, 8, 5, 2, 0]
    ws = _run_sweep(
        "competition_whitespace (_competition_whitespace_score, confident=True, "
        "count DESC)",
        lambda c: _competition_whitespace_score(int(c), confident=True),
        none_expected=None,
        sweep_values=whitespace_counts,
        probe_none=False,
    )
    # Document the missing/unknown-data neutral default explicitly (count<=0
    # with confident falsy -> 50.0).
    ws["unknown_default"] = _competition_whitespace_score(0, confident=None)
    results.append(ws)

    # 4. population: single float input (population_reach). reach<=0 -> 0.0
    #    (this leg does NOT default to neutral; absent pop scores 0).
    pop_reach = [0, 20000, 40000, 80000, 160000]
    results.append(
        _run_sweep(
            "demand:population (_population_score, qsr)",
            lambda r: _population_score(float(r), service_model="qsr"),
            none_expected=None,
            sweep_values=pop_reach,
            probe_none=False,
        )
    )

    # 5. delivery: single int input (delivery_listing_count). count<=0 -> 0.0.
    delivery_counts = [0, 5, 10, 20, 40]
    results.append(
        _run_sweep(
            "delivery (_delivery_score, listing_count)",
            lambda c: _delivery_score(int(c)),
            none_expected=None,
            sweep_values=delivery_counts,
            probe_none=False,
        )
    )

    # 6. confidence (parcel path): cap at 70. Sweep population_reach as the
    #    monotonic driver while holding the other adders fixed.
    results.append(
        _run_sweep(
            "confidence:parcel (_confidence_score is_listing=False, pop driver, cap 70)",
            lambda r: _confidence_score(
                is_listing=False,
                landuse_label="commercial",
                population_reach=float(r),
                delivery_listing_count=0,
            ),
            none_expected=None,
            sweep_values=pop_reach,
            probe_none=False,
        )
    )

    # 7. listing_quality (parcel path): not a listing -> always neutral 50
    #    (degenerate by design for parcels). Sweep effective_age_days for the
    #    listing path to show the freshness monotonicity (older => lower, so
    #    sweep DESCending age to assert non-decreasing output).
    age_desc = [400, 300, 200, 100, 50, 10]
    lq = _run_sweep(
        "listing_quality (_listing_quality_score is_listing=True, age DESC driver)",
        lambda d: _listing_quality_score(
            is_listing=True,
            effective_age_days=int(d),
            is_furnished=False,
            unit_restaurant_score=None,
            has_image=False,
            district_momentum_score=None,
        ),
        none_expected=None,
        sweep_values=age_desc,
        probe_none=False,
    )
    # effective_age_days=None -> neutral 50.0 freshness; the whole-parcel
    # neutral default is is_listing=False -> 50.0.
    lq["age_none_out"] = _listing_quality_score(
        is_listing=True,
        effective_age_days=None,
        is_furnished=False,
        unit_restaurant_score=None,
        has_image=False,
        district_momentum_score=None,
    )
    lq["parcel_neutral"] = _listing_quality_score(
        is_listing=False,
        effective_age_days=None,
        is_furnished=None,
        unit_restaurant_score=None,
        has_image=False,
    )
    results.append(lq)

    return results


def part2_score_breakdown() -> dict[str, Any]:
    """Build a synthetic candidate, call _score_breakdown, assert invariants."""
    bd = _score_breakdown(
        demand_score=72.0,
        whitespace_score=64.0,
        brand_fit_score=58.0,
        economics_score=66.5,
        provider_intelligence_composite=49.0,
        access_visibility_score=70.0,
        confidence_score=80.0,
        listing_quality_score=55.0,
        landlord_signal_score=62,
        chain_strength_score=44.0,
        chain_strength_max=88.0,
        brand_profile=None,
        service_model="qsr",
    )

    checks: list[tuple[str, bool, str]] = []

    weights = bd["weights"]
    wsum = sum(weights.values())
    checks.append(
        ("sum(weights) == 100", abs(wsum - 100.0) < 1e-3, f"sum={wsum:.6f}")
    )

    # weighted_points == round(raw * weight / 100, 2) for each leg.
    display = bd["display"]
    wp_ok = True
    wp_detail: list[str] = []
    for name, d in display.items():
        raw = d["raw_input_score"]
        w = d["weight_percent"]
        wp = d["weighted_points"]
        expect = round(raw * w / 100.0, 2)
        ok = abs(wp - expect) < 1e-9
        wp_ok = wp_ok and ok
        wp_detail.append(
            f"{name}: raw={raw} w={w} wp={wp} expect={expect} {'OK' if ok else 'MISMATCH'}"
        )
    checks.append(
        ("each weighted_points == round(raw*weight/100, 2)", wp_ok, "")
    )

    # final_score == round(sum(weighted_points), 2)
    wp_sum = round(sum(d["weighted_points"] for d in display.values()), 2)
    fs = bd["final_score"]
    checks.append(
        (
            "final_score == round(sum(weighted_points), 2)",
            abs(fs - wp_sum) < 1e-9,
            f"final_score={fs} sum(wp)={wp_sum}",
        )
    )

    return {
        "breakdown": bd,
        "checks": checks,
        "wp_detail": wp_detail,
    }


def main() -> int:
    print("=" * 78)
    print("EXPANSION ADVISOR — OFFLINE SCORING COMPONENT PROBE")
    print(f"EXPANSION_CHAIN_STRENGTH_WEIGHT = {settings.EXPANSION_CHAIN_STRENGTH_WEIGHT}")
    print("=" * 78)

    overall_ok = True

    # ── Part 1 ──────────────────────────────────────────────────────────
    print("\nPART 1 — COMPONENT MONOTONICITY SWEEP")
    print("-" * 78)
    hdr = f"{'component':<58}{'range':>6}{'mono':>6}{'flat':>6}"
    print(hdr)
    print("-" * 78)
    p1 = part1_component_sweeps()
    for r in p1:
        leg_ok = r["in_range"] and r["monotonic"] and r["none_ok"]
        # Flatness is a WARNING (degenerate), not a hard failure, unless the
        # component is supposed to vary. We surface it but do not fail on it.
        overall_ok = overall_ok and leg_ok
        print(
            f"{r['name']:<58}"
            f"{'PASS' if r['in_range'] else 'FAIL':>6}"
            f"{'PASS' if r['monotonic'] else 'FAIL':>6}"
            f"{'FLAT' if r['flat'] else 'vary':>6}"
        )
        sweep_str = ", ".join(
            f"{_fmt(x)}->{_fmt(y)}" for x, y in zip(r["values"], r["outputs"])
        )
        print(f"      sweep: {sweep_str}")
        if r["none_out"] is not None:
            print(f"      None-input -> {_fmt(r['none_out'])} (neutral-default check)")
        for extra_key in ("unknown_default", "age_none_out", "parcel_neutral"):
            if extra_key in r:
                print(f"      {extra_key} -> {_fmt(r[extra_key])}")
        if r["notes"]:
            print(f"      NOTES: {'; '.join(r['notes'])}")

    # ── Part 2 ──────────────────────────────────────────────────────────
    print("\nPART 2 — _score_breakdown INVARIANTS")
    print("-" * 78)
    p2 = part2_score_breakdown()
    for label, ok, detail in p2["checks"]:
        overall_ok = overall_ok and ok
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}" + (f"  ({detail})" if detail else ""))
    print("\n  Per-leg weighted_points detail:")
    for line in p2["wp_detail"]:
        print(f"    {line}")
    print(f"\n  weights dict: {p2['breakdown']['weights']}")
    print(f"  final_score : {p2['breakdown']['final_score']}")

    print("\n" + "=" * 78)
    print(f"OVERALL: {'ALL CHECKS PASS' if overall_ok else 'FAILURES PRESENT'}")
    print("=" * 78)
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
