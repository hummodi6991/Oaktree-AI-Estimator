"""Offline scoring-component probe (UNCOMMITTED diagnostics harness).

Imports the REAL scoring functions from app.services.expansion_advisor and:

  1. Sweeps each leg's *driving* input across [0,25,50,75,100] + the
     None/missing case, asserting the output stays within [0,100] and is
     monotonic in the score-improving direction. Flags any leg whose output
     is FLAT (degenerate) across the sweep.

  2. Builds one synthetic candidate, calls _score_breakdown, and asserts the
     weight/arithmetic invariants:
       - sum(weights) == 100      (+/- 1e-3)
       - weighted_points == round(raw * weight / 100, 2) per component
       - final_score == round(sum(weighted_points), 2)

Run from the repo root:
    python scripts/diagnostics/score_component_probe.py
"""
from __future__ import annotations

import os
import sys

# Keep imports cheap / deterministic. No DB needed: every leg is exercised in
# its db=None / absolute_legacy path.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from app.services import expansion_advisor as ea  # noqa: E402

SWEEP = [0.0, 25.0, 50.0, 75.0, 100.0]

NEUTRAL_BRAND = {}  # empty profile -> all multipliers 1.0, no reweight


# ---------------------------------------------------------------------------
# Per-leg probes.  Each returns (label, driver_values, outputs, direction)
# direction: "up" = score should not DECREASE as driver rises (non-decreasing)
#            "down" = score should not INCREASE as driver rises (inverse leg)
# ---------------------------------------------------------------------------
def probe_landlord_signal():
    outs = [ea._landlord_signal_component(v) for v in SWEEP]
    none_out = ea._landlord_signal_component(None)
    return ("landlord_signal", SWEEP, outs, none_out, "up")


def probe_chain_strength():
    outs = [ea._chain_strength_score(v) for v in SWEEP]
    none_out = ea._chain_strength_score(None)
    return ("chain_strength", SWEEP, outs, none_out, "up")


def probe_competition_whitespace():
    # Driver is competitor_count (an inverse driver: more competitors -> less
    # whitespace).  Use confident=True so count=0 maps to the wide-open 100.
    counts = [0, 1, 3, 8, 20]
    outs = [ea._competition_whitespace_score(c, confident=True) for c in counts]
    # Missing/unknown case: confident=None with count 0 -> neutral 50.
    none_out = ea._competition_whitespace_score(0, confident=None)
    return ("competition_whitespace", counts, outs, none_out, "down")


def probe_demand_population():
    # demand_potential is a blend of _population_score and _delivery_score.
    # Probe the population half across a realistic reach sweep (qsr reference).
    reaches = [0.0, 5000.0, 30000.0, 80000.0, 200000.0]
    outs = [ea._population_score(r, service_model="qsr") for r in reaches]
    none_out = ea._population_score(0.0, service_model="qsr")
    return ("demand_potential.population", reaches, outs, none_out, "up")


def probe_demand_delivery():
    counts = [0, 5, 15, 40, 120]
    outs = [ea._delivery_score(c) for c in counts]
    none_out = ea._delivery_score(0)
    return ("demand_potential.delivery", counts, outs, none_out, "up")


def probe_confidence():
    # Listing path: accumulate ground-truth signals.  Drive on number of
    # signals present (0..5) so the score should be non-decreasing.
    def _conf(n):
        return ea._confidence_score(
            is_listing=True,
            rent_confidence="actual" if n >= 1 else None,
            area_confidence="actual" if n >= 2 else None,
            unit_street_width_m=10.0 if n >= 3 else None,
            image_url="http://x" if n >= 4 else None,
            landuse_label="commercial" if n >= 5 else None,
        )
    drivers = [0, 1, 2, 3, 5]
    outs = [_conf(n) for n in drivers]
    # "missing" -> parcel path with no context, capped at 70 family (base 40).
    none_out = ea._confidence_score(is_listing=False)
    return ("confidence", drivers, outs, none_out, "up")


def probe_access_visibility():
    outs = [
        ea._access_visibility_score(
            frontage_score=v, access_score=v, brand_profile=NEUTRAL_BRAND
        )
        for v in SWEEP
    ]
    none_out = ea._access_visibility_score(
        frontage_score=0.0, access_score=0.0, brand_profile=NEUTRAL_BRAND
    )
    return ("access_visibility", SWEEP, outs, none_out, "up")


def probe_listing_quality():
    # Drive on district_momentum_score (the largest sub-weight, 0.35) holding
    # everything else fixed.  None -> neutral 50 momentum sub-signal.
    def _lq(m):
        return ea._listing_quality_score(
            is_listing=True,
            effective_age_days=60,
            is_furnished=False,
            unit_restaurant_score=25.0,
            has_image=True,
            district_momentum_score=m,
        )
    outs = [_lq(v) for v in SWEEP]
    # Parcel path -> neutral 50 (the documented missing-data default).
    none_out = ea._listing_quality_score(
        is_listing=False,
        effective_age_days=None,
        is_furnished=None,
        unit_restaurant_score=None,
        has_image=False,
    )
    return ("listing_quality", SWEEP, outs, none_out, "up")


def probe_brand_fit():
    # Drive on fit_score (frontage/area composite) through the default
    # ("balanced") goal path with a neutral brand profile.
    def _bf(v):
        return ea._brand_fit_score(
            district="Al Olaya",
            area_m2=300.0,
            demand_score=v,
            fit_score=v,
            cannibalization_score=50.0,
            provider_density_score=v,
            provider_whitespace_score=v,
            multi_platform_presence_score=v,
            delivery_competition_score=50.0,
            visibility_signal=v,
            parking_signal=v,
            brand_profile=NEUTRAL_BRAND,
            service_model="qsr",
        )
    outs = [_bf(v) for v in SWEEP]
    none_out = _bf(0.0)
    return ("brand_fit", SWEEP, outs, none_out, "up")


def probe_occupancy_economics():
    # Drive on estimated_revenue_index (largest weight ~0.38) through the
    # absolute_legacy path (db=None, is_listing=False) holding rent/fitout/
    # area/cannibalization/fit fixed.
    def _econ(v):
        score, _meta = ea._economics_score(
            estimated_revenue_index=v,
            estimated_annual_rent_sar=300_000.0,
            estimated_fitout_cost_sar=900_000.0,
            area_m2=300.0,
            cannibalization_score=50.0,
            fit_score=50.0,
            db=None,
            is_listing=False,
        )
        return score
    outs = [_econ(v) for v in SWEEP]
    none_out = _econ(0.0)
    return ("occupancy_economics", SWEEP, outs, none_out, "up")


PROBES = [
    probe_occupancy_economics,
    probe_listing_quality,
    probe_brand_fit,
    probe_landlord_signal,
    probe_competition_whitespace,
    probe_chain_strength,
    probe_demand_population,
    probe_demand_delivery,
    probe_access_visibility,
    probe_confidence,
]


def _monotonic(outs, direction):
    if direction == "up":
        return all(b >= a - 1e-9 for a, b in zip(outs, outs[1:]))
    return all(b <= a + 1e-9 for a, b in zip(outs, outs[1:]))


def run_component_sweeps():
    print("=" * 92)
    print("PART B.1 — COMPONENT SWEEPS")
    print("=" * 92)
    header = f"{'component':28} {'driver->output':45} {'range_ok':8} {'mono':6} {'flat':5}"
    print(header)
    print("-" * 92)
    all_ok = True
    for probe in PROBES:
        label, drivers, outs, none_out, direction = probe()
        in_range = all(-1e-9 <= o <= 100.0 + 1e-9 for o in outs + [none_out])
        mono = _monotonic(outs, direction)
        flat = max(outs) - min(outs) < 1e-9
        ok = in_range and mono and not flat
        all_ok = all_ok and ok
        pairs = ", ".join(
            f"{d}->{round(o, 1)}" for d, o in zip(drivers, outs)
        )
        pairs += f" | None->{round(none_out, 1)}"
        print(f"{label:28} {pairs:45} {str(in_range):8} {str(mono):6} {str(flat):5}")
        notes = []
        if not in_range:
            notes.append("OUT OF [0,100]")
        if not mono:
            notes.append(f"NOT MONOTONIC ({direction})")
        if flat:
            notes.append("DEGENERATE/FLAT")
        if notes:
            print(f"    !! {label}: {'; '.join(notes)}")
    print("-" * 92)
    print(f"PART B.1 RESULT: {'PASS' if all_ok else 'FAIL'}")
    return all_ok


def run_breakdown_invariants():
    print()
    print("=" * 92)
    print("PART B.2 — _score_breakdown INVARIANTS (synthetic candidate)")
    print("=" * 92)
    bd = ea._score_breakdown(
        demand_score=72.0,
        whitespace_score=64.0,
        brand_fit_score=58.0,
        economics_score=70.0,
        provider_intelligence_composite=55.0,
        access_visibility_score=66.0,
        confidence_score=80.0,
        listing_quality_score=61.0,
        landlord_signal_score=75,
        chain_strength_score=50.0,
        chain_strength_max=82.0,
        brand_profile=NEUTRAL_BRAND,
        service_model="qsr",
    )
    weights = bd["weights"]
    raw = bd["inputs"]
    wc = bd["weighted_components"]

    wsum = sum(weights.values())
    inv_sum = abs(wsum - 100.0) < 1e-3
    print(f"sum(weights) = {wsum!r}  -> {'PASS' if inv_sum else 'FAIL'}")
    print()
    print(f"{'component':24} {'raw':>8} {'weight%':>10} {'wpts':>8} {'expect':>8} {'ok':>4}")
    print("-" * 70)
    inv_wpts = True
    for name in weights:
        expect = round(raw[name] * weights[name] / 100.0, 2)
        got = wc[name]
        ok = abs(got - expect) < 1e-9
        inv_wpts = inv_wpts and ok
        print(
            f"{name:24} {raw[name]:>8} {weights[name]:>10} {got:>8} {expect:>8} {str(ok):>4}"
        )
    print("-" * 70)
    expect_final = round(sum(wc.values()), 2)
    got_final = bd["final_score"]
    # final_score in the dict is clamped(round(sum,2)); compare to the clamp.
    inv_final = abs(got_final - max(0.0, min(100.0, expect_final))) < 1e-9
    print(f"sum(weighted_points) = {expect_final}   final_score = {got_final}  "
          f"-> {'PASS' if inv_final else 'FAIL'}")

    all_ok = inv_sum and inv_wpts and inv_final
    print()
    print(f"PART B.2 RESULT: {'PASS' if all_ok else 'FAIL'}")
    return all_ok


if __name__ == "__main__":
    a = run_component_sweeps()
    b = run_breakdown_invariants()
    print()
    print("=" * 92)
    print(f"OVERALL: {'ALL PASS' if (a and b) else 'FAILURES PRESENT (see above)'}")
    print("=" * 92)
    sys.exit(0 if (a and b) else 1)
