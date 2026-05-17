"""One-shot capture/regeneration tool for the PR #2a golden fixtures.

PR #2a proves the five Expansion Advisor heuristic producers keep their
English output byte-identical while gaining a parallel structured
record. This script captures, for a curated set of inputs (one per
firing condition), the producer's English output and — once the
producers have been updated — its structured record, into
``tests/fixtures/pr2_golden/``.

Usage
-----
Phase 1 (run against HEAD, BEFORE editing the producers):

    python scripts/gen_pr2_golden.py --phase baseline

  Writes one ``<id>.json`` per fixture (input + ``expected_english``)
  and ``baseline_english.json``.

Phase 2 (run AFTER editing the producers):

    python scripts/gen_pr2_golden.py --phase structured

  Adds ``expected_structured`` to each fixture and re-verifies that the
  English output still equals the committed baseline (empty-diff check).

The script auto-detects the producer return signature, so it tolerates
being run in either phase regardless of the producer code state.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "pr2_golden"

from app.services.expansion_advisor import (  # noqa: E402
    _build_cost_thesis,
    _build_demand_thesis,
    _decision_summary,
    _gate_key_to_label,
    _top_positives_and_risks,
)

# ---------------------------------------------------------------------------
# Fixture definitions — one per firing condition (audit §0).
# ---------------------------------------------------------------------------

# Neutral candidate that fires NOTHING in _top_positives_and_risks:
#  - economics_score 60 -> no P5/P8/K2/K11
#  - provider_density_score 10 -> delivery_observed True (suppresses K7/K8)
#  - competitor_count 5 -> no P10 (<=2) and no K13 (>=8)
#  - all other scores 0, no area / distance / feature snapshot
_TPR_BASE = {
    "demand_score": 0.0,
    "whitespace_score": 0.0,
    "brand_fit_score": 0.0,
    "economics_score": 60.0,
    "delivery_competition_score": 0.0,
    "cannibalization_score": 0.0,
    "provider_density_score": 10.0,
    "multi_platform_presence_score": 0.0,
    "provider_whitespace_score": 0.0,
    "competitor_count": 5,
    "gate_status_json": {"overall_pass": False},
}
_TPR_EMPTY_GATES = {"passed": [], "failed": [], "unknown": []}


def _tpr(**overrides):
    cand = dict(_TPR_BASE)
    gate_reasons = dict(_TPR_EMPTY_GATES)
    for key in ("passed", "failed", "unknown"):
        if key in overrides:
            gate_reasons[key] = overrides.pop(key)
    cand.update(overrides)
    return {"candidate": cand, "gate_reasons": gate_reasons}


# _top_positives_and_risks fixtures. K7 ("delivery district estimates")
# is intentionally absent: it is unreachable code at HEAD — its guard
# `not delivery_observed and provider_density_score > 0` is a
# contradiction, since provider_density_score > 0 forces
# delivery_observed True. Documented in the validation report.
_TPR_FIXTURES = {
    "tpr_P1_demand_strong": _tpr(demand_score=75.0),
    "tpr_P2_bnm_whitespace_favorable": _tpr(whitespace_score=70.0, provider_whitespace_score=30.0),
    "tpr_P3_inferred_whitespace": _tpr(
        whitespace_score=70.0, provider_density_score=0.0,
        multi_platform_presence_score=0.0, delivery_competition_score=0.0,
    ),
    "tpr_P4_brand_fit_aligned": _tpr(brand_fit_score=75.0),
    "tpr_P5_economics_meets_band": _tpr(economics_score=65.0),
    "tpr_P6_all_gates_pass": _tpr(gate_status_json={"overall_pass": True}),
    "tpr_P7_area_well_aligned": _tpr(area_m2=290.0),
    "tpr_P8_strong_economics": _tpr(economics_score=72.0),
    "tpr_P9_well_separated_branch": _tpr(distance_to_nearest_branch_m=6300.0),
    "tpr_P10_low_competitor_density": _tpr(competitor_count=1),
    "tpr_P11_new_in_top_market": _tpr(
        feature_snapshot_json={
            "listing_age": {"created_days": 2, "updated_days": 2},
            "district_momentum": {"momentum_score": 85.0, "sample_floor_applied": False},
        },
    ),
    "tpr_P12_refreshed_in_top_market": _tpr(
        feature_snapshot_json={
            "listing_age": {"created_days": 30, "updated_days": 2},
            "district_momentum": {"momentum_score": 85.0, "sample_floor_applied": False},
        },
    ),
    "tpr_P13_newly_listed": _tpr(
        feature_snapshot_json={"listing_age": {"created_days": 2}},
    ),
    "tpr_P14_refreshed_listing": _tpr(
        feature_snapshot_json={"listing_age": {"created_days": 30, "updated_days": 3}},
    ),
    "tpr_P15_top_tier_market": _tpr(
        feature_snapshot_json={
            "district_momentum": {"momentum_score": 85.0, "sample_floor_applied": False},
        },
    ),
    "tpr_K1_cannibalization_elevated": _tpr(cannibalization_score=75.0),
    "tpr_K2_economics_below_threshold": _tpr(economics_score=40.0),
    "tpr_K3_delivery_competition_high": _tpr(delivery_competition_score=70.0),
    "tpr_K4_delivery_whitespace_limited": _tpr(
        delivery_competition_score=85.0, provider_whitespace_score=10.0,
    ),
    "tpr_K5_gate_failed": _tpr(failed=["parking_pass"]),
    "tpr_K6_gate_unknown": _tpr(unknown=["district_pass"]),
    "tpr_K8_delivery_inferred": _tpr(
        provider_density_score=0.0, multi_platform_presence_score=0.0,
        delivery_competition_score=0.0,
    ),
    "tpr_K9_area_near_min": _tpr(area_m2=85.0),
    "tpr_K10_area_near_max": _tpr(area_m2=470.0),
    "tpr_K11_economics_marginal": _tpr(economics_score=52.0),
    "tpr_K12_nearest_branch_close": _tpr(distance_to_nearest_branch_m=1200.0),
    "tpr_K13_high_competitor_density": _tpr(competitor_count=11),
    # Multi-fire fixtures: exercise [:5]/[:6] truncation + ordering.
    "tpr_MULTI_positives_overflow": _tpr(
        demand_score=80.0, whitespace_score=70.0, provider_whitespace_score=30.0,
        brand_fit_score=80.0, economics_score=75.0, competitor_count=1,
        distance_to_nearest_branch_m=6300.0, area_m2=290.0,
        gate_status_json={"overall_pass": True},
    ),
    "tpr_MULTI_risks_overflow": _tpr(
        cannibalization_score=80.0, economics_score=40.0,
        delivery_competition_score=85.0, provider_whitespace_score=10.0,
        area_m2=85.0, distance_to_nearest_branch_m=1200.0, competitor_count=11,
        failed=["zoning_fit_pass", "parking_pass"],
        unknown=["frontage_access_pass"],
    ),
}

_DT_FIXTURES = {
    "dt_01_strong_observed_dense": dict(
        demand_score=75.0, population_reach=50000.0, provider_density_score=70.0,
        provider_whitespace_score=65.0, delivery_competition_score=70.0,
        delivery_observed=True,
    ),
    "dt_02_moderate_observed_steady": dict(
        demand_score=55.0, population_reach=20000.0, provider_density_score=50.0,
        provider_whitespace_score=45.0, delivery_competition_score=40.0,
        delivery_observed=True,
    ),
    "dt_03_limited_observed_thin": dict(
        demand_score=30.0, population_reach=8000.0, provider_density_score=10.0,
        provider_whitespace_score=10.0, delivery_competition_score=10.0,
        delivery_observed=True,
    ),
    "dt_04_strong_observed_tight_intense": dict(
        demand_score=72.0, population_reach=41000.0, provider_density_score=66.0,
        provider_whitespace_score=20.0, delivery_competition_score=80.0,
        delivery_observed=True,
    ),
    "dt_05_district_estimate_inferred": dict(
        demand_score=75.0, population_reach=50000.0, provider_density_score=70.0,
        provider_whitespace_score=65.0, delivery_competition_score=70.0,
        delivery_observed=False,
    ),
    "dt_06_limited_district_tight": dict(
        demand_score=55.0, population_reach=20000.0, provider_density_score=20.0,
        provider_whitespace_score=30.0, delivery_competition_score=70.0,
        delivery_observed=False,
    ),
    "dt_07_not_observed_inferred": dict(
        demand_score=60.0, population_reach=5000.0, provider_density_score=0.0,
        provider_whitespace_score=80.0, delivery_competition_score=0.0,
        delivery_observed=False,
    ),
    "dt_08_limited_not_observed": dict(
        demand_score=30.0, population_reach=3000.0, provider_density_score=0.0,
        provider_whitespace_score=10.0, delivery_competition_score=0.0,
        delivery_observed=False,
    ),
    "dt_09_default_delivery_observed": dict(
        demand_score=68.0, population_reach=33333.0, provider_density_score=46.0,
        provider_whitespace_score=41.0, delivery_competition_score=64.0,
    ),
}

_CT_FIXTURES = {
    "ct_01_typical": dict(
        estimated_rent_sar_m2_year=1850.0,
        estimated_annual_rent_sar=462500.0,
        estimated_fitout_cost_sar=390000.0,
    ),
    "ct_02_large_thousands": dict(
        estimated_rent_sar_m2_year=2000.0,
        estimated_annual_rent_sar=1234567.0,
        estimated_fitout_cost_sar=9876543.0,
    ),
    "ct_03_zeros": dict(
        estimated_rent_sar_m2_year=0.0,
        estimated_annual_rent_sar=0.0,
        estimated_fitout_cost_sar=0.0,
    ),
    "ct_04_fractional_rounding": dict(
        estimated_rent_sar_m2_year=1849.7,
        estimated_annual_rent_sar=462499.5,
        estimated_fitout_cost_sar=389999.4,
    ),
}

_DS_FIXTURES = {
    "ds_01_compact_from_key_risks_qsr": dict(
        district="Al Olaya", final_score=70.0, economics_score=60.0,
        key_risks=["Cannibalization risk is elevated versus branch network."],
        service_model="qsr", area_m2=150.0,
    ),
    "ds_02_standard_tight_economics_qsr": dict(
        district="Al Olaya", final_score=50.0, economics_score=50.0,
        key_risks=[], service_model="qsr", area_m2=200.0,
    ),
    "ds_03_standard_execution_qsr": dict(
        district="Al Olaya", final_score=70.0, economics_score=60.0,
        key_risks=[], service_model="qsr", area_m2=200.0,
    ),
    "ds_04_compact_execution_neighborhood_dine_in": dict(
        district="Al Olaya", final_score=70.0, economics_score=60.0,
        key_risks=[], service_model="dine_in", area_m2=150.0,
    ),
    "ds_05_standard_execution_flagship_dine_in": dict(
        district="Al Olaya", final_score=70.0, economics_score=60.0,
        key_risks=[], service_model="dine_in", area_m2=300.0,
    ),
    "ds_06_execution_delivery_led_branch": dict(
        district="Al Olaya", final_score=70.0, economics_score=60.0,
        key_risks=[], service_model="delivery_first", area_m2=200.0,
    ),
    "ds_07_compact_execution_compact_cafe": dict(
        district="Al Olaya", final_score=70.0, economics_score=60.0,
        key_risks=[], service_model="cafe", area_m2=150.0,
    ),
    "ds_08_standard_execution_destination_cafe": dict(
        district="Al Olaya", final_score=70.0, economics_score=60.0,
        key_risks=[], service_model="cafe", area_m2=200.0,
    ),
    "ds_09_district_none_default": dict(
        district=None, final_score=70.0, economics_score=60.0,
        key_risks=[], service_model="qsr", area_m2=200.0,
    ),
    "ds_10_compact_tight_economics_cafe": dict(
        district="Al Murabba", final_score=48.0, economics_score=54.0,
        key_risks=[], service_model="cafe", area_m2=120.0,
    ),
}

_GATE_FIXTURES = {
    "zoning_fit_pass", "area_fit_pass", "frontage_access_pass", "parking_pass",
    "district_pass", "cannibalization_pass", "delivery_market_pass",
    "economics_pass", "radiance_growth_pass", "population_floor_pass",
    "commercial_floor_pass", "construction_proximity_pass",
}


# ---------------------------------------------------------------------------
# Producer invocation + return-signature normalization.
# ---------------------------------------------------------------------------

def _norm_two(value):
    """Return (english, structured) from a producer that emits one string."""
    if isinstance(value, tuple):
        return value[0], value[1]
    return value, None


def _capture(fixture_id, producer, kwargs):
    if producer == "_top_positives_and_risks":
        result = _top_positives_and_risks(**kwargs)
        if len(result) == 2:
            positives, risks = result
            structured = None
        else:
            positives, risks, pos_struct, risk_struct = result
            structured = {"positives": pos_struct, "risks": risks_struct_alias(risk_struct)}
        english = {"positives": positives, "risks": risks}
        return english, structured
    if producer == "_build_demand_thesis":
        return _norm_two(_build_demand_thesis(**kwargs))
    if producer == "_build_cost_thesis":
        return _norm_two(_build_cost_thesis(**kwargs))
    if producer == "_decision_summary":
        return _norm_two(_decision_summary(**kwargs))
    if producer == "_gate_key_to_label":
        return _gate_key_to_label(kwargs["gate_key"]), None
    raise ValueError(f"unknown producer {producer}")


def risks_struct_alias(value):
    return value


def _all_specs():
    specs = []
    for fid, kw in _TPR_FIXTURES.items():
        specs.append((fid, "_top_positives_and_risks", kw))
    for fid, kw in _DT_FIXTURES.items():
        specs.append((fid, "_build_demand_thesis", kw))
    for fid, kw in _CT_FIXTURES.items():
        specs.append((fid, "_build_cost_thesis", kw))
    for fid, kw in _DS_FIXTURES.items():
        specs.append((fid, "_decision_summary", kw))
    for key in sorted(_GATE_FIXTURES):
        specs.append((f"gate_{key}", "_gate_key_to_label", {"gate_key": key}))
    return specs


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=("baseline", "structured"), required=True)
    args = parser.parse_args()

    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    baseline_path = FIXTURE_DIR / "baseline_english.json"

    baseline = {}
    if args.phase == "structured":
        if not baseline_path.exists():
            print("ERROR: baseline_english.json missing; run --phase baseline first")
            return 1
        baseline = json.loads(baseline_path.read_text(encoding="utf-8"))

    captured_english = {}
    drift = []
    count = 0
    for fixture_id, producer, kwargs in _all_specs():
        english, structured = _capture(fixture_id, producer, kwargs)
        captured_english[fixture_id] = english

        fixture_path = FIXTURE_DIR / f"{fixture_id}.json"
        if args.phase == "baseline":
            record = {
                "id": fixture_id,
                "producer": producer,
                "kwargs": kwargs,
                "expected_english": english,
            }
        else:
            if fixture_id in baseline and baseline[fixture_id] != english:
                drift.append(fixture_id)
            existing = json.loads(fixture_path.read_text(encoding="utf-8"))
            record = dict(existing)
            record["expected_english"] = existing.get("expected_english", english)
            record["expected_structured"] = structured
        fixture_path.write_text(
            json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        count += 1

    if args.phase == "baseline":
        baseline_path.write_text(
            json.dumps(captured_english, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(f"baseline: wrote {count} fixtures + baseline_english.json")
        return 0

    if drift:
        print(f"ENGLISH DRIFT in {len(drift)} fixtures: {drift}")
        return 1
    print(f"structured: updated {count} fixtures; English byte-identical to baseline")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
