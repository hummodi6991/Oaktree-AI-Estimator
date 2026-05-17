"""Generate PR #2b golden fixtures for the read-path byte-identity guard.

Run this ONCE against HEAD (before the PR #2b edits to
``_normalize_candidate_payload``) to capture the English read-path
output. ``tests/test_pr2b_lang_en_byte_identity.py`` then asserts the
post-edit code reproduces these byte-for-byte (discipline rule #2).

Usage:  python scripts/gen_pr2b_golden.py
"""

from __future__ import annotations

import json
from pathlib import Path

from app.services.expansion_advisor import (
    _normalize_candidate_payload,
    _normalize_saved_search_payload,
)

OUT = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "pr2b_golden"

# PR #2b's _normalize_candidate_payload explicitly drops these internal
# columns from the outgoing payload (spec §2.1). The byte-identity guard
# therefore compares against HEAD output with these keys removed — every
# other key (English strings, gate chain, value bands, district canon)
# stays byte-identical.
_STRUCTURED_COLS = (
    "top_positives_structured_json",
    "top_risks_structured_json",
    "decision_summary_structured_json",
    "demand_thesis_structured_json",
    "cost_thesis_structured_json",
)


def _strip_structured(payload: dict) -> dict:
    out = {k: v for k, v in payload.items() if k not in _STRUCTURED_COLS}
    cands = out.get("candidates")
    if isinstance(cands, list):
        out["candidates"] = [
            {k: v for k, v in c.items() if k not in _STRUCTURED_COLS}
            if isinstance(c, dict) else c
            for c in cands
        ]
    return out


_GATE_STATUS = {
    "overall_pass": False,
    "zoning_fit_pass": True,
    "area_fit_pass": False,
    "parking_pass": None,
    "economics_pass": True,
}
_GATE_REASONS = {
    "passed": ["zoning_fit_pass", "economics_pass"],
    "failed": ["area_fit_pass"],
    "unknown": ["parking_pass"],
    "thresholds": {"area_fit_pass": {"min": 80}},
    "explanations": {"area_fit_pass": "Area below the requested minimum."},
}
_SCORE_BREAKDOWN = {
    "weights": {"demand": 0.3},
    "inputs": {"landlord_signal": 0.7},
    "weighted_components": {"demand": 21.0},
    "display": {},
    "final_score": 72.5,
    "economics_detail": {
        "value_score": 63.0,
        "value_band": "best_value",
        "value_band_low_confidence": False,
        "rent_burden": {"mode": "percentile", "median_monthly_rent_per_m2": 90.0},
    },
}

_CANDIDATES: dict[str, dict] = {
    "full_en_fields": {
        "candidate_id": "c-full-1",
        "parcel_id": "P-1001",
        "district": "Al Olaya",
        "area_m2": 220.0,
        "final_score": 81.4,
        "economics_score": 73.2,
        "estimated_rent_sar_m2_year": 1850.6,
        "estimated_annual_rent_sar": 407132.0,
        "top_positives_json": [
            "Demand potential is strong for this district.",
            "Economics profile meets target screening band.",
        ],
        "top_risks_json": ["Economics score is below preferred threshold."],
        "decision_summary": "This standard candidate in Al Olaya scores 81.4/100.",
        "demand_thesis": "Demand is strong (score 81.4).",
        "cost_thesis": "Estimated rent is 1851 SAR/m²/year.",
    },
    "gate_chain": {
        "candidate_id": "c-gate-1",
        "parcel_id": "P-1002",
        "district": "Al Malqa",
        "area_m2": 140.0,
        "final_score": 55.0,
        "gate_status_json": dict(_GATE_STATUS),
        "gate_reasons_json": dict(_GATE_REASONS),
        "top_positives_json": [],
        "top_risks_json": ["Area fit gate failed."],
        "decision_summary": "Caution advised.",
        "demand_thesis": "Demand is moderate.",
        "cost_thesis": "Rent estimate pending.",
    },
    "structured_cols_populated": {
        "candidate_id": "c-struct-1",
        "parcel_id": "P-1003",
        "district": "Al Narjis",
        "area_m2": 300.0,
        "final_score": 67.0,
        "economics_score": 60.0,
        "top_positives_json": ["Demand potential is strong for this district."],
        "top_risks_json": ["Economics are marginal."],
        "decision_summary": "Standard candidate.",
        "demand_thesis": "Demand is moderate.",
        "cost_thesis": "Estimated rent is 1200 SAR/m²/year.",
        # PR #2a structured columns — must be DROPPED from the en payload;
        # en output identical to a candidate without them.
        "top_positives_structured_json": [{"id": "pos.demand_strong", "params": {}}],
        "top_risks_structured_json": [{"id": "risk.economics_marginal", "params": {}}],
        "decision_summary_structured_json": {"id": "decision_summary", "params": {}},
        "demand_thesis_structured_json": {"id": "demand_thesis", "params": {}},
        "cost_thesis_structured_json": {"id": "cost_thesis", "params": {}},
    },
    "structured_cols_null": {
        "candidate_id": "c-struct-null",
        "parcel_id": "P-1004",
        "district": "Al Narjis",
        "area_m2": 300.0,
        "final_score": 67.0,
        "economics_score": 60.0,
        "top_positives_json": ["Demand potential is strong for this district."],
        "top_risks_json": ["Economics are marginal."],
        "decision_summary": "Standard candidate.",
        "demand_thesis": "Demand is moderate.",
        "cost_thesis": "Estimated rent is 1200 SAR/m²/year.",
        "top_positives_structured_json": None,
        "top_risks_structured_json": None,
        "decision_summary_structured_json": None,
        "demand_thesis_structured_json": None,
        "cost_thesis_structured_json": None,
    },
    "listing_candidate": {
        "candidate_id": "c-listing-1",
        "parcel_id": "P-1005",
        "district": "Hittin",
        "area_m2": 160.0,
        "final_score": 70.0,
        "source_type": "commercial_unit",
        "commercial_unit_id": "CU-77",
        "listing_url": "https://example.com/listing/77",
        "image_url": "https://example.com/img/77.jpg",
        "unit_price_sar_annual": 250000.0,
        "unit_area_sqm": 160.0,
        "unit_street_width_m": 20.0,
        "unit_neighborhood": "Hittin",
        "unit_listing_type": "rent",
        "top_positives_json": ["Newly listed within the last week."],
        "top_risks_json": [],
        "decision_summary": "Listing-backed candidate.",
        "demand_thesis": "Demand is strong.",
        "cost_thesis": "Rent measured from listing.",
    },
    "value_band": {
        "candidate_id": "c-value-1",
        "parcel_id": "P-1006",
        "district": "Al Yasmin",
        "area_m2": 250.0,
        "final_score": 75.0,
        "score_breakdown_json": dict(_SCORE_BREAKDOWN),
        "top_positives_json": ["Strong economics with favorable rent-to-revenue ratio."],
        "top_risks_json": [],
        "decision_summary": "Best value pick.",
        "demand_thesis": "Demand is strong.",
        "cost_thesis": "Economics favorable.",
    },
    "display_rent": {
        "candidate_id": "c-rent-1",
        "parcel_id": "P-1007",
        "estimated_rent_sar_m2_year": 2000.04,
        "area_m2": 192.0,
        "estimated_annual_rent_sar": 2000.04 * 192.0,
    },
    "minimal": {
        "candidate_id": "c-min-1",
        "parcel_id": "P-1008",
    },
    "empty_lists": {
        "candidate_id": "c-empty-1",
        "parcel_id": "P-1009",
        "district": "Al Olaya",
        "top_positives_json": None,
        "top_risks_json": None,
        "decision_summary": None,
        "demand_thesis": None,
        "cost_thesis": None,
    },
    "district_only": {
        "candidate_id": "c-dist-1",
        "parcel_id": "P-1010",
        "district": "حي العليا",
        "area_m2": 200.0,
        "final_score": 60.0,
        "top_positives_json": ["Demand potential is strong for this district."],
        "top_risks_json": ["Economics score is below preferred threshold."],
        "decision_summary": "Standard candidate.",
        "demand_thesis": "Demand is moderate.",
        "cost_thesis": "Rent estimate pending.",
    },
}


def _build_candidate_fixtures() -> None:
    for fid, candidate in _CANDIDATES.items():
        expected_en = _strip_structured(_normalize_candidate_payload(dict(candidate)))
        (OUT / f"cand_{fid}.json").write_text(
            json.dumps(
                {
                    "id": f"cand_{fid}",
                    "kind": "candidate",
                    "candidate": candidate,
                    "expected_en": expected_en,
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )


def _build_saved_search_fixtures() -> None:
    saved = {
        "id": "ss-1",
        "title": "Riyadh QSR shortlist",
        "status": "active",
        "selected_candidate_ids": ["c-full-1"],
        "search": {
            "id": "search-1",
            "brand_name": "TestBrand",
            "category": "qsr",
            "service_model": "qsr",
            "target_districts": ["Al Olaya"],
        },
        "candidates": [dict(_CANDIDATES["full_en_fields"]), dict(_CANDIDATES["gate_chain"])],
    }
    expected_en = _strip_structured(_normalize_saved_search_payload(dict(saved)))
    (OUT / "saved_basic.json").write_text(
        json.dumps(
            {
                "id": "saved_basic",
                "kind": "saved_search",
                "saved": saved,
                "expected_en": expected_en,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    _build_candidate_fixtures()
    _build_saved_search_fixtures()
    print(f"Wrote PR #2b golden fixtures to {OUT}")


if __name__ == "__main__":
    main()
