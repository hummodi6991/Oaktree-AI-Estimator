"""Payload-size regression test for the Expansion Advisor rerank trimmer.

A 20-candidate shortlist with realistic fat ``score_breakdown_json``,
``feature_snapshot_json``, and ``comparable_competitors_json`` payloads
must serialize to at most ``_MAX_USER_MESSAGE_CHARS`` after progressive
trimming — otherwise gpt-4o-mini returns structurally corrupt JSON.

The new score_breakdown slim trim tier (see expansion_rerank.py) is what
makes this possible; before it, a 20-candidate shortlist with fully
populated score_breakdown_json would exceed the cap at ~86 KB in prod.
"""
from __future__ import annotations

from app.services.expansion_rerank import (
    _MAX_USER_MESSAGE_CHARS,
    _serialize_shortlist_for_prompt,
)


_COMPONENTS = (
    "occupancy_economics",
    "listing_quality",
    "brand_fit",
    "landlord_signal",
    "competition_whitespace",
    "demand_potential",
    "access_visibility",
    "delivery_demand",
    "confidence",
)


def _fat_score_breakdown(i: int) -> dict:
    """Mirror the real score_breakdown_json shape. The narrative fields in
    ``display`` plus ``economics_detail`` are the bulk that score_breakdown
    slim drops; the numeric ``inputs`` + ``weights`` are what survives."""
    weights = {
        "occupancy_economics": 30, "listing_quality": 11, "brand_fit": 11,
        "landlord_signal": 8, "competition_whitespace": 10,
        "demand_potential": 10, "access_visibility": 10,
        "delivery_demand": 5, "confidence": 5,
    }
    inputs = {c: round(50.0 + (i * 1.3) % 50, 2) for c in _COMPONENTS}
    weighted_components = {
        c: round(inputs[c] * weights[c] / 100.0, 2) for c in _COMPONENTS
    }
    display = {
        c: {
            "raw_input_score": inputs[c],
            "weight_percent": weights[c],
            "weighted_points": weighted_components[c],
            "explanation": (
                f"{c} component scored {inputs[c]:.1f} based on observed "
                f"signals across the district, with typical comparables in "
                f"Riyadh showing a median of 62.4 and P75 of 74.1; this "
                f"candidate's contribution to final_score is "
                f"{weighted_components[c]:.2f} weighted points."
            ),
            "contribution": (
                f"{c} contributed {weighted_components[c]:.2f} points "
                f"toward the final score of {sum(weighted_components.values()):.2f}."
            ),
            "reasoning": (
                f"Derived from {c}_raw signal with rent_fallback_used=false, "
                f"parking_context_available=true, road_context_available=true. "
                f"Supporting comparables pulled from 14 nearby listings."
            ),
        }
        for c in _COMPONENTS
    }
    economics_detail = {
        "rent_p50_sar_per_m2": 1400.0 + i,
        "rent_burden_pct": round(22.5 + (i * 0.3) % 10, 2),
        "fitout_cost_estimate_sar": 350000 + i * 500,
        "comparables_used": [
            {
                "listing_id": f"listing-{i}-{j}",
                "rent_sar_per_m2": 1350 + j * 40,
                "area_m2": 280 + j * 15,
                "notes": (
                    "Comparable listing within 800m — similar frontage, "
                    "same land-use class, freshness acceptable."
                ),
            }
            for j in range(6)
        ],
        "commentary": (
            "Occupancy economics driven primarily by rent_burden and fitout "
            "amortization; cannibalization penalty is mild given branch "
            "density. Sensitivity to rent shocks is moderate."
        ),
    }
    return {
        "weights": weights,
        "inputs": inputs,
        "weighted_components": weighted_components,
        "display": display,
        "economics_detail": economics_detail,
        "final_score": round(sum(weighted_components.values()), 2),
    }


def _fat_feature_snapshot(i: int) -> dict:
    """Realistic snapshot: a few whitelist fields populated, a narrative
    string (non-whitelist, gets stripped by the whitelist-only tier), and
    a sprinkling of non-whitelist extras (also stripped)."""
    return {
        "district": f"al-olaya-{i:02d}",
        "area_m2": 300 + i * 12,
        "street_width_m": 18 + (i % 5),
        "frontage_m": 16 + (i % 7),
        "narrative_context": (
            "District has seen steady QSR demand growth; parking supply is "
            "adequate along the corridor, with street frontage on primary "
            "arterials. Observed delivery activity is dense, suggesting "
            "strong platform reach. Rent comparables within 600m are broadly "
            "consistent with our hedonic expectation for mid-tier commercial "
            "zones in central Riyadh."
        ),
    }


def _fat_competitors(i: int) -> list[dict]:
    return [
        {
            "name": f"Competitor {i}-{k}",
            "brand": f"Brand-{(i + k) % 12}",
            "distance_m": 180 + k * 90,
            "description": (
                "Established QSR burger brand with drive-thru, operating "
                "7am-2am, multiple menu price tiers, mid-tier footfall, "
                "last observed renovation in the past 18 months."
            ),
        }
        for k in range(4)
    ]


def _fat_candidate(i: int) -> dict:
    return {
        "parcel_id": f"parcel-{i:03d}",
        "deterministic_rank": i,
        "final_score": round(90.0 - i * 0.5, 2),
        "score_breakdown_json": _fat_score_breakdown(i),
        "feature_snapshot_json": _fat_feature_snapshot(i),
        "comparable_competitors_json": _fat_competitors(i),
    }


def test_serialize_shortlist_stays_under_cap_for_fat_payload():
    shortlist = [_fat_candidate(i) for i in range(1, 21)]
    brand_profile = {
        "brand": "QSR Burger",
        "category": "fast-food",
        "service_model": "drive-thru",
    }

    serialized = _serialize_shortlist_for_prompt(
        shortlist, brand_profile, shortlist_size=20
    )

    assert len(serialized) <= _MAX_USER_MESSAGE_CHARS, (
        f"payload {len(serialized)} chars exceeds cap {_MAX_USER_MESSAGE_CHARS}"
    )
