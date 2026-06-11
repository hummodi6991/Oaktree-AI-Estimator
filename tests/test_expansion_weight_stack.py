"""Weight stack v2 (EXPANSION_WEIGHT_STACK) — flag-gated rebalance tests.

Covers the five contract points of the v2 patch:
  * v1 (default) is inert — weights / contributions / ordering unchanged;
  * v2 weights sum to exactly 100, pre- and post-brand-multiplier;
  * momentum is single-paid under v2 (own component, out of listing_quality,
    no +2 sort-time bonus);
  * confidence contributes 0 weighted points under v2 but its raw value
    stays in the breakdown JSON;
  * listing_quality sub-weights renormalize to 1.0 under v2.
"""

from __future__ import annotations

import pytest

from app.services import expansion_advisor as expansion_service


def _breakdown_kwargs(**overrides):
    kwargs = dict(
        demand_score=80,
        whitespace_score=70,
        brand_fit_score=75,
        economics_score=60,
        provider_intelligence_composite=65,
        access_visibility_score=55,
        confidence_score=50,
        listing_quality_score=60,
        landlord_signal_score=40,
        chain_strength_score=30,
    )
    kwargs.update(overrides)
    return kwargs


def _set_stack(monkeypatch, value: str) -> None:
    monkeypatch.setattr(
        expansion_service.settings, "EXPANSION_WEIGHT_STACK", value, raising=False
    )


def _delta_candidate(momentum_score=90.0, sample_floor_applied=False):
    return {
        "parcel_id": "p1",
        "final_score": 70.0,
        "score_breakdown_json": {},
        "feature_snapshot_json": {
            "listing_age": {"created_days": 100, "updated_days": 100},
            "district_momentum": {
                "momentum_score": momentum_score,
                "sample_floor_applied": sample_floor_applied,
            },
        },
    }


# ── v1 inertness ─────────────────────────────────────────────────────


def test_weight_stack_v1_is_inert(monkeypatch):
    """Default setting ⇒ weights, contributions, bonus shape, and ordering
    are byte-identical to the pre-patch v1 stack."""
    assert expansion_service.settings.EXPANSION_WEIGHT_STACK == "v1"

    breakdown = expansion_service._score_breakdown(**_breakdown_kwargs())
    # Pinned pre-patch v1 weights (2026-05-07 rebalance + Patch B split).
    assert breakdown["weights"] == {
        "occupancy_economics": 26.2924,
        "listing_quality": 22.0,
        "brand_fit": 9.6404,
        "landlord_signal": 7.0112,
        "competition_whitespace": 5.764,
        "chain_strength": 3.0,
        "demand_potential": 8.7640,
        "access_visibility": 8.7640,
        "delivery_demand": 4.3820,
        "confidence": 4.3820,
    }
    # Pinned contributions: raw_input × weight / 100, rounded to 2 dp.
    assert breakdown["weighted_components"] == {
        "occupancy_economics": 15.78,
        "listing_quality": 13.2,
        "brand_fit": 7.23,
        "landlord_signal": 2.8,
        "competition_whitespace": 4.03,
        "chain_strength": 0.9,
        "demand_potential": 7.01,
        "access_visibility": 4.82,
        "delivery_demand": 2.85,
        "confidence": 2.19,
    }
    assert "district_momentum" not in breakdown["weights"]
    assert "district_momentum" not in breakdown["inputs"]
    assert "display_only" not in breakdown

    # district_momentum_score is accepted but ignored under v1.
    with_momentum = expansion_service._score_breakdown(
        **_breakdown_kwargs(), district_momentum_score=95.0
    )
    assert with_momentum == breakdown

    # Sort-time deltas keep the momentum bonus leg and its bonus_detail key.
    candidates = [
        _delta_candidate(),
        {
            **_delta_candidate(),
            "parcel_id": "p2",
            "final_score": 71.0,
            "score_breakdown_json": {},
        },
    ]
    out = expansion_service._apply_score_deltas_and_sort(candidates)
    detail = out[0]["score_breakdown_json"]["bonus_detail"]
    assert detail["momentum_bonus"] == 2.0
    assert list(detail.keys()) == [
        "base_deterministic",
        "value_band_delta",
        "viability_legs_fired",
        "viability_delta",
        "freshness_bonus",
        "freshness_label",
        "momentum_bonus",
        "total_delta",
        "final_score_clamped",
    ]
    assert [c["parcel_id"] for c in out] == ["p2", "p1"]


# ── v2 weights ───────────────────────────────────────────────────────


def test_weight_stack_v2_sums_100(monkeypatch):
    _set_stack(monkeypatch, "v2")
    breakdown = expansion_service._score_breakdown(**_breakdown_kwargs())
    weights = breakdown["weights"]
    assert weights == {
        "occupancy_economics": 20.0,
        "demand_potential": 18.0,
        "competition_whitespace": 12.0,
        "access_visibility": 11.0,
        "listing_quality": 9.0,
        "brand_fit": 8.0,
        "district_momentum": 7.0,
        "delivery_demand": 6.0,
        "landlord_signal": 5.0,
        "chain_strength": 4.0,
    }
    assert sum(weights.values()) == 100.0

    # Post-brand-multiplier renormalization also sums to 100.
    monkeypatch.setattr(
        expansion_service.settings, "EXPANSION_BRAND_WEIGHT_GAIN", 0.35, raising=False
    )
    reweighted = expansion_service._score_breakdown(
        **_breakdown_kwargs(),
        brand_profile={"primary_channel": "delivery", "expansion_goal": "flagship"},
        service_model="qsr",
    )["weights"]
    assert abs(sum(reweighted.values()) - 100) < 1e-3
    assert "district_momentum" in reweighted
    assert "confidence" not in reweighted


def test_v2_momentum_single_payment(monkeypatch):
    _set_stack(monkeypatch, "v2")

    # Own component, fed by district_momentum_score (None → neutral 50).
    breakdown = expansion_service._score_breakdown(
        **_breakdown_kwargs(), district_momentum_score=90.0
    )
    assert breakdown["weights"]["district_momentum"] == 7.0
    assert breakdown["inputs"]["district_momentum"] == 90.0
    assert breakdown["weighted_components"]["district_momentum"] == 6.3
    neutral = expansion_service._score_breakdown(**_breakdown_kwargs())
    assert neutral["inputs"]["district_momentum"] == 50.0

    # listing_quality no longer pays momentum: the composite is invariant
    # to district_momentum_score under v2.
    lq_kwargs = dict(
        is_listing=True,
        effective_age_days=10,
        is_furnished=True,
        unit_restaurant_score=40.0,
        has_image=True,
    )
    assert expansion_service._listing_quality_score(
        **lq_kwargs, district_momentum_score=100.0
    ) == expansion_service._listing_quality_score(
        **lq_kwargs, district_momentum_score=0.0
    )

    # No +2 sort-time bonus; bonus_detail omits the momentum_bonus key.
    out = expansion_service._apply_score_deltas_and_sort([_delta_candidate()])
    detail = out[0]["score_breakdown_json"]["bonus_detail"]
    assert "momentum_bonus" not in detail
    assert detail["total_delta"] == 0.0
    assert out[0]["final_score"] == 70.0


def test_v2_confidence_not_weighted(monkeypatch):
    _set_stack(monkeypatch, "v2")
    breakdown = expansion_service._score_breakdown(
        **_breakdown_kwargs(confidence_score=83.0)
    )
    assert "confidence" not in breakdown["weights"]
    assert "confidence" not in breakdown["weighted_components"]
    # Raw value stays in the breakdown JSON for the UI data-quality grade.
    assert breakdown["inputs"]["confidence"] == 83.0
    assert breakdown["display_only"]["confidence"] == {
        "raw_input_score": 83.0,
        "weight_percent": 0.0,
    }
    # final_score is exactly the sum of the weighted set (no confidence pts).
    assert breakdown["final_score"] == pytest.approx(
        sum(breakdown["weighted_components"].values()), abs=0.01
    )


def test_v2_listing_quality_renormalized(monkeypatch):
    _set_stack(monkeypatch, "v2")
    # Renormalized sub-weights sum to 1.0 exactly.
    sub = (0.30 / 0.65, 0.20 / 0.65, 0.10 / 0.65, 0.05 / 0.65)
    assert sum(sub) == pytest.approx(1.0, abs=1e-12)

    # Fixture: freshness band 100 (10 days), suitability 80 (structural
    # 40 × 2), image 100, furnished 100 → expected renormalized composite.
    score = expansion_service._listing_quality_score(
        is_listing=True,
        effective_age_days=10,
        is_furnished=True,
        unit_restaurant_score=40.0,
        has_image=True,
    )
    expected = 100.0 * sub[0] + 80.0 * sub[1] + 100.0 * sub[2] + 100.0 * sub[3]
    assert score == pytest.approx(expected, abs=1e-9)

    # Drive-thru bump unchanged (+5) and parcels stay neutral 50.
    assert expansion_service._listing_quality_score(
        is_listing=True,
        effective_age_days=10,
        is_furnished=True,
        unit_restaurant_score=40.0,
        has_image=True,
        has_drive_thru=True,
    ) == pytest.approx(min(expected + 5.0, 100.0), abs=1e-9)
    assert (
        expansion_service._listing_quality_score(
            is_listing=False,
            effective_age_days=None,
            is_furnished=None,
            unit_restaurant_score=None,
            has_image=False,
        )
        == 50.0
    )
