"""Unit tests for ``_confidence_grade``.

Cover the listings branch where the road and parking evidence bands now
gate the maximum grade — both bands missing caps at B, regardless of how
strong the underlying score is — and verify that the parcels branch
ignores the new params (its existing critical-missing logic already
covers the same ground).
"""

from app.services.expansion_advisor import _confidence_grade


def _listings_grade(score: float, road: str | None, parking: str | None) -> str:
    return _confidence_grade(
        confidence_score=score,
        district=None,
        provider_platform_count=None,
        multi_platform_presence_score=0.0,
        rent_source="conservative_default",
        is_listing=True,
        road_evidence_band=road,
        parking_evidence_band=parking,
    )


def test_listings_score_100_both_primary_returns_A():
    assert _listings_grade(100.0, "primary", "primary") == "A"


def test_listings_score_100_one_band_missing_still_returns_A():
    assert _listings_grade(100.0, "none_found", "primary") == "A"


def test_listings_score_100_both_none_found_caps_at_B():
    assert _listings_grade(100.0, "none_found", "none_found") == "B"


def test_listings_score_100_both_bands_none_caps_at_B():
    assert _listings_grade(100.0, None, None) == "B"


def test_listings_score_60_both_none_found_floor_wins_at_C():
    assert _listings_grade(60.0, "none_found", "none_found") == "C"


def test_parcels_branch_ignores_new_params():
    base_kwargs = dict(
        confidence_score=100.0,
        district=None,
        provider_platform_count=None,
        multi_platform_presence_score=0.0,
        rent_source="conservative_default",
        road_context_available=True,
        parking_context_available=True,
        zoning_available=True,
        delivery_observed=True,
        data_completeness_score=100,
        is_listing=False,
    )
    baseline = _confidence_grade(**base_kwargs)
    with_bands = _confidence_grade(
        **base_kwargs,
        road_evidence_band="none_found",
        parking_evidence_band="none_found",
    )
    assert baseline == with_bands
