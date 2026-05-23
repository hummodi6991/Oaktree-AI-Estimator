"""Unit tests for the price-tier cannibalization weighting helpers.

Wires Google ``restaurant_poi.price_level`` (1-4 / $-$$$$) into the
cannibalization side of competition scoring so that price-distant
competitors weigh less than same-tier competitors. See
``_price_tier_weight`` and ``_price_tier_weighted_competitor_count``
in ``app/services/expansion_advisor.py``.
"""

import pytest

from app.services.expansion_advisor import (
    _competition_whitespace_score,
    _expected_price_tier,
    _normalize_user_price_tier,
    _price_tier_weight,
    _price_tier_weighted_competitor_count,
)


# ---------------------------------------------------------------------------
# _price_tier_weight
# ---------------------------------------------------------------------------

class TestPriceTierWeight:
    def test_same_tier_weights_at_1_0(self):
        for tier in (1, 2, 3, 4):
            assert _price_tier_weight(tier, tier) == pytest.approx(1.0)

    def test_one_tier_away_weights_at_0_6(self):
        assert _price_tier_weight(1, 2) == pytest.approx(0.6)
        assert _price_tier_weight(3, 2) == pytest.approx(0.6)
        assert _price_tier_weight(4, 3) == pytest.approx(0.6)

    def test_two_tiers_away_weights_at_0_2(self):
        assert _price_tier_weight(1, 3) == pytest.approx(0.2)
        assert _price_tier_weight(4, 2) == pytest.approx(0.2)

    def test_three_tiers_away_weights_at_0_1_floor(self):
        # Floor case: linear formula yields -0.2, clamped to 0.1.
        assert _price_tier_weight(1, 4) == pytest.approx(0.1)
        assert _price_tier_weight(4, 1) == pytest.approx(0.1)

    def test_null_price_level_returns_0_7(self):
        # Neutral weight for unknown-price competitors. Must not be 0.0
        # (over-discount) or 1.0 (over-weight).
        for expected_tier in (1, 2, 3, 4):
            assert _price_tier_weight(None, expected_tier) == pytest.approx(0.7)


# ---------------------------------------------------------------------------
# _price_tier_weighted_competitor_count
# ---------------------------------------------------------------------------

class TestPriceTierWeightedCompetitorCount:
    def test_same_tier_competitors_sum_to_count(self):
        # 5 same-tier competitors -> 5 effective units (the unweighted
        # equivalent: each contributes 1.0).
        levels = [2, 2, 2, 2, 2]
        assert _price_tier_weighted_competitor_count(levels, 2) == pytest.approx(5.0)

    def test_one_tier_away_competitors_sum_to_60_percent(self):
        # 5 one-tier-away competitors -> ~60% of the unweighted value.
        levels = [3, 3, 3, 3, 3]
        assert _price_tier_weighted_competitor_count(levels, 2) == pytest.approx(3.0)

    def test_three_tier_away_competitors_sum_to_floor(self):
        # The floor case: each competitor contributes 0.1.
        levels = [4, 4, 4, 4, 4]
        assert _price_tier_weighted_competitor_count(levels, 1) == pytest.approx(0.5)

    def test_mixed_tier_catchment(self):
        # From the spec: 2 same-tier (1.0), 2 one-tier-away (0.6),
        # 2 two-tier-away (0.2) → 2*1.0 + 2*0.6 + 2*0.2 = 3.6.
        levels = [2, 2, 3, 3, 4, 4]  # expected tier=2
        assert _price_tier_weighted_competitor_count(levels, 2) == pytest.approx(3.6)

    def test_null_competitors_weight_at_0_7(self):
        # 5 unknown-price competitors → 5 * 0.7 = 3.5.
        levels = [None, None, None, None, None]
        assert _price_tier_weighted_competitor_count(levels, 2) == pytest.approx(3.5)

    def test_mixed_null_and_known(self):
        # 2 same-tier (1.0) + 2 NULL (0.7) → 2*1.0 + 2*0.7 = 3.4.
        levels = [2, 2, None, None]
        assert _price_tier_weighted_competitor_count(levels, 2) == pytest.approx(3.4)

    def test_empty_list_returns_zero(self):
        assert _price_tier_weighted_competitor_count([], 2) == 0.0

    def test_none_input_returns_zero(self):
        assert _price_tier_weighted_competitor_count(None, 2) == 0.0


# ---------------------------------------------------------------------------
# _expected_price_tier
# ---------------------------------------------------------------------------

class TestExpectedPriceTier:
    def test_user_input_string_value_overrides_median(self):
        # User says "value" ($) — must win over the local median (3).
        levels = [3, 3, 3, 3]
        assert _expected_price_tier("value", levels) == 1

    def test_user_input_string_mid_overrides_median(self):
        levels = [4, 4, 4]
        assert _expected_price_tier("mid", levels) == 2

    def test_user_input_string_premium_overrides_median(self):
        levels = [1, 1, 1]
        assert _expected_price_tier("premium", levels) == 3

    def test_user_input_int_passes_through(self):
        # Integer user-side input is accepted directly.
        assert _expected_price_tier(4, [1, 1, 1]) == 4

    def test_user_input_out_of_range_falls_back_to_median(self):
        # Garbage value → ignored, fall through to median.
        assert _expected_price_tier(99, [3, 3, 3]) == 3
        assert _expected_price_tier("luxury", [3, 3, 3]) == 3

    def test_median_used_when_no_user_input(self):
        # No user tier → median of the catchment.
        assert _expected_price_tier(None, [1, 2, 3]) == 2
        assert _expected_price_tier(None, [3, 3, 4]) == 3
        assert _expected_price_tier(None, [4, 4, 4]) == 4

    def test_median_ignores_null_price_levels(self):
        # NULLs in the catchment are dropped from the median calculation.
        assert _expected_price_tier(None, [None, None, 3, 3]) == 3

    def test_default_tier_2_when_no_signal(self):
        # Empty catchment, no user input → mid-market default.
        assert _expected_price_tier(None, []) == 2
        assert _expected_price_tier(None, None) == 2
        assert _expected_price_tier(None, [None, None]) == 2


# ---------------------------------------------------------------------------
# _normalize_user_price_tier
# ---------------------------------------------------------------------------

class TestNormalizeUserPriceTier:
    def test_strings(self):
        assert _normalize_user_price_tier("value") == 1
        assert _normalize_user_price_tier("mid") == 2
        assert _normalize_user_price_tier("premium") == 3
        assert _normalize_user_price_tier("VALUE") == 1  # case-insensitive
        assert _normalize_user_price_tier("  mid  ") == 2  # trimmed

    def test_unknown_strings(self):
        assert _normalize_user_price_tier("luxury") is None
        assert _normalize_user_price_tier("") is None
        assert _normalize_user_price_tier("budget") is None

    def test_integers(self):
        assert _normalize_user_price_tier(1) == 1
        assert _normalize_user_price_tier(4) == 4

    def test_out_of_range_ints(self):
        assert _normalize_user_price_tier(0) is None
        assert _normalize_user_price_tier(5) is None
        assert _normalize_user_price_tier(-1) is None

    def test_none_and_bool(self):
        assert _normalize_user_price_tier(None) is None
        # bool subclasses int but should not be treated as a tier.
        assert _normalize_user_price_tier(True) is None
        assert _normalize_user_price_tier(False) is None


# ---------------------------------------------------------------------------
# End-to-end interaction with _competition_whitespace_score
# ---------------------------------------------------------------------------

class TestWhitespaceScoreWithWeighting:
    """Validate the weighted count fed into the existing whitespace scorer."""

    def test_same_tier_density_unchanged_vs_raw(self):
        # When all competitors share the candidate's tier, the weighted
        # count equals the raw count → the whitespace score is unchanged.
        same_tier_levels = [2, 2, 2, 2, 2]
        weighted = _price_tier_weighted_competitor_count(same_tier_levels, 2)
        assert _competition_whitespace_score(weighted) == pytest.approx(
            _competition_whitespace_score(5)
        )

    def test_price_distant_density_scores_higher_whitespace(self):
        # The product story: a fast-food candidate (tier 1) surrounded by
        # fine-dining (tier 4) sees less cannibalization pressure than the
        # raw count would imply, so the whitespace score is *higher*
        # (more opportunity) than the unweighted equivalent.
        distant_levels = [4, 4, 4, 4, 4]
        weighted = _price_tier_weighted_competitor_count(distant_levels, 1)
        raw_score = _competition_whitespace_score(5)
        weighted_score = _competition_whitespace_score(weighted)
        assert weighted_score > raw_score

    def test_null_only_catchment_partially_discounts(self):
        # 5 unknown-price competitors weight at 0.7 each → 3.5 effective.
        # That should score higher (more whitespace) than 5 raw competitors
        # but lower than 0 competitors.
        levels = [None] * 5
        weighted = _price_tier_weighted_competitor_count(levels, 2)
        assert weighted == pytest.approx(3.5)
        assert _competition_whitespace_score(weighted) > _competition_whitespace_score(5)
        assert _competition_whitespace_score(weighted) < _competition_whitespace_score(0)


# ---------------------------------------------------------------------------
# _competition_whitespace_score float compatibility
# ---------------------------------------------------------------------------

class TestCompetitionWhitespaceScoreAcceptsFloat:
    """The signature was widened from int to int|float so weighted counts
    interpolate between the legacy integer calibration points."""

    def test_zero_float_matches_zero_int(self):
        assert _competition_whitespace_score(0.0) == _competition_whitespace_score(0)

    def test_float_between_ints_interpolates(self):
        # 1.5 should land between the integer-1 and integer-2 scores.
        s1 = _competition_whitespace_score(1)
        s2 = _competition_whitespace_score(2)
        s_mid = _competition_whitespace_score(1.5)
        assert s2 < s_mid < s1

    def test_low_confidence_still_uses_50_floor(self):
        # F4 path: confident=False AND count<=0 still returns 50.0
        # regardless of whether count is int or float.
        assert _competition_whitespace_score(0.0, confident=False) == 50.0
        assert _competition_whitespace_score(0, confident=False) == 50.0
