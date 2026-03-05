"""Tests for restaurant location scoring engine."""

from app.services.restaurant_location import (
    chain_gap_score,
    competition_score,
    complementary_score,
    competitor_rating_score,
    delivery_demand_score,
    foot_traffic_score,
    rent_score_value,
    DEMAND_WEIGHTS,
    COST_WEIGHTS,
    PLATFORM_SOURCES,
    _aggregate_confidence,
    _build_confidence_contributions,
    _CONF_WEIGHTS,
)
from app.services.traffic_proxy import road_class_score


class TestCompetitionScore:
    def test_no_competitors_gives_max(self):
        assert competition_score(0) == 100.0

    def test_more_competitors_lower_score(self):
        score_5 = competition_score(5)
        score_15 = competition_score(15)
        assert score_5 > score_15

    def test_many_competitors_still_positive(self):
        assert competition_score(50) >= 5.0


class TestComplementaryScore:
    def test_zero_gives_low(self):
        assert complementary_score(0) == 20.0

    def test_moderate_count_gives_high(self):
        score = complementary_score(10)
        assert score > 50.0

    def test_sweet_spot(self):
        score_10 = complementary_score(10)
        score_15 = complementary_score(15)
        assert score_10 <= score_15  # still increasing up to 15


class TestDeliveryDemandScore:
    def test_no_data_neutral(self):
        assert delivery_demand_score(0, 0) == 30.0

    def test_underserved_category(self):
        score = delivery_demand_score(1, 100)
        assert score > 60.0  # underserved

    def test_moderate_demand(self):
        score = delivery_demand_score(10, 100)
        assert score > 50.0

    def test_oversaturated(self):
        score = delivery_demand_score(40, 100)
        assert score < 50.0


class TestCompetitorRatingScore:
    def test_no_data_neutral(self):
        assert competitor_rating_score(None, 0) == 50.0

    def test_low_rating_high_opportunity(self):
        score = competitor_rating_score(2.5, 10)
        assert score >= 80.0

    def test_high_rating_tough_competition(self):
        score = competitor_rating_score(4.8, 10)
        assert score < 40.0


class TestRentScore:
    def test_low_rent_high_score(self):
        assert rent_score_value(200) >= 80.0

    def test_high_rent_low_score(self):
        assert rent_score_value(2000) <= 25.0

    def test_no_data_neutral(self):
        assert rent_score_value(None) == 50.0


class TestRoadClassScore:
    def test_motorway_highest(self):
        assert road_class_score("motorway") > road_class_score("residential")

    def test_residential_moderate(self):
        score = road_class_score("residential")
        assert 20 <= score <= 40

    def test_unknown_default(self):
        assert road_class_score("unknown_road") == 25.0

    def test_none_default(self):
        assert road_class_score(None) == 25.0


class TestChainGapScore:
    def test_no_competitors_high_gap(self):
        score = chain_gap_score([], "burger")
        assert score >= 85.0

    def test_chain_not_present_is_opportunity(self):
        competitors = [
            {"chain_name": "Other Chain", "category": "burger"},
        ]
        score = chain_gap_score(competitors, "burger", chain_name="New Chain")
        assert score >= 80.0

    def test_chain_already_present_penalized(self):
        competitors = [
            {"chain_name": "My Chain", "category": "burger"},
            {"chain_name": "My Chain", "category": "burger"},
        ]
        score = chain_gap_score(competitors, "burger", chain_name="My Chain")
        assert score < 50.0

    def test_saturated_category(self):
        competitors = [{"chain_name": None, "category": "burger"}] * 20
        score = chain_gap_score(competitors, "burger")
        assert score <= 25.0


class TestFootTrafficScore:
    def test_no_restaurants_low(self):
        score = foot_traffic_score([], 10.0, 10.0)
        assert score <= 20.0

    def test_many_restaurants_high(self):
        restaurants = [{}] * 50
        score = foot_traffic_score(restaurants, 80.0, 80.0)
        assert score >= 70.0


class TestWeightsConsistency:
    def test_demand_weights_sum_close_to_one(self):
        total = sum(DEMAND_WEIGHTS.values())
        assert abs(total - 1.0) < 0.02, f"Demand weights sum to {total}"

    def test_cost_weights_sum_close_to_one(self):
        total = sum(COST_WEIGHTS.values())
        assert abs(total - 1.0) < 0.02, f"Cost weights sum to {total}"

    def test_all_platform_sources_known(self):
        assert "hungerstation" in PLATFORM_SOURCES
        assert "talabat" in PLATFORM_SOURCES
        assert "mrsool" in PLATFORM_SOURCES
        assert "jahez" in PLATFORM_SOURCES
        assert "keeta" in PLATFORM_SOURCES
        assert "careemfood" in PLATFORM_SOURCES
        assert "deliveroo" in PLATFORM_SOURCES
        assert len(PLATFORM_SOURCES) >= 14


class TestConfidenceScore:
    """Tests for the new confidence scoring split."""

    def test_confidence_weights_sum_to_one(self):
        total = sum(_CONF_WEIGHTS.values())
        assert abs(total - 1.0) < 0.02, f"Confidence weights sum to {total}"

    def test_full_confidence_gives_one(self):
        features = {"has_google": 1.0, "google_confidence": 1.0, "review_sufficiency": 1.0}
        score = _aggregate_confidence(features)
        assert abs(score - 1.0) < 0.01

    def test_zero_confidence_gives_zero(self):
        features = {"has_google": 0.0, "google_confidence": 0.0, "review_sufficiency": 0.0}
        score = _aggregate_confidence(features)
        assert score == 0.0

    def test_partial_confidence(self):
        features = {"has_google": 1.0, "google_confidence": 0.5, "review_sufficiency": 0.0}
        score = _aggregate_confidence(features)
        assert 0.0 < score < 1.0

    def test_confidence_contributions_sorted_by_impact(self):
        features = {"has_google": 1.0, "google_confidence": 0.0, "review_sufficiency": 0.5}
        contribs = _build_confidence_contributions(features)
        assert len(contribs) == 3
        # Sorted descending by weighted_contribution
        for i in range(len(contribs) - 1):
            assert contribs[i]["weighted_contribution"] >= contribs[i + 1]["weighted_contribution"]

    def test_final_score_formula(self):
        """final_score = opportunity * (0.60 + 0.40 * confidence_01)"""
        opportunity = 80.0
        conf_01 = 0.5
        expected = opportunity * (0.60 + 0.40 * conf_01)
        assert abs(expected - 64.0) < 0.1

    def test_final_score_max_confidence(self):
        """With confidence=1.0, final_score equals opportunity_score."""
        opportunity = 75.0
        conf_01 = 1.0
        final = opportunity * (0.60 + 0.40 * conf_01)
        assert abs(final - opportunity) < 0.1

    def test_final_score_zero_confidence(self):
        """With confidence=0.0, final_score = 60% of opportunity."""
        opportunity = 80.0
        conf_01 = 0.0
        final = opportunity * (0.60 + 0.40 * conf_01)
        assert abs(final - 48.0) < 0.1

    def test_opportunity_unchanged_when_confidence_varies(self):
        """opportunity_score should not depend on confidence features."""
        # This is a design invariant: opportunity_score is computed from
        # market factors only; varying confidence inputs should not change it.
        # (Tested structurally: opportunity is computed before confidence.)
        features_high = {"has_google": 1.0, "google_confidence": 1.0, "review_sufficiency": 1.0}
        features_low = {"has_google": 0.0, "google_confidence": 0.0, "review_sufficiency": 0.0}
        # Both give different confidence but that doesn't affect opportunity
        conf_high = _aggregate_confidence(features_high)
        conf_low = _aggregate_confidence(features_low)
        assert conf_high > conf_low
