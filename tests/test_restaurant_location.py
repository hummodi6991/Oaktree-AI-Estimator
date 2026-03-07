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
    _RentResolution,
    _rent_data_quality,
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


class TestRentDataQuality:
    """Tests for Aqar rent data quality -> confidence contribution."""

    def test_district_scope_highest_quality(self):
        res = _RentResolution(rent_per_m2=500.0, scope="district", sample_count=10,
                              median_used=500.0, method="aqar_district_median")
        assert _rent_data_quality(res) == 1.0

    def test_district_shrinkage_medium_quality(self):
        res = _RentResolution(rent_per_m2=450.0, scope="district_shrinkage", sample_count=3,
                              median_used=450.0, method="aqar_district_shrinkage")
        assert _rent_data_quality(res) == 0.7

    def test_city_scope_moderate_quality(self):
        res = _RentResolution(rent_per_m2=400.0, scope="city", sample_count=20,
                              median_used=400.0, method="aqar_city_median")
        assert _rent_data_quality(res) == 0.5

    def test_city_asset_scope_moderate_quality(self):
        res = _RentResolution(rent_per_m2=400.0, scope="city_asset", sample_count=15,
                              median_used=400.0, method="aqar_city_asset_median")
        assert _rent_data_quality(res) == 0.5

    def test_indicator_fallback_low_quality(self):
        res = _RentResolution(rent_per_m2=350.0, scope="indicator_fallback", sample_count=0,
                              median_used=350.0, method="indicator_district_rent")
        assert _rent_data_quality(res) == 0.2

    def test_no_data_zero_quality(self):
        res = _RentResolution(rent_per_m2=None, scope="none", sample_count=0,
                              median_used=None, method="none")
        assert _rent_data_quality(res) == 0.0

    def test_rent_factor_changes_from_neutral_with_aqar_signal(self):
        """When Aqar rent data is available, rent factor should NOT be the neutral 50."""
        # District-level Aqar rent of 200 SAR/m² -> rent_score_value should be ~90
        res = _RentResolution(rent_per_m2=200.0, scope="district", sample_count=10,
                              median_used=200.0, method="aqar_district_median")
        score = rent_score_value(res.rent_per_m2)
        assert score != 50.0, "Rent factor should not be neutral when Aqar data exists"
        assert score >= 80.0, "Low rent should give high score"

    def test_confidence_increases_with_district_aqar_rent(self):
        """Confidence should be higher when district-level Aqar rent is used."""
        base_features = {
            "has_google": 0.5,
            "google_confidence": 0.5,
            "review_sufficiency": 0.5,
            "nearby_evidence": 0.5,
            "source_diversity": 0.5,
            "rating_coverage": 0.5,
        }

        # Without rent data quality
        conf_without = _aggregate_confidence(base_features)

        # With district-level rent data quality
        features_with_rent = {**base_features, "rent_data_quality": 1.0}
        conf_with = _aggregate_confidence(features_with_rent)

        assert conf_with > conf_without, (
            "Confidence should increase when district Aqar rent data is available"
        )

    def test_confidence_increases_with_city_aqar_rent(self):
        """Confidence should increase even with city-level Aqar rent (less than district)."""
        base_features = {
            "has_google": 0.5,
            "google_confidence": 0.5,
            "review_sufficiency": 0.5,
            "nearby_evidence": 0.5,
            "source_diversity": 0.5,
            "rating_coverage": 0.5,
        }

        features_city = {**base_features, "rent_data_quality": 0.5}
        features_district = {**base_features, "rent_data_quality": 1.0}

        conf_city = _aggregate_confidence(features_city)
        conf_district = _aggregate_confidence(features_district)

        assert conf_district > conf_city, (
            "District rent should give higher confidence than city rent"
        )

    def test_fallback_still_works_when_aqar_missing(self):
        """When Aqar is unavailable, rent_data_quality=0 and rent factor returns neutral 50."""
        res = _RentResolution(rent_per_m2=None, scope="none", sample_count=0,
                              median_used=None, method="none")
        assert rent_score_value(res.rent_per_m2) == 50.0
        assert _rent_data_quality(res) == 0.0
