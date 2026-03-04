"""Tests for restaurant location scoring engine."""

from app.services.restaurant_location import (
    competition_score,
    complementary_score,
    competitor_rating_score,
    delivery_demand_score,
    rent_score_value,
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
