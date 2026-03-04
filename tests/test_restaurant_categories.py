"""Tests for restaurant category taxonomy and normalization."""

from app.services.restaurant_categories import (
    CATEGORIES,
    list_categories,
    normalize_category,
    normalize_osm_cuisine,
    normalize_overture_taxonomy,
)


class TestNormalizeCategory:
    def test_burger_keywords(self):
        assert normalize_category("burger") == "burger"
        assert normalize_category("Hamburger Joint") == "burger"
        assert normalize_category("BURGER_KING") == "burger"

    def test_pizza_keywords(self):
        assert normalize_category("pizza") == "pizza"
        assert normalize_category("Pizzeria") == "pizza"

    def test_chicken_keywords(self):
        assert normalize_category("fried_chicken") == "chicken"
        assert normalize_category("Broasted Chicken") == "chicken"

    def test_traditional_keywords(self):
        assert normalize_category("arabic") == "traditional"
        assert normalize_category("saudi") == "traditional"
        assert normalize_category("shawarma") == "traditional"
        assert normalize_category("kabsa") == "traditional"
        assert normalize_category("كبسة") == "traditional"

    def test_asian_keywords(self):
        assert normalize_category("chinese") == "asian"
        assert normalize_category("sushi") == "asian"
        assert normalize_category("korean") == "asian"

    def test_seafood_keywords(self):
        assert normalize_category("seafood") == "seafood"
        assert normalize_category("fish") == "seafood"

    def test_coffee_bakery_keywords(self):
        assert normalize_category("cafe") == "coffee_bakery"
        assert normalize_category("coffee_shop") == "coffee_bakery"
        assert normalize_category("bakery") == "coffee_bakery"

    def test_healthy_keywords(self):
        assert normalize_category("salad") == "healthy"
        assert normalize_category("healthy_bowl") == "healthy"

    def test_fallback_to_international(self):
        assert normalize_category("unknown_food") == "international"
        assert normalize_category(None) == "international"
        assert normalize_category("") == "international"


class TestNormalizeOsmCuisine:
    def test_single_value(self):
        assert normalize_osm_cuisine("burger") == "burger"
        assert normalize_osm_cuisine("chinese") == "asian"

    def test_semicolon_multi_value(self):
        assert normalize_osm_cuisine("burger;pizza") == "burger"
        assert normalize_osm_cuisine("chinese;japanese") == "asian"

    def test_none(self):
        assert normalize_osm_cuisine(None) == "international"


class TestNormalizeOvertureTaxonomy:
    def test_hierarchical_path(self):
        result = normalize_overture_taxonomy("restaurant > asian_restaurant > chinese_restaurant")
        assert result == "asian"

    def test_simple_path(self):
        assert normalize_overture_taxonomy("burger_restaurant") == "burger"

    def test_none(self):
        assert normalize_overture_taxonomy(None) == "international"


class TestListCategories:
    def test_returns_all_categories(self):
        cats = list_categories()
        assert len(cats) == len(CATEGORIES)
        keys = {c["key"] for c in cats}
        assert "burger" in keys
        assert "traditional" in keys

    def test_has_bilingual_names(self):
        cats = list_categories()
        for c in cats:
            assert "name_en" in c
            assert "name_ar" in c
            assert c["name_en"]
            assert c["name_ar"]
