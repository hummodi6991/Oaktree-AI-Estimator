"""Tests for delivery platform scraper registry and helpers."""

from app.connectors.delivery_platforms import (
    SCRAPER_REGISTRY,
    list_all_scrapers,
    _slug_to_name,
    _is_riyadh_url,
)


class TestScraperRegistry:
    def test_registry_has_all_platforms(self):
        expected = {
            "hungerstation", "talabat", "mrsool", "jahez", "toyou",
            "keeta", "thechefz", "lugmety", "shgardi", "ninja",
            "nana", "dailymealz", "careemfood", "deliveroo",
        }
        registered = set(SCRAPER_REGISTRY.keys())
        assert expected.issubset(registered), f"Missing: {expected - registered}"

    def test_registry_entries_have_required_keys(self):
        for source, entry in SCRAPER_REGISTRY.items():
            assert "fn" in entry, f"{source} missing 'fn'"
            assert "label" in entry, f"{source} missing 'label'"
            assert "url" in entry, f"{source} missing 'url'"
            assert callable(entry["fn"]), f"{source} fn is not callable"

    def test_list_all_scrapers(self):
        scrapers = list_all_scrapers()
        assert len(scrapers) >= 14
        for s in scrapers:
            assert "source" in s
            assert "label" in s
            assert "url" in s


class TestHelpers:
    def test_slug_to_name(self):
        assert _slug_to_name("burger-king") == "Burger King"
        assert _slug_to_name("al_baik_riyadh") == "Al Baik Riyadh"

    def test_is_riyadh_url(self):
        assert _is_riyadh_url("https://example.com/riyadh/restaurants")
        assert _is_riyadh_url("https://example.com/الرياض/food")
        assert not _is_riyadh_url("https://example.com/jeddah/restaurants")
