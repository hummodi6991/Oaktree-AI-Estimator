"""Tests for delivery platform scraper registry and helpers."""

from unittest.mock import patch, MagicMock

from app.connectors.delivery_platforms import (
    SCRAPER_REGISTRY,
    DiscoveryStats,
    list_all_scrapers,
    get_discovery_stats,
    _discover_sitemaps,
    _expand_sitemap_index,
    _extract_sitemap_hints_from_robots,
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


# ---------------------------------------------------------------------------
# Multi-strategy discovery tests
# ---------------------------------------------------------------------------


class TestDiscoveryStats:
    def test_to_dict_round_trip(self):
        s = DiscoveryStats(platform="jahez", discovery_success_path="configured")
        d = s.to_dict()
        assert d["platform"] == "jahez"
        assert d["discovery_success_path"] == "configured"
        assert isinstance(d["discovery_attempts"], list)

    def test_defaults(self):
        s = DiscoveryStats()
        assert s.fetch_failures == 0
        assert s.parse_failures == 0
        assert s.candidate_urls_found == 0


class TestExtractSitemapHintsFromRobots:
    @patch("app.connectors.delivery_platforms._fetch_with_retries")
    def test_extracts_sitemap_lines(self, mock_fetch):
        resp = MagicMock()
        resp.text = (
            "User-agent: *\n"
            "Disallow: /admin\n"
            "Sitemap: https://example.com/sitemap1.xml\n"
            "Sitemap: https://example.com/sitemap2.xml\n"
        )
        mock_fetch.return_value = resp

        hints = _extract_sitemap_hints_from_robots("https://example.com")
        assert hints == [
            "https://example.com/sitemap1.xml",
            "https://example.com/sitemap2.xml",
        ]

    @patch("app.connectors.delivery_platforms._fetch_with_retries")
    def test_returns_empty_on_failure(self, mock_fetch):
        mock_fetch.return_value = None
        assert _extract_sitemap_hints_from_robots("https://down.com") == []


class TestDiscoverSitemaps:
    """Test the multi-strategy discovery pipeline."""

    @patch("app.connectors.delivery_platforms._parse_sitemap")
    def test_strategy1_configured_succeeds(self, mock_parse):
        mock_parse.return_value = ["https://jahez.net/riyadh/restaurant-1"]

        urls, stats = _discover_sitemaps(
            "https://www.jahez.net",
            "https://www.jahez.net/sitemap.xml",
        )
        assert len(urls) == 1
        assert stats.discovery_success_path == "configured"
        assert len(stats.discovery_attempts) == 1
        assert stats.discovery_attempts[0]["strategy"] == "configured"

    @patch("app.connectors.delivery_platforms._extract_sitemap_hints_from_robots")
    @patch("app.connectors.delivery_platforms._parse_sitemap")
    def test_strategy2_robots_hint_fallback(self, mock_parse, mock_robots):
        # Configured sitemap fails, robots.txt hint succeeds
        mock_parse.side_effect = [
            [],  # configured sitemap returns nothing
            ["https://jahez.net/riyadh/r1", "https://jahez.net/riyadh/r2"],
        ]
        mock_robots.return_value = ["https://www.jahez.net/alt-sitemap.xml"]

        urls, stats = _discover_sitemaps(
            "https://www.jahez.net",
            "https://www.jahez.net/sitemap.xml",
        )
        assert len(urls) == 2
        assert stats.discovery_success_path == "robots_hint"

    @patch("app.connectors.delivery_platforms._extract_sitemap_hints_from_robots")
    @patch("app.connectors.delivery_platforms._parse_sitemap")
    def test_strategy3_common_path_fallback(self, mock_parse, mock_robots):
        # Configured and robots both fail; common path succeeds
        call_count = 0

        def side_effect(url):
            nonlocal call_count
            call_count += 1
            if "sitemap_index.xml" in url:
                return ["https://keeta.com/riyadh/place-1"]
            return []

        mock_parse.side_effect = side_effect
        mock_robots.return_value = []

        urls, stats = _discover_sitemaps(
            "https://www.keeta.com",
            "https://www.keeta.com/sitemap.xml",
        )
        assert len(urls) == 1
        assert stats.discovery_success_path is not None
        assert "common_path" in stats.discovery_success_path

    @patch("app.connectors.delivery_platforms._extract_sitemap_hints_from_robots")
    @patch("app.connectors.delivery_platforms._parse_sitemap")
    def test_all_strategies_fail(self, mock_parse, mock_robots):
        mock_parse.return_value = []
        mock_robots.return_value = []

        urls, stats = _discover_sitemaps(
            "https://www.keeta.com",
            "https://www.keeta.com/sitemap.xml",
        )
        assert urls == []
        assert stats.discovery_success_path is None
        # Should have tried configured + common paths (minus duplicate)
        assert len(stats.discovery_attempts) > 1

    @patch("app.connectors.delivery_platforms._parse_sitemap")
    def test_sitemap_index_expansion(self, mock_parse):
        # The configured sitemap returns index entries that get expanded
        mock_parse.side_effect = [
            ["https://jahez.net/sitemap-restaurants.xml"],  # configured — index
            ["https://jahez.net/riyadh/r1", "https://jahez.net/riyadh/r2"],  # expanded
        ]

        urls, stats = _discover_sitemaps(
            "https://www.jahez.net",
            "https://www.jahez.net/sitemap.xml",
        )
        assert len(urls) == 2
        assert stats.discovery_success_path == "configured"


class TestExpandSitemapIndex:
    @patch("app.connectors.delivery_platforms._parse_sitemap")
    def test_expands_xml_entries(self, mock_parse):
        mock_parse.return_value = ["https://ex.com/page1", "https://ex.com/page2"]
        stats = DiscoveryStats()
        result = _expand_sitemap_index(
            ["https://ex.com/child.xml", "https://ex.com/plain-page"],
            stats,
        )
        assert "https://ex.com/page1" in result
        assert "https://ex.com/page2" in result
        assert "https://ex.com/plain-page" in result
        assert stats.sitemap_urls_found == 1  # one index entry

    def test_no_xml_entries_passthrough(self):
        stats = DiscoveryStats()
        urls = ["https://ex.com/page1", "https://ex.com/page2"]
        result = _expand_sitemap_index(urls, stats)
        assert result == urls
        assert stats.sitemap_urls_found == 2

    @patch("app.connectors.delivery_platforms._parse_sitemap")
    def test_tracks_parse_failures(self, mock_parse):
        mock_parse.side_effect = Exception("network error")
        stats = DiscoveryStats()
        result = _expand_sitemap_index(["https://ex.com/child.xml"], stats)
        assert result == []
        assert stats.parse_failures == 1


class TestGetDiscoveryStats:
    def test_returns_dict(self):
        # Just verify function is importable and returns a dict
        result = get_discovery_stats()
        assert isinstance(result, dict)
