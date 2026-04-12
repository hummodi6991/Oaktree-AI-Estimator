"""Tests for scripts/scrape_aqar.py parser helpers.

Focused on the area-token disambiguator that handles Aqar's ambiguous
comma usage. Aqar renders areas with commas in two mutually incompatible
ways using identical syntax:

  - Thousands separator: ``"5,000 m²"`` = 5000 m² (real large store)
  - Saudi deed-registry decimal-comma: ``"120,205 m²"`` = 120.205 m²

The disambiguator uses plausibility to pick the right interpretation:
prefer thousands; fall back to decimal only when the thousands value is
implausibly large for a store/showroom.
"""

import logging

import pytest

from scripts.scrape_aqar import _parse_area_token


class TestParseAreaTokenDecimalComma:
    """Saudi deed-registry convention: comma is the decimal separator.

    Listings render areas like ``120,205 m²`` — that is **120.205 m²**,
    not 120 205 m². Pre-Patch-14 parsing treated comma as thousands and
    produced 1000× inflated values. The disambiguator falls back to the
    decimal interpretation here because the thousands reading (120205)
    is far outside the store/showroom plausibility ceiling.
    """

    def test_listing_6294239_real_failing_case(self):
        # Listing 6294239 page: "Area: 120,205 m²" → 120.205 m²
        assert _parse_area_token("120,205 m²", listing_type="store") == pytest.approx(
            120.205
        )

    def test_all_failing_rows_from_diagnostic(self):
        # Every row from the production diagnostic query gets pinned here.
        # Each thousands reading (28580, 22376, ...) is > _AREA_MAX_SQM
        # so the disambiguator falls back to decimal.
        cases = [
            ("28,580 m²", 28.580),
            ("22,376 m²", 22.376),
            ("22,108 m²", 22.108),
            ("16,446 m²", 16.446),
            ("16,205 m²", 16.205),
            ("14,285 m²", 14.285),
        ]
        for raw, expected in cases:
            assert _parse_area_token(raw, listing_type="store") == pytest.approx(
                expected
            ), f"{raw!r} did not parse to {expected}"


class TestParseAreaTokenThousandsComma:
    """Plain thousands separator: comma groups the integer part.

    These inputs have the same ``N,NNN m²`` shape as the deed-convention
    cases above, but the thousands reading is plausible for a real large
    store, so the disambiguator sticks with thousands. This is the
    regression guard for the bug the Patch-14 dry-run surfaced: the
    earlier "comma only → decimal" rule corrupted these into 5.0-class
    values.
    """

    def test_store_five_thousand(self):
        # "5,000 m²" is a real large store, not 5.0 m². thousands=5000 is
        # exactly at the plausibility ceiling → use thousands.
        assert _parse_area_token("5,000 m²", listing_type="store") == 5000.0

    def test_store_forty_five_hundred(self):
        assert _parse_area_token("4,500 m²", listing_type="store") == 4500.0

    def test_store_twelve_hundred(self):
        assert _parse_area_token("1,200 m²", listing_type="store") == 1200.0

    def test_showroom_thousands_separator(self):
        # Same rule applies to showrooms.
        assert _parse_area_token("3,000 m²", listing_type="showroom") == 3000.0
        assert _parse_area_token("2,500 m²", listing_type="showroom") == 2500.0

    def test_thousands_boundary_exact_ceiling(self):
        # Exactly at the ceiling stays as thousands.
        assert _parse_area_token("5,000 m²", listing_type="store") == 5000.0

    def test_just_over_ceiling_falls_back_to_decimal(self):
        # "5,500 m²" thousands=5500 > 5000 → decimal=5.5 (plausible for store).
        assert _parse_area_token("5,500 m²", listing_type="store") == pytest.approx(5.5)


class TestParseAreaTokenPeriodDecimal:
    def test_simple_period_decimal(self):
        assert _parse_area_token("85.5 m²", listing_type="store") == pytest.approx(85.5)

    def test_period_decimal_three_digits(self):
        assert _parse_area_token("120.205 m²", listing_type="store") == pytest.approx(
            120.205
        )


class TestParseAreaTokenPlainInteger:
    def test_plain_integers(self):
        assert _parse_area_token("60 m²", listing_type="store") == 60.0
        assert _parse_area_token("200 m²", listing_type="store") == 200.0
        assert _parse_area_token("450 m²", listing_type="showroom") == 450.0


class TestParseAreaTokenArabicNumerals:
    def test_plain_arabic_integer(self):
        # ٦٠ متر مربع → 60 m²
        assert _parse_area_token("٦٠ متر مربع", listing_type="store") == 60.0

    def test_arabic_decimal_comma(self):
        # Mixed Arabic decimal-comma: ١٢٠,٢٠٥
        assert _parse_area_token(
            "١٢٠,٢٠٥ متر مربع", listing_type="store"
        ) == pytest.approx(120.205)

    def test_arabic_decimal_period(self):
        # ٨٥.٥ → 85.5
        assert _parse_area_token(
            "٨٥.٥ متر مربع", listing_type="store"
        ) == pytest.approx(85.5)


class TestParseAreaTokenBuildingWarehouseLegacy:
    """For building/warehouse the population legitimately uses
    comma-as-thousands, so the parser keeps the legacy behavior AND
    skips the plausibility guardrail (real buildings can be 8 000+ m²).
    """

    def test_building_comma_is_thousands(self):
        assert _parse_area_token("1,200 m²", listing_type="building") == 1200.0

    def test_warehouse_comma_is_thousands(self):
        assert _parse_area_token("2,500 m²", listing_type="warehouse") == 2500.0

    def test_building_large_area_passes(self):
        # Real buildings can exceed the store/showroom plausibility ceiling.
        assert _parse_area_token("8,000 m²", listing_type="building") == 8000.0
        assert _parse_area_token("12,500 m²", listing_type="warehouse") == 12500.0

    def test_building_both_comma_and_period(self):
        # "1,200.50 m²" on a building = 1200.50 m² (comma thousands, period decimal).
        assert _parse_area_token(
            "1,200.50 m²", listing_type="building"
        ) == pytest.approx(1200.50)


class TestParseAreaTokenPlausibilityGuard:
    """Plausibility guardrail rejects junk values for store/showroom.

    Rejected values emit a warning log line and return None so the caller
    leaves ``area_sqm`` NULL rather than corrupting the DB.
    """

    def test_implausibly_large_store(self, caplog):
        with caplog.at_level(logging.WARNING, logger="scripts.scrape_aqar"):
            assert _parse_area_token("99999 m²", listing_type="store") is None
        assert any("Rejected implausible area" in rec.message for rec in caplog.records)

    def test_implausibly_small_store(self):
        assert _parse_area_token("2 m²", listing_type="store") is None

    def test_implausibly_large_showroom(self):
        assert _parse_area_token("8000 m²", listing_type="showroom") is None

    def test_building_bypasses_plausibility(self):
        # Same literal value that's rejected for store is accepted for building.
        assert _parse_area_token("8000 m²", listing_type="building") == 8000.0


class TestParseAreaTokenEmptyOrGarbage:
    def test_none_input(self):
        assert _parse_area_token(None) is None

    def test_empty_string(self):
        assert _parse_area_token("") is None

    def test_non_numeric_garbage(self):
        assert _parse_area_token("not a number") is None

    def test_only_unit(self):
        assert _parse_area_token("m²") is None

    def test_only_unit_with_whitespace(self):
        assert _parse_area_token("  m²  ") is None
