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
        """Diagnostic rows from production. Some now correctly return None
        after the Patch-14b decimal-fallback floor (anything < 20 m²)."""
        accepted_cases = [
            ("28,580 m²", 28.580),
            ("22,376 m²", 22.376),
            ("22,108 m²", 22.108),
        ]
        for raw, expected in accepted_cases:
            assert _parse_area_token(raw, listing_type="store") == pytest.approx(
                expected
            ), f"{raw!r} did not parse to {expected}"

        # These now correctly return None: their decimal-fallback values
        # (16.446, 16.205, 14.285) all fall below the 20 m² fallback floor
        # and are treated as contamination rather than tiny kiosks.
        rejected_cases = ["16,446 m²", "16,205 m²", "14,285 m²"]
        for raw in rejected_cases:
            assert _parse_area_token(raw, listing_type="store") is None, \
                f"{raw!r} should be rejected by the decimal-fallback floor"


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

    def test_just_over_ceiling_falls_back_to_decimal_rejected(self):
        # "5,500 m²" thousands=5500 > 5000 → decimal=5.5 → now rejected
        # by the Patch-14b decimal-fallback floor (5.5 < 20.0). The old
        # behavior accepted 5.5 as a plausible-looking kiosk value, but
        # no real F&B store in Riyadh is 5.5 m², and the fallback path
        # is low-confidence by construction — treat as contamination.
        assert _parse_area_token("5,500 m²", listing_type="store") is None


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


class TestParseAreaTokenMultiSeparatorReject:
    """Multi-separator garbage like '5,028,580 m²' must be rejected."""

    def test_rejects_three_commas(self, caplog):
        with caplog.at_level(logging.WARNING, logger="scripts.scrape_aqar"):
            assert _parse_area_token("5,028,580 m²", listing_type="store") is None
        assert any("multi-separator" in rec.message.lower() for rec in caplog.records)

    def test_rejects_two_commas(self):
        assert _parse_area_token("1,234,567 m²", listing_type="store") is None

    def test_rejects_two_periods(self):
        assert _parse_area_token("12.345.678 m²", listing_type="store") is None

    def test_rejects_for_building_too(self):
        # Multi-separator garbage is invalid for ALL listing types, not
        # just store/showroom. A 1.2 million sqm building is nonsense.
        assert _parse_area_token("1,200,500 m²", listing_type="building") is None

    def test_accepts_legit_thousands_decimal(self):
        # Exactly one comma followed by exactly one period, in order, is the
        # international thousands+decimal form and stays valid.
        assert _parse_area_token("1,200.50 m²", listing_type="building") == pytest.approx(1200.50)

    def test_rejects_period_then_comma(self):
        # period-then-comma is invalid (would be European decimal-with-thousands,
        # which Aqar doesn't use, and is too risky to disambiguate)
        assert _parse_area_token("1.200,50 m²", listing_type="store") is None


class TestParseAreaTokenDecimalFallbackFloor:
    """Stricter floor on decimal-fallback path to catch contamination
    like '12,500 m²' falling back to 12.5 m²."""

    def test_rejects_fallback_below_20(self, caplog):
        with caplog.at_level(logging.WARNING, logger="scripts.scrape_aqar"):
            # 12,500 → thousands=12500 > 5000 → fallback decimal=12.5 → < 20 → reject
            assert _parse_area_token("12,500 m²", listing_type="store") is None
        assert any("decimal-fallback" in rec.message.lower() for rec in caplog.records)

    def test_rejects_fallback_14_class(self):
        # 14,285 → thousands=14285 > 5000 → fallback decimal=14.285 → < 20 → reject
        assert _parse_area_token("14,285 m²", listing_type="store") is None

    def test_accepts_fallback_above_20(self):
        # 28,580 → thousands=28580 > 5000 → fallback decimal=28.580 → ≥ 20 → accept
        assert _parse_area_token("28,580 m²", listing_type="store") == pytest.approx(28.580)

    def test_accepts_fallback_120_class(self):
        # The original Patch 14 canary case still works
        assert _parse_area_token("120,205 m²", listing_type="store") == pytest.approx(120.205)

    def test_general_floor_unchanged_for_non_fallback(self):
        # A direct "10 m²" (no comma, no fallback) still passes the
        # general floor of 5.0 because it didn't come through fallback.
        assert _parse_area_token("10 m²", listing_type="store") == 10.0

    def test_general_floor_still_rejects_below_5(self):
        # The general floor is unchanged.
        assert _parse_area_token("3 m²", listing_type="store") is None


class TestParseAreaTelemetryCounters:
    """Verify the telemetry counters increment on each return path."""

    def test_counters_increment_on_success(self):
        from scripts.scrape_aqar import _area_parse_stats, _reset_area_parse_telemetry
        _reset_area_parse_telemetry()
        _parse_area_token("100 m²", listing_type="store")
        assert _area_parse_stats["attempted"] == 1
        assert _area_parse_stats["succeeded"] == 1

    def test_counters_increment_on_multi_separator_reject(self):
        from scripts.scrape_aqar import _area_parse_stats, _reset_area_parse_telemetry
        _reset_area_parse_telemetry()
        _parse_area_token("5,028,580 m²", listing_type="store")
        assert _area_parse_stats["attempted"] == 1
        assert _area_parse_stats["succeeded"] == 0
        assert _area_parse_stats["rejected_multi_separator"] == 1

    def test_counters_increment_on_fallback_too_small(self):
        from scripts.scrape_aqar import _area_parse_stats, _reset_area_parse_telemetry
        _reset_area_parse_telemetry()
        _parse_area_token("14,285 m²", listing_type="store")
        assert _area_parse_stats["attempted"] == 1
        assert _area_parse_stats["rejected_decimal_fallback_too_small"] == 1


class _FakeResult:
    def __init__(self, scalar_value=None, rowcount=0):
        self._scalar = scalar_value
        self.rowcount = rowcount

    def scalar(self):
        return self._scalar


class _FakeDB:
    """Records execute() calls and returns canned results in order."""

    def __init__(self, results):
        self._results = list(results)
        self.calls: list[tuple[str, dict]] = []

    def execute(self, stmt, params=None):
        self.calls.append((str(stmt), dict(params or {})))
        return self._results.pop(0)


class TestMarkUnseenListingsClosed:
    """Immediate-close sweep: closed aqar listings drop same-day, not in 28."""

    def test_flips_unseen_listings_to_stale_when_coverage_ok(self):
        from scripts.scrape_aqar import mark_unseen_listings_closed

        db = _FakeDB([_FakeResult(scalar_value=100), _FakeResult(rowcount=7)])
        seen = {f"id_{i}" for i in range(80)}  # 80/100 = 80% coverage

        closed = mark_unseen_listings_closed(db, "store", seen)

        assert closed == 7
        assert len(db.calls) == 2
        count_sql, count_params = db.calls[0]
        assert "SELECT COUNT(*)" in count_sql
        assert count_params == {"lt": "store"}
        update_sql, update_params = db.calls[1]
        assert "UPDATE commercial_unit SET status = 'stale'" in update_sql
        assert update_params["lt"] == "store"
        assert set(update_params["seen_ids"]) == seen

    def test_coverage_guard_skips_sweep_when_crawl_looks_broken(self):
        from scripts.scrape_aqar import mark_unseen_listings_closed

        db = _FakeDB([_FakeResult(scalar_value=1000)])
        seen = {f"id_{i}" for i in range(10)}  # 10/1000 = 1% coverage

        closed = mark_unseen_listings_closed(db, "store", seen)

        assert closed == -1
        assert len(db.calls) == 1  # only the COUNT, no UPDATE issued

    def test_zero_active_pool_is_noop(self):
        from scripts.scrape_aqar import mark_unseen_listings_closed

        db = _FakeDB([_FakeResult(scalar_value=0)])

        closed = mark_unseen_listings_closed(db, "store", set())

        assert closed == 0
        assert len(db.calls) == 1

    def test_full_coverage_with_all_unseen_closes_everything(self):
        from scripts.scrape_aqar import mark_unseen_listings_closed

        # Edge: scrape saw exactly as many listings as are currently active,
        # but none of them overlap. All 50 active rows should close.
        db = _FakeDB([_FakeResult(scalar_value=50), _FakeResult(rowcount=50)])
        seen = {f"new_id_{i}" for i in range(50)}

        closed = mark_unseen_listings_closed(db, "store", seen)

        assert closed == 50
