"""Unit tests for ``parse_relative_time``.

Every example phrase called out in the Phase 2 spec is covered here, plus
edge cases — empty strings, case variants, whitespace noise, future-dated
inputs (must return None), and Arabic equivalents.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pytest

from app.ingest.aqar.relative_time import parse_relative_time


ANCHOR = datetime(2026, 4, 21, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# English: minute / hour / day / week / month / year
# ---------------------------------------------------------------------------


def test_one_minute_ago():
    assert parse_relative_time("1 minute ago", ANCHOR) == ANCHOR - timedelta(minutes=1)


def test_three_minutes_ago():
    assert parse_relative_time("3 minutes ago", ANCHOR) == ANCHOR - timedelta(minutes=3)


def test_two_hours_ago():
    assert parse_relative_time("2 hours ago", ANCHOR) == ANCHOR - timedelta(hours=2)


def test_one_day_ago():
    assert parse_relative_time("1 day ago", ANCHOR) == ANCHOR - timedelta(days=1)


def test_five_days_ago():
    assert parse_relative_time("5 days ago", ANCHOR) == ANCHOR - timedelta(days=5)


def test_two_weeks_ago():
    assert parse_relative_time("2 weeks ago", ANCHOR) == ANCHOR - timedelta(weeks=2)


def test_one_month_ago_uses_30day_approximation():
    # Month → 30 days per project spec.
    assert parse_relative_time("1 month ago", ANCHOR) == ANCHOR - timedelta(days=30)


def test_three_months_ago_uses_30day_approximation():
    assert parse_relative_time("3 months ago", ANCHOR) == ANCHOR - timedelta(days=90)


def test_one_year_ago_uses_365day_approximation():
    assert parse_relative_time("1 year ago", ANCHOR) == ANCHOR - timedelta(days=365)


# ---------------------------------------------------------------------------
# "just now" family
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("phrase", ["just now", "Just now", "JUST NOW", "now", "right now"])
def test_just_now_returns_anchor_unchanged(phrase):
    assert parse_relative_time(phrase, ANCHOR) == ANCHOR


def test_arabic_just_now_returns_anchor_unchanged():
    assert parse_relative_time("الآن", ANCHOR) == ANCHOR
    assert parse_relative_time("منذ لحظات", ANCHOR) == ANCHOR


# ---------------------------------------------------------------------------
# Case / whitespace variants
# ---------------------------------------------------------------------------


def test_uppercase_input():
    assert parse_relative_time("3 MINUTES AGO", ANCHOR) == ANCHOR - timedelta(minutes=3)


def test_mixed_case():
    assert parse_relative_time("2 Hours Ago", ANCHOR) == ANCHOR - timedelta(hours=2)


def test_leading_trailing_whitespace():
    assert parse_relative_time("   5 days ago   ", ANCHOR) == ANCHOR - timedelta(days=5)


def test_collapsed_internal_whitespace():
    assert parse_relative_time("5   days   ago", ANCHOR) == ANCHOR - timedelta(days=5)


def test_a_minute_ago_is_one_minute():
    assert parse_relative_time("a minute ago", ANCHOR) == ANCHOR - timedelta(minutes=1)


def test_an_hour_ago_is_one_hour():
    assert parse_relative_time("an hour ago", ANCHOR) == ANCHOR - timedelta(hours=1)


# ---------------------------------------------------------------------------
# Arabic equivalents — spec examples
# ---------------------------------------------------------------------------


def test_arabic_one_minute():
    # "منذ دقيقة" = 1 minute ago
    assert parse_relative_time("منذ دقيقة", ANCHOR) == ANCHOR - timedelta(minutes=1)


def test_arabic_one_hour():
    assert parse_relative_time("منذ ساعة", ANCHOR) == ANCHOR - timedelta(hours=1)


def test_arabic_one_day():
    assert parse_relative_time("منذ يوم", ANCHOR) == ANCHOR - timedelta(days=1)


def test_arabic_two_days_dual():
    # Arabic has a dual form — "يومين" means "two days" without a leading 2.
    assert parse_relative_time("منذ يومين", ANCHOR) == ANCHOR - timedelta(days=2)


def test_arabic_three_days_explicit_number():
    assert parse_relative_time("منذ 3 أيام", ANCHOR) == ANCHOR - timedelta(days=3)


def test_arabic_three_days_with_arabic_indic_digit():
    # Arabic-Indic "3" (٣) should be normalized to ASCII.
    assert parse_relative_time("منذ ٣ أيام", ANCHOR) == ANCHOR - timedelta(days=3)


def test_arabic_one_month():
    assert parse_relative_time("منذ شهر", ANCHOR) == ANCHOR - timedelta(days=30)


def test_arabic_one_year():
    assert parse_relative_time("منذ سنة", ANCHOR) == ANCHOR - timedelta(days=365)


# ---------------------------------------------------------------------------
# Malformed / unrecognized inputs → None
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", ["", "   ", "\n\t"])
def test_empty_or_whitespace_returns_none(bad):
    assert parse_relative_time(bad, ANCHOR) is None


def test_none_input_returns_none():
    assert parse_relative_time(None, ANCHOR) is None  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "garbage",
    [
        "tomorrow",
        "yesterday",
        "asdf",
        "3 ago",
        "minutes ago",
        "some time",
        "ago 3 minutes",
    ],
)
def test_garbage_returns_none(garbage):
    assert parse_relative_time(garbage, ANCHOR) is None


def test_future_dated_in_form_returns_none():
    # We never want to silently produce ``anchor + offset`` values.
    assert parse_relative_time("in 3 minutes", ANCHOR) is None


def test_future_dated_after_form_returns_none():
    assert parse_relative_time("after 1 hour", ANCHOR) is None


# ---------------------------------------------------------------------------
# Approximation logging — audit trail for month/year units
# ---------------------------------------------------------------------------


def test_month_approximation_logs_at_debug(caplog):
    with caplog.at_level(logging.DEBUG, logger="app.ingest.aqar.relative_time"):
        parse_relative_time("1 month ago", ANCHOR)
    assert any("approximating" in rec.message for rec in caplog.records)


def test_year_approximation_logs_at_debug(caplog):
    with caplog.at_level(logging.DEBUG, logger="app.ingest.aqar.relative_time"):
        parse_relative_time("1 year ago", ANCHOR)
    assert any("approximating" in rec.message for rec in caplog.records)


def test_minute_does_not_log_approximation(caplog):
    with caplog.at_level(logging.DEBUG, logger="app.ingest.aqar.relative_time"):
        parse_relative_time("3 minutes ago", ANCHOR)
    assert not any("approximating" in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# Timezone preservation — anchor tz must flow through unchanged
# ---------------------------------------------------------------------------


def test_anchor_timezone_is_preserved():
    tz_anchor = datetime(2026, 4, 21, 15, 0, 0, tzinfo=timezone.utc)
    got = parse_relative_time("1 hour ago", tz_anchor)
    assert got is not None
    assert got.tzinfo is timezone.utc


def test_naive_anchor_returns_naive_datetime():
    naive_anchor = datetime(2026, 4, 21, 15, 0, 0)
    got = parse_relative_time("1 hour ago", naive_anchor)
    assert got is not None
    assert got.tzinfo is None
