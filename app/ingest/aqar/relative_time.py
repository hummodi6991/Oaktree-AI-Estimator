"""Parse Aqar's "Last Update" relative-time strings into absolute datetimes.

Aqar renders the "Last Update" value on every detail page as a relative
phrase ("3 minutes ago", "1 hour ago", "منذ دقيقة") instead of an
absolute timestamp. The only way to turn that into something we can
store and query is to subtract the offset from the moment of fetch.

This module is intentionally pure — no HTTP, no regex compilation at
import time in a way that'd surprise a reader. Callers pass the raw
text and an ``anchor`` datetime and get a ``datetime`` back (or ``None``
when the input is not parseable).

Approximations: "month" and "year" get mapped to 30 and 365 days
respectively. Those are the only two units that cannot be expressed
exactly as a ``timedelta``. We log them at DEBUG so the approximation
is auditable if a downstream consumer ever cares.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

_MINUTE = 60
_HOUR = 60 * _MINUTE
_DAY = 24 * _HOUR
_WEEK = 7 * _DAY
_MONTH_APPROX = 30 * _DAY
_YEAR_APPROX = 365 * _DAY

# English unit → seconds. Keys are singular; we strip a trailing "s" before
# lookup so both "minute" and "minutes" resolve to the same entry.
_UNIT_SECONDS_EN: dict[str, int] = {
    "second": 1,
    "sec": 1,
    "minute": _MINUTE,
    "min": _MINUTE,
    "hour": _HOUR,
    "hr": _HOUR,
    "day": _DAY,
    "week": _WEEK,
    "month": _MONTH_APPROX,
    "year": _YEAR_APPROX,
}

# Arabic unit → (seconds, duality count for the "منذ يومين"/"dual" form).
# Dual count 0 means the word is a unit only; dual count N means when the
# phrase is exactly "منذ <word>" (no number) it means N units.
_UNIT_SECONDS_AR: dict[str, tuple[int, int]] = {
    # singular forms — "منذ دقيقة" = "1 minute ago"
    "ثانية": (1, 1),
    "ثواني": (1, 0),
    "دقيقة": (_MINUTE, 1),
    "دقائق": (_MINUTE, 0),
    "ساعة": (_HOUR, 1),
    "ساعات": (_HOUR, 0),
    "يوم": (_DAY, 1),
    "يومين": (_DAY, 2),  # dual: two days
    "أيام": (_DAY, 0),
    "ايام": (_DAY, 0),
    "أسبوع": (_WEEK, 1),
    "اسبوع": (_WEEK, 1),
    "أسبوعين": (_WEEK, 2),
    "اسبوعين": (_WEEK, 2),
    "أسابيع": (_WEEK, 0),
    "اسابيع": (_WEEK, 0),
    "شهر": (_MONTH_APPROX, 1),
    "شهرين": (_MONTH_APPROX, 2),
    "أشهر": (_MONTH_APPROX, 0),
    "اشهر": (_MONTH_APPROX, 0),
    "سنة": (_YEAR_APPROX, 1),
    "سنتين": (_YEAR_APPROX, 2),
    "سنوات": (_YEAR_APPROX, 0),
    "سنين": (_YEAR_APPROX, 0),
    "عام": (_YEAR_APPROX, 1),
    "عامين": (_YEAR_APPROX, 2),
    "أعوام": (_YEAR_APPROX, 0),
    "اعوام": (_YEAR_APPROX, 0),
}

_APPROX_UNITS = {"month", "year", "شهر", "شهرين", "أشهر", "اشهر",
                 "سنة", "سنتين", "سنوات", "سنين",
                 "عام", "عامين", "أعوام", "اعوام"}


_EN_RE = re.compile(
    r"(?P<num>\d+)\s+(?P<unit>second|sec|minute|min|hour|hr|day|week|month|year)s?\s+ago",
    re.IGNORECASE,
)
_EN_A_RE = re.compile(
    r"(?:a|an)\s+(?P<unit>second|minute|hour|day|week|month|year)\s+ago",
    re.IGNORECASE,
)

# Arabic: "منذ <num> <unit>" or "منذ <unit>" (implicit 1 or dual).
_AR_RE = re.compile(
    r"منذ\s+(?:(?P<num>\d+)\s+)?(?P<unit>\S+)"
)


def parse_relative_time(text: str, anchor: datetime) -> datetime | None:
    """Return ``anchor - <offset>`` for an Aqar relative-time string.

    Supported English forms (case-insensitive):
      * ``"just now"``                  → anchor unchanged
      * ``"N <unit> ago"``              e.g. ``"3 minutes ago"``
      * ``"a <unit> ago"``/``"an hour ago"`` → equivalent to 1 unit

    Supported Arabic forms:
      * ``"منذ لحظات"`` / ``"الآن"``    → anchor unchanged
      * ``"منذ <unit>"``                → 1 unit (dual forms decode to 2)
      * ``"منذ N <unit>"``              e.g. ``"منذ 3 أيام"``

    Returns ``None`` on empty input, garbage, or future-dated strings
    ("in 3 minutes"). Units larger than a week (month, year) use
    30/365-day approximations and log at DEBUG level.
    """
    if text is None:
        return None
    s = text.strip()
    if not s:
        return None

    # Normalize Arabic-Indic digits and collapse whitespace so the regexes
    # only need to know about ASCII digits.
    s = s.translate(_ARABIC_DIGITS)
    s = re.sub(r"\s+", " ", s)

    lowered = s.lower()

    # "just now" / "now" / "right now" — zero offset.
    if lowered in ("just now", "now", "right now"):
        return anchor
    if s in ("منذ لحظات", "الآن", "الان", "منذ قليل"):
        return anchor

    # Defensive guard against future-dated strings — we never want to
    # silently hand back an ``anchor + offset`` value. Aqar should never
    # emit these, but a parser that accepts them would be a latent bug.
    if lowered.startswith("in ") and " ago" not in lowered:
        return None
    if lowered.startswith("after "):
        return None

    # English: "N <unit> ago" (also accepts "a/an <unit> ago").
    m = _EN_RE.search(lowered)
    if m:
        num = int(m.group("num"))
        unit_key = _normalize_en_unit(m.group("unit"))
        seconds = _UNIT_SECONDS_EN.get(unit_key)
        if seconds is None:
            return None
        if unit_key in _APPROX_UNITS:
            logger.debug(
                "parse_relative_time: approximating %r using %d-day %s",
                text, seconds // _DAY, unit_key,
            )
        return anchor - timedelta(seconds=num * seconds)

    m = _EN_A_RE.search(lowered)
    if m:
        unit_key = _normalize_en_unit(m.group("unit"))
        seconds = _UNIT_SECONDS_EN.get(unit_key)
        if seconds is None:
            return None
        if unit_key in _APPROX_UNITS:
            logger.debug(
                "parse_relative_time: approximating %r using %d-day %s",
                text, seconds // _DAY, unit_key,
            )
        return anchor - timedelta(seconds=seconds)

    # Arabic: "منذ [N] <unit>".
    m = _AR_RE.search(s)
    if m:
        unit_word = m.group("unit")
        entry = _UNIT_SECONDS_AR.get(unit_word)
        if entry is None:
            return None
        seconds, dual_count = entry
        num_str = m.group("num")
        if num_str is not None:
            num = int(num_str)
        elif dual_count > 0:
            num = dual_count  # singular (1) or dual (2) implicit
        else:
            return None  # plural-only word with no number is ambiguous
        if unit_word in _APPROX_UNITS:
            logger.debug(
                "parse_relative_time: approximating %r using %d-day %s",
                text, seconds // _DAY, unit_word,
            )
        return anchor - timedelta(seconds=num * seconds)

    return None


def _normalize_en_unit(unit: str) -> str:
    unit = unit.lower()
    # strip trailing "s" (minutes → minute, hours → hour). The regex already
    # made "s" optional, so this only fires on the captured singular form.
    if unit.endswith("s") and unit not in ("sec", "hrs"):
        unit = unit.rstrip("s")
    return unit
