"""Tests for the multi-district per_district_cap formula.

Regression coverage for Issue B: the previous formula
    min(_PER_DISTRICT_MAX_CAP, _CANDIDATE_POOL_LIMIT // N)
was dead code because _CANDIDATE_POOL_LIMIT=2000 divided by any realistic
N (<=10) always exceeded _PER_DISTRICT_MAX_CAP=200 and collapsed to the
ceiling. The new formula scales the cap with the caller's requested
`limit` so tiny requests no longer pull the maximum headroom and large
requests still get meaningful per-district depth.
"""
from __future__ import annotations

import logging
import re

import pytest

from app.services import expansion_advisor as expansion_service
from app.services.expansion_advisor import (
    _PER_DISTRICT_HEADROOM_MULTIPLIER,
    _PER_DISTRICT_MAX_CAP,
    _PER_DISTRICT_MIN_CAP,
    clear_expansion_caches,
    run_expansion_search,
)


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def scalar(self):
        if self._rows and isinstance(self._rows[0], dict):
            return next(iter(self._rows[0].values()), None)
        if self._rows:
            return self._rows[0]
        return None

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None


class _Nested:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class _StopAfterCap(BaseException):
    """Sentinel: the test only needs run_expansion_search to reach the
    multi-district logger.info at ~line 5017. We raise from the first
    execute() call that happens after it to avoid mocking the entire
    downstream pipeline. Inherits from BaseException so inner
    `except Exception:` handlers don't swallow it."""


class _CapProbeDB:
    """FakeDB that lets run_expansion_search advance through constant-time
    setup and the multi-district cap log, then aborts via _StopAfterCap
    before any real SQL runs.

    Pre-cap probes (column_exists, information_schema lookups) return
    empty/false results so the function can progress to the cap block.
    The first post-cap execute() raises _StopAfterCap to short-circuit.
    """

    _PRE_CAP_SQL_MARKERS = (
        "information_schema.columns",
    )

    def begin_nested(self):
        return _Nested()

    def execute(self, stmt, params=None):
        sql = stmt.text if hasattr(stmt, "text") else str(stmt)
        for marker in self._PRE_CAP_SQL_MARKERS:
            if marker in sql:
                # Return "column not found" so _cached_column_exists → False.
                return _Result([])
        raise _StopAfterCap()


_MULTI_LOG_RE = re.compile(
    r"expansion_search stratified multi-district mode: "
    r"target_count=(\d+) per_district_cap=(\d+) search_id="
)


def _run_and_capture_cap(
    caplog: pytest.LogCaptureFixture,
    *,
    limit: int,
    n_districts: int,
) -> int:
    """Drive run_expansion_search just far enough to emit the cap log
    and return the per_district_cap integer that was actually used.
    """
    districts = [f"حي اختبار {i}" for i in range(n_districts)]

    clear_expansion_caches()
    caplog.clear()
    caplog.set_level(logging.INFO, logger="app.services.expansion_advisor")

    with pytest.raises(_StopAfterCap):
        run_expansion_search(
            _CapProbeDB(),
            search_id="search-test",
            brand_name="Brand",
            category="burger",
            service_model="qsr",
            min_area_m2=100,
            max_area_m2=300,
            target_area_m2=200,
            limit=limit,
            target_districts=districts,
        )

    for record in caplog.records:
        match = _MULTI_LOG_RE.search(record.getMessage())
        if match:
            assert int(match.group(1)) == n_districts
            return int(match.group(2))

    raise AssertionError(
        "multi-district cap log was not emitted; check that "
        "use_stratified/target_district_norm branch ran"
    )


def _expected_cap(limit: int, n_districts: int) -> int:
    n = max(n_districts, 1)
    effective = max(limit, 25)
    fair = (effective * _PER_DISTRICT_HEADROOM_MULTIPLIER) // n
    return max(_PER_DISTRICT_MIN_CAP, min(_PER_DISTRICT_MAX_CAP, fair))


@pytest.mark.parametrize(
    "limit, n_districts, expected",
    [
        (14, 3, 25),    # tiny limit → lifted by 25-floor: max(5, min(200, 25*3//3)) = 25
        (50, 3, 50),    # mid limit, 3 districts: max(5, min(200, 50*3//3)) = 50
        (14, 10, 7),    # tiny limit, many districts: max(5, min(200, 25*3//10)) = 7
        (200, 2, 200),  # large limit, 2 districts: hits MAX_CAP ceiling (600 clamped)
        (2, 20, 5),     # extreme starvation: hits MIN_CAP floor
    ],
)
def test_per_district_cap_matches_new_formula(
    caplog: pytest.LogCaptureFixture,
    limit: int,
    n_districts: int,
    expected: int,
) -> None:
    got = _run_and_capture_cap(caplog, limit=limit, n_districts=n_districts)
    assert got == expected
    assert got == _expected_cap(limit, n_districts)


def test_issue_b_regression_limit_14_three_districts(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Old formula: min(_PER_DISTRICT_MAX_CAP=200, _CANDIDATE_POOL_LIMIT=2000 // 3)
    # = min(200, 666) = 200. The cap silently collapsed to the ceiling for
    # any realistic request. New formula must scale with `limit` and return
    # 25 here. See Issue B close-out in app/services/expansion_advisor.py.
    got = _run_and_capture_cap(caplog, limit=14, n_districts=3)
    assert got == 25, (
        "Issue B regression: limit=14 N=3 used to return cap=200 under the "
        "old _CANDIDATE_POOL_LIMIT // N formula; new formula must yield 25."
    )


def test_single_district_uses_max_cap_default(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Single-district → use_stratified=False → multi-district branch is
    # skipped and per_district_cap stays at the _PER_DISTRICT_MAX_CAP
    # default. Confirm by asserting the multi-district log never fires.
    clear_expansion_caches()
    caplog.clear()
    caplog.set_level(logging.INFO, logger="app.services.expansion_advisor")

    with pytest.raises(_StopAfterCap):
        run_expansion_search(
            _CapProbeDB(),
            search_id="search-test",
            brand_name="Brand",
            category="burger",
            service_model="qsr",
            min_area_m2=100,
            max_area_m2=300,
            target_area_m2=200,
            limit=25,
            target_districts=["العليا"],
        )

    assert not any(
        _MULTI_LOG_RE.search(r.getMessage()) for r in caplog.records
    ), "single-district request must not hit the multi-district cap branch"


def test_city_wide_branch_unchanged(caplog: pytest.LogCaptureFixture) -> None:
    # City-wide (no target_districts) takes the elif branch, which still
    # uses _CANDIDATE_POOL_LIMIT // district_count_row. That branch needs
    # a real COUNT(DISTINCT district_label) from a DB; with _CapProbeDB
    # the first execute() for that count raises _StopAfterCap inside the
    # try/except, logging a warning and leaving per_district_cap at
    # _PER_DISTRICT_MAX_CAP. The multi-district log must never fire.
    clear_expansion_caches()
    caplog.clear()
    caplog.set_level(logging.INFO, logger="app.services.expansion_advisor")

    with pytest.raises(_StopAfterCap):
        run_expansion_search(
            _CapProbeDB(),
            search_id="search-test",
            brand_name="Brand",
            category="burger",
            service_model="qsr",
            min_area_m2=100,
            max_area_m2=300,
            target_area_m2=200,
            limit=50,
            target_districts=None,
        )

    assert not any(
        _MULTI_LOG_RE.search(r.getMessage()) for r in caplog.records
    ), "city-wide request must not hit the multi-district cap branch"


def test_headroom_multiplier_constant_value() -> None:
    # Locks the multiplier so future tweaks cause a visible test break.
    assert expansion_service._PER_DISTRICT_HEADROOM_MULTIPLIER == 3
