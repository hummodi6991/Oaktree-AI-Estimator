"""End-to-end SQL tests for the rolling-6 radiance YoY query (B1.5).

These tests exercise the rolling-window CTE introduced in B1.5 against a
synthetic district_radiance_monthly table.  They require a live PostgreSQL
connection because the query uses FILTER clauses inside window functions,
which SQLite does not support.

Test-infrastructure requirement: a running PostgreSQL instance reachable via
the DATABASE_URL env var (or the default psycopg2 localhost connection).
If no connection is available the tests are automatically skipped.

NOTE: pytest-postgresql is not installed in this environment.  Tests use a
plain SQLAlchemy + psycopg2 connection instead, following the project's
existing pattern of creating in-process SQLite sessions where possible and
skipping when the needed engine is unavailable.
"""
from __future__ import annotations

import os
from datetime import date
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Skip guard — no postgres, no test.
# ---------------------------------------------------------------------------

_PG_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres@localhost:5432/postgres",
)


def _pg_available() -> bool:
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(_PG_URL, connect_args={"connect_timeout": 3})
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _pg_available(),
    reason=(
        "Rolling-6 radiance SQL tests require a live PostgreSQL instance "
        "(pytest-postgresql not installed; DATABASE_URL not set or unreachable). "
        "Run with a reachable PostgreSQL to enable these tests."
    ),
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ROLLING6_SQL = """
WITH ordered AS (
    SELECT
        district_key,
        year_month,
        radiance_mean,
        pixel_count_valid,
        ROW_NUMBER() OVER (
            PARTITION BY district_key ORDER BY year_month
        ) AS rn
    FROM district_radiance_monthly
    WHERE source = :src AND quality_filter = :ql
),
windowed AS (
    SELECT
        district_key,
        year_month,
        rn,
        pixel_count_valid,
        SUM(radiance_mean * pixel_count_valid)
            FILTER (WHERE radiance_mean IS NOT NULL AND pixel_count_valid > 0)
            OVER w_cur
        / NULLIF(
            SUM(pixel_count_valid)
                FILTER (WHERE radiance_mean IS NOT NULL AND pixel_count_valid > 0)
                OVER w_cur,
            0
        ) AS rad_cur6,
        SUM(radiance_mean * pixel_count_valid)
            FILTER (WHERE radiance_mean IS NOT NULL AND pixel_count_valid > 0)
            OVER w_prev
        / NULLIF(
            SUM(pixel_count_valid)
                FILTER (WHERE radiance_mean IS NOT NULL AND pixel_count_valid > 0)
                OVER w_prev,
            0
        ) AS rad_prev6,
        MIN(pixel_count_valid) OVER w_cur  AS min_pixels_cur6,
        MIN(pixel_count_valid) OVER w_prev AS min_pixels_prev6,
        COUNT(*) OVER w_cur  AS rows_cur6,
        COUNT(*) OVER w_prev AS rows_prev6
    FROM ordered
    WINDOW
        w_cur  AS (PARTITION BY district_key ORDER BY year_month
                   ROWS BETWEEN 5 PRECEDING AND CURRENT ROW),
        w_prev AS (PARTITION BY district_key ORDER BY year_month
                   ROWS BETWEEN 17 PRECEDING AND 12 PRECEDING)
),
latest_per_district AS (
    SELECT *
    FROM windowed
    WHERE (district_key, rn) IN (
        SELECT district_key, MAX(rn) FROM windowed GROUP BY district_key
    )
)
SELECT
    district_key,
    rad_cur6                    AS rad_cur,
    min_pixels_cur6             AS pixels_cur,
    year_month                  AS ym_cur,
    rad_prev6                   AS rad_prev,
    min_pixels_prev6            AS pixels_prev,
    rows_cur6, rows_prev6
FROM latest_per_district
WHERE rad_cur6  IS NOT NULL
  AND rad_prev6  IS NOT NULL
  AND rows_cur6  = 6
  AND rows_prev6 = 6
"""

_SRC = "nasa_blackmarble_vnp46a3_c2"
_QL = "lenient_qa_lt_2"


@pytest.fixture()
def pg_conn():
    """Yield a SQLAlchemy connection inside a rolled-back transaction.

    Creates the synthetic district_radiance_monthly table, runs the test,
    then rolls back so no state persists between tests.
    """
    from sqlalchemy import create_engine, text

    engine = create_engine(_PG_URL)
    with engine.connect() as conn:
        conn.execute(text("BEGIN"))
        conn.execute(text("""
            CREATE TEMP TABLE district_radiance_monthly (
                district_key    TEXT    NOT NULL,
                year_month      DATE    NOT NULL,
                source          TEXT    NOT NULL,
                quality_filter  TEXT    NOT NULL,
                radiance_mean   NUMERIC,
                pixel_count_valid INTEGER NOT NULL DEFAULT 0
            )
        """))
        yield conn
        conn.execute(text("ROLLBACK"))


def _insert_rows(conn, rows: list[dict[str, Any]]) -> None:
    from sqlalchemy import text

    for r in rows:
        conn.execute(text("""
            INSERT INTO district_radiance_monthly
                (district_key, year_month, source, quality_filter,
                 radiance_mean, pixel_count_valid)
            VALUES
                (:dk, :ym, :src, :ql, :rad, :px)
        """), {
            "dk": r["district_key"],
            "ym": r["year_month"],
            "src": _SRC,
            "ql": _QL,
            "rad": r["radiance_mean"],
            "px": r["pixel_count_valid"],
        })


def _run_query(conn) -> list[dict]:
    from sqlalchemy import text

    rows = conn.execute(
        text(_ROLLING6_SQL),
        {"src": _SRC, "ql": _QL},
    ).mappings().all()
    return [dict(r) for r in rows]


def _build_lookup(conn) -> dict[str, dict]:
    """Replicate the Python loop from expansion_advisor.py for assertions."""
    from app.connectors import blackmarble

    rows = _run_query(conn)
    lookup: dict[str, dict] = {}
    for r in rows:
        dk = r["district_key"]
        pixels_cur = int(r["pixels_cur"] or 0)
        pixels_prev = int(r["pixels_prev"] or 0)
        confident, reason = blackmarble.evaluate_confidence(
            pixels_cur=pixels_cur,
            pixels_prev=pixels_prev,
            area_km2=None,  # not testing area gate here
            district_key=dk,
        )
        yoy_pct: float | None = None
        if confident and r["rad_prev"] and float(r["rad_prev"]) > 0:
            yoy_pct = (
                float(r["rad_cur"]) - float(r["rad_prev"])
            ) / float(r["rad_prev"]) * 100.0
        lookup[dk] = {
            "value_yoy_pct": round(yoy_pct, 2) if yoy_pct is not None else None,
            "source_label": "blackmarble_district_yoy_rolling6",
            "confident": confident and yoy_pct is not None,
            "confidence_reason": reason,
            "pixel_count": pixels_cur,
            "year_month": str(r["ym_cur"]),
        }
    return lookup


def _make_monthly_rows(
    district_key: str,
    start: date,
    n_months: int,
    radiance_fn,
    pixel_fn=None,
    default_pixels: int = 50,
) -> list[dict]:
    """Generate n_months consecutive monthly rows starting from start."""
    import calendar

    rows = []
    y, m = start.year, start.month
    for i in range(n_months):
        ym = date(y, m, 1)
        rows.append({
            "district_key": district_key,
            "year_month": ym,
            "radiance_mean": radiance_fn(i),
            "pixel_count_valid": pixel_fn(i) if pixel_fn else default_pixels,
        })
        m += 1
        if m > 12:
            m = 1
            y += 1
    return rows


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_radiance_yoy_rolling6_matches_manual(pg_conn):
    """Pixel-weighted mean reduces to arithmetic mean when pixels are constant.

    24 monthly rows from 2024-04-01 through 2026-03-01, linearly increasing
    radiance, pixel_count_valid=50 for all.  The expected value is:
      cur_window  = mean(rows 19..24, i.e. indices 18..23) = mean(rad[18..23])
      prev_window = mean(rows 7..12,  i.e. indices 6..11)  = mean(rad[6..11])
    where rad[i] = 10.0 + i (0-indexed).
    """
    rows = _make_monthly_rows(
        district_key="test_d1",
        start=date(2024, 4, 1),
        n_months=24,
        radiance_fn=lambda i: 10.0 + i,
        default_pixels=50,
    )
    _insert_rows(pg_conn, rows)

    lookup = _build_lookup(pg_conn)
    assert "test_d1" in lookup

    # indices 18-23 → radiance 28.0 .. 33.0 → mean = 30.5
    cur_mean = sum(10.0 + i for i in range(18, 24)) / 6
    # indices 6-11 → radiance 16.0 .. 21.0 → mean = 18.5
    prev_mean = sum(10.0 + i for i in range(6, 12)) / 6
    expected_yoy = (cur_mean - prev_mean) / prev_mean * 100.0

    assert lookup["test_d1"]["confident"] is True
    assert abs(lookup["test_d1"]["value_yoy_pct"] - round(expected_yoy, 2)) < 0.01


def test_radiance_yoy_rolling6_pixel_weighted(pg_conn):
    """Pixel weighting produces area-weighted mean, not arithmetic mean.

    Latest 6 months have pixels [50,50,100,50,50,50]; prior-year 6 all have
    pixels=50.  Manually compute SUM(rad*px)/SUM(px) for both windows.
    """
    # 24 months; constant radiance=5.0 except for custom pixel counts in cur window
    pixel_override = {18: 50, 19: 50, 20: 100, 21: 50, 22: 50, 23: 50}
    rows = _make_monthly_rows(
        district_key="test_d2",
        start=date(2024, 4, 1),
        n_months=24,
        radiance_fn=lambda i: 5.0 + i * 0.5,
        pixel_fn=lambda i: pixel_override.get(i, 50),
    )
    _insert_rows(pg_conn, rows)

    # Manual calc for current window (indices 18..23)
    cur_rads = [5.0 + i * 0.5 for i in range(18, 24)]
    cur_pxs = [pixel_override.get(i, 50) for i in range(18, 24)]
    cur_weighted = sum(r * p for r, p in zip(cur_rads, cur_pxs)) / sum(cur_pxs)

    # Prior-year window (indices 6..11) — all px=50, reduces to arithmetic mean
    prev_rads = [5.0 + i * 0.5 for i in range(6, 12)]
    prev_weighted = sum(prev_rads) / 6

    expected_yoy = (cur_weighted - prev_weighted) / prev_weighted * 100.0

    lookup = _build_lookup(pg_conn)
    assert "test_d2" in lookup
    assert lookup["test_d2"]["confident"] is True
    assert abs(lookup["test_d2"]["value_yoy_pct"] - round(expected_yoy, 2)) < 0.01


def test_radiance_confidence_rolling_min_gate_cur_fails(pg_conn):
    """One month in the latest 6 below pixel floor → confident=False."""
    def pixel_fn(i):
        # index 20 is within the latest-6 window (indices 18..23)
        return 5 if i == 20 else 50

    rows = _make_monthly_rows(
        district_key="test_d3",
        start=date(2024, 4, 1),
        n_months=24,
        radiance_fn=lambda i: 10.0 + i,
        pixel_fn=pixel_fn,
    )
    _insert_rows(pg_conn, rows)

    lookup = _build_lookup(pg_conn)
    assert "test_d3" in lookup
    assert lookup["test_d3"]["confident"] is False
    assert lookup["test_d3"]["confidence_reason"] == "pixel_floor"


def test_radiance_confidence_rolling_min_gate_prev_fails(pg_conn):
    """One month in the prior-year 6 below pixel floor → confident=False."""
    def pixel_fn(i):
        # index 7 is within the prior-year window (indices 6..11)
        return 5 if i == 7 else 50

    rows = _make_monthly_rows(
        district_key="test_d4",
        start=date(2024, 4, 1),
        n_months=24,
        radiance_fn=lambda i: 10.0 + i,
        pixel_fn=pixel_fn,
    )
    _insert_rows(pg_conn, rows)

    lookup = _build_lookup(pg_conn)
    assert "test_d4" in lookup
    assert lookup["test_d4"]["confident"] is False
    assert lookup["test_d4"]["confidence_reason"] == "pixel_floor"


def test_radiance_confidence_rolling_min_gate_passes(pg_conn):
    """All months at exactly pixel floor=10 → confident=True."""
    rows = _make_monthly_rows(
        district_key="test_d5",
        start=date(2024, 4, 1),
        n_months=24,
        radiance_fn=lambda i: 10.0 + i,
        default_pixels=10,  # boundary: exactly at floor
    )
    _insert_rows(pg_conn, rows)

    lookup = _build_lookup(pg_conn)
    assert "test_d5" in lookup
    assert lookup["test_d5"]["confident"] is True
    assert lookup["test_d5"]["value_yoy_pct"] is not None


def test_radiance_lookup_dict_keys_stable(pg_conn):
    """Regression: lookup dict has exactly the expected keys and correct values."""
    rows = _make_monthly_rows(
        district_key="test_d6",
        start=date(2024, 4, 1),
        n_months=24,
        radiance_fn=lambda i: 15.0,
        default_pixels=50,
    )
    _insert_rows(pg_conn, rows)

    lookup = _build_lookup(pg_conn)
    assert "test_d6" in lookup

    entry = lookup["test_d6"]
    expected_keys = {
        "value_yoy_pct",
        "source_label",
        "confident",
        "confidence_reason",
        "pixel_count",
        "year_month",
    }
    assert set(entry.keys()) == expected_keys
    assert entry["source_label"] == "blackmarble_district_yoy_rolling6"
    # Latest month in the 24-row sequence starting 2024-04 is 2026-03-01
    assert entry["year_month"] == "2026-03-01"


def test_radiance_yoy_rolling6_skips_districts_with_partial_window(pg_conn):
    """Districts with < 18 months are absent; adjacent full districts are present.

    rows_cur6=6 AND rows_prev6=6 requires ≥ 18 months of data.  A district
    with only 17 rows cannot satisfy rows_prev6=6 for its latest row.
    """
    # Partial district: 17 rows (one short of 18-month minimum)
    rows_partial = _make_monthly_rows(
        district_key="partial_d",
        start=date(2024, 4, 1),
        n_months=17,
        radiance_fn=lambda i: 10.0,
        default_pixels=50,
    )
    # Full district: 24 rows
    rows_full = _make_monthly_rows(
        district_key="full_d",
        start=date(2024, 4, 1),
        n_months=24,
        radiance_fn=lambda i: 10.0 + i,
        default_pixels=50,
    )
    _insert_rows(pg_conn, rows_partial + rows_full)

    raw = _run_query(pg_conn)
    keys = {r["district_key"] for r in raw}
    assert "partial_d" not in keys, "district with 17 rows must be filtered out"
    assert "full_d" in keys, "district with 24 rows must be present"
