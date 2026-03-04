"""Verify that the DuckDB spatial extension loads and ST_X / ST_Y work."""

import pytest

duckdb = pytest.importorskip("duckdb")


def _spatial_available() -> bool:
    """Return True if the spatial extension can be installed+loaded."""
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        return True
    except Exception:
        return False
    finally:
        con.close()


requires_spatial = pytest.mark.skipif(
    not _spatial_available(),
    reason="DuckDB spatial extension not available (offline / sandbox)",
)


@requires_spatial
def test_spatial_extension_loads():
    """Spatial extension should load and ST_X / ST_Y should work."""
    from app.connectors.duckdb_conn import _install_and_load

    con = duckdb.connect()
    try:
        _install_and_load(con, "spatial")
        row = con.execute(
            "SELECT ST_X(ST_Point(46.7, 24.7)) AS x, "
            "       ST_Y(ST_Point(46.7, 24.7)) AS y;"
        ).fetchone()
        assert row is not None
        assert abs(row[0] - 46.7) < 1e-6
        assert abs(row[1] - 24.7) < 1e-6
    finally:
        con.close()


@requires_spatial
def test_check_spatial_helper():
    """check_spatial() should return True when duckdb is available."""
    from app.connectors.duckdb_conn import check_spatial

    assert check_spatial() is True
