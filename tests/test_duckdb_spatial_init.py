"""Tests for DuckDB spatial extension initialisation in overture_places connector."""

from unittest.mock import MagicMock

import pytest

duckdb = pytest.importorskip("duckdb")

from app.connectors.overture_places import _init_duckdb_extensions


def _extensions_available() -> bool:
    """Return True if DuckDB can install+load spatial in this environment."""
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        return True
    except Exception:
        return False
    finally:
        con.close()


_SKIP_MSG = "DuckDB extensions cannot be downloaded in this environment"


@pytest.mark.skipif(not _extensions_available(), reason=_SKIP_MSG)
def test_init_duckdb_extensions_loads_spatial():
    """_init_duckdb_extensions should install+load httpfs and spatial."""
    con = duckdb.connect()
    _init_duckdb_extensions(con)

    # ST_X / ST_Y must be available after init.
    row = con.execute(
        "SELECT ST_X(ST_Point(1.5, 2.5)) AS x, ST_Y(ST_Point(1.5, 2.5)) AS y"
    ).fetchone()
    assert row[0] == pytest.approx(1.5)
    assert row[1] == pytest.approx(2.5)
    con.close()


@pytest.mark.skipif(not _extensions_available(), reason=_SKIP_MSG)
def test_init_duckdb_extensions_idempotent():
    """Calling _init_duckdb_extensions twice on the same connection must not raise."""
    con = duckdb.connect()
    _init_duckdb_extensions(con)
    _init_duckdb_extensions(con)  # second call — should be a no-op
    con.close()


def test_init_duckdb_extensions_raises_on_load_failure():
    """If LOAD fails, _init_duckdb_extensions raises RuntimeError."""
    mock_con = MagicMock()

    call_count = 0

    def _side_effect(sql, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        if sql.strip().upper().startswith("LOAD"):
            raise duckdb.IOException("mock: extension not found")

    mock_con.execute.side_effect = _side_effect

    with pytest.raises(RuntimeError, match="Failed to LOAD"):
        _init_duckdb_extensions(mock_con)
