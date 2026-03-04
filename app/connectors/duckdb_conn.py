"""
Shared DuckDB connection factory.

Ensures the ``spatial`` and ``httpfs`` extensions are installed and loaded
before any query that uses spatial functions (ST_X, ST_Y, etc.).
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _install_and_load(con, extension: str) -> None:
    """Install (if needed) and load a DuckDB extension."""
    try:
        con.execute(f"INSTALL {extension};")
    except Exception:
        pass  # already installed
    con.execute(f"LOAD {extension};")


def get_duckdb_connection(*, s3_region: str = "us-west-2"):
    """Return a DuckDB connection with spatial + httpfs extensions loaded.

    Parameters
    ----------
    s3_region:
        AWS region used for unsigned S3 reads (default ``us-west-2``).

    Raises
    ------
    RuntimeError
        If the *spatial* or *httpfs* extension cannot be loaded.
    """
    import duckdb

    con = duckdb.connect()

    for ext in ("httpfs", "spatial"):
        try:
            _install_and_load(con, ext)
        except Exception as exc:
            con.close()
            raise RuntimeError(
                f"Failed to load the DuckDB '{ext}' extension. "
                f"Ensure 'duckdb' is installed with {ext} support. "
                f"Original error: {exc}"
            ) from exc

    con.execute(f"SET s3_region='{s3_region}';")
    logger.debug("DuckDB connection ready (httpfs + spatial loaded)")
    return con


def check_spatial() -> bool:
    """Quick self-check: verify ST_X is available in a throwaway connection.

    Only loads the *spatial* extension (no httpfs needed).
    Returns ``True`` if spatial functions work, raises on failure.
    """
    import duckdb

    con = duckdb.connect()
    try:
        _install_and_load(con, "spatial")
        result = con.execute(
            "SELECT ST_X(ST_Point(46.7, 24.7)) AS x;"
        ).fetchone()
        assert result is not None and abs(result[0] - 46.7) < 1e-6
    finally:
        con.close()
    return True
