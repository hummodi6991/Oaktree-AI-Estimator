"""Tests for population_density ingestion semantics."""

from unittest.mock import MagicMock, patch

from app.ingest import population_density as ingest_mod


def test_ingest_purges_existing_source_rows_before_insert():
    """The H3 resolution upgrade requires deleting prior-resolution rows
    before inserting new ones; otherwise different h3_index values from
    different resolutions coexist and catchment SUMs double-count.
    """
    db = MagicMock()
    delete_result = MagicMock()
    delete_result.rowcount = 42
    db.execute.return_value = delete_result
    db.query.return_value.filter_by.return_value.first.return_value = None

    with patch.object(ingest_mod, "load_hdx_population_raster", return_value=iter([])):
        ingest_mod.ingest_population_hdx(db, "/tmp/fake.tif")

    delete_calls = [
        call for call in db.execute.call_args_list
        if "DELETE FROM population_density" in str(call.args[0])
    ]
    assert delete_calls, "ingest must DELETE existing source rows before inserting"
    assert delete_calls[0].args[1] == {"src": ingest_mod.SOURCE_NAME}
    db.commit.assert_called_once()


def test_ingest_delete_runs_before_first_insert():
    """The DELETE must precede any add() so transactional readers never see
    a half-empty table once the resolution swap commits."""
    db = MagicMock()
    delete_result = MagicMock()
    delete_result.rowcount = 0
    db.execute.return_value = delete_result
    db.query.return_value.filter_by.return_value.first.return_value = None

    call_order: list[str] = []
    db.execute.side_effect = lambda *a, **kw: (call_order.append("execute"), delete_result)[1]
    db.add.side_effect = lambda *a, **kw: call_order.append("add")

    fake_cells = [
        {"h3_index": "892a10a1cbfffff", "lat": 24.7, "lon": 46.7, "population": 100.0},
        {"h3_index": "892a10a1cb7ffff", "lat": 24.71, "lon": 46.71, "population": 200.0},
    ]
    with patch.object(ingest_mod, "load_hdx_population_raster", return_value=iter(fake_cells)):
        ingest_mod.ingest_population_hdx(db, "/tmp/fake.tif")

    assert call_order[0] == "execute", "DELETE must run before any insert"
    assert "add" in call_order, "expected at least one row insert"
