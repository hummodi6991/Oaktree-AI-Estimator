from pathlib import Path


def test_suhail_parcels_proxy_view_uses_raw_geom():
    view_sql = Path(
        "alembic/versions/8d7fb94e2f3e_use_4326_geom_for_suhail_proxy.py"
    ).read_text()
    upgrade_sql = view_sql.split("def downgrade", 1)[0]

    assert "ST_Transform(r.geom, 32638) AS geom" not in upgrade_sql
    assert "r.geom AS geom" in upgrade_sql
