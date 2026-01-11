from pathlib import Path


def _migration_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_derived_parcels_v1_creation_has_no_data():
    migration_text = _migration_text(
        "alembic/versions/e3f4a5b6c7d8_create_derived_parcels_v1.py"
    )

    assert "CREATE MATERIALIZED VIEW public.derived_parcels_v1" in migration_text
    assert "WITH NO DATA" in migration_text


def test_recreate_derived_parcels_v1_creation_has_no_data():
    migration_text = _migration_text(
        "alembic/versions/f2a1b3c4d5e6_recreate_derived_parcels_v1_with_no_data.py"
    )

    assert "CREATE MATERIALIZED VIEW public.derived_parcels_v1" in migration_text
    assert "WITH NO DATA" in migration_text
