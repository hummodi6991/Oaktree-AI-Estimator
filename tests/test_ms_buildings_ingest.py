import csv
import gzip
import json
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.db.session import SessionLocal
from app.ingest.ms_buildings import ingest_ms_buildings


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.ms_buildings_raw (
    id bigserial PRIMARY KEY,
    source text NOT NULL DEFAULT 'microsoft_globalml',
    country text NULL,
    quadkey text NULL,
    source_id text NOT NULL,
    geom geometry(MultiPolygon,4326) NOT NULL,
    area_m2 double precision NOT NULL DEFAULT 0,
    observed_at timestamptz NOT NULL DEFAULT now()
);
"""


@pytest.fixture
def db_session():
    session = SessionLocal()
    try:
        session.execute(text("SELECT 1"))
    except Exception as exc:
        session.close()
        pytest.skip(f"Postgres unavailable: {exc}")

    try:
        session.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
        session.execute(text("SELECT PostGIS_Version();"))
    except SQLAlchemyError as exc:
        session.rollback()
        session.close()
        pytest.skip(f"PostGIS unavailable: {exc}")

    session.execute(text(CREATE_TABLE_SQL))
    session.execute(
        text(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ms_buildings_raw_source_id_ux
                ON public.ms_buildings_raw (source, source_id);
            """
        )
    )
    session.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS ms_buildings_raw_geom_gix
                ON public.ms_buildings_raw USING GIST (geom);
            """
        )
    )
    session.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS ms_buildings_raw_source_country_idx
                ON public.ms_buildings_raw (source, country);
            """
        )
    )
    session.commit()
    try:
        yield session
    finally:
        session.close()


def _write_geojsonl(path: Path, rows: list[dict]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def _assert_stats(db_session, source: str, expected_count: int) -> None:
    stats = db_session.execute(
        text(
            """
            SELECT
                COUNT(*) AS count,
                MIN(ST_SRID(geom)) AS srid_min,
                MAX(ST_SRID(geom)) AS srid_max,
                MIN(area_m2) AS area_min
            FROM public.ms_buildings_raw
            WHERE source = :source
            """
        ),
        {"source": source},
    ).mappings().one()

    assert stats["count"] == expected_count
    assert stats["srid_min"] == 4326
    assert stats["srid_max"] == 4326
    assert stats["area_min"] > 0


def test_ingest_ms_buildings_geojsonl(db_session):
    source = "microsoft_globalml_test"
    db_session.execute(text("DELETE FROM public.ms_buildings_raw WHERE source = :source"), {"source": source})
    db_session.commit()

    feature_1 = {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [46.675, 24.713],
                    [46.676, 24.713],
                    [46.676, 24.714],
                    [46.675, 24.714],
                    [46.675, 24.713],
                ]
            ],
        },
        "properties": {},
    }
    feature_2 = {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [46.677, 24.713],
                    [46.678, 24.713],
                    [46.678, 24.714],
                    [46.677, 24.714],
                    [46.677, 24.713],
                ]
            ],
        },
        "properties": {},
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "saudi_0123.csv.gz"
        _write_geojsonl(path, [feature_1, feature_2, feature_1])
        ingest_ms_buildings(Path(tmpdir), country="Saudi Arabia", source=source, batch_size=2)

    _assert_stats(db_session, source, expected_count=2)

    db_session.execute(text("DELETE FROM public.ms_buildings_raw WHERE source = :source"), {"source": source})
    db_session.commit()


def test_ingest_ms_buildings_csv_geojson(db_session):
    source = "microsoft_globalml_test_csv"
    db_session.execute(text("DELETE FROM public.ms_buildings_raw WHERE source = :source"), {"source": source})
    db_session.commit()

    geom_1 = {
        "type": "Polygon",
        "coordinates": [
            [
                [46.681, 24.713],
                [46.682, 24.713],
                [46.682, 24.714],
                [46.681, 24.714],
                [46.681, 24.713],
            ]
        ],
    }
    geom_2 = {
        "type": "Polygon",
        "coordinates": [
            [
                [46.683, 24.713],
                [46.684, 24.713],
                [46.684, 24.714],
                [46.683, 24.714],
                [46.683, 24.713],
            ]
        ],
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "saudi_geojson.csv.gz"
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["geometry", "name"])
            writer.writeheader()
            writer.writerow({"geometry": json.dumps(geom_1), "name": "A"})
            writer.writerow({"geometry": json.dumps(geom_2), "name": "B"})
            writer.writerow({"geometry": "{bad json}", "name": "C"})

        ingest_ms_buildings(Path(tmpdir), country="Saudi Arabia", source=source, batch_size=2)

    _assert_stats(db_session, source, expected_count=2)

    db_session.execute(text("DELETE FROM public.ms_buildings_raw WHERE source = :source"), {"source": source})
    db_session.commit()
