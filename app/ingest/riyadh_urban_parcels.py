from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine.url import make_url
from shapely.geometry import MultiPolygon, Polygon

from app.db.session import DATABASE_URL, SessionLocal

DEFAULT_ZIP_PATH = Path(
    "data/riyadh_urban_parcels/Riyadh-Urban-Parcel-Dataset.zip"
)
RAW_TABLE = "public.riyadh_urban_parcels_raw"
STAGE_TABLE = "public.riyadh_urban_parcels_stage"


def _zip_path() -> Path:
    value = os.getenv("RIYADH_URBAN_PARCELS_ZIP", str(DEFAULT_ZIP_PATH))
    return Path(value).expanduser()


def _maybe_truncate(db) -> None:
    if os.getenv("RIYADH_URBAN_PARCELS_APPEND", "false").lower() in {"1", "true", "yes"}:
        return
    db.execute(text(f"TRUNCATE TABLE {RAW_TABLE};"))
    db.commit()


def _conninfo_value(value: str | None) -> str | None:
    if value is None:
        return None
    text_value = str(value)
    if not text_value:
        return None
    if any(ch.isspace() for ch in text_value) or any(ch in text_value for ch in "\"'\\"):
        escaped = text_value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"
    return text_value


def _ogr_conninfo() -> str:
    url = make_url(DATABASE_URL)
    sslmode = url.query.get("sslmode") if url.query else None
    sslmode = sslmode or os.getenv("PGSSLMODE") or os.getenv("DB_SSLMODE")
    parts = {
        "host": url.host,
        "port": url.port,
        "dbname": url.database,
        "user": url.username,
        "password": url.password,
        "sslmode": sslmode,
    }
    rendered = []
    for key, value in parts.items():
        conn_value = _conninfo_value(value)
        if conn_value is None:
            continue
        rendered.append(f"{key}={conn_value}")
    return " ".join(rendered)


def _extract_zip(zip_path: Path, target_dir: Path) -> Path:
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(target_dir)

    shapefiles = sorted(target_dir.rglob("*.shp"))
    if not shapefiles:
        raise FileNotFoundError("No .shp files found in the zip archive.")
    return shapefiles[0]


def _load_with_ogr2ogr(shp_path: Path) -> None:
    conninfo = _ogr_conninfo()
    cmd = [
        "ogr2ogr",
        "-f",
        "PostgreSQL",
        f"PG:{conninfo}",
        str(shp_path),
        "-nln",
        STAGE_TABLE,
        "-lco",
        "GEOMETRY_NAME=geom",
        "-lco",
        "FID=id",
        "-nlt",
        "PROMOTE_TO_MULTI",
        "-t_srs",
        "EPSG:4326",
        "-overwrite",
    ]
    subprocess.run(cmd, check=True)

    with SessionLocal() as db:
        db.execute(
            text(
                f"""
                INSERT INTO {RAW_TABLE} (geom, raw_props)
                SELECT
                    ST_Multi(ST_CollectionExtract(geom, 3)) AS geom,
                    to_jsonb(s) - 'geom' - 'id' AS raw_props
                FROM {STAGE_TABLE} s;
                """
            )
        )
        db.execute(text(f"DROP TABLE IF EXISTS {STAGE_TABLE};"))
        db.commit()


def _load_with_geopandas(shp_path: Path) -> None:
    try:
        import geopandas as gpd
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("geopandas is required for fallback ingestion.") from exc

    gdf = gpd.read_file(shp_path)
    if gdf.crs:
        gdf = gdf.to_crs("EPSG:4326")

    insert_sql = text(
        f"""
        INSERT INTO {RAW_TABLE} (geom, raw_props)
        VALUES (ST_SetSRID(ST_GeomFromText(:wkt), 4326), :raw_props::jsonb);
        """
    )

    with SessionLocal() as db:
        for _, row in gdf.iterrows():
            geom = row.geometry
            if geom is None or geom.is_empty:
                continue
            if isinstance(geom, Polygon):
                geom = MultiPolygon([geom])
            if not isinstance(geom, MultiPolygon):
                continue
            raw_props = row.drop(labels=["geometry"]).to_dict()
            db.execute(
                insert_sql,
                {
                    "wkt": geom.wkt,
                    "raw_props": json.dumps(raw_props),
                },
            )
        db.commit()


def main() -> None:
    zip_path = _zip_path()
    if not zip_path.exists():
        raise FileNotFoundError(f"Zip file not found: {zip_path}")

    with SessionLocal() as db:
        _maybe_truncate(db)

    with tempfile.TemporaryDirectory() as tmpdir:
        shp_path = _extract_zip(zip_path, Path(tmpdir))
        if shutil.which("ogr2ogr"):
            _load_with_ogr2ogr(shp_path)
            return
        _load_with_geopandas(shp_path)


if __name__ == "__main__":
    main()
