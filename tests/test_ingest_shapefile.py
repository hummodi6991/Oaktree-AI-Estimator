import os
import tempfile
import zipfile

import shapefile
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles

from app.db.deps import get_db
from app.main import app
from app.models import tables  # noqa: F401  # ensure models registered
from app.models.tables import ExternalFeature


@compiles(JSONB, "sqlite")
def _compile_jsonb(element, compiler, **kw):
    return "JSON"


def _setup_test_db():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    ExternalFeature.__table__.create(bind=engine)

    def _override_get_db():
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    return _override_get_db, TestingSessionLocal


def test_ingest_shapefile_roundtrip():
    override_get_db, session_factory = _setup_test_db()
    app.dependency_overrides[get_db] = override_get_db
    client = TestClient(app)

    with tempfile.TemporaryDirectory() as tmpdir:
        shp_base = os.path.join(tmpdir, "points")
        writer = shapefile.Writer(shp_base)
        writer.field("name", "C")
        writer.point(10.0, 20.0)
        writer.record("A")
        writer.close()

        prj_path = f"{shp_base}.prj"
        with open(prj_path, "w", encoding="utf-8") as prj_file:
            prj_file.write("GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563]]]" )

        zip_path = os.path.join(tmpdir, "points.zip")
        with zipfile.ZipFile(zip_path, "w") as zf:
            for ext in ("shp", "shx", "dbf", "prj"):
                zf.write(f"{shp_base}.{ext}", arcname=f"points.{ext}")

        with open(zip_path, "rb") as fh:
            response = client.post(
                "/v1/ingest/shapefile?layer=test_points",
                files={"file": ("points.zip", fh, "application/zip")},
            )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["rows"] == 1
    assert payload["layer"] == "test_points"

    with session_factory() as session:
        stored = session.query(ExternalFeature).all()
        assert len(stored) == 1
        assert stored[0].properties.get("name") == "A"
        assert stored[0].geometry["type"].lower() == "point"

    app.dependency_overrides.pop(get_db, None)
