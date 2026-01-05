import importlib
import json
import os

import app.api.geo_portal as geo_portal


class _DummyResult:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def first(self):
        if isinstance(self._row, list):
            return self._row[0] if self._row else None
        return self._row

    def scalars(self):
        return self

    def all(self):
        if isinstance(self._row, list):
            return self._row
        if self._row is None:
            return []
        return self._row


class _DummyDB:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.calls.append((sql, params or {}))
        if "WITH q AS" in sql:
            return _DummyResult(self.responses.get("identify"))
        if "information_schema.columns" in sql:
            return _DummyResult(self.responses.get("columns", []))
        if "FROM overture_buildings WHERE id" in sql:
            return _DummyResult(self.responses.get("attr"))
        if "FROM overture_buildings o" in sql:
            return _DummyResult(self.responses.get("ovt_overlay"))
        if "planet_osm_polygon" in sql:
            return _DummyResult(self.responses.get("osm_overlay"))
        raise AssertionError(f"Unexpected SQL: {sql}")


def _geom_json():
    geometry = {
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
    }
    return json.dumps(geometry)


def _reload_geo_portal(table_name: str):
    os.environ["PARCEL_IDENTIFY_TABLE"] = table_name
    importlib.reload(geo_portal)
    return geo_portal


def _identify_row(landuse=None, classification="overture_building"):
    return {
        "id": "ovt:build2",
        "landuse": landuse,
        "classification": classification,
        "source": "parcel",
        "area_m2": 120,
        "perimeter_m": 45,
        "geom": _geom_json(),
        "distance_m": 0,
        "hits": 1,
        "near": 1,
        "is_ovt": 1,
    }


def test_identify_postgis_overture_attr_sets_landuse():
    geo_portal = _reload_geo_portal("parcels")
    db = _DummyDB(
        {
            "identify": _identify_row(),
            "attr": {"subtype": "residential", "class": None},
            "ovt_overlay": {"res_share": 0.1, "com_share": 0.1},
            "osm_overlay": {"res_share": 0.1, "com_share": 0.1},
            "columns": [],
        }
    )
    result = geo_portal._identify_postgis(46.675, 24.713, 25.0, db)
    parcel = result["parcel"]
    assert parcel["landuse_code"] == "s"
    assert parcel["landuse_method"] == "overture_building_attr"
    assert parcel["landuse_raw"] == "residential"


def test_identify_postgis_overture_overlay_wins_when_osm_weak():
    geo_portal = _reload_geo_portal("parcels")
    db = _DummyDB(
        {
            "identify": _identify_row(landuse="building", classification="parcel"),
            "attr": None,
            "ovt_overlay": {"res_share": 0.1, "com_share": 0.7},
            "osm_overlay": {"res_share": 0.2, "com_share": 0.1},
            "columns": [],
        }
    )
    result = geo_portal._identify_postgis(46.675, 24.713, 25.0, db)
    parcel = result["parcel"]
    assert parcel["landuse_code"] == "m"
    assert parcel["landuse_method"] == "overture_overlay"
    assert parcel["residential_share"] == 0.1
    assert parcel["commercial_share"] == 0.7


def test_identify_postgis_osm_overlay_wins_when_strong():
    geo_portal = _reload_geo_portal("parcels")
    db = _DummyDB(
        {
            "identify": _identify_row(landuse="building", classification="parcel"),
            "attr": None,
            "ovt_overlay": {"res_share": 0.2, "com_share": 0.4},
            "osm_overlay": {"res_share": 0.75, "com_share": 0.1},
            "columns": [],
        }
    )
    result = geo_portal._identify_postgis(46.675, 24.713, 25.0, db)
    parcel = result["parcel"]
    assert parcel["landuse_code"] == "s"
    assert parcel["landuse_method"] == "osm_overlay"
    assert parcel["residential_share"] == 0.75
    assert parcel["commercial_share"] == 0.1


def test_identify_postgis_overture_overlay_wins_when_osm_not_strong():
    geo_portal = _reload_geo_portal("parcels")
    db = _DummyDB(
        {
            "identify": _identify_row(landuse="building", classification="parcel"),
            "attr": None,
            "ovt_overlay": {"res_share": 0.5, "com_share": 0.1},
            "osm_overlay": {"res_share": 0.45, "com_share": 0.15},
            "columns": [],
        }
    )
    result = geo_portal._identify_postgis(46.675, 24.713, 25.0, db)
    parcel = result["parcel"]
    assert parcel["landuse_code"] == "s"
    assert parcel["landuse_method"] == "overture_overlay"


def test_identify_sql_translates_suhail_geometry():
    geo_portal = _reload_geo_portal("parcels")
    sql = str(
        geo_portal._build_identify_sql(
            geo_portal._PARCEL_TABLE, geo_portal._PARCEL_GEOM_COLUMN, tuple()
        )
    )
    assert "WHEN source = 'suhail'" in sql
    assert "ST_Translate(ST_Transform(geom, 3857), :suhail_dx_m, :suhail_dy_m)" in sql


def test_identify_default_offsets_zero():
    geo_portal = _reload_geo_portal("parcels")
    db = _DummyDB(
        {
            "identify": _identify_row(),
            "attr": None,
            "ovt_overlay": {"res_share": 0.1, "com_share": 0.1},
            "osm_overlay": {"res_share": 0.1, "com_share": 0.1},
            "columns": [],
        }
    )
    geo_portal._identify_postgis(46.675, 24.713, 25.0, db)
    params_with_offsets = None
    for _, params in db.calls:
        if "suhail_dx_m" in params:
            params_with_offsets = params
            break
    assert params_with_offsets is not None
    assert params_with_offsets["suhail_dx_m"] == 0.0
    assert params_with_offsets["suhail_dy_m"] == 0.0


def test_identify_postgis_suhail_includes_optional_metadata():
    geo_portal = _reload_geo_portal("suhail_parcels_proxy")
    db = _DummyDB(
        {
            "identify": {
                **_identify_row(landuse="residential", classification="parcel"),
                "zoning_id": "Z-1",
                "municipality_name": "Test City",
            },
            "attr": None,
            "ovt_overlay": {"res_share": 0.0, "com_share": 0.0},
            "osm_overlay": {"res_share": 0.0, "com_share": 0.0},
            "columns": ["zoning_id", "municipality_name"],
        }
    )
    result = geo_portal._identify_postgis(46.675, 24.713, 25.0, db)
    parcel_meta = result["parcel"]["parcel_meta"]
    assert parcel_meta["zoning_id"] == "Z-1"
    assert parcel_meta["municipality_name"] == "Test City"
    assert parcel_meta["street_name"] is None


def test_identify_postgis_osm_table_returns_null_optional_metadata():
    geo_portal = _reload_geo_portal("osm_parcels_proxy")
    db = _DummyDB(
        {
            "identify": _identify_row(landuse="building", classification="parcel"),
            "attr": None,
            "ovt_overlay": {"res_share": 0.0, "com_share": 0.0},
            "osm_overlay": {"res_share": 0.0, "com_share": 0.0},
            "columns": [],
        }
    )
    result = geo_portal._identify_postgis(46.675, 24.713, 25.0, db)
    parcel_meta = result["parcel"]["parcel_meta"]
    assert parcel_meta["zoning_id"] is None
    assert parcel_meta["municipality_name"] is None
