import json

from app.api.geo_portal import _identify_postgis


class _DummyResult:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row


class _DummyDB:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.calls.append(sql)
        if "WITH q AS" in sql:
            if "suhail_parcels_proxy" in sql:
                return _DummyResult(self.responses.get("identify_suhail") or self.responses.get("identify"))
            if "osm_parcels_proxy" in sql:
                return _DummyResult(self.responses.get("identify_osm") or self.responses.get("identify"))
            return _DummyResult(self.responses.get("identify"))
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


def _identify_row(landuse=None, classification="overture_building", source="osm"):
    return {
        "id": "ovt:build2",
        "source": source,
        "landuse": landuse,
        "classification": classification,
        "area_m2": 120,
        "perimeter_m": 45,
        "geom": _geom_json(),
        "distance_m": 0,
        "hits": 1,
        "near": 1,
        "is_ovt": 1 if classification == "overture_building" else 0,
    }


def test_identify_postgis_overture_attr_sets_landuse():
    db = _DummyDB(
        {
            "identify": _identify_row(),
            "attr": {"subtype": "residential", "class": None},
            "ovt_overlay": {"res_share": 0.1, "com_share": 0.1},
            "osm_overlay": {"res_share": 0.1, "com_share": 0.1},
        }
    )
    result = _identify_postgis(46.675, 24.713, 25.0, db)
    parcel = result["parcel"]
    assert parcel["landuse_code"] == "s"
    assert parcel["landuse_method"] == "overture_building_attr"
    assert parcel["landuse_raw"] == "residential"


def test_identify_postgis_overture_overlay_wins_when_osm_weak():
    db = _DummyDB(
        {
            "identify": _identify_row(landuse="building", classification="parcel"),
            "attr": None,
            "ovt_overlay": {"res_share": 0.1, "com_share": 0.7},
            "osm_overlay": {"res_share": 0.2, "com_share": 0.1},
        }
    )
    result = _identify_postgis(46.675, 24.713, 25.0, db)
    parcel = result["parcel"]
    assert parcel["landuse_code"] == "m"
    assert parcel["landuse_method"] == "overture_overlay"
    assert parcel["residential_share"] == 0.1
    assert parcel["commercial_share"] == 0.7


def test_identify_postgis_osm_overlay_wins_when_strong():
    db = _DummyDB(
        {
            "identify": _identify_row(landuse="building", classification="parcel"),
            "attr": None,
            "ovt_overlay": {"res_share": 0.2, "com_share": 0.4},
            "osm_overlay": {"res_share": 0.75, "com_share": 0.1},
        }
    )
    result = _identify_postgis(46.675, 24.713, 25.0, db)
    parcel = result["parcel"]
    assert parcel["landuse_code"] == "s"
    assert parcel["landuse_method"] == "osm_overlay"
    assert parcel["residential_share"] == 0.75
    assert parcel["commercial_share"] == 0.1


def test_identify_postgis_overture_overlay_wins_when_osm_not_strong():
    db = _DummyDB(
        {
            "identify": _identify_row(landuse="building", classification="parcel"),
            "attr": None,
            "ovt_overlay": {"res_share": 0.5, "com_share": 0.1},
            "osm_overlay": {"res_share": 0.45, "com_share": 0.15},
        }
    )
    result = _identify_postgis(46.675, 24.713, 25.0, db)
    parcel = result["parcel"]
    assert parcel["landuse_code"] == "s"
    assert parcel["landuse_method"] == "overture_overlay"


def test_identify_prefers_suhail_label_when_present():
    db = _DummyDB(
        {
            "identify_suhail": _identify_row(landuse="residential", classification="parcel", source="suhail"),
            "attr": None,
            "ovt_overlay": {"res_share": 0.1, "com_share": 0.1},
            "osm_overlay": {"res_share": 0.8, "com_share": 0.05},
        }
    )
    result = _identify_postgis(46.675, 24.713, 25.0, db, source="suhail")
    parcel = result["parcel"]
    assert parcel["landuse_code"] == "s"
    assert parcel["landuse_method"] == "suhail_parcel_label"
    assert parcel["source"] == "suhail"


def test_identify_suhail_falls_back_to_osm_overlay_when_label_missing():
    db = _DummyDB(
        {
            "identify_suhail": _identify_row(landuse=None, classification="parcel", source="suhail"),
            "attr": None,
            "ovt_overlay": {"res_share": 0.0, "com_share": 0.0},
            "osm_overlay": {"res_share": 0.7, "com_share": 0.1},
        }
    )
    result = _identify_postgis(46.675, 24.713, 25.0, db, source="suhail")
    parcel = result["parcel"]
    assert parcel["landuse_code"] == "s"
    assert parcel["landuse_method"] == "osm_overlay"
