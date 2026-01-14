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
            return _DummyResult(self.responses.get("identify"))
        if "FROM public.suhail_parcels_mat" in sql:
            return _DummyResult(self.responses.get("suhail"))
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


def _identify_row(landuse=None, classification="parcel"):
    return {
        "id": "ovt:build2",
        "landuse": landuse,
        "classification": classification,
        "area_m2": 120,
        "perimeter_m": 45,
        "geom": _geom_json(),
        "distance_m": 0,
        "hits": 1,
        "near": 1,
        "is_ovt": 1,
    }


def test_identify_postgis_prefers_parcel_label_when_signal():
    db = _DummyDB(
        {
            "identify": _identify_row(landuse="residential", classification="parcel"),
            "osm_overlay": {"res_share": 0.2, "com_share": 0.2},
        }
    )
    result = _identify_postgis(46.675, 24.713, 25.0, db)
    parcel = result["parcel"]
    assert parcel["landuse_code"] == "s"
    assert parcel["landuse_method"] == "parcel_label"
    assert all("suhail_parcels_mat" not in sql for sql in db.calls)
    assert all("planet_osm_polygon" not in sql for sql in db.calls)


def test_identify_postgis_uses_suhail_when_label_weak():
    db = _DummyDB(
        {
            "identify": _identify_row(landuse="building", classification="parcel"),
            "suhail": {"landuse": "سكني", "classification": None},
        }
    )
    result = _identify_postgis(46.675, 24.713, 25.0, db)
    parcel = result["parcel"]
    assert parcel["landuse_code"] == "s"
    assert parcel["landuse_method"] == "suhail_overlay"
    assert parcel["landuse_raw"] == "سكني"


def test_identify_postgis_falls_back_to_osm_when_labels_weak():
    db = _DummyDB(
        {
            "identify": _identify_row(landuse="building", classification="parcel"),
            "suhail": None,
            "osm_overlay": {"res_share": 0.75, "com_share": 0.1},
        }
    )
    result = _identify_postgis(46.675, 24.713, 25.0, db)
    parcel = result["parcel"]
    assert parcel["landuse_code"] == "s"
    assert parcel["landuse_method"] == "osm_overlay"
    assert parcel["residential_share"] == 0.75
    assert parcel["commercial_share"] == 0.1
