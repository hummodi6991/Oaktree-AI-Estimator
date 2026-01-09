from app.api.geo_portal import _debug_layers


class _DummyResult:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def one(self):
        return self._row


class _DummyDB:
    def __init__(self, row):
        self.row = row
        self.calls = []

    def execute(self, statement):
        self.calls.append(str(statement))
        return _DummyResult(self.row)


def _assert_wgs84_bbox(bbox):
    assert bbox is not None
    xmin, ymin, xmax, ymax = bbox
    assert -180 <= xmin < xmax <= 180
    assert -90 <= ymin < ymax <= 90


def _assert_mercator_bbox(bbox):
    assert bbox is not None
    xmin, ymin, xmax, ymax = bbox
    limit = 20037508.34
    assert -limit <= xmin < xmax <= limit
    assert -limit <= ymin < ymax <= limit


def _assert_utm_bbox(bbox):
    assert bbox is not None
    xmin, ymin, xmax, ymax = bbox
    assert 100000 <= xmin < xmax <= 900000
    assert 0 <= ymin < ymax <= 10000000


def test_debug_layers_parses_srids_and_bbox() -> None:
    row = {
        "suhail_geom_srid": 4326,
        "suhail_geom_bbox": "BOX(46.5 24.6,46.9 24.9)",
        "suhail_geom_32638_srid": 32638,
        "suhail_geom_32638_bbox": "BOX(500000 2700000,510000 2710000)",
        "osm_geom_4326_srid": 4326,
        "osm_geom_4326_bbox": "BOX(46.0 24.0,47.0 25.0)",
        "osm_geom_3857_srid": 3857,
        "osm_geom_3857_bbox": "BOX(5180000 2780000,5230000 2830000)",
        "overture_geom_srid": 32638,
        "overture_geom_bbox": "BOX(500000 2700000,510000 2710000)",
    }
    db = _DummyDB(row)

    result = _debug_layers(db)

    assert result.suhail_geom.srid == 4326
    assert result.suhail_geom_32638.srid == 32638
    assert result.osm_geom_4326.srid == 4326
    assert result.osm_geom_3857.srid == 3857
    assert result.overture_geom.srid == 32638

    _assert_wgs84_bbox(result.suhail_geom.bbox)
    _assert_utm_bbox(result.suhail_geom_32638.bbox)
    _assert_wgs84_bbox(result.osm_geom_4326.bbox)
    _assert_mercator_bbox(result.osm_geom_3857.bbox)
    _assert_utm_bbox(result.overture_geom.bbox)
