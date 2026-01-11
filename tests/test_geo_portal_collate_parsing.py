from app.api.geo_portal import _build_ms_building_parcel_payload, _parse_ms_building_parcel_id


def test_parse_ms_building_parcel_id() -> None:
    assert _parse_ms_building_parcel_id("197904:1") == (197904, 1)
    assert _parse_ms_building_parcel_id("197904:0") is None
    assert _parse_ms_building_parcel_id("197904") is None
    assert _parse_ms_building_parcel_id("abc:1") is None


def test_build_ms_building_parcel_payload_filters_invalid() -> None:
    payload = _build_ms_building_parcel_payload(["197904:1", "bad", "22:2"])
    assert payload == [
        {"parcel_id": "197904:1", "building_id": 197904, "part_index": 1},
        {"parcel_id": "22:2", "building_id": 22, "part_index": 2},
    ]
