from app.api.search import (
    SearchItem,
    _merge_round_robin,
    normalize_search_text,
    parse_coords,
    parse_parcel_tokens,
)


def test_normalize_search_text_arabic_variants():
    assert normalize_search_text("الـعـلـيـا") == "العليا"
    assert normalize_search_text("أبو ١٢٣") == "ابو 123"
    assert normalize_search_text("حي، العليا") == "حي العليا"


def test_parse_coords_from_lat_lon():
    assert parse_coords("24.7136,46.6753") == (24.7136, 46.6753)


def test_parse_coords_from_google_maps():
    assert parse_coords("https://www.google.com/maps/@24.7136,46.6753,14z") == (24.7136, 46.6753)


def test_parse_parcel_tokens_with_keywords():
    assert parse_parcel_tokens("مخطط 123 بلوك 4 قطعة 56") == ("123", "4", "56")


def test_parse_parcel_tokens_with_pattern():
    assert parse_parcel_tokens("123-4-56") == ("123", "4", "56")


def test_merge_round_robin_includes_parcel_when_available():
    parcel_row = {
        "type": "parcel",
        "id": "suhail:1",
        "label": "Parcel 1",
        "subtitle": None,
        "lng": 46.0,
        "lat": 24.0,
    }
    road_row = {
        "type": "road",
        "id": "osm_line:1",
        "label": "Main Rd",
        "subtitle": "road",
        "lng": 46.1,
        "lat": 24.1,
    }
    poi_row = {
        "type": "poi",
        "id": "osm_point:1",
        "label": "Cafe",
        "subtitle": "cafe",
        "lng": 46.2,
        "lat": 24.2,
    }
    scored_rows = {
        "road": [(0.9, road_row)],
        "poi": [(0.8, poi_row)],
        "parcel": [(0.1, parcel_row)],
    }

    items = _merge_round_robin(scored_rows, limit=2, type_order=["parcel", "district", "road", "poi"])

    assert any(isinstance(item, SearchItem) for item in items)
    assert any(item.type == "parcel" for item in items)
