from app.ingest.fetch_ms_buildings_dataset_links import (
    parse_dataset_links,
    quadkey_to_tile,
    select_dataset_rows,
    tile_to_bbox,
)


def test_quadkey_to_tile_roundtrip() -> None:
    tile_x, tile_y, zoom = quadkey_to_tile("123")
    assert (tile_x, tile_y, zoom) == (5, 3, 3)

    bbox = tile_to_bbox(tile_x, tile_y, zoom)
    assert bbox[0] < bbox[2]
    assert bbox[1] < bbox[3]


def test_select_dataset_rows_by_bbox() -> None:
    csv_text = """Location,QuadKey,Url,Size,UploadDate
KingdomofSaudiArabia,0,https://example.com/a.csv.gz,10,2024-01-01
saudiarabia-test,1,https://example.com/b.csv.gz,11,2024-01-02
Canada,0,https://example.com/c.csv.gz,12,2024-01-03
"""
    rows = parse_dataset_links(csv_text)
    min_lon, min_lat, max_lon, max_lat = tile_to_bbox(0, 0, 1)
    bbox = (min_lon + 0.1, min_lat + 0.1, max_lon - 0.1, max_lat - 0.1)
    selected, filtered_by_location, filtered_by_bbox = select_dataset_rows(
        rows,
        bbox,
        location_filter="KingdomofSaudiArabia",
    )
    urls = [row["Url"] for row in selected]

    assert urls == ["https://example.com/a.csv.gz"]
    assert filtered_by_location == 1
    assert filtered_by_bbox == 1
