from app.ingest.fetch_ms_buildings_dataset_links import (
    latlon_to_quadkey,
    parse_dataset_links,
    select_dataset_rows,
)


def test_latlon_to_quadkey_zoom12() -> None:
    assert latlon_to_quadkey(24.7136, 46.6753, 12) == "123022032213"


def test_select_dataset_rows_by_prefix() -> None:
    csv_text = """Location,QuadKey,Url,Size,UploadDate
KingdomofSaudiArabia,01234999,https://example.com/a.csv.gz,10,2024-01-01
saudiarabia-test,01234000,https://example.com/b.csv.gz,11,2024-01-02
Canada,01234999,https://example.com/c.csv.gz,12,2024-01-03
saudiarabia-test,77770000,https://example.com/d.csv.gz,13,2024-01-04
"""
    rows = parse_dataset_links(csv_text)
    selected = select_dataset_rows(rows, {"01234"})
    urls = [row["Url"] for row in selected]

    assert urls == ["https://example.com/a.csv.gz", "https://example.com/b.csv.gz"]
