from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import httpx

DATASET_LINKS_URL = (
    "https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv"
)
DEFAULT_BBOX = (46.2, 24.2, 47.3, 25.1)
DEFAULT_MAX_FILES = 3
DEFAULT_OUTPUT_DIR = Path("data/ms_buildings")
DEFAULT_LOCATION_FILTER = "KingdomofSaudiArabia"
GZIP_MAGIC = b"\x1f\x8b"


@dataclass
class SelectionSummary:
    candidates: int
    downloaded: list[Path]
    total_bytes: int
    filtered_by_location: int
    filtered_by_bbox: int


def _clip(value: float, min_value: float, max_value: float) -> float:
    return min(max(value, min_value), max_value)


def latlon_to_quadkey(lat: float, lon: float, zoom: int) -> str:
    clipped_lat = _clip(lat, -85.05112878, 85.05112878)
    clipped_lon = _clip(lon, -180.0, 180.0)
    sin_lat = math.sin(math.radians(clipped_lat))
    x = (clipped_lon + 180.0) / 360.0
    y = 0.5 - math.log((1 + sin_lat) / (1 - sin_lat)) / (4 * math.pi)

    map_size = 1 << zoom
    tile_x = int(_clip(x * map_size, 0, map_size - 1))
    tile_y = int(_clip(y * map_size, 0, map_size - 1))

    quadkey_digits = []
    for i in range(zoom, 0, -1):
        digit = 0
        mask = 1 << (i - 1)
        if tile_x & mask:
            digit += 1
        if tile_y & mask:
            digit += 2
        quadkey_digits.append(str(digit))
    return "".join(quadkey_digits)


def quadkey_to_tile(quadkey: str) -> tuple[int, int, int]:
    tile_x = 0
    tile_y = 0
    zoom = len(quadkey)
    for index, digit in enumerate(quadkey):
        if digit not in {"0", "1", "2", "3"}:
            raise ValueError(f"Invalid quadkey digit: {digit}")
        mask = 1 << (zoom - index - 1)
        value = int(digit)
        if value & 1:
            tile_x |= mask
        if value & 2:
            tile_y |= mask
    return tile_x, tile_y, zoom


def tile_to_bbox(tile_x: int, tile_y: int, zoom: int) -> tuple[float, float, float, float]:
    n = 2**zoom
    lon_left = tile_x / n * 360.0 - 180.0
    lon_right = (tile_x + 1) / n * 360.0 - 180.0
    lat_top = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * tile_y / n))))
    lat_bottom = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (tile_y + 1) / n))))
    return lon_left, lat_bottom, lon_right, lat_top


def bboxes_intersect(
    bbox_a: tuple[float, float, float, float],
    bbox_b: tuple[float, float, float, float],
) -> bool:
    min_lon_a, min_lat_a, max_lon_a, max_lat_a = bbox_a
    min_lon_b, min_lat_b, max_lon_b, max_lat_b = bbox_b
    return not (
        max_lon_a < min_lon_b
        or min_lon_a > max_lon_b
        or max_lat_a < min_lat_b
        or min_lat_a > max_lat_b
    )


def _location_matches(location: str | None, location_filter: str | None) -> bool:
    if not location:
        return False
    if location == "KingdomofSaudiArabia":
        return True
    location_lower = location.lower()
    if "saudiarabia" in location_lower:
        return True
    if location_filter:
        return location_filter.lower() in location_lower
    return False


def parse_dataset_links(csv_content: str) -> list[dict[str, str]]:
    reader = csv.DictReader(csv_content.splitlines())
    return [row for row in reader if row]


def load_dataset_links(source: str | None) -> list[dict[str, str]]:
    if source is None or source.startswith("http://") or source.startswith("https://"):
        url = source or DATASET_LINKS_URL
        response = httpx.get(url, timeout=60)
        response.raise_for_status()
        return parse_dataset_links(response.text)

    path = Path(source)
    return parse_dataset_links(path.read_text(encoding="utf-8"))


def select_dataset_rows(
    rows: Iterable[dict[str, str]],
    bbox: tuple[float, float, float, float],
    location_filter: str | None,
) -> tuple[list[dict[str, str]], int, int]:
    selected = []
    filtered_by_location = 0
    filtered_by_bbox = 0
    for row in rows:
        if not _location_matches(row.get("Location"), location_filter):
            filtered_by_location += 1
            continue
        quadkey = row.get("QuadKey") or ""
        if not quadkey:
            filtered_by_bbox += 1
            continue
        try:
            tile_x, tile_y, zoom = quadkey_to_tile(str(quadkey))
        except ValueError:
            filtered_by_bbox += 1
            continue
        tile_bbox = tile_to_bbox(tile_x, tile_y, zoom)
        if bboxes_intersect(tile_bbox, bbox):
            selected.append(row)
        else:
            filtered_by_bbox += 1
    return selected, filtered_by_location, filtered_by_bbox


def _ensure_gzip(path: Path) -> bool:
    with path.open("rb") as handle:
        magic = handle.read(2)
    return magic == GZIP_MAGIC


def _filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    return Path(parsed.path).name


def _download_file(
    client: httpx.Client,
    url: str,
    output_dir: Path,
) -> tuple[Path, int] | None:
    filename = _filename_from_url(url)
    if not filename:
        print(f"Skipping URL with no filename: {url}")
        return None
    destination = output_dir / filename
    if destination.exists():
        print(f"Skipping existing file: {destination}")
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    total_bytes = 0
    with client.stream("GET", url, timeout=120) as response:
        response.raise_for_status()
        with destination.open("wb") as handle:
            for chunk in response.iter_bytes():
                handle.write(chunk)
                total_bytes += len(chunk)

    if not _ensure_gzip(destination):
        destination.unlink(missing_ok=True)
        print(f"Warning: {filename} was not gzip data and was deleted.")
        return None

    return destination, total_bytes


def fetch_dataset_links(
    dataset_links: str | None,
    bbox: tuple[float, float, float, float],
    max_files: int,
    output_dir: Path,
    location_filter: str | None = DEFAULT_LOCATION_FILTER,
) -> SelectionSummary:
    rows = load_dataset_links(dataset_links)
    selected, filtered_by_location, filtered_by_bbox = select_dataset_rows(
        rows,
        bbox,
        location_filter,
    )
    print(f"Selected {len(selected)} candidate rows.")
    print(f"Filtered out by location: {filtered_by_location}")
    print(f"Filtered out by bbox: {filtered_by_bbox}")
    downloaded: list[Path] = []
    total_bytes = 0

    with httpx.Client() as client:
        for row in selected:
            if max_files and len(downloaded) >= max_files:
                break
            url = row.get("Url")
            if not url:
                continue
            result = _download_file(client, url, output_dir)
            if result is not None:
                path, size = result
                downloaded.append(path)
                total_bytes += size

    print(f"Total bytes downloaded: {total_bytes}")
    return SelectionSummary(
        candidates=len(selected),
        downloaded=downloaded,
        total_bytes=total_bytes,
        filtered_by_location=filtered_by_location,
        filtered_by_bbox=filtered_by_bbox,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Microsoft GlobalML building footprints using dataset-links.csv."
    )
    parser.add_argument(
        "--bbox",
        nargs=4,
        type=float,
        metavar=("MIN_LON", "MIN_LAT", "MAX_LON", "MAX_LAT"),
        default=list(DEFAULT_BBOX),
        help="Bounding box for selecting quadkeys.",
    )
    parser.add_argument("--max-files", type=int, default=DEFAULT_MAX_FILES)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--dataset-links", default=None)
    parser.add_argument("--location-filter", default=DEFAULT_LOCATION_FILTER)
    args = parser.parse_args()

    bbox = tuple(args.bbox)
    summary = fetch_dataset_links(
        args.dataset_links,
        bbox,
        args.max_files,
        args.output_dir,
        location_filter=args.location_filter,
    )

    print(
        "Summary: "
        f"candidates={summary.candidates} "
        f"downloaded={len(summary.downloaded)} "
        f"filtered_location={summary.filtered_by_location} "
        f"filtered_bbox={summary.filtered_by_bbox}"
    )
    if summary.downloaded:
        print("Downloaded files:")
        for path in summary.downloaded:
            print(f"- {path.name}")


if __name__ == "__main__":
    main()
