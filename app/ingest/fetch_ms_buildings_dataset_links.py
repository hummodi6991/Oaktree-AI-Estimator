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
DEFAULT_QUADKEY_ZOOM = 12
DEFAULT_PREFIX_LENGTH = 9
DEFAULT_GRID_N = 7
GZIP_MAGIC = b"\x1f\x8b"


@dataclass
class SelectionSummary:
    candidates: int
    downloaded: list[Path]
    total_bytes: int


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


def quadkey_prefixes_for_bbox(
    bbox: tuple[float, float, float, float],
    zoom: int,
    prefix_length: int,
    grid_n: int,
) -> set[str]:
    min_lon, min_lat, max_lon, max_lat = bbox
    if grid_n < 1:
        raise ValueError("grid_n must be >= 1")
    if grid_n == 1:
        lats = [(min_lat + max_lat) / 2]
        lons = [(min_lon + max_lon) / 2]
    else:
        lat_step = (max_lat - min_lat) / (grid_n - 1)
        lon_step = (max_lon - min_lon) / (grid_n - 1)
        lats = [min_lat + lat_step * i for i in range(grid_n)]
        lons = [min_lon + lon_step * i for i in range(grid_n)]
    prefixes = set()
    for lat in lats:
        for lon in lons:
            quadkey = latlon_to_quadkey(lat, lon, zoom)
            prefixes.add(quadkey[:prefix_length])
    return prefixes


def _location_matches(location: str | None) -> bool:
    if not location:
        return False
    if location == "KingdomofSaudiArabia":
        return True
    return "saudiarabia" in location.lower()


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
    quadkey_prefixes: set[str],
) -> list[dict[str, str]]:
    selected = []
    for row in rows:
        if not _location_matches(row.get("Location")):
            continue
        quadkey = row.get("QuadKey") or ""
        if not quadkey:
            continue
        if any(quadkey.startswith(prefix) for prefix in quadkey_prefixes):
            selected.append(row)
    return selected


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
    zoom: int = DEFAULT_QUADKEY_ZOOM,
    prefix_length: int = DEFAULT_PREFIX_LENGTH,
    grid_n: int = DEFAULT_GRID_N,
) -> SelectionSummary:
    rows = load_dataset_links(dataset_links)
    prefixes = quadkey_prefixes_for_bbox(bbox, zoom, prefix_length, grid_n)
    sorted_prefixes = sorted(prefixes)
    print(f"Generated {len(prefixes)} quadkey prefixes.")
    print(f"First 10 prefixes: {sorted_prefixes[:10]}")
    selected = select_dataset_rows(rows, prefixes)
    print(f"Selected {len(selected)} candidate rows.")
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
    return SelectionSummary(candidates=len(selected), downloaded=downloaded, total_bytes=total_bytes)


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
    parser.add_argument("--prefix-length", type=int, default=DEFAULT_PREFIX_LENGTH)
    parser.add_argument("--zoom", type=int, default=DEFAULT_QUADKEY_ZOOM)
    parser.add_argument("--grid-n", type=int, default=DEFAULT_GRID_N)
    args = parser.parse_args()

    bbox = tuple(args.bbox)
    summary = fetch_dataset_links(
        args.dataset_links,
        bbox,
        args.max_files,
        args.output_dir,
        zoom=args.zoom,
        prefix_length=args.prefix_length,
        grid_n=args.grid_n,
    )

    print(f"Summary: candidates={summary.candidates} downloaded={len(summary.downloaded)}")
    if summary.downloaded:
        print("Downloaded files:")
        for path in summary.downloaded:
            print(f"- {path.name}")


if __name__ == "__main__":
    main()
