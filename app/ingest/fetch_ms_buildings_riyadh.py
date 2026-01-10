from __future__ import annotations

import argparse
import gzip
import json
import math
import tempfile
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import httpx
import planetary_computer as pc
import pyarrow.parquet as pq
from pyarrow import fs
from shapely import wkb
from shapely.geometry import mapping

STAC_API_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"
COLLECTION_ID = "ms-buildings"
DEFAULT_BBOX = (46.5, 24.6, 46.9, 24.9)
DEFAULT_MAX_ITEMS = 2
DEFAULT_MAX_FEATURES = 50000
DEFAULT_OUTPUT_DIR = Path("data/ms_buildings")
DEFAULT_BATCH_SIZE = 2000


@dataclass
class DownloadStats:
    features: int = 0
    min_lon: float | None = None
    min_lat: float | None = None
    max_lon: float | None = None
    max_lat: float | None = None

    def update_bounds(self, bounds: tuple[float, float, float, float]) -> None:
        minx, miny, maxx, maxy = bounds
        if self.min_lon is None:
            self.min_lon = minx
            self.min_lat = miny
            self.max_lon = maxx
            self.max_lat = maxy
            return
        self.min_lon = min(self.min_lon, minx)
        self.min_lat = min(self.min_lat, miny)
        self.max_lon = max(self.max_lon, maxx)
        self.max_lat = max(self.max_lat, maxy)


def _search_items(client: httpx.Client, bbox: tuple[float, float, float, float], limit: int) -> list[dict]:
    response = client.post(
        f"{STAC_API_URL}/search",
        json={"collections": [COLLECTION_ID], "bbox": list(bbox), "limit": limit},
    )
    response.raise_for_status()
    items = response.json().get("features", [])
    items.sort(key=lambda item: item.get("id", ""))
    return items


def _parse_signed_href(signed_href: str) -> tuple[str, str, str, str]:
    parsed = urllib.parse.urlparse(signed_href)
    path_parts = parsed.path.lstrip("/").split("/", 1)
    if len(path_parts) < 2:
        raise ValueError(f"Unexpected signed href path: {parsed.path}")
    container, prefix = path_parts
    base = f"{parsed.scheme}://{parsed.netloc}/{container}"
    return container, prefix, base, parsed.query


def _list_parquet_parts(signed_href: str) -> list[str]:
    if signed_href.endswith(".parquet"):
        return [signed_href]
    container, prefix, base, query = _parse_signed_href(signed_href)
    account_name = urllib.parse.urlparse(base).netloc.split(".")[0]
    filesystem = fs.AzureBlobFileSystem(account_name=account_name, sas_token=query)
    selector = fs.FileSelector(f"{container}/{prefix}", recursive=True)
    file_infos = filesystem.get_file_info(selector)
    parts: list[str] = []
    for info in file_infos:
        if info.type == fs.FileType.File and info.path.endswith(".parquet"):
            parts.append(info.path)
    parts.sort()
    urls: list[str] = []
    for part in parts:
        relative = part.split("/", 1)[1] if part.startswith(f"{container}/") else part
        urls.append(f"{base}/{relative}?{query}")
    return urls


def _iter_parquet_batches(file_path: Path, batch_size: int) -> Iterable[tuple[list[bytes], list[float | None]]]:
    parquet_file = pq.ParquetFile(file_path)
    columns = parquet_file.schema.names
    if "geometry" not in columns:
        raise ValueError("Parquet file missing geometry column")
    height_column = "meanHeight" if "meanHeight" in columns else None
    read_columns = ["geometry"] + ([height_column] if height_column else [])
    for batch in parquet_file.iter_batches(batch_size=batch_size, columns=read_columns):
        geometry_values = batch.column(0)
        height_values = batch.column(1) if height_column else None
        geoms: list[bytes] = []
        heights: list[float | None] = []
        for idx in range(batch.num_rows):
            geom_value = geometry_values[idx].as_py()
            if geom_value is None:
                continue
            geoms.append(geom_value)
            if height_values is None:
                heights.append(None)
                continue
            height_value = height_values[idx].as_py()
            heights.append(height_value if height_value is None else float(height_value))
        yield geoms, heights


def _download_parquet(client: httpx.Client, url: str) -> Path:
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    with client.stream("GET", url, timeout=120) as response:
        response.raise_for_status()
        for chunk in response.iter_bytes():
            tmp.write(chunk)
    tmp.close()
    return Path(tmp.name)


def _sanitize_filename(value: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value)


def _write_features(
    parquet_urls: list[str],
    client: httpx.Client,
    output_path: Path,
    max_features: int,
    batch_size: int,
) -> DownloadStats:
    stats = DownloadStats()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(suffix=output_path.suffix, delete=False) as tmp_out:
        with gzip.open(tmp_out.name, "wt", encoding="utf-8") as handle:
            for parquet_url in parquet_urls:
                parquet_path = _download_parquet(client, parquet_url)
                try:
                    for geoms, heights in _iter_parquet_batches(parquet_path, batch_size):
                        for geom_bytes, height in zip(geoms, heights, strict=True):
                            geom = wkb.loads(geom_bytes)
                            if geom.is_empty:
                                continue
                            stats.features += 1
                            stats.update_bounds(geom.bounds)
                            feature = {
                                "type": "Feature",
                                "geometry": mapping(geom),
                                "properties": {},
                            }
                            if height is not None and not math.isnan(height):
                                feature["properties"]["meanHeight"] = height
                            handle.write(json.dumps(feature, separators=(",", ":")) + "\n")
                            if stats.features >= max_features:
                                break
                        if stats.features >= max_features:
                            break
                finally:
                    parquet_path.unlink(missing_ok=True)
                if stats.features >= max_features:
                    break
    Path(tmp_out.name).replace(output_path)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Microsoft GlobalML Building Footprints for Riyadh via the Planetary Computer STAC API."
    )
    parser.add_argument("--max-items", type=int, default=DEFAULT_MAX_ITEMS)
    parser.add_argument("--max-features", type=int, default=DEFAULT_MAX_FEATURES)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--bbox",
        type=float,
        nargs=4,
        metavar=("MIN_LON", "MIN_LAT", "MAX_LON", "MAX_LAT"),
        default=DEFAULT_BBOX,
    )
    args = parser.parse_args()

    bbox = tuple(args.bbox)
    print(f"Search bbox: {bbox}")
    with httpx.Client(timeout=60) as client:
        items = _search_items(client, bbox, args.max_items)
        if not items:
            raise SystemExit("No STAC items returned for Riyadh bbox.")
        print(f"Collection: {COLLECTION_ID}")
        print(f"Item ids: {[item.get('id') for item in items]}")
        first_assets = items[0].get("assets", {})
        asset_keys = list(first_assets.keys())
        print(f"Available asset keys (first item): {asset_keys}")
        if "data" in first_assets:
            asset_key = "data"
        else:
            asset_key = None
            for key, asset in first_assets.items():
                href = asset.get("href", "")
                if href.endswith(".parquet") or "parquet" in href:
                    asset_key = key
                    break
            if asset_key is None and asset_keys:
                asset_key = asset_keys[0]
        if asset_key is None:
            raise SystemExit("No assets available on first STAC item.")

        total_stats = DownloadStats()
        remaining = args.max_features
        for index, item in enumerate(items, start=1):
            signed_item = pc.sign(item)
            asset = signed_item.get("assets", {}).get(asset_key)
            if not asset or "href" not in asset:
                continue
            signed_href = asset["href"]
            print(f"Signed href: {signed_href}")
            parquet_urls = _list_parquet_parts(signed_href)
            if not parquet_urls:
                continue
            output_name = f"riyadh_{_sanitize_filename(item.get('id', f'item_{index}'))}.csv.gz"
            output_path = args.output_dir / output_name
            stats = _write_features(
                parquet_urls,
                client,
                output_path,
                max_features=remaining,
                batch_size=DEFAULT_BATCH_SIZE,
            )
            total_stats.features += stats.features
            if stats.min_lon is not None:
                total_stats.update_bounds((stats.min_lon, stats.min_lat, stats.max_lon, stats.max_lat))
            remaining = args.max_features - total_stats.features
            if remaining <= 0:
                break

    if total_stats.features == 0:
        raise SystemExit("No features downloaded from Planetary Computer.")

    bbox_str = (
        f"min_lon={total_stats.min_lon:.4f}, min_lat={total_stats.min_lat:.4f}, "
        f"max_lon={total_stats.max_lon:.4f}, max_lat={total_stats.max_lat:.4f}"
    )
    print(f"Downloaded {total_stats.features} features.")
    print(f"Downloaded geometry bbox: {bbox_str}")


if __name__ == "__main__":
    main()
