from __future__ import annotations

import argparse
import gzip
import json
import math
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import httpx
import planetary_computer as pc
import pystac_client
from planetary_computer import sign_url
import pyarrow.parquet as pq
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


def _signed_https_href(href: str) -> str:
    if href.startswith("https://"):
        return sign_url(href)
    raise ValueError(f"Unsupported asset href scheme: {href}")


def _asset_alternates(asset: object) -> dict:
    extra_fields = getattr(asset, "extra_fields", {})
    alternates = extra_fields.get("alternate", {})
    return alternates if isinstance(alternates, dict) else {}


def _select_https_href(signed_item: object, asset_key: str) -> str | None:
    asset = getattr(signed_item, "assets", {}).get(asset_key)
    if asset is None:
        return None
    primary_href = getattr(asset, "href", "") or ""
    if primary_href.startswith("https://"):
        return primary_href
    alternates = _asset_alternates(asset)
    download_href = alternates.get("download", {}).get("href", "")
    if download_href.startswith("https://"):
        return download_href
    for alt in alternates.values():
        alt_href = alt.get("href", "")
        if alt_href.startswith("https://"):
            return alt_href
    for link in getattr(signed_item, "links", []):
        href = getattr(link, "href", "") or ""
        if href.startswith("https://") and (href.endswith(".parquet") or "parquet" in href):
            return href
    return None


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
    try:
        with client.stream("GET", url, timeout=120) as response:
            response.raise_for_status()
            validated = False
            buffer = bytearray()
            for chunk in response.iter_bytes():
                if not validated:
                    buffer.extend(chunk)
                    if len(buffer) < 4:
                        continue
                    if buffer[:4] != b"PAR1":
                        raise SystemExit(
                            "Fetch returned non-parquet data (likely HTML error page). Aborting."
                        )
                    tmp.write(buffer)
                    validated = True
                else:
                    tmp.write(chunk)
            if not validated:
                raise SystemExit(
                    "Fetch returned non-parquet data (likely HTML error page). Aborting."
                )
        tmp.close()
        return Path(tmp.name)
    except BaseException:
        tmp.close()
        Path(tmp.name).unlink(missing_ok=True)
        raise


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
        try:
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
        except BaseException:
            Path(tmp_out.name).unlink(missing_ok=True)
            raise
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
        stac_client = pystac_client.Client.open(STAC_API_URL)
        search = stac_client.search(
            collections=[COLLECTION_ID],
            bbox=bbox,
            max_items=args.max_items,
        )
        items = list(search.items())
        items.sort(key=lambda item: item.id)
        if not items:
            raise SystemExit("No STAC items returned for Riyadh bbox.")
        print(f"Collection: {COLLECTION_ID}")
        print(f"Item ids: {[item.id for item in items]}")
        first_assets = items[0].assets
        asset_keys = list(first_assets.keys())
        print(f"Available asset keys (first item): {asset_keys}")
        print("First item assets hrefs:")
        for key, asset in first_assets.items():
            print(f"  - {key}: {asset.href}")
        signed_first_item = pc.sign(items[0])
        print("First signed item assets hrefs (including alternates):")
        for key, asset in signed_first_item.assets.items():
            print(f"  - {key}: {asset.href}")
            alternates = _asset_alternates(asset)
            for alt_key, alt in alternates.items():
                alt_href = alt.get("href")
                print(f"    alternate {alt_key}: {alt_href}")
        if "data" in first_assets:
            asset_key = "data"
        else:
            asset_key = None
            for key, asset in first_assets.items():
                href = asset.href
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
            signed_href = _select_https_href(signed_item, asset_key)
            if not signed_href:
                asset = signed_item.assets.get(asset_key)
                asset_href = asset.href if asset else None
                alternates = _asset_alternates(asset) if asset else {}
                alternate_keys = list(alternates.keys())
                print(
                    "No HTTPS href found; "
                    f"item_id={item.id}, asset_key={asset_key}, "
                    f"asset_href={asset_href}, alternate_keys={alternate_keys}"
                )
                continue
            signed_href = _signed_https_href(signed_href)
            print(f"Signed href: {signed_href}")
            output_name = f"riyadh_{_sanitize_filename(item.id or f'item_{index}')}.csv.gz"
            output_path = args.output_dir / output_name
            stats = _write_features(
                [signed_href],
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
