"""Extract Overture buildings within a bounding box to TSV (gzipped)."""

import argparse
import sys
from typing import Tuple

import duckdb


DEFAULT_RELEASE = "2025-11-19.0"
DEFAULT_BBOX = "46.20,24.20,47.30,25.10"
S3_PATH_TMPL = (
    "s3://overturemaps-us-west-2/release/{release}/theme=buildings/type=building/*"
)


def _parse_bbox(raw: str) -> Tuple[float, float, float, float]:
    try:
        min_lon, min_lat, max_lon, max_lat = [float(x.strip()) for x in raw.split(",")]
    except Exception as exc:  # pragma: no cover - simple CLI parsing
        raise argparse.ArgumentTypeError(f"Invalid bbox '{raw}': {exc}")

    if min_lon >= max_lon or min_lat >= max_lat:
        raise argparse.ArgumentTypeError(
            "BBox must be minLon,minLat,maxLon,maxLat with min < max for lon/lat",
        )
    return min_lon, min_lat, max_lon, max_lat


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--release",
        default=DEFAULT_RELEASE,
        help="Overture Maps release tag (e.g. 2025-11-19.0)",
    )
    parser.add_argument(
        "--bbox",
        default=DEFAULT_BBOX,
        type=_parse_bbox,
        help="BBox as minLon,minLat,maxLon,maxLat",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Output path for gzipped TSV",
    )

    args = parser.parse_args(argv)
    min_lon, min_lat, max_lon, max_lat = args.bbox

    s3_path = S3_PATH_TMPL.format(release=args.release)
    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("SET s3_region='us-west-2';")
    con.execute("SET s3_endpoint='s3.amazonaws.com';")
    con.execute("SET s3_use_ssl=true;")

    query = f"""
    COPY (
      SELECT
        id,
        subtype,
        class,
        height,
        num_floors,
        hex(geometry) AS geom_wkb_hex
      FROM read_parquet('{s3_path}')
      WHERE
        bbox.xmin <= {max_lon}
        AND bbox.xmax >= {min_lon}
        AND bbox.ymin <= {max_lat}
        AND bbox.ymax >= {min_lat}
    ) TO '{args.out}' (FORMAT 'csv', DELIMITER '\t', HEADER, COMPRESSION 'gzip');
    """

    con.execute(query)
    return 0


if __name__ == "__main__":
    sys.exit(main())
