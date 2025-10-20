#!/usr/bin/env python3
import argparse, math, pathlib, sys, time
import httpx
from typing import Optional

UA = "oaktree-estimator/0.1 (offline prefetch)"

def lonlat_to_tile(lon, lat, z):
    lat_rad = math.radians(lat)
    n = 2.0 ** z
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.log(math.tan(lat_rad) + (1.0 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return x, y

def parse_zooms(spec: str):
    if "-" in spec:
        a, b = spec.split("-", 1)
        return range(int(a), int(b) + 1)
    return (int(z) for z in spec.split(","))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bbox", required=True, help="minLon,minLat,maxLon,maxLat")
    ap.add_argument("--zooms", default="12-15", help="e.g. 12-15 or 12,13,14")
    ap.add_argument("--dest", required=True, help="directory to write tiles into")
    ap.add_argument("--upstream", default="https://tile.openstreetmap.org")
    ap.add_argument("--max-seconds", type=int, default=None,
                    help="Gracefully stop after N seconds (so the workflow can commit/push).")
    ap.add_argument("--log-every", type=int, default=250,
                    help="Print progress every N tiles.")
    args = ap.parse_args()

    lon_min, lat_min, lon_max, lat_max = [float(x) for x in args.bbox.split(",")]
    dest = pathlib.Path(args.dest)
    dest.mkdir(parents=True, exist_ok=True)

    client = httpx.Client(timeout=30.0, headers={"User-Agent": UA})
    total = 0
    started = time.time()
    deadline: Optional[float] = (started + args.max_seconds) if args.max_seconds else None

    for z in parse_zooms(args.zooms):
        x0, y_max = lonlat_to_tile(lon_min, lat_min, z)
        x1, y_min = lonlat_to_tile(lon_max, lat_max, z)
        for x in range(min(x0, x1), max(x0, x1) + 1):
            for y in range(min(y_min, y_max), max(y_min, y_max) + 1):
                # Stop early if we've hit the time budget
                if deadline is not None and time.time() >= deadline:
                    elapsed = time.time() - started
                    print(f"[graceful-exit] Time budget reached after {elapsed:0.1f}s; "
                          f"downloaded {total} tiles. Exiting so the workflow can commit.",
                          file=sys.stderr)
                    print(f"Partial. Tiles stored under: {dest} (count={total})")
                    return
                out = dest / str(z) / str(x) / f"{y}.png"
                if out.exists():
                    total += 1
                    continue
                out.parent.mkdir(parents=True, exist_ok=True)
                url = f"{args.upstream}/{z}/{x}/{y}.png"
                r = client.get(url)
                r.raise_for_status()
                out.write_bytes(r.content)
                total += 1
                if total % args.log_every == 0:
                    elapsed = time.time() - started
                    print(f"Fetched {total} tiles in {elapsed:0.1f}s", file=sys.stderr)
    print(f"Done. Tiles stored under: {dest} (count={total})")

if __name__ == "__main__":
    main()
