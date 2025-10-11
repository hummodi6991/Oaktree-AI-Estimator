import math, os, pathlib, httpx, argparse, itertools

UPSTREAM = os.getenv("TILE_UPSTREAM", "https://tile.openstreetmap.org")
UA = os.getenv("TILE_USER_AGENT", "oaktree-estimator/0.1 (contact: ops@example.com)")

def deg2num(lat, lon, z):
    lat_rad = math.radians(lat)
    n = 2.0 ** z
    xtile = int((lon + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2.0 * n)
    return xtile, ytile

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bbox", default="46.4,24.3,47.1,25.1", help="minLon,minLat,maxLon,maxLat (Riyadh-ish default)")
    ap.add_argument("--zooms", default="11-16", help="e.g. 10-15 or 12,13,14")
    ap.add_argument("--dest", default="/app/tiles_cache", help="tile cache root")
    args = ap.parse_args()

    minlon, minlat, maxlon, maxlat = [float(x) for x in args.bbox.split(",")]
    if "-" in args.zooms:
        z0, z1 = [int(x) for x in args.zooms.split("-")]
        zooms = range(z0, z1 + 1)
    else:
        zooms = [int(z) for z in args.zooms.split(",")]

    dest = pathlib.Path(args.dest)
    dest.mkdir(parents=True, exist_ok=True)

    with httpx.Client(headers={"User-Agent": UA}, timeout=30.0) as client:
        for z in zooms:
            x0, y1 = deg2num(minlat, minlon, z)
            x1, y0 = deg2num(maxlat, maxlon, z)
            xs = range(min(x0, x1), max(x0, x1) + 1)
            ys = range(min(y0, y1), max(y0, y1) + 1)
            print(f"Zoom {z}: {len(xs)*len(ys)} tiles")
            for x, y in itertools.product(xs, ys):
                target = dest / str(z) / str(x) / f"{y}.png"
                if target.exists():
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                url = f"{UPSTREAM}/{z}/{x}/{y}.png"
                try:
                    r = client.get(url)
                    r.raise_for_status()
                    target.write_bytes(r.content)
                except Exception:
                    pass  # keep going

if __name__ == "__main__":
    main()
