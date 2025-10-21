#!/usr/bin/env python3
import argparse, math, pathlib, sys, time, subprocess
import httpx

UA = "oaktree-estimator/0.1 (offline prefetch)"

def lonlat_to_tile(lon, lat, z):
    import math as _m
    lat_rad = _m.radians(lat)
    n = 2.0 ** z
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - _m.log(_m.tan(lat_rad) + (1.0 / _m.cos(lat_rad))) / _m.pi) / 2.0 * n)
    return x, y

def parse_zooms(spec: str):
    if "-" in spec:
        a, b = spec.split("-", 1)
        return range(int(a), int(b) + 1)
    return (int(z) for z in spec.split(","))

def _sh(*args: str) -> int:
    return subprocess.run(list(args), check=False).returncode

def checkpoint(dest: pathlib.Path, note: str, total: int) -> None:
    # Configure identity once per runner
    _sh("git", "config", "user.name",  "github-actions[bot]")
    _sh("git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com")
    # Stage only the tiles dir (faster than scanning the whole repo)
    _sh("git", "add", "-A", str(dest))
    # Commit if there are changes; ignore "nothing to commit"
    _sh("git", "commit", "-m", f"chore(tiles): checkpoint {note} (count={total})")
    # Push incrementally so the final push is tiny
    _sh("git", "push")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bbox", required=True, help="minLon,minLat,maxLon,maxLat")
    ap.add_argument("--zooms", default="12-15", help="e.g. 12-15 or 12,13,14")
    ap.add_argument("--dest", required=True, help="directory to write tiles into")
    ap.add_argument("--upstream", default="https://tile.openstreetmap.org")
    ap.add_argument("--checkpoint-secs", type=int, default=600, help="git add/commit/push every N seconds (default 600=10m)")
    ap.add_argument("--deadline-secs", type=int, default=17100, help="stop and final push before job timeout (default ~4h45m)")
    args = ap.parse_args()

    lon_min, lat_min, lon_max, lat_max = [float(x) for x in args.bbox.split(",")]
    dest = pathlib.Path(args.dest)
    dest.mkdir(parents=True, exist_ok=True)

    client = httpx.Client(timeout=30.0, headers={"User-Agent": UA})
    total = 0
    started = time.time()
    next_ckpt = started + max(60, args.checkpoint_secs)  # min 60s cadence
    hard_stop = started + max(600, args.deadline_secs)   # min 10m runway

    for z in parse_zooms(args.zooms):
        x0, y_max = lonlat_to_tile(lon_min, lat_min, z)
        x1, y_min = lonlat_to_tile(lon_max, lat_max, z)
        for x in range(min(x0, x1), max(x0, x1) + 1):
            for y in range(min(y_min, y_max), max(y_min, y_max) + 1):
                # Skip if already fetched (additive, not overwriting)
                out = dest / str(z) / str(x) / f"{y}.png"
                if out.exists():
                    total += 1
                    # periodic checkpoint even when mostly cached
                    if time.time() >= next_ckpt:
                        checkpoint(dest, "cached", total)
                        next_ckpt = time.time() + args.checkpoint_secs
                    # bail out if deadline approached
                    if time.time() >= hard_stop:
                        checkpoint(dest, "final(deadline)", total)
                        print(f"Stopped at deadline. Tiles under: {dest} (count={total})")
                        return
                    continue

                out.parent.mkdir(parents=True, exist_ok=True)
                url = f"{args.upstream}/{z}/{x}/{y}.png"
                r = client.get(url)
                r.raise_for_status()
                out.write_bytes(r.content)
                total += 1

                if total % 250 == 0:
                    elapsed = time.time() - started
                    print(f"Fetched {total} tiles in {elapsed:0.1f}s", file=sys.stderr)

                # periodic checkpoint
                now = time.time()
                if now >= next_ckpt:
                    checkpoint(dest, "interval", total)
                    next_ckpt = now + args.checkpoint_secs

                # stop safely before the runner's absolute cap
                if now >= hard_stop:
                    checkpoint(dest, "final(deadline)", total)
                    print(f"Stopped at deadline. Tiles under: {dest} (count={total})")
                    return

    # Finished the loops; do a last small commit & push
    checkpoint(dest, "final(done)", total)
    print(f"Done. Tiles stored under: {dest} (count={total})")

if __name__ == "__main__":
    main()
