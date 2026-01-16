import json
import os
import subprocess
import time
from typing import List

import requests

BASE = os.environ.get("BASE", "").strip()
if not BASE:
    raise SystemExit("BASE env var is required")

BATCH = int(os.getenv("BATCH", "200"))
SLEEP = float(os.getenv("SLEEP", "0.25"))
MAX_MINUTES = float(os.getenv("MAX_MINUTES", "330"))
OUT = os.getenv("OUT", "riyadh_parcels_arcgis.gpkg")
LAYER = os.getenv("LAYER", "riyadh_parcels_arcgis")
CHECKPOINT = os.getenv("CHECKPOINT", "checkpoint.txt")

DEADLINE = time.time() + MAX_MINUTES * 60.0


def fetch_ids() -> List[int]:
    r = requests.get(
        f"{BASE}/query",
        params={"where": "1=1", "returnIdsOnly": "true", "f": "json"},
        timeout=120,
    )
    r.raise_for_status()
    data = r.json()
    ids = data.get("objectIds") or []
    if not ids:
        raise SystemExit("No objectIds returned from ArcGIS")
    return list(map(int, ids))


def fetch_geojson(object_ids: List[int]) -> dict:
    # POST avoids URI length issues
    params = {
        "f": "geojson",
        "objectIds": ",".join(map(str, object_ids)),
        "outFields": "*",
        "outSR": 4326,
        "returnGeometry": "true",
    }

    last_exc = None
    for attempt in range(5):
        try:
            r = requests.post(f"{BASE}/query", data=params, timeout=180)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(2.0 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last_exc = exc
            time.sleep(2.0 * (attempt + 1))
    raise RuntimeError(f"ArcGIS batch failed after retries: {last_exc}")


def ogr_write(tmp_geojson: str, out_gpkg: str, layer_name: str) -> None:
    create_mode = not os.path.exists(out_gpkg)

    cmd = [
        "ogr2ogr",
        "-q",  # quiet (reduces warning spam)
        "-f",
        "GPKG",
        out_gpkg,
        tmp_geojson,
        "-nln",
        layer_name,
        "-t_srs",
        "EPSG:4326",
        "-nlt",
        "MULTIPOLYGON",
    ]

    if create_mode:
        # create fresh
        cmd += ["-overwrite"]
        # only meaningful at creation time
        cmd += ["-lco", "GEOMETRY_NAME=geom"]
        cmd += ["-lco", "FID=fid"]
    else:
        cmd += ["-update", "-append"]

    cmd += ["-skipfailures"]

    subprocess.check_call(cmd)


def read_checkpoint() -> int:
    if not os.path.exists(CHECKPOINT):
        return 0
    try:
        return int((open(CHECKPOINT).read().strip() or "0"))
    except Exception:
        return 0


def write_checkpoint(n: int) -> None:
    with open(CHECKPOINT, "w", encoding="utf-8") as f:
        f.write(str(n))


def main() -> None:
    if os.path.exists(CHECKPOINT):
        try:
            with open(CHECKPOINT) as f:
                cp = int((f.read().strip() or "0"))
        except Exception:
            cp = 0
    else:
        cp = 0

    if cp <= 0 and os.path.exists(OUT):
        os.remove(OUT)

    ids = fetch_ids()
    total = len(ids)

    start = read_checkpoint()
    if start < 0 or start > total:
        start = 0

    print(f"Exporting {total} features in batches of {BATCH} -> {OUT}")
    print(f"Resume checkpoint: {CHECKPOINT} (start at index {start})")

    tmp = "tmp_batch.geojson"

    for i in range(start, total, BATCH):
        if time.time() > DEADLINE:
            print("Stopping before max runtime; checkpoint saved.")
            break

        chunk = ids[i : i + BATCH]
        data = fetch_geojson(chunk)

        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        ogr_write(tmp, OUT, LAYER)

        done = i + len(chunk)
        write_checkpoint(done)
        print(f"Appended {done}/{total}")

        time.sleep(SLEEP)

    print("Done (or checkpointed).")
    print("Output:", OUT)
    print("Checkpoint:", CHECKPOINT)


if __name__ == "__main__":
    main()
