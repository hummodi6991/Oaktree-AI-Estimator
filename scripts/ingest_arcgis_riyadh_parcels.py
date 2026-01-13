#!/usr/bin/env python3
import argparse
import json
import os
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2 import extras
import requests

BASE_URL = (
    "https://services-ap1.arcgis.com/if9krLUYaMWhxyMO/ArcGIS/rest/services/"
    "Riyadh_Parcel/FeatureServer/0/query"
)
JOB_NAME = "arcgis_riyadh_parcel"


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def get_connection():
    return psycopg2.connect(
        host=get_env("PGHOST"),
        port=get_env("PGPORT"),
        dbname=get_env("PGDATABASE"),
        user=get_env("PGUSER"),
        password=get_env("PGPASSWORD"),
        sslmode=get_env("PGSSLMODE"),
    )


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.riyadh_parcels_arcgis_raw (
              fid integer PRIMARY KEY,
              parcelno text,
              planno text,
              parcelsubt integer,
              parcel_dra integer,
              parcelsub text,
              area_attr integer,
              raw_props jsonb,
              geom geometry(MultiPolygon,4326),
              observed_at timestamptz NOT NULL DEFAULT now()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS riyadh_parcels_arcgis_raw_geom_idx
            ON public.riyadh_parcels_arcgis_raw
            USING GIST (geom);
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.ingest_checkpoints (
              job_name text PRIMARY KEY,
              updated_at timestamptz NOT NULL DEFAULT now(),
              next_index bigint NOT NULL DEFAULT 0,
              total_ids bigint,
              notes text
            );
            """
        )
        cur.execute(
            """
            INSERT INTO public.ingest_checkpoints (job_name)
            VALUES (%s)
            ON CONFLICT (job_name) DO NOTHING;
            """,
            (JOB_NAME,),
        )
    conn.commit()


def fetch_object_ids(session: requests.Session) -> list[int]:
    params = {"where": "1=1", "returnIdsOnly": "true", "f": "json"}
    resp = session.get(BASE_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    object_ids = data.get("objectIds") or []
    return sorted(object_ids)


def request_features(session: requests.Session, object_ids: list[int]):
    params = {
        "f": "json",
        "objectIds": ",".join(str(x) for x in object_ids),
        "outFields": "FID,PARCELNO,PLANNO,PARCELSUBT,PARCEL_DRA,ParcelSub,Area",
        "outSR": "4326",
        "returnGeometry": "true",
    }
    resp = session.get(BASE_URL, params=params, timeout=60)
    if resp.status_code != 200:
        raise ValueError(f"Request failed with status {resp.status_code}")
    try:
        data = resp.json()
    except json.JSONDecodeError as exc:
        raise ValueError("Invalid JSON response") from exc
    if "error" in data:
        raise ValueError(f"ArcGIS error: {data['error']}")
    return data.get("features") or []


def bisect_fetch(session: requests.Session, object_ids: list[int], stats: dict):
    if not object_ids:
        return []
    try:
        return request_features(session, object_ids)
    except Exception as exc:
        stats["failed_batches"] += 1
        if len(object_ids) == 1:
            stats["skipped_ids"] += 1
            print(f"Skipping objectId {object_ids[0]} due to error: {exc}")
            return []
        mid = len(object_ids) // 2
        left = bisect_fetch(session, object_ids[:mid], stats)
        right = bisect_fetch(session, object_ids[mid:], stats)
        return left + right


def build_records(features: list[dict], stats: dict):
    records = []
    for feature in features:
        attrs = feature.get("attributes") or {}
        fid = attrs.get("FID")
        if fid is None:
            stats["skipped_ids"] += 1
            continue
        geometry = feature.get("geometry") or {}
        if "rings" not in geometry:
            stats["skipped_ids"] += 1
            continue
        geojson = {"type": "Polygon", "coordinates": geometry["rings"]}
        records.append(
            (
                fid,
                attrs.get("PARCELNO"),
                attrs.get("PLANNO"),
                attrs.get("PARCELSUBT"),
                attrs.get("PARCEL_DRA"),
                attrs.get("ParcelSub"),
                attrs.get("Area"),
                extras.Json(attrs),
                json.dumps(geojson),
            )
        )
    return records


def upsert_records(conn, records: list[tuple]):
    if not records:
        return 0
    sql = """
        INSERT INTO public.riyadh_parcels_arcgis_raw (
          fid,
          parcelno,
          planno,
          parcelsubt,
          parcel_dra,
          parcelsub,
          area_attr,
          raw_props,
          geom
        )
        VALUES %s
        ON CONFLICT (fid) DO UPDATE SET
          parcelno = EXCLUDED.parcelno,
          planno = EXCLUDED.planno,
          parcelsubt = EXCLUDED.parcelsubt,
          parcel_dra = EXCLUDED.parcel_dra,
          parcelsub = EXCLUDED.parcelsub,
          area_attr = EXCLUDED.area_attr,
          raw_props = EXCLUDED.raw_props,
          geom = EXCLUDED.geom,
          observed_at = now();
    """
    template = "(%s,%s,%s,%s,%s,%s,%s,%s,ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s),4326)))"
    with conn.cursor() as cur:
        extras.execute_values(cur, sql, records, template=template)
    conn.commit()
    return len(records)


def update_checkpoint(conn, next_index: int, total_ids: int | None = None):
    with conn.cursor() as cur:
        if total_ids is None:
            cur.execute(
                """
                UPDATE public.ingest_checkpoints
                SET next_index = %s,
                    updated_at = now()
                WHERE job_name = %s;
                """,
                (next_index, JOB_NAME),
            )
        else:
            cur.execute(
                """
                UPDATE public.ingest_checkpoints
                SET next_index = %s,
                    total_ids = %s,
                    updated_at = now()
                WHERE job_name = %s;
                """,
                (next_index, total_ids, JOB_NAME),
            )
    conn.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=300)
    parser.add_argument("--sleep-s", type=float, default=0.25)
    parser.add_argument("--max-minutes", type=float, default=330)
    parser.add_argument("--resume", type=str, default="true")
    parser.add_argument("--start-from-index", type=int, default=0)
    args = parser.parse_args()

    if args.batch_size <= 0 or args.batch_size > 2000:
        raise ValueError("batch_size must be between 1 and 2000")
    if args.max_minutes <= 0 or args.max_minutes >= 360:
        raise ValueError("max_minutes must be between 1 and 359")

    resume = parse_bool(args.resume)
    start_from_index = max(args.start_from_index, 0)

    start_time = time.monotonic()

    conn = get_connection()
    ensure_schema(conn)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT next_index, total_ids FROM public.ingest_checkpoints WHERE job_name = %s;",
            (JOB_NAME,),
        )
        row = cur.fetchone() or (0, None)
    checkpoint_next, checkpoint_total = row

    if resume:
        start_index = max(checkpoint_next or 0, start_from_index)
    else:
        start_index = start_from_index
        update_checkpoint(conn, start_index, None)

    session = requests.Session()
    object_ids = fetch_object_ids(session)
    total_ids = len(object_ids)
    update_checkpoint(conn, start_index, total_ids)

    stats = {
        "fetched": 0,
        "upserted": 0,
        "skipped_ids": 0,
        "failed_batches": 0,
    }

    for batch_num, index in enumerate(range(start_index, total_ids, args.batch_size), start=1):
        elapsed_minutes = (time.monotonic() - start_time) / 60
        if elapsed_minutes >= args.max_minutes:
            update_checkpoint(conn, index)
            print(
                f"Reached max runtime at {elapsed_minutes:.2f} mins, "
                f"checkpointed index {index}."
            )
            print_summary(stats, total_ids, start_time)
            return

        batch_ids = object_ids[index : index + args.batch_size]
        features = bisect_fetch(session, batch_ids, stats)
        stats["fetched"] += len(features)
        records = build_records(features, stats)
        upserted = upsert_records(conn, records)
        stats["upserted"] += upserted
        update_checkpoint(conn, index + len(batch_ids))

        elapsed_minutes = (time.monotonic() - start_time) / 60
        print(
            "batch={batch} index={index}/{total} upserted={upserted} "
            "skipped={skipped} elapsed={elapsed:.2f} mins".format(
                batch=batch_num,
                index=index + len(batch_ids),
                total=total_ids,
                upserted=stats["upserted"],
                skipped=stats["skipped_ids"],
                elapsed=elapsed_minutes,
            )
        )

        if args.sleep_s > 0:
            time.sleep(args.sleep_s)

    update_checkpoint(conn, total_ids)
    print_summary(stats, total_ids, start_time)


def print_summary(stats: dict, total_ids: int, start_time: float):
    elapsed_minutes = (time.monotonic() - start_time) / 60
    finished_at = datetime.now(timezone.utc).isoformat()
    print(
        "Summary: total_ids={total} fetched={fetched} upserted={upserted} "
        "skipped_ids={skipped} failed_batches={failed} "
        "elapsed={elapsed:.2f} mins finished_at={finished}".format(
            total=total_ids,
            fetched=stats["fetched"],
            upserted=stats["upserted"],
            skipped=stats["skipped_ids"],
            failed=stats["failed_batches"],
            elapsed=elapsed_minutes,
            finished=finished_at,
        )
    )


if __name__ == "__main__":
    main()
