from __future__ import annotations

import argparse
import datetime as dt
import re
from typing import Any, Iterable

import httpx
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import SessionLocal
from app.models.tables import SuhailLandMetric


DEFAULT_REGION_ID = 10
DEFAULT_LIMIT = 100
DEFAULT_PROVINCE_ID = 101000


def _parse_date(value: Any) -> dt.date | None:
    if not value:
        return None
    if isinstance(value, (dt.date, dt.datetime)):
        return value.date() if isinstance(value, dt.datetime) else value
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except Exception:
        return None


def _normalize_district(name: Any) -> str | None:
    if not name:
        return None
    txt = str(name).strip()
    txt = re.sub(r"^(?:حي|حى)\s+", "", txt)
    txt = re.sub(r"\s+", " ", txt)
    return txt or None


def _extract_median_ppm2(metric: dict[str, Any]) -> Any:
    for key in ("medianPriceOfMeter", "medianPriceMeter", "medianPricePerMeter", "medianPrice"):
        if key in metric:
            return metric.get(key)
    return metric.get("median")


def _extract_last(metric: dict[str, Any]) -> tuple[Any, dt.date | None]:
    last = metric.get("lastExecutionPrice") or metric.get("lastPrice") or {}
    if not isinstance(last, dict):
        return None, None
    last_price = last.get("priceOfMeter") or last.get("price_per_meter") or last.get("price")
    last_date = _parse_date(last.get("transactionDate") or last.get("date"))
    return last_price, last_date


def _page_items(payload: Any) -> tuple[list[dict[str, Any]], int]:
    if isinstance(payload, list):
        return payload, len(payload)
    if not isinstance(payload, dict):
        return [], 0

    for key in ("items", "data", "result", "results", "neighborhoods", "records"):
        items = payload.get(key)
        if isinstance(items, list):
            break
    else:
        items = []

    total_count = payload.get("totalCount") or payload.get("total") or payload.get("count")
    if total_count is None:
        total_count = len(items)

    return items, int(total_count)


def _build_rows(
    item: dict[str, Any],
    region_id: int,
    province_id: int,
    apply_province_filter: bool,
) -> Iterable[dict[str, Any]]:
    if apply_province_filter and item.get("provinceId") != province_id:
        return []

    as_of_date = _parse_date(item.get("presentDate"))
    base = dict(
        as_of_date=as_of_date,
        region_id=region_id,
        province_id=item.get("provinceId"),
        province_name=item.get("provinceName"),
        neighborhood_id=item.get("neighborhoodId"),
        neighborhood_name=item.get("neighborhoodName"),
        district_norm=_normalize_district(item.get("neighborhoodName")),
    )

    rows = []
    total_metric = item.get("totalMetricData") or {}
    rows.append(_row_from_metric(total_metric, land_use_group="الكل", base=base))

    for group_metric in item.get("landUseGroup") or []:
        lug = group_metric.get("landUseGroup") or group_metric.get("land_use_group")
        rows.append(_row_from_metric(group_metric, land_use_group=lug, base=base))

    return rows


def _row_from_metric(metric: dict[str, Any], land_use_group: str | None, base: dict[str, Any]) -> dict[str, Any]:
    last_price_ppm2, last_txn_date = _extract_last(metric or {})
    row = {
        **base,
        "land_use_group": land_use_group or "غير محدد",
        "median_ppm2": _extract_median_ppm2(metric or {}),
        "last_price_ppm2": last_price_ppm2,
        "last_txn_date": last_txn_date,
        "raw": metric or {},
    }
    return row


def _upsert_metric(db: Session, data: dict[str, Any]) -> None:
    obj = (
        db.query(SuhailLandMetric)
        .filter(
            SuhailLandMetric.as_of_date == data["as_of_date"],
            SuhailLandMetric.region_id == data["region_id"],
            SuhailLandMetric.neighborhood_id == data["neighborhood_id"],
            SuhailLandMetric.land_use_group == data["land_use_group"],
        )
        .one_or_none()
    )

    if obj:
        obj.median_ppm2 = data["median_ppm2"]
        obj.last_price_ppm2 = data["last_price_ppm2"]
        obj.last_txn_date = data["last_txn_date"]
        obj.raw = data["raw"]
        obj.observed_at = dt.datetime.utcnow()
    else:
        obj = SuhailLandMetric(**data, observed_at=dt.datetime.utcnow())
        db.add(obj)


def ingest(
    region_id: int,
    province_id: int,
    limit: int,
    apply_province_filter: bool,
    api_url: str,
) -> tuple[int, int, dt.date | None]:
    headers = {}
    if settings.SUHAIL_API_KEY:
        headers["Authorization"] = f"Bearer {settings.SUHAIL_API_KEY}"

    neighborhoods_processed = 0
    rows_upserted = 0
    max_as_of: dt.date | None = None

    with httpx.Client(headers=headers, timeout=30.0) as client, SessionLocal() as db:
        offset = 0
        total_count = None

        while True:
            resp = client.get(
                api_url,
                params={"regionId": region_id, "offset": offset, "limit": limit},
            )
            resp.raise_for_status()
            payload = resp.json()
            items, total_count = _page_items(payload)

            for item in items:
                rows = list(
                    _build_rows(
                        item=item,
                        region_id=region_id,
                        province_id=province_id,
                        apply_province_filter=apply_province_filter,
                    )
                )
                if not rows:
                    continue
                neighborhoods_processed += 1
                for row in rows:
                    _upsert_metric(db, row)
                rows_upserted += len(rows)
                for row in rows:
                    if row["as_of_date"]:
                        max_as_of = max(max_as_of, row["as_of_date"]) if max_as_of else row["as_of_date"]

            db.commit()

            offset += limit
            if total_count is not None and offset >= total_count:
                break
            if not items:
                break

    return neighborhoods_processed, rows_upserted, max_as_of


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Suhail land metrics")
    parser.add_argument("--region-id", type=int, default=DEFAULT_REGION_ID, help="Region ID to fetch")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Page size for requests")
    parser.add_argument(
        "--province-id",
        type=int,
        default=DEFAULT_PROVINCE_ID,
        help="Province ID filter (default: Riyadh)",
    )
    parser.add_argument(
        "--no-province-filter",
        action="store_true",
        help="Disable province filter (ingest all provinces in region)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    api_url = settings.SUHAIL_API_URL
    if not api_url:
        raise SystemExit("SUHAIL_API_URL is not configured")

    neighborhoods, rows, max_as_of = ingest(
        region_id=args.region_id,
        province_id=args.province_id,
        limit=args.limit,
        apply_province_filter=not args.no_province_filter,
        api_url=api_url,
    )

    print(f"Neighborhoods processed: {neighborhoods}")
    print(f"Rows inserted/updated: {rows}")
    print(f"Max as_of_date: {max_as_of.isoformat() if max_as_of else '-'}")


if __name__ == "__main__":
    main()
