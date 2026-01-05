from __future__ import annotations

from typing import Any, Dict, Optional
import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.ml.name_normalization import norm_city
from app.services.district_resolver import DistrictResolution, resolve_district, resolution_meta
from app.services.aqar_utils import norm_city_for_aqar
from app.services.aqar_district_match import find_aqar_mv_district_price
from app.services.pricing import SUHAIL_RIYADH_PROVINCE_ID

logger = logging.getLogger(__name__)

# Minimum evidence to keep the standard blend weights
AQAR_STRONG_SAMPLE = 20


def _suhail_land_signal(
    db: Session,
    *,
    city_norm: str,
    district_norm: str | None,
    land_use_group: str | None = None,
) -> tuple[Optional[float], Dict[str, Any]]:
    """
    Fetch Suhail land median for the district (anchor) or Riyadh-wide fallback.
    Returns (value, meta).
    """
    meta: Dict[str, Any] = {
        "source": "suhail_land_metrics",
        "level": None,
        "land_use_group": None,
        "land_use_group_requested": land_use_group,
        "land_use_group_used": None,
        "used_fallback": False,
    }

    land_use_candidates: list[dict[str, Any]] = []
    if land_use_group is not None:
        land_use_candidates.append(
            {"land_use_group": land_use_group, "like": False, "allow_any": False, "used_fallback": False}
        )
        land_use_candidates.append(
            {"land_use_group": land_use_group, "like": True, "allow_any": False, "used_fallback": True}
        )
        if land_use_group != "الكل":
            land_use_candidates.append(
                {"land_use_group": "الكل", "like": False, "allow_any": False, "used_fallback": True}
            )
    else:
        land_use_candidates.append(
            {"land_use_group": "الكل", "like": False, "allow_any": False, "used_fallback": False}
        )
        land_use_candidates.append(
            {"land_use_group": None, "like": False, "allow_any": True, "used_fallback": True}
        )

    def _district_row(
        candidate: dict[str, Any],
    ):
        params: dict[str, Any] = {"district_norm": district_norm} if district_norm else {}
        conditions: list[str] = []
        if candidate["allow_any"]:
            pass
        elif candidate["like"]:
            conditions.append("land_use_group ILIKE :land_use_group_like")
            params["land_use_group_like"] = f"%{candidate['land_use_group']}%"
        else:
            conditions.append("land_use_group = :land_use_group")
            params["land_use_group"] = candidate["land_use_group"]

        if district_norm:
            conditions.append("district_norm = :district_norm")
        if city_norm == "riyadh":
            conditions.append("province_id = :province_id")
            params["province_id"] = SUHAIL_RIYADH_PROVINCE_ID

        if not conditions:
            return None

        query = f"""
            SELECT land_use_group, as_of_date, median_ppm2, last_price_ppm2, last_txn_date
            FROM suhail_land_metrics
            WHERE {' AND '.join(conditions)}
            ORDER BY as_of_date DESC
            LIMIT 1
        """
        try:
            return db.execute(text(query), params).mappings().first()
        except SQLAlchemyError as exc:
            logger.warning("suhail_land_metrics district lookup failed: %s", exc)
            return None

    def _riyadh_row(candidate: dict[str, Any]):
        params: dict[str, Any] = {"province_id": SUHAIL_RIYADH_PROVINCE_ID}
        conditions: list[str] = ["province_id = :province_id"]
        land_use_group_used = candidate["land_use_group"] if not candidate["allow_any"] else "الكل"

        if candidate["allow_any"]:
            pass
        elif candidate["like"]:
            conditions.append("land_use_group ILIKE :land_use_group_like")
            params["land_use_group_like"] = f"%{candidate['land_use_group']}%"
        else:
            conditions.append("land_use_group = :land_use_group")
            params["land_use_group"] = candidate["land_use_group"]

        where_clause = " AND ".join(conditions)
        query = f"""
            WITH latest AS (
                SELECT max(as_of_date) AS max_date
                FROM suhail_land_metrics
                WHERE {where_clause}
            )
            SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY median_ppm2) AS median_ppm2,
                   (SELECT max_date FROM latest) AS as_of_date,
                   :land_use_group_used AS land_use_group
            FROM suhail_land_metrics s
            JOIN latest l ON s.as_of_date = l.max_date
            WHERE {where_clause}
        """
        params["land_use_group_used"] = land_use_group_used
        try:
            return db.execute(text(query), params).mappings().first()
        except SQLAlchemyError as exc:
            logger.warning("suhail_land_metrics Riyadh median lookup failed: %s", exc)
            return None

    for candidate in land_use_candidates:
        row = None
        level = None
        method_name = "suhail_land_metrics_median"
        if district_norm:
            row = _district_row(candidate)
            if row:
                level = "district"
        if not row and city_norm == "riyadh":
            row = _riyadh_row(candidate)
            if row:
                level = "city"
                method_name = "suhail_land_metrics_median_city"

        if row and row.get("median_ppm2") is not None:
            used_group = row.get("land_use_group") or candidate.get("land_use_group") or "الكل"
            meta.update(
                {
                    "as_of_date": row.get("as_of_date"),
                    "last_price_ppm2": float(row.get("last_price_ppm2") or 0)
                    if row.get("last_price_ppm2") is not None
                    else None,
                    "last_txn_date": row.get("last_txn_date"),
                    "level": level,
                    "method": method_name,
                    "land_use_group": used_group,
                    "land_use_group_used": used_group,
                    "used_fallback": candidate.get("used_fallback", False),
                }
            )
            val = float(row["median_ppm2"])
            if val > 0:
                return val, meta

    return None, meta


def _aqar_land_signal(
    db: Session,
    *,
    city: str | None,
    district_norm: str | None,
    district_raw: str | None,
) -> tuple[Optional[float], Dict[str, Any]]:
    """
    Fetch Aqar district median with evidence count; fallback to city median.
    Returns (value, meta).
    """
    meta: Dict[str, Any] = {"source": "aqar.mv_city_price_per_sqm", "n": None, "level": None}
    if not city:
        meta.update({"reason": "missing_city", "city_used": None, "district_used": district_raw})
        return None, meta

    aqar_city = norm_city_for_aqar(city)
    meta.update({"city_used": aqar_city, "district_used": district_raw})

    if district_raw:
        val, mv_meta = find_aqar_mv_district_price(
            db, city_ar=aqar_city, district_raw=district_raw, property_type=None
        )
        meta.update(mv_meta)
        if val is not None:
            return float(val), meta

    try:
        row = db.execute(
            text(
                """
                SELECT
                    percentile_disc(0.5) WITHIN GROUP (ORDER BY price_per_sqm) AS price_per_sqm,
                    count(*) AS n
                FROM aqar.listings
                WHERE lower(city)=lower(:city)
                  AND price_per_sqm IS NOT NULL
                  AND (
                    lower(property_type) ~ '\\m(أرض|ارض|land|plot)\\M'
                    OR lower(coalesce(title,'')) ~ '\\m(أرض|ارض|land|plot)\\M'
                    OR lower(coalesce(description,'')) ~ '\\m(أرض|ارض|land|plot)\\M'
                  )
                """
            ),
            {"city": aqar_city, "aqar_city": aqar_city},
        ).mappings().first()
    except SQLAlchemyError as exc:
        logger.warning("aqar.listings city median query failed: %s", exc)
        row = None

    if row and row.get("price_per_sqm") is not None:
        val = float(row["price_per_sqm"])
        meta.update(
            {
                "source": "aqar.listings_median_fallback",
                "n": int(row.get("n") or 0),
                "level": "city",
                "method": "aqar_city_median",
            }
        )
        if val > 0:
            return val, meta

    return None, meta


def _blend_values(
    suhail: tuple[Optional[float], Dict[str, Any]],
    aqar: tuple[Optional[float], Dict[str, Any]],
) -> tuple[Optional[float], str, Dict[str, Any]]:
    """
    Blend Suhail + Aqar with investor-grade guardrails.
    Returns (value, method, meta).
    """
    suhail_val, suhail_meta = suhail
    aqar_val, aqar_meta = aqar
    components: Dict[str, Any] = {}
    weights: Dict[str, float] = {}
    guardrails: Dict[str, Any] = {}
    method = "blended_v1"
    value: Optional[float] = None
    reason: str | None = None

    if suhail_val and suhail_val > 0:
        components["suhail"] = {"value": float(suhail_val), **suhail_meta}
    if aqar_val and aqar_val > 0:
        components["aqar"] = {"value": float(aqar_val), **aqar_meta}

    if "suhail" in components and "aqar" in components:
        weight_suhail = 0.7
        weight_aqar = 0.3
        aqar_n = int(aqar_meta.get("n") or 0)
        low_evidence = aqar_n < AQAR_STRONG_SAMPLE
        if low_evidence:
            weight_suhail = 0.9
            weight_aqar = 0.1
        value = (weight_suhail * float(suhail_val)) + (weight_aqar * float(aqar_val))
        weights = {"suhail": weight_suhail, "aqar": weight_aqar}
        guardrails["aqar_low_evidence"] = low_evidence
    elif "suhail" in components:
        value = float(suhail_val)
        weights = {"suhail": 1.0}
        reason = "missing_aqar"
    elif "aqar" in components:
        value = float(aqar_val)
        weights = {"aqar": 1.0}
        reason = "missing_suhail"
    else:
        reason = "missing_suhail"

    meta = {"components": components, "weights": weights, "guardrails": guardrails, "reason": reason}
    return value, method, meta


def quote_land_price_blended_v1(
    db: Session,
    city: str,
    district: str | None = None,
    lon: float | None = None,
    lat: float | None = None,
    geom_geojson: dict | None = None,
    land_use_group: str | None = None,
) -> Dict[str, Any]:
    """
    Resolve district once, fetch Suhail + Aqar signals, and blend into a single quote.
    """
    resolution: DistrictResolution = resolve_district(
        db,
        city=city or "",
        district=district,
        lon=lon,
        lat=lat,
        geom_geojson=geom_geojson,
    )

    city_norm = resolution.city_norm or norm_city(city) or city
    district_norm = resolution.district_norm
    district_raw = resolution.district_raw or district

    suhail = _suhail_land_signal(
        db, city_norm=city_norm or "", district_norm=district_norm, land_use_group=land_use_group
    )
    aqar = _aqar_land_signal(db, city=city, district_norm=district_norm, district_raw=district_raw)

    value, method, meta = _blend_values(suhail, aqar)
    meta["city_used"] = (
        meta.get("city_used")
        or (aqar[1].get("city_used") if isinstance(aqar, tuple) and isinstance(aqar[1], dict) else None)
        or norm_city_for_aqar(city)
        or city_norm
        or city
    )
    meta["district_used"] = district_raw
    meta["district_resolution"] = resolution_meta(resolution)
    meta["district_norm"] = district_norm
    meta["district_raw"] = district_raw

    return {
        "provider": "blended_v1",
        "method": method,
        "value": value,
        "district_raw": district_raw,
        "district_norm": district_norm,
        "district_resolution": resolution_meta(resolution),
        "meta": meta,
    }
