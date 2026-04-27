from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from math import ceil
from typing import Iterable, Optional

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.ml.name_normalization import norm_city, norm_district
from app.models.tables import RentComp
from app.services.aqar_district_match import (
    normalize_district_key,
    normalize_district_key_sql,
)

logger = logging.getLogger(__name__)


# Kaggle/Aqar scrapes frequently encode *annual* rents (SAR/year) even when the
# app expects SAR/month. We defensively normalize implausibly-high values.
_KAGGLE_AQAR_SOURCES = {"kaggle_aqar", "kaggle_aqar_rent"}


def _normalize_kaggle_rent_per_m2_month(value: float, source: Optional[str], asset_type: str) -> float:
    src = (source or "").lower()
    if src not in _KAGGLE_AQAR_SOURCES:
        return value

    at = (asset_type or "").lower()
    # Thresholds are intentionally conservative to avoid scaling legitimate monthly postings.
    threshold = 300.0 if at == "residential" else 800.0
    if value > threshold:
        return value / 12.0
    return value


def _percentile_disc(values: Iterable[float], percentile: float) -> Optional[float]:
    """
    Mirror PostgreSQL's percentile_disc: pick the smallest value with cumulative
    distribution >= percentile. Returns None when the iterable is empty.
    """
    vals = sorted(float(v) for v in values)
    if not vals:
        return None
    # percentile_disc uses 1-based indexing with ceiling
    idx = max(1, ceil(percentile * len(vals)))
    return vals[idx - 1]


def _clip(values: Iterable[float], low: Optional[float], high: Optional[float]) -> list[float]:
    clipped: list[float] = []
    for v in values:
        if low is not None and v < low:
            continue
        if high is not None and v > high:
            continue
        clipped.append(v)
    return clipped


@dataclass
class RentMedianResult:
    district_median: Optional[float]
    city_median: Optional[float]
    n_district: int
    n_city: int
    city_asset_median: Optional[float]
    n_city_asset: int
    scope: Optional[str]
    median_rent_per_m2_month: Optional[float]
    sample_count: int

    def __iter__(self):
        # Preserve backward compatibility with legacy unpacking (district, city, n_district, n_city).
        yield self.district_median
        yield self.city_median
        yield self.n_district
        yield self.n_city


def aqar_rent_median(
    db: Session,
    city: str,
    district: Optional[str] = None,
    city_norm: Optional[str] = None,
    district_norm: Optional[str] = None,
    asset_type: str = "residential",
    unit_type: Optional[str] = None,
    since_days: int = 365,
) -> RentMedianResult:
    """
    Median rent_per_m2 (SAR/month) from rent_comp, clipped to the p05–p95 band.

    Returns medians, sample counts, and the scope used for downstream selection.
    Selection (performed by callers) should try district → city (asset+unit) → city (asset only).
    """

    if not city:
        return RentMedianResult(None, None, 0, 0, None, 0, None, None, 0)

    city_norm = city_norm or norm_city(city)
    if not city_norm:
        return RentMedianResult(None, None, 0, 0, None, 0, None, None, 0)

    district_norm = district_norm or (norm_district(city_norm, district) if district else "")

    # Resolve English district names to their canonical Arabic norm-key via
    # the AR↔EN crosswalk so callers passing English (e.g. cu.neighborhood)
    # can hit Arabic-keyed RentComp rows. Lazy import avoids a circular
    # dependency with expansion_advisor.
    district_for_match = district_norm
    if district:
        try:
            from app.services.expansion_advisor import (
                _cached_district_lookup,
                _resolve_district_to_ar_key,
            )

            lookup = _cached_district_lookup(db)
            resolved = _resolve_district_to_ar_key(district, lookup)
            if resolved:
                district_for_match = resolved
                if normalize_district_key(district) == resolved:
                    logger.debug("rent_lookup: AR direct match for %s", district)
                else:
                    logger.debug("rent_lookup: EN→AR match %s → %s", district, resolved)
            else:
                logger.debug(
                    "rent_lookup: no district resolution for %s, using pass-through",
                    district,
                )
        except Exception:
            logger.debug("rent_lookup: crosswalk unavailable for %s", district, exc_info=True)

    dialect_name = ""
    try:
        bind = db.get_bind()
        dialect_name = getattr(getattr(bind, "dialect", None), "name", "") or ""
    except Exception:
        dialect_name = ""

    since_date = date.today() - timedelta(days=since_days) if since_days else None

    def _values(scope_district: bool, apply_unit: bool = True) -> list[float]:
        q = db.query(RentComp.rent_per_m2, RentComp.source).filter(RentComp.rent_per_m2.isnot(None))
        if asset_type:
            q = q.filter(func.lower(RentComp.asset_type) == asset_type.lower())
        if unit_type and apply_unit:
            q = q.filter(RentComp.unit_type.ilike(unit_type))
        if city_norm:
            q = q.filter(func.lower(RentComp.city) == city_norm.lower())
        if since_date:
            q = q.filter(RentComp.date >= since_date)
        if scope_district:
            # If no district was supplied, avoid "faking" a district median by returning city stats.
            if not district_for_match:
                return []
            if dialect_name == "postgresql":
                # Apply normalize_district_key semantics on the column side
                # so حي-prefix and alef/ya variants in stored data don't
                # break the join. The bind value is already a norm-key
                # (resolved above or normalized below), so wrapping it in
                # the same fragment is a no-op but keeps both sides
                # symmetric and dialect-friendly.
                q = q.filter(
                    text(
                        f"LOWER({normalize_district_key_sql('rent_comp.district')}) "
                        f"= LOWER({normalize_district_key_sql(':district_normalized')})"
                    ).bindparams(district_normalized=district_for_match)
                )
            else:
                q = q.filter(func.lower(RentComp.district) == district_for_match.lower())
        return [
            _normalize_kaggle_rent_per_m2_month(float(rent_per_m2), source, asset_type)
            for rent_per_m2, source in q.all()
            if rent_per_m2 is not None
        ]

    city_values = _values(scope_district=False, apply_unit=True)
    city_asset_values = _values(scope_district=False, apply_unit=False)
    district_values = _values(scope_district=True, apply_unit=True)

    if not city_values and not city_asset_values:
        return RentMedianResult(None, None, 0, 0, None, 0, None, None, 0)

    city_low = _percentile_disc(city_values, 0.05) if city_values else None
    city_high = _percentile_disc(city_values, 0.95) if city_values else None
    city_filtered = _clip(city_values, city_low, city_high) if city_values else []
    city_median = _percentile_disc(city_filtered, 0.5) if city_filtered else None

    district_median = None
    district_filtered: list[float] = []
    if district_values:
        dist_low = _percentile_disc(district_values, 0.05)
        dist_high = _percentile_disc(district_values, 0.95)
        # Apply both local (district) and city-wide clipping to keep a conservative band
        district_filtered = _clip(district_values, dist_low, dist_high)
        district_filtered = _clip(district_filtered, city_low, city_high)
        district_median = _percentile_disc(district_filtered, 0.5) if district_filtered else None

    city_asset_low = _percentile_disc(city_asset_values, 0.05) if city_asset_values else None
    city_asset_high = _percentile_disc(city_asset_values, 0.95) if city_asset_values else None
    city_asset_filtered = _clip(city_asset_values, city_asset_low, city_asset_high) if city_asset_values else []
    city_asset_median = _percentile_disc(city_asset_filtered, 0.5) if city_asset_filtered else None

    # Decide the best scope and median (for reporting only; selection happens upstream).
    scope = None
    median_used = None
    sample_count = 0
    if district_norm and district_median is not None and len(district_filtered) > 0:
        scope = "district"
        median_used = float(district_median)
        sample_count = len(district_filtered)
    elif city_median is not None and len(city_filtered) > 0:
        scope = "city_unit_type" if unit_type else "city_asset_type"
        median_used = float(city_median)
        sample_count = len(city_filtered)
    elif city_asset_median is not None and len(city_asset_filtered) > 0:
        scope = "city_asset_type"
        median_used = float(city_asset_median)
        sample_count = len(city_asset_filtered)

    return RentMedianResult(
        float(district_median) if district_median is not None else None,
        float(city_median) if city_median is not None else None,
        len(district_filtered),
        len(city_filtered),
        float(city_asset_median) if city_asset_median is not None else None,
        len(city_asset_filtered),
        scope,
        median_used if median_used is not None else None,
        sample_count,
    )
