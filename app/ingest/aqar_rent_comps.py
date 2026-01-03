from __future__ import annotations

from datetime import date
from typing import Any, Dict, Iterable, Optional, Set

from sqlalchemy import text

from app.db.session import SessionLocal
from app.ml.name_normalization import norm_city, norm_district
from app.models.tables import RentComp

SOURCE = "kaggle_aqar"
LEGACY_SOURCES = ("kaggle_aqar_rent", "kaggle_aqar")

# Conservative rent keywords for heuristics when the dataset lacks an explicit rent/sale flag
_RENT_PATTERNS = ("rent", "for rent", "lease", "إيجار", "ايجار", "استئجار", "للإيجار", "لللايجار")
_SALE_PATTERNS = ("sale", "for sale", "buy", "بيع", "للبيع")
_COMMERCIAL_OFFICE_KEYWORDS = ("مكتب", "مكاتب", "office", "إداري", "اداري")
_COMMERCIAL_RETAIL_KEYWORDS = ("محل", "تجاري", "shop", "retail", "store", "معرض", "معارض")

# Price frequency hints to normalize listing prices to a *monthly* basis.
# Many KSA rental listings are posted as an annual rent (e.g., "ريال/سنوي").
_YEARLY_PATTERNS = (
    "year",
    "yearly",
    "annual",
    "per year",
    "/year",
    "yr",
    "سنوي",
    "سنوياً",
    "سنويا",
    "بالسنة",
    "بالسنه",
    "ريال/سنوي",
)
_MONTHLY_PATTERNS = (
    "month",
    "monthly",
    "per month",
    "/month",
    "mo",
    "شهري",
    "شهرياً",
    "شهريا",
    "بالشهر",
    "ريال/شهري",
)
_DAILY_PATTERNS = (
    "day",
    "daily",
    "per day",
    "/day",
    "يومي",
    "يومياً",
    "يوميا",
    "ريال/يومي",
)


def _infer_rent_period_months(row: Dict[str, Any]) -> Optional[int]:
    """
    Return the rent pricing period in months when it can be inferred:
      - 12 for yearly prices
      - 1 for monthly prices
      - 0 for daily/unsupported prices
      - None when unknown
    """

    # Prefer explicit encoded rent period when present (common in Kaggle/Aqar scrapes)
    rp = row.get("rent_period")
    if rp is not None and str(rp).strip() != "":
        try:
            rp_i = int(float(rp))
            if rp_i in (0, 3):  # yearly (some sources use 3 as yearly)
                return 12
            if rp_i == 2:  # monthly
                return 1
            if rp_i == 1:  # daily
                return 0
        except Exception:
            pass

    # Next best: scan text fields for frequency hints
    blob = " ".join(
        str(row.get(col) or "")
        for col in (
            "price_frequency",
            "listing_type",
            "purpose",
            "ad_type",
            "title",
            "description",
            "property_type",
        )
    )
    if _has_pattern(blob, _YEARLY_PATTERNS):
        return 12
    if _has_pattern(blob, _MONTHLY_PATTERNS):
        return 1
    if _has_pattern(blob, _DAILY_PATTERNS):
        return 0

    return None


def _available_columns(db) -> Set[str]:
    rows = db.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='aqar' AND table_name='listings'
            """
        )
    )
    return {r[0] for r in rows}


def _has_pattern(value: Any, patterns: Iterable[str]) -> bool:
    s = str(value or "").lower()
    return any(pat in s for pat in patterns)


def _looks_like_rent(row: Dict[str, Any], rent_flag_cols: Set[str]) -> bool:
    # Prefer the dataset's explicit rent/sale marker when available
    for col in rent_flag_cols:
        if col not in row:
            continue
        val = row.get(col)
        if _has_pattern(val, _RENT_PATTERNS):
            return True
        if _has_pattern(val, _SALE_PATTERNS):
            return False

    # Fallback: conservative heuristic based on the title/description/property_type text
    text_blob = " ".join(
        str(row.get(key, "") or "") for key in ("title", "description", "property_type", "category")
    ).lower()
    if _has_pattern(text_blob, _SALE_PATTERNS):
        return False
    if _has_pattern(text_blob, _COMMERCIAL_OFFICE_KEYWORDS + _COMMERCIAL_RETAIL_KEYWORDS):
        return True
    return _has_pattern(text_blob, _RENT_PATTERNS)


def _asset_and_unit(row: Dict[str, Any]) -> tuple[str, str | None]:
    text_blob = " ".join(
        str(row.get(col) or "")
        for col in (
            "property_type",
            "title",
            "description",
            "category",
            "listing_type",
            "ad_type",
        )
    ).lower()
    if any(keyword in text_blob for keyword in _COMMERCIAL_OFFICE_KEYWORDS):
        return "commercial", "office"
    if any(keyword in text_blob for keyword in _COMMERCIAL_RETAIL_KEYWORDS):
        return "commercial", "retail"
    if any(keyword in text_blob for keyword in ["شقة", "فيلا", "دور", "apartment", "villa", "سكني", "residential"]):
        return "residential", None
    return "commercial", None


def main() -> None:
    db = SessionLocal()
    try:
        db.execute(
            text("DELETE FROM rent_comp WHERE source = :src OR source = :legacy_src"),
            {"src": SOURCE, "legacy_src": LEGACY_SOURCES[0]},
        )
        db.commit()

        available_cols = _available_columns(db)
        select_cols = ["city", "district", "area_sqm", "price_sar"]
        optional_cols = [
            "property_type",
            "title",
            "description",
            "listing_type",
            "price_frequency",
            "rent_period",
            "ad_type",
            "purpose",
            "category",
        ]
        select_cols.extend([c for c in optional_cols if c in available_cols])
        rent_flag_cols = set(c for c in optional_cols if c in available_cols)

        sql = f"""
            SELECT {", ".join(select_cols)}
            FROM aqar.listings
            WHERE area_sqm IS NOT NULL
              AND price_sar IS NOT NULL
              AND area_sqm > 20
              AND price_sar > 250
        """
        rows = db.execute(text(sql)).mappings()

        today = date.today()
        inserted = 0
        for idx, r in enumerate(rows, start=1):
            if not _looks_like_rent(r, rent_flag_cols):
                continue
            area = float(r["area_sqm"])
            if area <= 0:
                continue
            price = float(r["price_sar"])

            city_raw = r.get("city") or ""
            district_raw = r.get("district") or ""
            city_norm = norm_city(city_raw) or "riyadh"
            district_norm = norm_district(city_norm, district_raw) or None
            text_blob = " ".join(
                str(r.get(col) or "") for col in ("property_type", "title", "description", "category")
            )
            asset_type, unit_type = _asset_and_unit(r)

            # Normalize price to a monthly basis when possible (Aqar often lists annual rents).
            period_months = _infer_rent_period_months(r)
            if period_months == 0:
                # Skip daily/short-stay listings for feasibility benchmarking
                continue

            if period_months is None:
                # Last-resort inference based on magnitude (values here are SAR per m² *per period*)
                per_m2_raw = price / area
                if asset_type.lower() == "residential":
                    period_months = 12 if per_m2_raw > 300 else 1
                else:
                    period_months = 12 if per_m2_raw > 800 else 1

            price_monthly = price / float(period_months) if period_months and period_months > 1 else price

            comp = RentComp(
                id=f"{SOURCE}_{idx}",
                date=today,
                asof_date=today,
                city=city_norm,
                district=district_norm,
                asset_type=asset_type,
                unit_type=unit_type,
                lease_term_months=12,
                rent_per_unit=price_monthly,
                rent_per_m2=price_monthly / area,
                source=SOURCE,
                source_url=None,
            )
            db.add(comp)
            inserted += 1

        db.commit()
        print(f"Inserted {inserted} Kaggle rent comps into rent_comp (source='{SOURCE}')")
    finally:
        db.close()


if __name__ == "__main__":
    main()
