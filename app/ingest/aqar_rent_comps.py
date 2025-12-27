from __future__ import annotations

from datetime import date
from typing import Any, Dict, Iterable, Set

from sqlalchemy import text

from app.db.session import SessionLocal
from app.ml.name_normalization import norm_city, norm_district
from app.models.tables import RentComp

SOURCE = "kaggle_aqar_rent"

# Conservative rent keywords for heuristics when the dataset lacks an explicit rent/sale flag
_RENT_PATTERNS = ("rent", "for rent", "lease", "إيجار", "ايجار", "استئجار", "للإيجار", "لللايجار")
_SALE_PATTERNS = ("sale", "for sale", "buy", "بيع", "للبيع")


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
        str(row.get(key, "") or "") for key in ("title", "description", "property_type")
    ).lower()
    if _has_pattern(text_blob, _SALE_PATTERNS):
        return False
    return _has_pattern(text_blob, _RENT_PATTERNS)


def _asset_and_unit(property_type: str) -> tuple[str, str | None]:
    pt = (property_type or "").lower()
    if any(k in pt for k in ["apartment", "flat", "شقة"]):
        return "residential", "apartment"
    if any(k in pt for k in ["villa", "house", "بيت"]):
        return "residential", "villa"
    if any(k in pt for k in ["office", "retail", "shop", "store", "commercial", "مكتب", "تجاري"]):
        return "commercial", None
    return "residential", None


def main() -> None:
    db = SessionLocal()
    try:
        db.execute(text("DELETE FROM rent_comp WHERE source = :src"), {"src": SOURCE})
        db.commit()

        available_cols = _available_columns(db)
        select_cols = ["city", "district", "area_sqm", "price_sar"]
        optional_cols = [
            "property_type",
            "title",
            "description",
            "listing_type",
            "price_frequency",
            "ad_type",
            "purpose",
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
            asset_type, unit_type = _asset_and_unit(r.get("property_type") or "")

            comp = RentComp(
                id=f"{SOURCE}_{idx}",
                date=today,
                asof_date=today,
                city=city_norm,
                district=district_norm,
                asset_type=asset_type,
                unit_type=unit_type,
                lease_term_months=12,
                rent_per_unit=price,
                rent_per_m2=price / area,
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
