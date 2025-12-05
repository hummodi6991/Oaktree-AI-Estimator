from datetime import date
import io
import pathlib
import tempfile
import zipfile
from importlib.util import find_spec
from typing import List

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_
from sqlalchemy.orm import Session
import shapefile
from shapely.geometry import shape as shapely_shape

from app.db.deps import get_db
from app.models.tables import (
    CostIndexMonthly,
    Rate,
    MarketIndicator,
    RentComp,
    SaleComp,
    ExternalFeature,
    LandUseStat,
    FarRule,
    TaxRule,
)

router = APIRouter(prefix="/v1/ingest", tags=["ingest"])

multipart_available = bool(find_spec("multipart"))


def _multipart_not_installed() -> None:
    raise HTTPException(
        status_code=501,
        detail=(
            "File upload endpoints are disabled because python-multipart is not installed."
        ),
    )


def _read_table(upload) -> pd.DataFrame:
    raw = upload.file.read()
    name = (upload.filename or "").lower()
    buf = io.BytesIO(raw)
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(buf)
    return pd.read_csv(io.StringIO(raw.decode("utf-8")), encoding_errors="ignore")


def _clean_optional(value):
    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return value


def _coerce_date(value, default: date | None = None) -> date | None:
    if value is None:
        return default
    try:
        text = str(value).strip()
    except Exception:
        return default
    if not text:
        return default
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return default


def _coerce_float(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


if multipart_available:
    from fastapi import File, UploadFile

    @router.post("/cci")
    def ingest_cci(
        file: UploadFile = File(...),
        sector: str = "construction",
        db: Session = Depends(get_db),
    ):
        """
        Columns expected: month (YYYY-MM or YYYY-MM-DD), cci_index, [source_url]
        """
        df = _read_table(file)
        required = {"month", "cci_index"}
        if not required.issubset({c.lower() for c in df.columns}):
            raise HTTPException(400, f"Missing columns; need {required}")
        # normalize
        df.columns = [c.lower() for c in df.columns]
        upserted = 0
        for _, r in df.iterrows():
            m = str(r["month"])[:10]
            d = date.fromisoformat(m[:7] + "-01") if len(m) >= 7 else None
            if not d:
                continue
            obj = db.query(CostIndexMonthly).filter_by(month=d, sector=sector).first()
            if obj:
                obj.cci_index = float(r["cci_index"])
                obj.source_url = r.get("source_url")
            else:
                db.add(
                    CostIndexMonthly(
                        month=d,
                        sector=sector,
                        cci_index=float(r["cci_index"]),
                        source_url=r.get("source_url"),
                    )
                )
            upserted += 1
        db.commit()
        return {"status": "ok", "rows": int(upserted)}


    @router.post("/comps")
    def ingest_comps(
        file: UploadFile = File(...),
        comp_type: str = "sale",
        db: Session = Depends(get_db),
    ):
        """
        Columns (sale): id,date,city,district,asset_type,net_area_m2,price_total,price_per_m2,source,source_url,asof_date
        Columns (rent): id,date,city,district,asset_type,unit_type,lease_term_months,rent_per_unit,rent_per_m2,source,source_url,asof_date
        """

        df = _read_table(file)
        df.columns = [c.lower() for c in df.columns]

        required_sale = {"id", "date", "city", "asset_type", "price_per_m2"}
        required_rent = {"id", "date", "city", "asset_type", "rent_per_m2"}

        is_sale = comp_type.lower() == "sale"
        required = required_sale if is_sale else required_rent
        missing = required - set(df.columns)
        if missing:
            raise HTTPException(400, f"Missing columns: {sorted(missing)}")

        upserted = 0
        for _, r in df.iterrows():
            comp_id = str(r["id"])
            comp_date = date.fromisoformat(str(r["date"])[:10])
            city = str(r["city"])
            asset_type = str(r["asset_type"])
            asof_value = _clean_optional(r.get("asof_date"))
            asof_date = (
                date.fromisoformat(str(asof_value)[:10]) if asof_value is not None else None
            )

            if is_sale:
                obj = db.get(SaleComp, comp_id)
                data = dict(
                    id=comp_id,
                    date=comp_date,
                    city=city,
                    district=_clean_optional(r.get("district")),
                    asset_type=asset_type,
                    net_area_m2=_clean_optional(r.get("net_area_m2")),
                    price_total=_clean_optional(r.get("price_total")),
                    price_per_m2=_clean_optional(r.get("price_per_m2")),
                    source=_clean_optional(r.get("source")),
                    source_url=_clean_optional(r.get("source_url")),
                    asof_date=asof_date,
                )
                if obj:
                    for key, value in data.items():
                        setattr(obj, key, value)
                else:
                    db.add(SaleComp(**data))
            else:
                obj = db.get(RentComp, comp_id)
                data = dict(
                    id=comp_id,
                    date=comp_date,
                    city=city,
                    district=_clean_optional(r.get("district")),
                    asset_type=asset_type,
                    unit_type=_clean_optional(r.get("unit_type")),
                    lease_term_months=_clean_optional(r.get("lease_term_months")),
                    rent_per_unit=_clean_optional(r.get("rent_per_unit")),
                    rent_per_m2=_clean_optional(r.get("rent_per_m2")),
                    source=_clean_optional(r.get("source")),
                    source_url=_clean_optional(r.get("source_url")),
                    asof_date=asof_date,
                )
                if obj:
                    for key, value in data.items():
                        setattr(obj, key, value)
                else:
                    db.add(RentComp(**data))
            upserted += 1

        db.commit()
        return {"status": "ok", "rows": int(upserted)}


    @router.post("/rates")
    def ingest_rates(
        file: UploadFile = File(...),
        rate_type: str = "SAMA_base",
        tenor: str = "overnight",
        db: Session = Depends(get_db),
    ):
        """
        Columns expected: date (YYYY-MM-DD), value, [tenor], [rate_type], [source_url]
        """
        df = _read_table(file)
        df.columns = [c.lower() for c in df.columns]
        if "date" not in df.columns or "value" not in df.columns:
            raise HTTPException(400, "Missing columns: date, value")
        upserted = 0
        for _, r in df.iterrows():
            d = date.fromisoformat(str(r["date"])[:10])
            t = str(r.get("tenor") or tenor)
            rt = str(r.get("rate_type") or rate_type)
            obj = (
                db.query(Rate)
                .filter(and_(Rate.date == d, Rate.tenor == t, Rate.rate_type == rt))
                .first()
            )
            if obj:
                obj.value = float(r["value"])
                obj.source_url = r.get("source_url")
            else:
                db.add(
                    Rate(
                        date=d,
                        tenor=t,
                        rate_type=rt,
                        value=float(r["value"]),
                        source_url=r.get("source_url"),
                    )
                )
            upserted += 1
        db.commit()
        return {"status": "ok", "rows": int(upserted)}


    @router.post("/shapefile")
    def ingest_shapefile(
        file: UploadFile = File(...),
        layer: str = Query(default="default"),
        db: Session = Depends(get_db),
    ):
        """Ingest features from an uploaded shapefile (.zip or raw .shp)."""

        raw = file.file.read()
        if not raw:
            raise HTTPException(400, "Empty upload")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = pathlib.Path(tmpdir) / (file.filename or "upload.bin")
            tmp_path.write_bytes(raw)

            if zipfile.is_zipfile(tmp_path):
                with zipfile.ZipFile(tmp_path) as zf:
                    zf.extractall(tmpdir)
                shp_candidates = list(pathlib.Path(tmpdir).rglob("*.shp"))
                if not shp_candidates:
                    raise HTTPException(400, "Zip does not contain a .shp file")
                shp_path = str(shp_candidates[0])
            else:
                if tmp_path.suffix.lower() != ".shp":
                    raise HTTPException(
                        400, "Upload a .zip containing the shapefile components"
                    )
                shp_path = str(tmp_path)

            try:
                reader = shapefile.Reader(shp_path)
            except Exception as exc:  # pragma: no cover - passthrough of library errors
                raise HTTPException(400, f"Could not read shapefile: {exc}") from exc

            fields = [f[0] for f in reader.fields[1:]]
            upserted = 0
            feature_type = (reader.shapeTypeName or "").lower()

            for record in reader.iterShapeRecords():
                try:
                    props = record.record.as_dict()
                except Exception:  # pragma: no cover - fallback for old pyshp
                    props = dict(zip(fields, list(record.record)))

                geometry_geojson = record.shape.__geo_interface__
                try:
                    geom = shapely_shape(geometry_geojson)
                    if not geom.is_valid:
                        geom = geom.buffer(0)
                    geometry_geojson = geom.__geo_interface__
                except Exception:
                    pass

                db.add(
                    ExternalFeature(
                        layer_name=layer,
                        feature_type=feature_type
                        or geometry_geojson.get("type", "").lower(),
                        geometry=geometry_geojson,
                        properties=props,
                        source=file.filename,
                    )
                )
                upserted += 1

            db.commit()

        return {
            "status": "ok",
            "layer": layer,
            "rows": int(upserted),
            "feature_type": feature_type,
        }


    @router.post("/shapefile/components")
    def ingest_shapefile_components(
        files: List[UploadFile] = File(...),
        layer: str = Query(default="default"),
        db: Session = Depends(get_db),
    ):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = pathlib.Path(tmpdir)
            for upload in files:
                if not upload:
                    continue
                filename = upload.filename or "blob"
                (tmp_path / filename).write_bytes(upload.file.read())

            shp_candidates = list(tmp_path.rglob("*.shp"))
            if not shp_candidates:
                raise HTTPException(400, "No .shp among uploaded files")

            reader = shapefile.Reader(str(shp_candidates[0]))
            fields = [f[0] for f in reader.fields[1:]]
            upserted = 0
            feature_type = (reader.shapeTypeName or "").lower()

            for rec in reader.iterShapeRecords():
                try:
                    props = rec.record.as_dict()
                except Exception:  # pragma: no cover - fallback for old pyshp
                    props = dict(zip(fields, list(rec.record)))

                geometry_geojson = rec.shape.__geo_interface__
                try:
                    geom = shapely_shape(geometry_geojson)
                    if not geom.is_valid:
                        geom = geom.buffer(0)
                    geometry_geojson = geom.__geo_interface__
                except Exception:
                    pass

                db.add(
                    ExternalFeature(
                        layer_name=layer,
                        feature_type=feature_type
                        or geometry_geojson.get("type", "").lower(),
                        geometry=geometry_geojson,
                        properties=props,
                        source="multipart",
                    )
                )
                upserted += 1

            db.commit()

        return {
            "status": "ok",
            "layer": layer,
            "rows": int(upserted),
            "feature_type": feature_type,
        }


    @router.post("/indicators")
    def ingest_indicators(
        file: UploadFile = File(...),
        db: Session = Depends(get_db),
    ):
        """
        Columns expected: date, city, asset_type, indicator_type (rent_per_m2|sale_price_per_m2), value, unit, [source_url]
        """
        df = _read_table(file)
        need = {"date", "city", "asset_type", "indicator_type", "value", "unit"}
        df.columns = [c.lower() for c in df.columns]
        if not need.issubset(df.columns):
            raise HTTPException(400, f"Missing columns: {need}")
        upserted = 0
        for _, r in df.iterrows():
            d = date.fromisoformat(str(r["date"])[:10])
            row = (
                db.query(MarketIndicator)
                .filter_by(
                    date=d,
                    city=str(r["city"]),
                    asset_type=str(r["asset_type"]),
                    indicator_type=str(r["indicator_type"]),
                )
                .first()
            )
            if row:
                row.value = float(r["value"])
                row.unit = str(r["unit"])
                row.source_url = r.get("source_url")
            else:
                db.add(
                    MarketIndicator(
                        date=d,
                        city=str(r["city"]),
                        asset_type=str(r["asset_type"]),
                        indicator_type=str(r["indicator_type"]),
                        value=float(r["value"]),
                        unit=str(r["unit"]),
                        source_url=r.get("source_url"),
                    )
                )
            upserted += 1
        db.commit()
        return {"status": "ok", "rows": int(upserted)}


    @router.post("/land_use")
    def ingest_land_use(
        file: UploadFile = File(...),
        city: str = "Riyadh",
        db: Session = Depends(get_db),
    ):
        """Ingest reshaped land-use statistics."""

        df = _read_table(file)
        df.columns = [c.lower() for c in df.columns]
        required = {"date", "sub_municipality_en", "category_en", "metric_name_en", "unit", "value"}
        missing = required - set(df.columns)
        if missing:
            raise HTTPException(400, f"Missing columns: {sorted(missing)}")

        inserted = 0
        for _, row in df.iterrows():
            record_date = _coerce_date(row.get("date"), default=date(2019, 1, 1))
            raw_value = row.get("value")
            value = None
            if raw_value is not None and not pd.isna(raw_value):
                try:
                    value = float(raw_value)
                except (TypeError, ValueError):
                    value = None
            stat = LandUseStat(
                date=record_date or date(2019, 1, 1),
                city=city,
                sub_municipality=_clean_optional(row.get("sub_municipality_en")),
                category=_clean_optional(row.get("category_en")),
                metric=_clean_optional(row.get("metric_name_en")),
                unit=_clean_optional(row.get("unit")),
                value=value,
                source_url=_clean_optional(row.get("source_url")),
            )
            db.add(stat)
            inserted += 1

        db.commit()
        return {"status": "ok", "rows": int(inserted)}


    @router.post("/far_rules")
    def ingest_far_rules(
        file: UploadFile = File(...),
        city_default: str = "Riyadh",
        db: Session = Depends(get_db),
    ):
        """
        Ingest district-level FAR rules.
        Required columns: district, far_max
        Optional columns: city, zoning, road_class, frontage_min_m, asof_date, source_url
        Accepts CSV or Excel.
        """

        df = _read_table(file)
        df.columns = [c.lower() for c in df.columns]
        required = {"district", "far_max"}
        missing = required - set(df.columns)
        if missing:
            raise HTTPException(400, f"Missing columns: {sorted(missing)}")

        upserted = 0
        for _, r in df.iterrows():
            city = (r.get("city") or city_default or "Riyadh")
            district = str(r.get("district") or "").strip()
            if not district:
                continue
            data = dict(
                city=city,
                district=district,
                zoning=_clean_optional(r.get("zoning")),
                road_class=_clean_optional(r.get("road_class")),
                frontage_min_m=_coerce_float(r.get("frontage_min_m")),
                far_max=_coerce_float(r.get("far_max")) or 0.0,
                asof_date=_coerce_date(r.get("asof_date")),
                source_url=_clean_optional(r.get("source_url")),
            )
            q = db.query(FarRule).filter(
                FarRule.city == data["city"],
                FarRule.district == data["district"],
                FarRule.zoning.is_(data["zoning"]) if data["zoning"] is None else FarRule.zoning == data["zoning"],
                FarRule.road_class.is_(data["road_class"]) if data["road_class"] is None else FarRule.road_class == data["road_class"],
                FarRule.frontage_min_m.is_(data["frontage_min_m"]) if data["frontage_min_m"] is None else FarRule.frontage_min_m == data["frontage_min_m"],
            )
            obj = q.first()
            if obj:
                for k, v in data.items():
                    setattr(obj, k, v)
            else:
                db.add(FarRule(**data))
            upserted += 1
        db.commit()
        return {"status": "ok", "rows": int(upserted)}


    @router.post("/tax_rules")
    def ingest_tax_rules(
        file: UploadFile = File(...),
        db: Session = Depends(get_db),
    ):
        """
        Ingest tax rules (starting with Saudi RETT).

        Required columns:
          - rule_id
          - tax_type
          - rate

        Optional columns:
          - base_type
          - payer_default
          - exemptions
          - notes

        Accepts CSV or Excel.
        """

        df = _read_table(file)
        df.columns = [c.lower() for c in df.columns]
        required = {"rule_id", "tax_type", "rate"}
        missing = required - set(df.columns)
        if missing:
            raise HTTPException(400, f"Missing columns: {sorted(missing)}")

        def _coerce_int(val):
            if val is None:
                return None
            try:
                return int(float(str(val).replace(",", "").strip()))
            except Exception:
                return None

        upserted = 0
        for _, r in df.iterrows():
            tax_type = str(r.get("tax_type") or "").strip()
            if not tax_type:
                continue

            data = dict(
                rule_id=_coerce_int(r.get("rule_id")) or 0,
                tax_type=tax_type,
                rate=_coerce_float(r.get("rate")) or 0.0,
                base_type=_clean_optional(r.get("base_type")),
                payer_default=_clean_optional(r.get("payer_default")),
                exemptions=_clean_optional(r.get("exemptions")),
                notes=_clean_optional(r.get("notes")),
            )

            q = db.query(TaxRule).filter(
                TaxRule.rule_id == data["rule_id"],
                TaxRule.tax_type == data["tax_type"],
            )
            obj = q.first()
            if obj:
                for k, v in data.items():
                    setattr(obj, k, v)
            else:
                db.add(TaxRule(**data))
            upserted += 1

        db.commit()
        return {"status": "ok", "rows": int(upserted)}

else:

    @router.post("/cci")
    async def ingest_cci_unavailable():
        _multipart_not_installed()

    @router.post("/comps")
    async def ingest_comps_unavailable():
        _multipart_not_installed()

    @router.post("/rates")
    async def ingest_rates_unavailable():
        _multipart_not_installed()

    @router.post("/shapefile")
    async def ingest_shapefile_unavailable():
        _multipart_not_installed()

    @router.post("/shapefile/components")
    async def ingest_shapefile_components_unavailable():
        _multipart_not_installed()

    @router.post("/indicators")
    async def ingest_indicators_unavailable():
        _multipart_not_installed()

    @router.post("/land_use")
    async def ingest_land_use_unavailable():
        _multipart_not_installed()

    @router.post("/far_rules")
    async def ingest_far_rules_unavailable():
        _multipart_not_installed()

    @router.post("/tax_rules")
    async def ingest_tax_rules_unavailable():
        _multipart_not_installed()
