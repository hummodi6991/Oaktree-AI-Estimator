from datetime import date
import io
import pathlib
import tempfile
import zipfile

import pandas as pd
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, Query
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
)

router = APIRouter(prefix="/v1/ingest", tags=["ingest"])


def _read_table(upload: UploadFile) -> pd.DataFrame:
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
