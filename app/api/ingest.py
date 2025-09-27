from datetime import date
import io

import pandas as pd
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from sqlalchemy import and_
from sqlalchemy.orm import Session

from app.db.deps import get_db
from app.models.tables import CostIndexMonthly, Rate, MarketIndicator

router = APIRouter(prefix="/v1/ingest", tags=["ingest"])


def _read_table(upload: UploadFile) -> pd.DataFrame:
    raw = upload.file.read()
    name = (upload.filename or "").lower()
    buf = io.BytesIO(raw)
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(buf)
    return pd.read_csv(io.StringIO(raw.decode("utf-8")), encoding_errors="ignore")


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
