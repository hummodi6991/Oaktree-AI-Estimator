from __future__ import annotations

from pathlib import Path
from typing import Any

import math
import pandas as pd
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.models.tables import SaleComp


CSV_PATH = Path("data/riyadh_land_comps_2024_completed.csv")
SOURCE_NAME = "riyadh_land_comps_2024"  # internal source label for these comps


def _coerce_float(v: Any) -> float | None:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    try:
        return float(v)
    except Exception:
        return None


def main() -> None:
    if not CSV_PATH.exists():
        raise SystemExit(f"CSV not found at {CSV_PATH!s}")

    df = pd.read_csv(CSV_PATH)

    # Normalise column names just in case
    df.columns = [c.strip() for c in df.columns]

    session: Session = SessionLocal()
    n = 0

    try:
        for _, r in df.iterrows():
            comp_id = str(r["id"])

            obj = session.get(SaleComp, comp_id)
            data = dict(
                id=comp_id,
                date=pd.to_datetime(r["date"]).date(),
                city=str(r["city"]).strip() if not pd.isna(r["city"]) else None,
                district=str(r["district"]).strip()
                if ("district" in r and not pd.isna(r["district"]))
                else None,
                asset_type=str(r["asset_type"]).strip(),
                net_area_m2=_coerce_float(r.get("net_area_m2")),
                price_total=_coerce_float(r.get("price_total")),
                price_per_m2=_coerce_float(r.get("price_per_m2")),
                # keep the CSV source if present, but tag these rows
                source=str(r.get("source") or SOURCE_NAME),
                source_url=r.get("source_url") if not pd.isna(r.get("source_url")) else None,
                asof_date=pd.to_datetime(r.get("asof_date")).date()
                if not pd.isna(r.get("asof_date"))
                else None,
            )

            if obj:
                for k, v in data.items():
                    setattr(obj, k, v)
            else:
                obj = SaleComp(**data)
                session.add(obj)

            n += 1

        session.commit()
        print(f"Upserted {n} Riyadh land comps into sale_comp (source={SOURCE_NAME!r})")

    finally:
        session.close()


if __name__ == "__main__":
    main()
