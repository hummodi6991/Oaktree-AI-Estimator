from sqlalchemy import Column, Integer, String, Date, DateTime, Numeric, Text
from sqlalchemy.dialects.postgresql import JSONB

from app.models.base import Base


class CostIndexMonthly(Base):
    __tablename__ = "cost_index_monthly"

    id = Column(Integer, primary_key=True)
    month = Column(Date, nullable=False)
    sector = Column(String(64), nullable=False)
    cci_index = Column(Numeric(8, 2), nullable=False)
    source_url = Column(String(512))
    asof_date = Column(Date)


class Rate(Base):
    __tablename__ = "rates"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    tenor = Column(String(16), nullable=False)
    rate_type = Column(String(32), nullable=False)
    value = Column(Numeric(6, 3), nullable=False)
    source_url = Column(String(512))


class SaleComp(Base):
    __tablename__ = "sale_comp"

    id = Column(String(64), primary_key=True)
    date = Column(Date, nullable=False)
    city = Column(String(64), nullable=False)
    district = Column(String(128))
    asset_type = Column(String(32), nullable=False)
    net_area_m2 = Column(Numeric(14, 2))
    price_total = Column(Numeric(14, 2))
    price_per_m2 = Column(Numeric(12, 2))
    source = Column(String(64))
    source_url = Column(String(512))
    asof_date = Column(Date)


class RentComp(Base):
    __tablename__ = "rent_comp"

    id = Column(String(64), primary_key=True)
    date = Column(Date, nullable=False)
    city = Column(String(64), nullable=False)
    district = Column(String(128))
    asset_type = Column(String(32), nullable=False)
    unit_type = Column(String(32))
    lease_term_months = Column(Integer)
    rent_per_unit = Column(Numeric(12, 2))
    rent_per_m2 = Column(Numeric(12, 2))
    source = Column(String(64))
    source_url = Column(String(512))
    asof_date = Column(Date)


class Parcel(Base):
    __tablename__ = "parcel"

    id = Column(String(64), primary_key=True)
    gis_polygon = Column(JSONB)
    municipality = Column(String(64))
    district = Column(String(128))
    zoning = Column(String(64))
    far = Column(Numeric(6, 3))
    frontage_m = Column(Numeric(10, 2))
    road_class = Column(String(32))
    setbacks = Column(JSONB)
    source_url = Column(String(512))
    asof_date = Column(Date)


class AssumptionLedger(Base):
    __tablename__ = "assumption_ledger"

    id = Column(Integer, primary_key=True)
    estimate_id = Column(String(64), nullable=False)
    line_id = Column(String(64))
    source_type = Column(String(16), nullable=False)
    source_ref = Column(String(128))
    url = Column(String(512))
    value = Column(Numeric(18, 4))
    unit = Column(String(16))
    owner = Column(String(64))
    created_at = Column(DateTime)


class BoqItem(Base):
    __tablename__ = "boq_item"

    code = Column(String(32), primary_key=True)
    description = Column(String(256), nullable=False)
    uom = Column(String(16), nullable=False, default="m2")
    quantity_per_m2 = Column(Numeric(12, 4), nullable=False, default=1.0)
    baseline_unit_cost = Column(Numeric(12, 2), nullable=False)
    city_factor = Column(Numeric(6, 3), nullable=False, default=1.000)
    volatility_tag = Column(String(32))
    source_url = Column(String(512))


class MarketIndicator(Base):
    __tablename__ = "market_indicator"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    city = Column(String(64), nullable=False)
    asset_type = Column(String(32), nullable=False)
    indicator_type = Column(String(32), nullable=False)
    value = Column(Numeric(12, 2), nullable=False)
    unit = Column(String(16), nullable=False)
    source_url = Column(String(512))
    asof_date = Column(Date)


class EstimateHeader(Base):
    __tablename__ = "estimate_header"

    id = Column(String(36), primary_key=True)
    created_at = Column(DateTime)
    strategy = Column(String(32), nullable=False)
    input_json = Column(Text, nullable=False)
    totals_json = Column(Text, nullable=False)
    notes_json = Column(Text)


class EstimateLine(Base):
    __tablename__ = "estimate_line"

    id = Column(Integer, primary_key=True)
    estimate_id = Column(String(36), nullable=False)
    category = Column(String(32), nullable=False)
    key = Column(String(64), nullable=False)
    value = Column(Numeric(18, 4))
    unit = Column(String(16))
    source_type = Column(String(16))
    url = Column(String(512))
    model_version = Column(String(64))
    owner = Column(String(64))
    created_at = Column(DateTime)
