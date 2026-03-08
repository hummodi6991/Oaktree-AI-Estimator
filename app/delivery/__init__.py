"""
Delivery data pipeline for restaurant-delivery intelligence.

Modules:
- schemas: Pydantic models for structured scraper output
- models: SQLAlchemy ORM models (delivery_source_record, delivery_ingest_run)
- pipeline: Base scraper interface and pipeline orchestration
- parsers: Per-platform parsers extracting structured records
- location: Geographic resolution (coords, district, geocoding)
- resolver: Entity resolution from delivery records to restaurant_poi
- features: Delivery-derived scoring features
- stats: Data quality observability and metrics
"""
