.PHONY: api db-up db-down db-init test fmt lint harvest

MS_BUILDINGS_DIR ?= data/ms_buildings

api:
	uvicorn app.main:app --reload --port 8000

db-up:
	docker compose up -d db

db-down:
	docker compose down

db-init:
	alembic upgrade head

test:
	pytest -q

fmt:
	black app tests

lint:
	flake8 app tests

.PHONY: harvest
harvest:
	python -m app.ingest.harvest_open

.PHONY: ingest-real-estate-indices
ingest-real-estate-indices:
	poetry run python -m app.ingest.real_estate_indices

.PHONY: ingest-rega-indicators
ingest-rega-indicators:
	python -m app.ingest.rega_indicators

.PHONY: ingest-ms-buildings
ingest-ms-buildings:
	MS_BUILDINGS_DIR=$(MS_BUILDINGS_DIR) PYTHONPATH=. python -m app.ingest.ms_buildings

.PHONY: ingest-riyadh-urban-parcels
ingest-riyadh-urban-parcels:
	PYTHONPATH=. python -m app.ingest.riyadh_urban_parcels

.PHONY: fetch-ms-buildings-riyadh
fetch-ms-buildings-riyadh:
	PYTHONPATH=. python -m app.ingest.fetch_ms_buildings_riyadh

.PHONY: fetch-ms-buildings-riyadh-links
fetch-ms-buildings-riyadh-links:
	PYTHONPATH=. python -m app.ingest.fetch_ms_buildings_dataset_links --max-files 3
