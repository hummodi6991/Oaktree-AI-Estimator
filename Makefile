.PHONY: api db-up db-down db-init test fmt lint harvest

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
	PYTHONPATH=. python -m app.ingest.ms_buildings
