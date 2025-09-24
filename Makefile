.PHONY: api db-up db-down db-init test fmt lint

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
