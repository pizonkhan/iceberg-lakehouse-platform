.PHONY: up down seed build test docs clean

COMPOSE = docker compose -f infra/docker-compose.yml --env-file .env
DBT = uv run dbt
DBT_DIRS = --project-dir transform/lakehouse --profiles-dir transform/lakehouse

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down

seed: up
	uv run python -m ingestion.pipeline

build:
	$(DBT) build $(DBT_DIRS) --target duckdb

test:
	@echo "make test: not implemented yet (lands with the test pyramid work package)" && exit 1

docs:
	@echo "make docs: not implemented yet (lands in the documentation phase)" && exit 1

clean:
	$(COMPOSE) down -v
