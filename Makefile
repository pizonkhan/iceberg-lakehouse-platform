.PHONY: up down seed build test docs clean orchestrate

COMPOSE = docker compose -f infra/docker-compose.yml --env-file .env
DBT = uv run dbt
DBT_DIRS = --project-dir transform/lakehouse --profiles-dir transform/lakehouse
DAGSTER_HOME = $(CURDIR)/.dagster_home

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

# Starts the Dagster webserver + daemon for local development (orchestration/).
# Requires the stack up (`make up`) and a built dbt manifest
# (transform/lakehouse/target/manifest.json, produced by `make build` or a bare
# `dbt parse`), since the dbt-generated assets read that manifest at load time.
orchestrate:
	mkdir -p $(DAGSTER_HOME)
	DAGSTER_HOME=$(DAGSTER_HOME) uv run dagster dev -m orchestration.definitions
