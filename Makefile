.PHONY: up down generate seed build test docs clean orchestrate

COMPOSE = docker compose -f infra/docker-compose.yml --env-file .env
DBT = uv run dbt
DBT_DIRS = --project-dir transform/lakehouse --profiles-dir transform/lakehouse
DAGSTER_HOME = $(CURDIR)/.dagster_home

# generation/output/ is gitignored (3GB of derived data), so a fresh clone has
# none of it. full is the scale every dimension/fact model in this project has
# actually been built and verified against (see .notes/decisions.md); override
# with `make generate SCALE=small` for a fast local smoke run.
SCALE ?= full

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down

# Idempotent by design: skips regeneration if generation/output/ is already
# populated (checked via its own run_summary.json), so repeat `make seed`
# runs during normal development don't pay the full-scale generation cost
# again. Remove generation/output/ (or run `make generate` directly) to force
# a fresh run, for example after changing SCALE.
generate:
	@if [ -f generation/output/run_summary.json ]; then \
		echo "generation/output/ already populated, skipping (rm -rf generation/output to force regeneration)"; \
	else \
		uv run python -m generation.generate --scale $(SCALE); \
	fi

seed: up generate
	uv run python -m ingestion.pipeline

# trino is the real target: every dimension and fact model in this project has
# only ever been built and verified against the live Nessie/MinIO-backed
# warehouse through Trino. The duckdb target exists in profiles.yml but is not
# wired up to read the Nessie REST catalog and fails outright against every
# real model (see .notes/decisions.md); it is not a working substitute today.
build:
	$(DBT) build $(DBT_DIRS) --target trino

# Assumes `make build` has already populated dev_dimensions/dev_facts/dev_silver
# on the trino target: tests/integration queries that real, already-built gold
# data rather than building it itself (see tests/integration/conftest.py), and
# `dbt test` here audits the models `dbt build` already materialized rather than
# rebuilding them, matching the write (dbt run) / audit (dbt test) split
# ops/wap.py already uses for the same reason.
test:
	uv run pytest tests/unit tests/integration
	$(DBT) test $(DBT_DIRS) --target trino

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
