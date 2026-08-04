.PHONY: up down seed build test docs clean

COMPOSE = docker compose -f infra/docker-compose.yml --env-file .env

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down

seed:
	@echo "make seed: not implemented yet (lands with the ingestion work package)" && exit 1

build:
	@echo "make build: not implemented yet (lands with the dbt work package)" && exit 1

test:
	@echo "make test: not implemented yet (lands with the test pyramid work package)" && exit 1

docs:
	@echo "make docs: not implemented yet (lands in the documentation phase)" && exit 1

clean:
	$(COMPOSE) down -v
