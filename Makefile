.PHONY: test lint format check infra-up infra-down infra-logs infra-reset publish-gold dagster-check dagster-dev

test: 
	pytest

lint: 
	ruff check . 

format: 
	ruff format . 

checks: lint testdocker

infra-up:
	docker compose up -d

infra-down:
	docker compose down

infra-logs:
	docker compose logs -f

infra-reset:
	docker compose down -v

publish-gold:
	@set -a; . ./.env; set +a; \
	python -m taxi_pipeline.run_publish_gold

dagster-check:
	@set -a; . ./.env; set +a; \
	dg check defs

dagster-dev:
	@mkdir -p .dagster
	@set -a; . ./.env; set +a; \
	DAGSTER_HOME="$(CURDIR)/.dagster" dg dev