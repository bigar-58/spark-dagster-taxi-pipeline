.PHONY: test lint format check infra-up infra-down infra-logs infra-reset

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