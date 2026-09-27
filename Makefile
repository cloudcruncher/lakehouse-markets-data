SHELL := /bin/bash
.DEFAULT_GOAL := help
ASSET ?= markets_bronze/fx_rates

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

lint: ## ruff (lint + format check)
	uv run ruff check . && uv run ruff format --check .

test: ## Unit tests (no Docker, no network)
	uv run pytest -q

build: ## Build the code-location image on the platform base image
	docker compose build

run: build ## Materialise one asset against the local platform: make run ASSET=markets_bronze/fx_rates
	docker compose run --rm --entrypoint dagster code asset materialize -m markets_data.definitions --select $(ASSET)

contracts: ## The platform's contract check, as CI runs it (needs ../open-lakehouse)
	uv run --quiet ../open-lakehouse/contracts/check.py --tenant markets-data --dir contracts

.PHONY: help lint test build run contracts
