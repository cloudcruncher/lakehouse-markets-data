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

feed: build ## Run the Coinbase producer against the local platform's Kafka (Ctrl-C stops it)
	docker compose run --rm coinbase

stream: build ## Run the trades stream against the local platform (Ctrl-C stops it; resumes from /state)
	docker compose run --rm trades-stream

card-stream: build ## Run the card-auths stream -> markets_bronze.card_auths, markets_silver.card_auths (resumes from /state)
	docker compose run --rm card-auths-stream

card-auths: ## Run the card-authorisation generator against the local platform's Kafka (needs the licence)
	docker compose build card-auths && docker compose run --rm card-auths

card-auths-sample: ## Print 5 generated card authorisations instead of sending them (needs the licence)
	docker compose build -q card-auths && SHADOWTRAFFIC_ARGS="--stdout --sample 5" docker compose run --rm card-auths

spark-check: build ## Both streams' Spark transforms on sample records, inside the image (no platform needed)
	for check in check_stream.py check_card_auths.py; do \
	  docker run --rm -v "$$PWD/tests/spark:/checks:ro" --entrypoint /opt/spark/bin/spark-submit \
	    ghcr.io/cloudcruncher/lakehouse-markets-data:dev --master "local[1]" /checks/$$check || exit 1; \
	done

contracts: ## The platform's contract check, as CI runs it (needs ../open-lakehouse)
	uv run --quiet ../open-lakehouse/contracts/check.py --tenant markets-data --dir contracts

.PHONY: help lint test build run feed card-stream card-auths card-auths-sample stream spark-check contracts
