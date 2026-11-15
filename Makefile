# One command per step. Run `make help` for the list.
-include .env
export

YEAR  ?= 2025
MONTH ?= 1
PY    ?= python
AWS_PROFILE ?= nyc-taxi-pipeline
export PYTHONPATH := src

# Export short-lived credentials from your local AWS profile into the recipe's
# shell, so no key is ever written to the repo or to docker-compose.yml.
AWS_ENV = eval "$$(aws configure export-credentials --profile $(AWS_PROFILE) --format env)"

.PHONY: help install setup-aws polaris-up polaris-bootstrap polaris-down ingest transform \
        schema-evolution time-travel metrics snowflake-sql snowflake-queries dq test all teardown

help:
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-20s %s\n", $$1, $$2}'

install: ## Install Python dependencies
	$(PY) -m pip install -r requirements.txt

setup-aws: ## One-time: bucket + least-privilege IAM (ADMIN_PROFILE=<admin profile>)
	bash infra/setup_aws.sh

polaris-up: ## Start Polaris in Docker
	$(AWS_ENV); docker compose up -d polaris

polaris-bootstrap: ## Create Polaris catalog, principal and roles; writes .polaris.env
	$(PY) -m pipeline.polaris_bootstrap

polaris-down: ## Stop Polaris
	docker compose down

ingest: ## Part 1: download TLC files and upload to s3://$BUCKET/raw/
	$(AWS_ENV); $(PY) -m pipeline.ingest.ingest_to_s3 --year $(YEAR) --month $(MONTH)

transform: ## Part 2 + 3.1: raw -> trips_clean (MERGE) + trips_rejected
	$(AWS_ENV); $(PY) -m pipeline.transform.build_trips_clean --year $(YEAR) --month $(MONTH)

schema-evolution: ## Part 3.2: add is_airport_trip without rewriting data
	$(AWS_ENV); $(PY) -m pipeline.transform.schema_evolution

time-travel: ## Part 3.3: snapshot history, change, time travel, rollback
	$(AWS_ENV); $(PY) -m pipeline.transform.time_travel --year $(YEAR) --month $(MONTH)

metrics: ## Part 3.4: build daily_zone_metrics
	$(AWS_ENV); $(PY) -m pipeline.transform.build_daily_zone_metrics --year $(YEAR) --month $(MONTH)

snowflake-sql: ## Part 4: generate snowflake/generated/*.sql with current metadata paths
	$(AWS_ENV); $(PY) -m pipeline.snowflake.export_snowflake_sql

snowflake-queries: ## Part 4: run snowflake/queries.sql and save results/*.csv
	$(PY) -m pipeline.snowflake.run_queries

dq: ## Part 5: data quality checks (non-zero exit on failure)
	$(AWS_ENV); $(PY) -m pipeline.quality.dq_runner --year $(YEAR) --month $(MONTH)

test: ## Part 5: pytest unit tests (no cloud access needed)
	$(PY) -m pytest

all: ingest transform schema-evolution transform time-travel metrics dq snowflake-sql ## Parts 1-3 + 5 end to end

teardown: ## Delete AWS resources (asks for confirmation)
	bash infra/teardown_aws.sh
