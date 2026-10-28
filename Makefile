# StreamPulse developer commands. Each target is a thin wrapper around a
# docker compose command, documented in README.md for use without make
# (e.g. from PowerShell).

SHELL := /bin/bash
COMPOSE := docker compose

# Build output should belong to you, not root.
export HOST_UID := $(shell id -u)
export HOST_GID := $(shell id -g)

.DEFAULT_GOAL := help

.PHONY: help up down restart ps logs build-jobs test-jobs-integration smoke-test topics ingest ingest-stop ingest-logs test-producer clean

help: ## List available commands
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-12s %s\n", $$1, $$2}'

.env:
	cp .env.example .env
	@echo "Created .env from .env.example"

up: .env ## Start the platform and wait until every service is healthy
	$(COMPOSE) up -d
	./scripts/wait_for_stack.sh

down: ## Stop the platform (data volumes are kept)
	$(COMPOSE) down

restart: ## Restart all services (data volumes are kept)
	$(COMPOSE) restart

ps: ## Show service status and health
	$(COMPOSE) ps -a

logs: ## Follow logs of all services (Ctrl+C to stop)
	$(COMPOSE) logs -f --tail=100

build-jobs: .env ## Build and unit-test the Flink job jar (in a Maven container)
	$(COMPOSE) --profile build run --rm flink-jobs-build

test-jobs-integration: .env ## Flink pipeline on a mini-cluster against the stack's Kafka (needs up)
	$(COMPOSE) --profile build run --rm -e STREAMPULSE_TEST_KAFKA=kafka:9092 flink-jobs-build \
		mvn -B -ntp -Dmaven.repo.local=/workspace/flink-jobs/.m2/repository \
		-Dsurefire.excludedGroups=none -Dsurefire.groups=integration verify

smoke-test: ## Verify Kafka -> Flink -> checkpoints end to end (needs up + build-jobs)
	./scripts/smoke_test.sh

ingest: ## Start live ingestion from Wikimedia into Kafka (needs up)
	$(COMPOSE) --profile live up -d --build producer

ingest-stop: ## Stop live ingestion gracefully (flushes in-flight events)
	$(COMPOSE) --profile live stop producer

ingest-logs: ## Follow the producer's structured logs
	$(COMPOSE) --profile live logs -f --tail=50 producer

test-producer: ## Lint + unit tests + integration tests of the producer (integration needs up)
	cd producer && uv sync --locked && uv run ruff check . && uv run ruff format --check . && uv run pytest

topics: ## Describe the StreamPulse Kafka topics
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe

clean: ## Stop the platform and DELETE all data volumes (make clean CONFIRM=yes)
	@if [ "$(CONFIRM)" != "yes" ]; then \
		echo "This deletes Kafka data, the database and Flink checkpoints. Re-run: make clean CONFIRM=yes"; \
		exit 1; \
	fi
	$(COMPOSE) down -v --remove-orphans
	rm -rf flink-jobs/target flink-jobs/dist/*.jar
