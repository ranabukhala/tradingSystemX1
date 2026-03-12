# ─────────────────────────────────────────────
#  Trading System — Docker Makefile
#  Everything runs in Docker. No local Python needed.
# ─────────────────────────────────────────────

.PHONY: help build up down restart reset logs status ps \
        shell-benzinga shell-postgres shell-redis \
        db-shell topics migrate test lint release \
        up-infra up-connectors up-pipeline up-signals up-execution

# ── Build & Start ─────────────────────────────────────────────

build:        ## Build the Python app Docker image
	docker compose build --no-cache

up:           ## Build image + start full stack
	@echo "Building image..."
	docker compose build
	@echo "Starting infrastructure..."
	docker compose --profile infra up -d
	@echo "Waiting for infrastructure (30s)..."
	@sleep 30
	@echo "Setting up MinIO buckets..."
	docker compose up minio_setup
	@echo "Creating Kafka topics..."
	docker compose up topic_setup
	@echo "Running migrations..."
	docker compose up migrate
	@echo "Starting all services..."
	docker compose --profile connectors --profile pipeline --profile signals --profile execution up -d
	@echo ""
	@echo "Full stack running. Access points:"
	@echo "   Grafana:          http://localhost:3000  (admin / admin123)"
	@echo "   Redpanda Console: http://localhost:8088"
	@echo "   MinIO Console:    http://localhost:9001  (minioadmin / minioadmin123)"
	@echo "   Adminer (DB):     http://localhost:8080"
	@echo "   Prometheus:       http://localhost:9090"

up-infra:     ## Start infrastructure only
	docker compose --profile infra up -d
	docker compose up minio_setup
	docker compose up topic_setup
	docker compose up migrate

up-connectors: ## Start connectors only (infra must be running)
	docker compose --profile connectors up -d

up-pipeline:  ## Start pipeline services only
	docker compose --profile pipeline up -d

up-signals:   ## Start signal services only
	docker compose --profile signals up -d

up-execution: ## Start execution engine only
	docker compose --profile execution up -d

down:          ## Stop all containers (keep data volumes)
	docker compose --profile '*' down

restart:       ## Restart all connector services (picks up code changes)
	docker compose restart connector_benzinga connector_polygon_news connector_polygon_prices connector_earnings connector_fred

reset:         ## Destroy ALL data and start fresh
	@echo "This deletes ALL data (Postgres, Redis, Redpanda, MinIO)."
	@echo "   Press Ctrl+C to cancel or Enter to continue..."
	@read _
	docker compose --profile '*' down -v --remove-orphans
	$(MAKE) up

# ── Status & Logs ────────────────────────────────────────────

ps:           ## Show all container status
	docker compose ps

status:       ## Detailed health of all services
	@echo ""
	@echo "=== Container Status ==="
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
	@echo ""
	@echo "=== Kafka Topics ==="
	@docker exec trading_redpanda rpk topic list --brokers localhost:9092 2>/dev/null || echo "  Redpanda not ready"
	@echo ""

logs:          ## Tail all logs
	docker compose logs -f --tail=50

logs-benzinga: ## Tail Benzinga connector logs
	docker compose logs -f --tail=100 connector_benzinga

logs-prices:   ## Tail price connector logs
	docker compose logs -f --tail=100 connector_polygon_prices

logs-infra:    ## Tail infrastructure logs
	docker compose logs -f --tail=50 postgres redis redpanda

# ── Database ─────────────────────────────────────────────────

db-shell:      ## Open psql shell inside postgres container
	docker exec -it trading_postgres psql -U trading -d trading_db

migrate:       ## Run Alembic migrations
	docker compose run --rm migrate

migrate-new:   ## Create new migration: make migrate-new name=add_column
	docker compose run --rm migrate revision --autogenerate -m "$(name)"

redis-cli:     ## Open redis-cli
	docker exec -it trading_redis redis-cli

# ── Kafka ────────────────────────────────────────────────────

topics:        ## List topics + message counts
	docker exec trading_redpanda rpk topic list --brokers localhost:9092 --print-watermarks

topic-peek:    ## Peek at latest messages: make topic-peek t=news.raw
	docker exec trading_redpanda rpk topic consume $(t) --brokers localhost:9092 --num 5 --offset end

# ── DLQ Tools ────────────────────────────────────────────────

dlq-inspect:   ## Inspect DLQ messages: make dlq-inspect t=news.raw.dlq
	docker compose --profile infra --profile connectors run --no-deps --rm --entrypoint python connector_benzinga -m app.dlq inspect $(t)

dlq-replay:    ## Replay DLQ message by event_id: make dlq-replay id=<event_id> t=news.raw.dlq
	docker compose --profile infra --profile connectors run --no-deps --rm --entrypoint python connector_benzinga -m app.dlq replay $(t) --event-id $(id)

# ── Shells ───────────────────────────────────────────────────

shell-app:            ## Run one-off Python in app container
	docker compose run --rm --entrypoint /bin/bash connector_benzinga

# ── Testing ──────────────────────────────────────────────────

test:          ## Run all tests (requires infra up, or use test-unit)
	docker compose --profile infra --profile connectors run --rm --entrypoint pytest connector_benzinga tests/ -v --tb=short

test-unit:     ## Run unit tests only (no Kafka/Postgres needed; --no-deps skips starting infra)
	docker compose --profile infra --profile connectors build connector_benzinga 2>/dev/null || true
	docker compose --profile infra --profile connectors run --no-deps --rm --entrypoint pytest connector_benzinga tests/test_idempotency.py tests/test_schemas.py tests/test_execution_safety.py -v --tb=short

test-local:    ## Run tests locally (requires Python 3.11+)
	pytest tests/ -v --tb=short

# ── Release ──────────────────────────────────────────────────

release:       ## Create clean release archive (no secrets, no .git)
	@mkdir -p release
	@tar --create --gzip \
		--exclude='.env' \
		--exclude='.env.*' \
		--exclude='!.env.example' \
		--exclude='.git' \
		--exclude='.venv' \
		--exclude='venv' \
		--exclude='__pycache__' \
		--exclude='*.pyc' \
		--exclude='.idea' \
		--exclude='.vscode' \
		--exclude='*.db' \
		--exclude='*.sqlite' \
		--exclude='release' \
		--exclude='data' \
		--exclude='logs' \
		--exclude='node_modules' \
		-f release/trading-system-$$(date +%Y%m%d-%H%M%S).tar.gz .
	@echo "Release archive created in release/"

# ── Utilities ────────────────────────────────────────────────

clean:         ## Remove dangling images and stopped containers
	docker system prune -f

clean-all:     ## Remove all trading system images (forces full rebuild)
	docker compose --profile '*' down --rmi local -v

# ── Help ────────────────────────────────────────────────────

help:          ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
