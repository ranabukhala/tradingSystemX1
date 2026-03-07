# ─────────────────────────────────────────────
#  Trading System — Docker Makefile
#  Everything runs in Docker. No local Python needed.
# ─────────────────────────────────────────────

.PHONY: help build up down restart reset logs status ps \
        shell-benzinga shell-postgres shell-redis \
        db-shell topics migrate test lint

# ── Build & Start ─────────────────────────────────────────────

build:        ## Build the Python app Docker image
	docker compose build --no-cache

up:           ## Build image + start full stack
	@echo "🔨 Building image..."
	docker compose build
	@echo "🚀 Starting infrastructure..."
	docker compose up -d postgres redis redpanda minio prometheus grafana adminer
	@echo "⏳ Waiting for infrastructure (30s)..."
	@sleep 30
	@echo "🗂️  Setting up MinIO buckets..."
	docker compose up minio_setup
	@echo "📦 Creating Kafka topics..."
	docker compose up topic_setup
	@echo "🗄️  Running migrations..."
	docker compose up migrate
	@echo "🔌 Starting connectors..."
	docker compose up -d connector_benzinga connector_polygon_news connector_polygon_prices connector_earnings connector_fred connector_alphavantage
	@echo ""
	@echo "✅ Full stack running. Access points:"
	@echo "   Grafana:          http://localhost:3000  (admin / admin123)"
	@echo "   Redpanda Console: http://localhost:8088"
	@echo "   MinIO Console:    http://localhost:9001  (minioadmin / minioadmin123)"
	@echo "   Adminer (DB):     http://localhost:8080"
	@echo "   Prometheus:       http://localhost:9090"

up-infra:     ## Start infrastructure only (no connectors)
	docker compose up -d postgres redis redpanda minio prometheus grafana adminer
	docker compose up minio_setup
	docker compose up topic_setup
	docker compose up migrate

up-connectors: ## Start connectors only (infra must already be running)
	docker compose up -d connector_benzinga connector_polygon_news connector_polygon_prices connector_earnings connector_fred connector_alphavantage

down:          ## Stop all containers (keep data volumes)
	docker compose down

restart:       ## Restart all connector services (picks up code changes)
	docker compose restart connector_benzinga connector_polygon_news connector_polygon_prices connector_earnings connector_fred connector_alphavantage

restart-benzinga:   ## Restart Benzinga connector only
	docker compose restart connector_benzinga

restart-alphavantage: ## Restart Alpha Vantage connector only
	docker compose restart connector_alphavantage

reset:         ## ⚠️  Destroy ALL data and start fresh
	@echo "⚠️  This deletes ALL data (Postgres, Redis, Redpanda, MinIO)."
	@echo "   Press Ctrl+C to cancel or Enter to continue..."
	@read _
	docker compose down -v --remove-orphans
	$(MAKE) up

# ── Status & Logs ────────────────────────────────────────────

ps:           ## Show all container status
	docker compose ps

status:       ## Detailed health of all services
	@echo ""
	@echo "═══════════════════════════════════════"
	@echo "  Container Status"
	@echo "═══════════════════════════════════════"
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
	@echo ""
	@echo "═══════════════════════════════════════"
	@echo "  Kafka Topics"
	@echo "═══════════════════════════════════════"
	@docker exec trading_redpanda rpk topic list --brokers localhost:9092 2>/dev/null || echo "  Redpanda not ready"
	@echo ""

logs:          ## Tail all logs
	docker compose logs -f --tail=50

logs-benzinga: ## Tail Benzinga connector logs
	docker compose logs -f --tail=100 connector_benzinga

logs-prices:   ## Tail price connector logs
	docker compose logs -f --tail=100 connector_polygon_prices

logs-earnings: ## Tail earnings connector logs
	docker compose logs -f --tail=100 connector_earnings

logs-alphavantage: ## Tail Alpha Vantage connector logs
	docker compose logs -f --tail=100 connector_alphavantage

logs-infra:    ## Tail infrastructure logs (postgres, redis, redpanda)
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

# ── Shells ───────────────────────────────────────────────────

shell-benzinga:       ## Open shell in Benzinga container
	docker exec -it trading_connector_benzinga /bin/bash

shell-app:            ## Run one-off Python in app container
	docker compose run --rm connector_benzinga /bin/bash

# ── Testing ──────────────────────────────────────────────────

test:          ## Run tests inside Docker
	docker compose run --rm --entrypoint pytest connector_benzinga tests/ -v --tb=short

test-local:    ## Run tests locally (requires local Python env)
	pytest tests/ -v --tb=short

# ── Utilities ────────────────────────────────────────────────

clean:         ## Remove dangling images and stopped containers
	docker system prune -f

clean-all:     ## Remove all trading system images (forces full rebuild)
	docker compose down --rmi local -v

# ── Help ────────────────────────────────────────────────────

help:          ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
