# US Equities News Intelligence System
### Phase 1 — Full Docker Deployment

---

## Prerequisites

Only two things needed on your machine:

```
Docker Desktop   https://www.docker.com/products/docker-desktop/
Git              https://git-scm.com/
```

**Docker Desktop resource settings** (important — defaults are too low):
- Settings → Resources → Memory: **12GB minimum**
- Settings → Resources → CPUs: **4 minimum**
- Settings → Resources → Disk: **60GB**

---

## First-Time Setup

```bash
# 1. Copy env file and add your API keys
cp .env.example .env

# Open .env and fill in:
#   BENZINGA_API_KEY=...
#   POLYGON_API_KEY=...
#   EARNINGS_WHISPERS_API_KEY=...
#   FRED_API_KEY=...        ← free at fred.stlouisfed.org

# 2. Build and start everything
make up
```

That's it. `make up` handles:
- Building the Python Docker image
- Starting all infrastructure (Postgres, Redis, Redpanda, MinIO, Grafana, Prometheus)
- Creating MinIO buckets
- Creating all 8 Kafka topics
- Running database migrations
- Starting all 5 connector services

---

## Access Points

| Service | URL | Login |
|---|---|---|
| **Grafana** (dashboards) | http://localhost:3000 | admin / admin123 |
| **Redpanda Console** (Kafka UI) | http://localhost:8088 | — |
| **MinIO Console** (object storage) | http://localhost:9001 | minioadmin / minioadmin123 |
| **Adminer** (database browser) | http://localhost:8080 | Server: postgres, User: trading, Pass: tradingpass |
| **Prometheus** (raw metrics) | http://localhost:9090 | — |

---

## Daily Commands

```bash
make up              # Start full stack (first time or after reset)
make down            # Stop everything (data preserved)
make restart         # Restart connectors after code change
make status          # Health check + topic list
make logs            # Tail all logs
make logs-benzinga   # Tail Benzinga connector only
make topics          # Show Kafka topics + message counts
make db-shell        # Open psql
make reset           # ⚠️  Wipe all data and restart fresh
```

---

## How Code Changes Work

The `app/` directory is **volume-mounted** into every connector container. This means:

```bash
# Edit any file in app/
vim app/connectors/benzinga.py

# Restart that connector to pick up changes
make restart-benzinga

# No rebuild needed — code change takes effect immediately
```

If you change `pyproject.toml` (add a new dependency), you need to rebuild:
```bash
make build
make restart
```

---

## Container Architecture

```
Docker Network: trading_net
│
├── INFRASTRUCTURE
│   ├── trading_postgres          :5432  — Primary database (pgvector)
│   ├── trading_redis             :6379  — Dedup cache
│   ├── trading_redpanda          :9092  — Kafka-compatible message queue
│   ├── trading_minio             :9000  — S3-compatible object storage
│   ├── trading_prometheus        :9090  — Metrics collection
│   ├── trading_grafana           :3000  — Dashboards
│   └── trading_adminer           :8080  — DB browser
│
├── INIT (run once, exit)
│   ├── trading_minio_setup              — Creates MinIO buckets
│   ├── trading_topic_setup              — Creates Kafka topics
│   └── trading_migrate                  — Runs Alembic migrations
│
└── CONNECTORS (always running)
    ├── trading_connector_benzinga    :8001/metrics
    ├── trading_connector_polygon_news :8002/metrics
    ├── trading_connector_polygon_prices :8003/metrics
    ├── trading_connector_earnings    :8004/metrics
    └── trading_connector_fred        :8005/metrics
```

---

## Kafka Topics

| Topic | Retention | Producer |
|---|---|---|
| `news.raw` | 24h | Benzinga, Polygon news |
| `news.raw.dlq` | 7d | Failed payloads |
| `news.normalized` | 48h | Normalizer (Phase 2) |
| `news.deduped` | 48h | Deduplicator (Phase 2) |
| `news.enriched` | 7d | Entity resolver (Phase 2) |
| `news.summarized` | 7d | AI agents (Phase 3) |
| `events.calendar` | 30d | Earnings Whispers, FRED |
| `prices.bars` | 7d | Polygon prices |

---

## Troubleshooting

**Containers not starting:**
```bash
docker compose logs postgres     # Check Postgres startup
docker compose logs redpanda     # Check Redpanda startup
```

**Migrations failing:**
```bash
docker compose logs migrate      # See full migration error
make db-shell                    # Check DB state manually
```

**Connector crashing:**
```bash
make logs-benzinga               # See error
make restart-benzinga            # Restart after fixing
```

**Out of disk space:**
```bash
docker system prune -f           # Clean dangling images/containers
make clean-all                   # Remove images (requires full rebuild)
```

**Port already in use:**
Edit `docker-compose.yml` and change the left side of the port mapping:
`"3001:3000"` instead of `"3000:3000"` for Grafana, etc.

---

## Phase 1 Exit Criteria

- [ ] `make up` completes with no errors
- [ ] All containers show `healthy` in `make status`
- [ ] Kafka topics visible in Redpanda Console (http://localhost:8088)
- [ ] Database tables visible in Adminer (http://localhost:8080)
- [ ] Benzinga connector emitting to `news.raw` (check Redpanda Console)
- [ ] Price bars flowing for AAPL, MSFT, TSLA
- [ ] Earnings events in `event` table (`make db-shell` → `SELECT COUNT(*) FROM event`)
- [ ] Grafana showing connector metrics at http://localhost:3000

---

## API Keys

| Key | Where to get | Cost |
|---|---|---|
| `BENZINGA_API_KEY` | https://www.benzinga.com/apis/ | $150–400/mo |
| `POLYGON_API_KEY` | https://polygon.io/ | $79/mo |
| `EARNINGS_WHISPERS_API_KEY` | https://www.earningswhispers.com/api | $30–80/mo |
| `FRED_API_KEY` | https://fred.stlouisfed.org/docs/api/api_key.html | **Free** |
