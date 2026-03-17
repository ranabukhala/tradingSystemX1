<p align="center">
  <strong>📈 US Equities News Intelligence & Execution System</strong>
</p>

<p align="center">
  A production-grade, event-driven trading pipeline that transforms real-time financial news into conviction-scored trade signals — and executes them autonomously.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white" alt="Docker"/>
  <img src="https://img.shields.io/badge/Redpanda-Kafka--compatible-E4405F?style=flat-square&logo=apachekafka&logoColor=white" alt="Redpanda"/>
  <img src="https://img.shields.io/badge/Claude_AI-Haiku_%2B_Sonnet-D97706?style=flat-square&logo=anthropic&logoColor=white" alt="Claude AI"/>
  <img src="https://img.shields.io/badge/Broker-IBKR_%7C_Alpaca-DC143C?style=flat-square" alt="Brokers"/>
  <img src="https://img.shields.io/badge/pgvector-Postgres_16-336791?style=flat-square&logo=postgresql&logoColor=white" alt="Postgres"/>
  <img src="https://img.shields.io/badge/Lines_of_Code-25%2C000+-blue?style=flat-square" alt="LOC"/>
  <img src="https://img.shields.io/badge/Test_Coverage-9%2C400+_LOC-brightgreen?style=flat-square" alt="Tests"/>
</p>

---

## What This Is

Most algorithmic trading systems react to **price**. This system reacts to **information** — the catalyst behind price movement.

It ingests real-time news from 7+ financial data providers, deduplicates it across 5 tiers (URL hash → content hash → fuzzy → OpenAI embedding → SimHash), enriches it with AI-powered impact scoring via Claude, runs it through a multi-stage conviction pipeline with 11 risk constraints, and routes orders to Interactive Brokers or Alpaca — all within a fully containerized Docker stack of **38 services**.

---

## Architecture

```
  ┌─────────────────────────  INGESTION  ─────────────────────────┐
  │  Benzinga  ·  Polygon WS  ·  Finnhub (4 modes)  ·  FMP (4)  │
  │  Earnings Whispers  ·  FRED  ·  AlphaVantage                 │
  └──────────────────────────────┬────────────────────────────────┘
                                 │  news.raw
                                 ▼
                     ┌───────────────────────┐
                     │     Normalizer        │  Unified Pydantic schema
                     └───────────┬───────────┘
                                 │  news.normalized
                                 ▼
               ┌─────────────────────────────────────┐
               │     5-Tier Deduplication Engine      │
               │  URL hash → Content hash → Fuzzy    │
               │  → Embedding cosine → SimHash        │
               │  + Redis gating · pgvector HNSW      │
               └───────────┬─────────────────────────┘
                           │  news.deduped
                           ▼
               ┌───────────────────────────┐
               │     Entity Resolver       │  NER · Ticker extraction
               │     + Stock Context       │  Market cap · Float · Sector
               └───────────┬───────────────┘
                           │  news.enriched
                           ▼
               ┌───────────────────────────┐
               │   AI Summarizer (Claude)  │  Fast path: structured catalysts
               │   2-Tier LLM Pipeline     │  T1: Haiku — facts + impact
               │   + Fast-Path Router      │  T2: Sonnet — trade thesis
               └───────────┬───────────────┘
                           │  news.summarized
                           ▼
               ┌───────────────────────────┐
               │    Signal Aggregator      │  Multi-factor conviction
               │    + Conviction Features  │  Catalyst weights · Decay
               └───────────┬───────────────┘
                           │  signals.actionable
                           ▼
       ┌───────────────────────────────────────────────┐
       │            Pre-Trade Filter Suite              │
       │  Regime · Technicals · Options flow · Short   │
       │  + Catalyst Policy Engine (v1.9)              │
       └───────────┬───────────────────────────────────┘
                   │  signals.filtered
                   ▼
       ┌───────────────────────────────────────────────┐
       │       Portfolio Risk Manager (11 checks)      │
       │  Kill switch · Daily loss · Sector cap ·      │
       │  Correlation · Cluster dedup · Order rate     │
       └───────────┬───────────────────────────────────┘
                   │  signals.risk_approved
                   ▼
       ┌───────────────────────────────────────────────┐
       │           Execution Engine                     │
       │  Broker abstraction (IBKR / Alpaca)           │
       │  + Entry validator · Fill poller               │
       │  + Position monitor · Order policy             │
       └───────────┬───────────────────────────────────┘
                   │
          ┌────────┴─────────┐
          ▼                  ▼
  ┌──────────────┐   ┌─────────────┐
  │  Telegram    │   │  Grafana    │
  │  Alerts      │   │  Dashboard  │
  └──────────────┘   └─────────────┘

  ── All services communicate via Redpanda (Kafka-compatible) topics ──
  ── Infrastructure: Postgres (pgvector) · Redis · MinIO · Prometheus ──
```

---

## Key Engineering Decisions

| Area | Decision | Why |
|---|---|---|
| **Message broker** | Redpanda over Kafka | Lower latency, no ZooKeeper, Kafka API-compatible, simpler ops |
| **Deduplication** | 5-tier pipeline | URL hash (μs) → content hash (μs) → pg_trgm fuzzy (ms) → OpenAI embedding cosine (ms) → SimHash (μs); catches same-story cross-vendor rewrites that each prior tier misses |
| **AI/LLM** | 2-tier Claude + fast-path router | Fast path for deterministic catalysts (earnings, analyst, M&A, FDA) skips LLM entirely; Haiku for remaining items; Sonnet only for high-impact — cost-optimized without sacrificing speed |
| **Idempotency** | SQLite + Redis dual-layer | SQLite for at-least-once per stage, Redis for distributed mutex across consumers |
| **Risk** | 11-constraint fail-fast | Cheapest checks first; kill-switch architecture with manual + automatic halt |
| **Broker** | Abstraction layer via env var | Hot-swap paper ↔ live, IBKR ↔ Alpaca with zero code changes |
| **Observability** | Prometheus + Grafana + DLQ | Every service exposes `/metrics`; dead-letter queue with inspect/replay CLI |
| **Schema** | Pydantic v2 models with versioning | Type-safe message contracts, `schema_version` field for backward-compatible evolution |
| **DB** | pgvector on Postgres 16 | Embedding-based dedup + vector similarity search in the same DB as transactional data |

---

## Tech Stack

| Layer | Technologies |
|---|---|
| **Core** | Python 3.11, Pydantic v2, asyncio, httpx |
| **Messaging** | Redpanda (Kafka-compatible), confluent-kafka |
| **AI / LLM** | Anthropic Claude (Haiku + Sonnet), OpenAI text-embedding-3-small |
| **Database** | PostgreSQL 16 + pgvector, SQLite (state/idempotency), Redis 7 |
| **Brokers** | Interactive Brokers (ib_insync), Alpaca Markets API |
| **Data Providers** | Benzinga, Polygon.io, Finnhub, FMP, Earnings Whispers, FRED, AlphaVantage |
| **Object Storage** | MinIO (S3-compatible) — full article text archival |
| **Observability** | Prometheus, Grafana, structlog (structured JSON logging) |
| **Infra** | Docker Compose (38 services), 17 SQL migrations |
| **Testing** | pytest, pytest-asyncio, fakeredis — 9,400+ lines of tests, 711 passing |

---

## Pipeline Stages (7-Stage Flow)

| # | Stage | Topic In | Topic Out | Description |
|---|---|---|---|---|
| 1 | **Connectors** (7) | External APIs | `news.raw`, `events.calendar`, `prices.bars` | Multi-provider ingestion with rate limiting |
| 2 | **Normalizer** | `news.raw` | `news.normalized` | Schema standardization into Pydantic models |
| 3 | **Deduplicator** | `news.normalized` | `news.deduped` | 5-tier dedup: URL → hash → pg_trgm → embedding cosine → SimHash |
| 4 | **Entity Resolver** | `news.deduped` | `news.enriched` | NER, ticker extraction, stock context enrichment |
| 5 | **AI Summarizer** | `news.enriched` | `news.summarized` | Fast-path router for structured catalysts; T1 facts (Haiku); T2 trade thesis (Sonnet) |
| 6 | **Signal Aggregator** | `news.summarized` | `signals.actionable` | Conviction scoring, pattern detection, sympathy plays |
| 7 | **Pre-Trade → Risk → Execution** | `signals.actionable` | `trades.executed` | 4 filters → 11 risk checks → order placement |

---

## Deduplication Deep Dive

The 5-tier pipeline is designed so each layer catches what the previous one misses:

| Tier | Method | Latency | Catches |
|---|---|---|---|
| 1 | URL exact match (Redis SET) | ~0.5ms | Same article from same source |
| 2 | Content hash SHA-256 (Redis SET) | ~0.5ms | Same article from different sources |
| 3 | pg_trgm fuzzy similarity ≥ 0.75 | ~5ms | Near-identical rephrasing (trigram overlap) |
| 3.5 | OpenAI embedding cosine similarity ≥ 0.65 | ~150ms | Same event, very different wording (cross-vendor rewrites) |
| 4 | SimHash Hamming distance ≤ 8 bits (Redis) | ~1ms | Structurally similar articles missed by tier 3 |

**Why tier 3.5 matters:** Empirical test — "*Nebius shares pop on $27M AI infrastructure deal with Meta*" vs "*Meta stock pops on planned layoffs, $27 billion Nebius cloud-computing deal*" scores pg_trgm=0.25 (below threshold) and SimHash=26 bits (above threshold), but OpenAI embedding cosine=0.68 → correctly deduplicated. Without this tier, 10+ cross-vendor articles about the same event each generate their own signal.

If the OpenAI API is unavailable, the tier is bypassed transparently — the pipeline falls through to SimHash.

---

## Fast-Path Signal Routing

For deterministic catalyst types, the AI Summarizer bypasses the LLM entirely:

| Catalyst | Fast-Path Logic | Example |
|---|---|---|
| Earnings beat/miss | EPS vs estimate from structured facts | "EPS $5.16 vs $4.89 estimate → beat → LONG" |
| Analyst upgrade/downgrade | Rating action from facts_json | "Upgrade to Buy → LONG" |
| M&A announcement | Premium % from deal price vs current | "15% premium → LONG acquiree" |
| FDA approval/rejection | Outcome field | "Approved → LONG; Rejected → SHORT" |
| Guidance raise/cut | Explicit guidance action | "Raised FY guidance → LONG" |

Fast-path records have `route_type="fast"`, `llm_tokens_used=0`. The slow path (Claude Haiku → Sonnet) handles all other catalysts. A Prometheus counter `ai_summarizer_route_total{route_type, catalyst_type}` tracks the routing split.

---

## Risk & Safety Architecture

The system implements defense-in-depth risk management:

**Pre-Trade Filters** — 4 parallel quality gates: market regime assessment, technical confirmation (7-component score, 0–10), options flow alignment, and short interest squeeze detection. Each filter applies conviction multipliers; hard blocks kill the signal. The technical threshold (7, 8, or 9) is dynamically set by the Stock Context service based on ADX, ATR, and Bollinger Band regime.

**Portfolio Risk Manager** — 11 fail-fast constraints evaluated in cost order: manual/automatic kill switch, daily P&L cap, consecutive loss cooldown, hourly/daily order rate limits, max open positions, per-ticker exposure cap, sector concentration limit, catalyst concentration check, correlation proxy, and per-event cluster deduplication.

**Execution Safety** — `CONFIRM_LIVE_TRADING` env guard (must be explicitly `true` for live), broker abstraction layer, idempotent order submission, fill polling with timeout, and staleness guards that reject signals aged beyond threshold.

---

## Project Structure

```
├── app/
│   ├── connectors/          # 7 data source connectors (Benzinga, Polygon, Finnhub, FMP, etc.)
│   ├── pipeline/            # Core processing: normalizer, deduplicator, entity resolver
│   │   ├── deduplicator.py       # 5-tier dedup (hash, fuzzy, embedding, SimHash)
│   │   ├── embedding.py          # OpenAI embedding client with circuit breaker
│   │   ├── route_classifier.py   # Fast-path vs slow-path routing decision
│   │   ├── fast_path_builder.py  # LLM-free signal construction for structured catalysts
│   │   ├── entity_resolver.py    # NER + ticker extraction
│   │   ├── fact_cross_validator.py  # Cross-source fact verification
│   │   ├── llm_validation.py    # LLM output trust scoring
│   │   └── staleness.py         # Signal freshness enforcement
│   ├── signals/             # AI summarization + conviction scoring
│   │   ├── ai_summarizer.py      # 2-tier Claude pipeline + fast-path routing
│   │   ├── signal_aggregator.py  # Multi-factor conviction engine
│   │   ├── conviction_features.py # Feature extraction + calibration
│   │   └── telegram_alerts.py    # Real-time trade notifications
│   ├── filters/             # Pre-trade quality gates
│   │   ├── pretrade_filter.py    # Orchestrator — runs all 4 filters in parallel
│   │   ├── regime.py             # Market regime assessment
│   │   ├── technicals.py         # Price structure confirmation
│   │   ├── options_flow.py       # Smart money positioning
│   │   ├── short_interest.py     # Squeeze detection
│   │   └── catalyst_policy.py    # Catalyst-specific trade rules (v1.9)
│   ├── risk/                # Portfolio-level risk management
│   │   └── portfolio_risk.py     # 11-constraint fail-fast evaluator
│   ├── execution/           # Order lifecycle management
│   │   ├── execution_engine.py   # Signal → order flow orchestration
│   │   ├── alpaca_broker.py      # Alpaca API integration
│   │   ├── ibkr_broker.py        # Interactive Brokers integration
│   │   ├── risk_manager.py       # Position-level risk checks
│   │   └── position_monitor.py   # Open position tracking + exit logic
│   ├── admin/               # Telegram admin bot + watchdog
│   ├── scheduler/           # Market hours-aware scheduling
│   ├── models/              # Pydantic v2 message schemas
│   └── config.py            # Centralized pydantic-settings config
│
├── stock_context_service/   # Standalone microservice — real-time stock context
│   ├── classifier.py             # ADX/ATR/BB regime classifier (Wilder-smoothed)
│   ├── scheduler.py              # Per-ticker refresh with async Polygon client
│   └── polygon_client.py         # Polygon daily/hourly bar fetcher
├── migrations/              # 17 SQL migrations (004–017)
├── tests/                   # 9,400+ lines — unit + integration tests
├── grafana/                 # Dashboard provisioning
├── prometheus/              # Metrics collection config
├── scripts/                 # Kafka topic creation, healthcheck, DB init
├── docker-compose.yml       # 38-service orchestration
├── Makefile                 # Developer workflow commands
└── .env.example             # Environment template (no secrets)
```

---

## Getting Started

### Prerequisites

- **Docker Desktop** (Memory: 12GB+, CPUs: 4+, Disk: 60GB)
- API keys for data providers (see `.env.example`)
- Broker account — Alpaca (paper) or Interactive Brokers

### Quick Start

```bash
git clone https://github.com/ranabukhala/tradingSystem.git
cd tradingSystem

cp .env.example .env
# Edit .env — add your API keys and broker credentials

make up            # Builds + starts all 38 services
make status        # Verify health
make logs          # Tail all logs
```

### Key Commands

```bash
make up                 # Full stack start
make down               # Stop (data preserved)
make restart             # Restart after code change (no rebuild needed)
make up-infra           # Infrastructure only
make up-connectors      # Data connectors
make up-pipeline        # Processing pipeline
make up-signals         # AI + signal aggregation
make up-execution       # Execution engine + monitor
make test-unit          # Unit tests (no infra required)
make test               # Full test suite
make dlq-inspect t=...  # Dead-letter queue inspection
make dlq-replay id=...  # Replay failed messages
```

### Access Points

| Service | URL |
|---|---|
| Grafana (dashboards) | `http://localhost:3000` |
| Redpanda Console (Kafka UI) | `http://localhost:8088` |
| MinIO Console (object storage) | `http://localhost:9001` |
| Adminer (database browser) | `http://localhost:8080` |
| Prometheus (raw metrics) | `http://localhost:9090` |
| Stock Context API | `http://localhost:8082` |

---

## Signal Flow Example

```
1.  Benzinga WS → "NVIDIA reports Q4 EPS $5.16, beats $4.89 estimate"
2.  Normalizer → unified Pydantic schema → news.normalized
3.  Deduplicator → 5-tier check → unique → cluster assigned → news.deduped
4.  Entity Resolver → tickers: [NVDA], sector: Technology, cap: mega
5.  AI Summarizer:
      Route classifier → catalyst: earnings, EPS facts present → FAST PATH
      Fast-path builder → direction: long, impact_day: 0.85, t1_summary: "Beat EPS by $0.27"
      (T2 Sonnet runs for high-impact: trade thesis, entry/exit levels, sympathy plays: [AMD, AVGO])
6.  Signal Aggregator → conviction: 0.78, pattern: event_driven
7.  Pre-Trade Filter → regime: ✅ risk_on, technicals: ✅ 8/10 (threshold=8), options: ✅ bullish
8.  Portfolio Risk → all 11 checks pass
9.  Execution Engine → BUY NVDA via IBKR, bracket order (TP +10%, SL -4%)
10. Telegram → real-time alert with conviction breakdown + technical score block
```

---

## Testing

The test suite covers critical system properties across 9,400+ lines:

| Test Module | What It Validates |
|---|---|
| `test_event_dedup` | 5-tier dedup correctness, cluster assignment, race conditions |
| `test_embedding_dedup` | Vector similarity dedup, threshold calibration, circuit breaker |
| `test_fast_path` | Route classification, fast-path builder, Prometheus counter, direction derivation |
| `test_stock_context_classifier` | ADX/ATR Wilder-smooth math, bounds (ADX ≤ 100), ATR magnitude |
| `test_llm_trust` | LLM output validation, hallucination detection |
| `test_fact_cross_validator` | Cross-source fact verification accuracy |
| `test_conviction_features` | Conviction scoring math, feature extraction |
| `test_catalyst_policy` | Catalyst-specific trading rules |
| `test_portfolio_risk` | All 11 risk constraints, kill switch behavior |
| `test_execution_upgrade` | Order lifecycle, broker abstraction, fill handling |
| `test_staleness` | Signal freshness enforcement, timestamp propagation |
| `test_redis_idempotency` | Distributed idempotency under concurrency |

```bash
make test-unit    # Fast — no infrastructure needed
make test         # Full suite — requires Docker stack running
```

---

## Roadmap

- [ ] Backtesting framework with EODHD historical data replay (evaluation complete — implementation planned)
- [ ] Portfolio Risk Manager — signal-level 11-constraint evaluator (architecture designed)
- [ ] Grafana alerting rules for anomaly detection
- [ ] Multi-strategy routing (momentum, mean-reversion, event-driven)
- [ ] Options execution support (spreads, straddles)
- [ ] ML-based conviction calibration from trade outcome feedback

---

## Disclaimer

This project is for **educational and research purposes**. It is not financial advice. Algorithmic trading involves substantial risk of loss. Use paper trading for testing and never risk capital you cannot afford to lose.

---

## License

MIT
