# Changelog — Trading System Improvements

## Tasks A–J Summary

### Task A — Repo hygiene / shipping safety ✅
- **.dockerignore:** Excludes .env, .git, __pycache__, *.pyc, .venv, .idea, dist, *.db, *.sqlite; keeps tests for `make test`
- **.gitignore:** Excludes .env, *.db, *.sqlite, release/, *.zip
- **Release:** `make release` creates clean tar.gz excluding secrets and junk

### Task B — Unified configuration ✅
- **app/config.py:** Centralized pydantic-settings; all risk/trading/env vars
- **Replaced os.environ** in: connectors (base, finnhub/*, fmp/*), admin (admin_bot, watchdog), execution (position_monitor), signals (ai_summarizer)
- **New config fields:** finnhub_input_topic, topic_news_fmp_enriched, risk_max_orders_per_day

### Task C — Structured logging + correlation IDs ✅ (already done)
- **structlog** in app/logging.py; bind_event_context(event_id, **kwargs)
- **base_consumer:** Extracts event_id, propagates parent_id
- **execution_engine:** bind_event_context before process

### Task D — Message schemas / contracts ✅ (already done)
- **app/models/news.py:** BaseMessage (event_id, parent_id, schema_version), RawNewsRecord, NormalizedRecord, DedupedRecord, EnrichedRecord, SummarizedRecord, DLQMessage
- **tests/test_schemas.py:** Validation tests per model

### Task E — At-least-once safety (SQLite) ✅ (already done)
- **app/idempotency.py:** processed_events, order_idempotency, daily_loss_tracker
- **base_consumer:** check_and_mark before process; skip if duplicate
- **execution_engine:** was_order_submitted + record_order
- **Volume:** service_data:/data for state.db

### Task F — DLQ workflow ✅ (already done)
- **app/dlq.py:** inspect + replay CLIs
- **Makefile:** dlq-inspect, dlq-replay
- **Metrics:** pipeline_dlq_total (Prometheus); alert on rate spikes in Grafana

### Task G — Trading safety + order lifecycle ✅ (already done)
- **CONFIRM_LIVE_TRADING:** require_live_trading_confirmation() exits if live broker without flag
- **Risk checks:** daily loss cap, consecutive loss halt, order limits, cooldown
- **Lifecycle events:** order_submitted, order_rejected, order_filled, order_partially_filled, order_cancelled
- **risk_max_orders_per_day:** Separate from hourly limit for daily cap

### Task H — Docker / infra ✅ (already done)
- **Pinned versions:** redpanda v24.1.7, prometheus v2.53.0, grafana 11.1.0, minio RELEASE.2024-06-13, adminer 4.8.1
- **Profiles:** infra, connectors, pipeline, signals, execution
- **Make targets:** up-infra, up-connectors, up-pipeline, up-signals, up-execution

### Task I — Tests ✅ (already done)
- **test_idempotency.py:** check_and_mark, was_order_submitted, record_order, daily_loss_tracker
- **test_schemas.py:** BaseMessage, RawNewsRecord, NormalizedRecord, DLQMessage, etc.
- **test_execution_safety.py:** CONFIRM_LIVE_TRADING kill-switch, RiskConfig
- **Makefile:** test-unit (no infra), test (full)

### Task J — Docs ✅
- **README:** Required env vars, Compose profiles, SQLite state DB, DLQ inspect/replay, Risk controls, Paper trading quickstart, Testing
- **.env.example:** RISK_MAX_ORDERS_PER_DAY, DATA_DIR

---

## Commands to Run

```bash
# Unit tests (no Kafka/Postgres)
make test-unit

# Full tests (requires infra)
make test

# Compose profiles
make up-infra
make up-connectors
make up-pipeline
make up-signals
make up-execution

# DLQ tools
make dlq-inspect t=news.raw.dlq
make dlq-replay id=<event_id> t=news.raw.dlq

# Release archive
make release
```

---

## Breaking Changes & Migration

1. **Config:** All services must use `from app.config import settings`; remove direct `os.environ` reads.
2. **DATA_DIR:** Default `/data`; set `DATA_DIR` if using a different path for state.db.
3. **RISK_MAX_ORDERS_PER_DAY:** New setting (default 120); use for daily order cap (was incorrectly using risk_max_orders_per_hour for daily check).
4. **Python 3.11+:** Required; local `pytest` needs 3.11+ (or use `make test-unit` in Docker).

---

## Security Note

- **.env** excluded from Docker build context (.dockerignore)
- **.env** excluded from `make release` archive
- No secrets shipped in images or release artifacts
