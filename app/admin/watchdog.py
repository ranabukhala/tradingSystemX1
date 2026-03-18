"""
Watchdog Service — monitors all trading system services.

Responsibilities:
  - Polls Docker container health every 30s
  - Monitors Kafka consumer lag per service (via Redpanda Admin API)
  - Functional heartbeat checks (container up ≠ service healthy)
  - Postgres and Redis connectivity probes
  - Tracks LLM budget consumption rate (alert once per crossing, not every poll)
  - Detects connector silence (no messages for N minutes, market hours only)
  - Signal block-rate monitoring (alerts if pre-trade filter blocks everything)
  - Auto-restarts unhealthy containers (with cooldown + hourly limit)
  - Restart counts persisted in Redis (survives watchdog restarts)
  - Alert deduplication — no Telegram spam on sustained failures
  - Market hours awareness — suppresses silence/restart alerts outside hours
  - Sends alerts to admin Telegram bot on any anomaly
  - Maintains a health state snapshot for the admin bot to query
  - Graceful shutdown via asyncio.Event
"""
from __future__ import annotations

import asyncio
import json
import time
import zoneinfo
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta, date
from typing import Any

import httpx
import redis.asyncio as aioredis

from app.config import settings

# ── Constants ─────────────────────────────────────────────────────────────────

POLL_INTERVAL        = 30     # seconds between health checks
SILENCE_THRESHOLD    = 300    # seconds — connector silence = alert (market hours)
RESTART_COOLDOWN     = 120    # seconds — min time between auto-restarts
MAX_AUTO_RESTARTS    = 3      # per service per hour before giving up
BUDGET_WARN_PCT      = 0.80   # alert when daily LLM budget > 80%
KAFKA_LAG_ALERT      = 1000   # unprocessed messages before alerting
BLOCK_RATE_ALERT     = 0.90   # alert if >90% of signals are blocked (last hour)
ALERT_DEDUP_TTL      = 600    # seconds — suppress repeated alert for same event
HEARTBEAT_TTL        = 90     # seconds — service heartbeat key expiry in Redis

ET_TZ = zoneinfo.ZoneInfo("America/New_York")
MARKET_OPEN  = (9, 30)
MARKET_CLOSE = (16, 0)

# ── Service Registry ──────────────────────────────────────────────────────────

# container name → display name
MONITORED_SERVICES: dict[str, str] = {
    # Connectors
    "trading_connector_benzinga":           "Benzinga",
    "trading_connector_polygon_news":       "Polygon News",
    "trading_connector_polygon_prices":     "Polygon Prices",
    "trading_connector_earnings":           "Earnings",
    "trading_connector_fred":               "FRED",
    # Finnhub family
    "trading_finnhub_news":                 "Finnhub News",
    "trading_finnhub_press_releases":       "Finnhub Press Releases",
    "trading_finnhub_fundamentals":         "Finnhub Fundamentals",
    "trading_finnhub_websocket":            "Finnhub WebSocket",
    "trading_finnhub_sentiment":            "Finnhub Sentiment",
    # FMP family
    "trading_fmp_earnings":                 "FMP Earnings",
    "trading_fmp_enrichment":               "FMP Enrichment",
    "trading_fmp_sectors":                  "FMP Sectors",
    "trading_fmp_technical":                "FMP Technical",
    # Pipeline
    "trading_pipeline_normalizer":          "Normalizer",
    "trading_pipeline_deduplicator":        "Deduplicator",
    "trading_pipeline_entity_resolver":     "Entity Resolver",
    # Signals
    "trading_signals_ai_summarizer":        "AI Summarizer",
    "trading_signals_aggregator":           "Signal Aggregator",
    "trading_pretrade_filter":              "Pre-Trade Filter",
    "trading_signals_telegram":             "Telegram Alerts",
    # Execution & monitoring
    "trading_execution_engine":             "Execution Engine",
    "trading_position_monitor":             "Position Monitor",
    "trading_volatility_monitor":           "Volatility Monitor",
    "trading_stock_context":                "Stock Context",
    "trading_regime_poller":                "Regime Poller",
    "trading_scheduler":                    "Scheduler",
    # Infra
    "trading_redpanda":                     "Redpanda",
    "trading_postgres":                     "Postgres",
    "trading_redis":                        "Redis",
}

# Services eligible for auto-restart (not infra — those need manual intervention)
AUTO_RESTART_SERVICES: set[str] = {
    # Connectors
    "trading_connector_benzinga",
    "trading_connector_polygon_news",
    "trading_connector_polygon_prices",
    "trading_connector_earnings",
    "trading_connector_fred",
    # Finnhub
    "trading_finnhub_news",
    "trading_finnhub_press_releases",
    "trading_finnhub_fundamentals",
    "trading_finnhub_websocket",
    "trading_finnhub_sentiment",
    # FMP
    "trading_fmp_earnings",
    "trading_fmp_enrichment",
    "trading_fmp_sectors",
    "trading_fmp_technical",
    # Pipeline
    "trading_pipeline_normalizer",
    "trading_pipeline_deduplicator",
    "trading_pipeline_entity_resolver",
    # Signals
    "trading_signals_ai_summarizer",
    "trading_signals_aggregator",
    "trading_pretrade_filter",
    "trading_signals_telegram",
    # Execution & monitoring
    "trading_execution_engine",
    "trading_position_monitor",
    "trading_volatility_monitor",
    "trading_stock_context",
    "trading_regime_poller",
}

# Connectors that write heartbeat keys and should be silent-checked during hours
CONNECTOR_SERVICES: set[str] = {
    "trading_connector_benzinga",
    "trading_connector_polygon_news",
    "trading_connector_polygon_prices",
    "trading_connector_fred",
    "trading_finnhub_news",
    "trading_finnhub_press_releases",
    "trading_finnhub_websocket",
    "trading_finnhub_sentiment",
    "trading_fmp_earnings",
    "trading_fmp_enrichment",
    "trading_fmp_sectors",
    "trading_fmp_technical",
}

# Redis key prefixes
_REDIS_RESTART_PREFIX   = "watchdog:restarts:"     # + container_name
_REDIS_SNAPSHOT_KEY     = "watchdog:snapshot"
_REDIS_ALERT_DEDUP_PFX  = "watchdog:alerted:"      # + dedup_key
_REDIS_BUDGET_ALERTED   = "watchdog:budget_alerted:"  # + date
_REDIS_HEARTBEAT_PFX    = "service:heartbeat:"     # + short_name


# ── Helpers ───────────────────────────────────────────────────────────────────

def _log(level: str, event: str, **kw) -> None:
    entry = {"ts": datetime.now(timezone.utc).isoformat(),
             "level": level, "event": event, **kw}
    print(json.dumps(entry), flush=True)


def _is_market_hours() -> bool:
    """True if current ET time is within regular market hours, Mon–Fri."""
    now_et = datetime.now(ET_TZ)
    if now_et.weekday() >= 5:   # Saturday=5, Sunday=6
        return False
    t = (now_et.hour, now_et.minute)
    return MARKET_OPEN <= t < MARKET_CLOSE


# ── Data Classes ──────────────────────────────────────────────────────────────

@dataclass
class ServiceHealth:
    name: str
    container: str
    status: str = "unknown"       # running | stopped | unhealthy | unknown
    healthy: bool = False
    heartbeat_ok: bool = True     # functional liveness (not just container state)
    last_seen: str = ""
    restart_count: int = 0        # loaded from / persisted to Redis
    last_restart: float = 0.0
    error: str = ""


@dataclass
class SystemSnapshot:
    timestamp: str = ""
    services: dict[str, dict] = field(default_factory=dict)
    kafka_lag: dict[str, int] = field(default_factory=dict)
    infra_health: dict[str, bool] = field(default_factory=dict)
    llm_budget_used: float = 0.0
    llm_budget_limit: float = 5.0
    alerts_today: int = 0
    signals_today: int = 0
    signals_blocked_today: int = 0
    news_processed_today: int = 0
    block_rate_1h: float = 0.0
    market_hours: bool = False
    overall_healthy: bool = False


# ── Watchdog ──────────────────────────────────────────────────────────────────

class WatchdogService:

    def __init__(self) -> None:
        self._running = False
        self._stop_event = asyncio.Event()
        self._http: httpx.AsyncClient | None = None
        self._redis: aioredis.Redis | None = None
        self._health: dict[str, ServiceHealth] = {}
        self._restart_history: dict[str, list[float]] = {}  # in-memory hourly window
        self._snapshot = SystemSnapshot()

        self._bot_token    = settings.admin_bot_token
        self._admin_chat_id = settings.admin_chat_id or ""
        self._docker_socket = "/var/run/docker.sock"

        for container, name in MONITORED_SERVICES.items():
            self._health[container] = ServiceHealth(name=name, container=container)
            self._restart_history[container] = []

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        self._http = httpx.AsyncClient(timeout=10.0)
        self._redis = await aioredis.from_url(
            settings.redis_url,
            decode_responses=True,
        )

        await self._load_restart_counts()

        _log("info", "watchdog.started", services=len(MONITORED_SERVICES))
        await self._alert(
            "🐕 *Watchdog started* — monitoring system health every 30s",
            dedup_key="watchdog_start",
            dedup_ttl=60,
        )

        while self._running:
            try:
                await self._check_all()
                await self._save_snapshot()
            except Exception as e:
                _log("error", "watchdog.check_error", error=str(e))
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=POLL_INTERVAL)
                break  # stop event fired
            except asyncio.TimeoutError:
                pass   # normal — keep looping

        _log("info", "watchdog.stopped")

    def stop(self) -> None:
        self._running = False
        self._stop_event.set()

    # ── Restart count persistence ─────────────────────────────────────────────

    async def _load_restart_counts(self) -> None:
        """Load per-service restart counts from Redis on startup."""
        today = date.today().isoformat()
        for container, health in self._health.items():
            key = f"{_REDIS_RESTART_PREFIX}{container}:{today}"
            try:
                val = await self._redis.get(key)
                if val:
                    health.restart_count = int(val)
            except Exception:
                pass

    async def _persist_restart_count(self, container: str) -> None:
        """Persist restart count for today to Redis (survives watchdog restarts)."""
        today = date.today().isoformat()
        key = f"{_REDIS_RESTART_PREFIX}{container}:{today}"
        try:
            await self._redis.setex(key, 86400, self._health[container].restart_count)
        except Exception:
            pass

    # ── Master check ──────────────────────────────────────────────────────────

    async def _check_all(self) -> None:
        await asyncio.gather(
            self._check_containers(),
            self._check_heartbeats(),
            self._check_infra_connectivity(),
            self._check_kafka_lag(),
            self._check_budget(),
            self._check_signal_block_rate(),
            return_exceptions=True,
        )

    # ── Container health ──────────────────────────────────────────────────────

    async def _check_containers(self) -> None:
        """Query Docker API via unix socket for container state."""
        try:
            transport = httpx.AsyncHTTPTransport(uds=self._docker_socket)
            async with httpx.AsyncClient(transport=transport, timeout=5.0) as docker:
                resp = await docker.get("http://docker/containers/json?all=true")
                if resp.status_code != 200:
                    return
                containers = resp.json()
        except Exception as e:
            _log("warning", "watchdog.docker_unavailable", error=str(e))
            return

        container_map: dict[str, dict] = {}
        for c in containers:
            for cname in c.get("Names", []):
                container_map[cname.lstrip("/")] = c

        for container_name, health in self._health.items():
            c = container_map.get(container_name)
            was_healthy = health.healthy

            if not c:
                if health.status != "stopped":
                    health.status = "stopped"
                    health.healthy = False
                    await self._on_service_down(container_name, "container not found")
                continue

            state        = c.get("State", "unknown")
            docker_status = c.get("Status", "")
            health.last_seen = datetime.now(timezone.utc).isoformat()

            if state == "running" and "unhealthy" not in docker_status.lower():
                health.status  = "running"
                health.healthy = True
                health.error   = ""

            elif "unhealthy" in docker_status.lower():
                health.status  = "unhealthy"
                health.healthy = False
                health.error   = "Docker HEALTHCHECK failing"
                if was_healthy:
                    await self._on_service_down(container_name, "Docker health check failing")
                    await self._maybe_restart(container_name)

            else:
                # Any other non-running state (exited, paused, created…)
                prev_status    = health.status
                health.status  = state
                health.healthy = False
                health.error   = f"state={state}"
                # Only alert on transition to avoid Telegram spam
                if was_healthy or prev_status != state:
                    await self._on_service_down(container_name, f"state={state}")
                    if state not in ("exited", "stopped"):
                        await self._maybe_restart(container_name)

    # ── Functional heartbeat checks ───────────────────────────────────────────

    async def _check_heartbeats(self) -> None:
        """
        Each service writes service:heartbeat:<name> to Redis with a short TTL.
        If the key is missing, the service is running but frozen / stuck.
        Only checked during market hours for connector services.
        """
        in_hours = _is_market_hours()

        heartbeat_map = {
            # Connectors
            "trading_connector_benzinga":           "benzinga",
            "trading_connector_polygon_news":       "polygon_news",
            "trading_connector_polygon_prices":     "polygon_prices",
            "trading_connector_earnings":           "earnings",
            "trading_connector_fred":               "fred",
            # Finnhub
            "trading_finnhub_news":                 "finnhub_news",
            "trading_finnhub_press_releases":       "finnhub_press_releases",
            "trading_finnhub_fundamentals":         "finnhub_fundamentals",
            "trading_finnhub_websocket":            "finnhub_websocket",
            "trading_finnhub_sentiment":            "finnhub_sentiment",
            # FMP
            "trading_fmp_earnings":                 "fmp_earnings",
            "trading_fmp_enrichment":               "fmp_enrichment",
            "trading_fmp_sectors":                  "fmp_sectors",
            "trading_fmp_technical":                "fmp_technical",
            # Pipeline
            "trading_pipeline_normalizer":          "normalizer",
            "trading_pipeline_deduplicator":        "deduplicator",
            "trading_pipeline_entity_resolver":     "entity_resolver",
            # Signals & execution
            "trading_signals_ai_summarizer":        "ai_summarizer",
            "trading_signals_aggregator":           "signal_aggregator",
            "trading_pretrade_filter":              "pretrade_filter",
            "trading_execution_engine":             "execution_engine",
            "trading_position_monitor":             "position_monitor",
            "trading_volatility_monitor":           "volatility_monitor",
            "trading_stock_context":                "stock_context",
            "trading_regime_poller":                "regime_poller",
        }

        for container, short in heartbeat_map.items():
            health = self._health.get(container)
            if not health or not health.healthy:
                continue  # don't double-alert on already-down containers
            if container in CONNECTOR_SERVICES and not in_hours:
                continue  # connectors are legitimately quiet outside hours

            key = f"{_REDIS_HEARTBEAT_PFX}{short}"
            try:
                val = await self._redis.get(key)
                prev_ok = health.heartbeat_ok
                health.heartbeat_ok = val is not None

                if not health.heartbeat_ok and prev_ok:
                    _log("warning", "watchdog.heartbeat_missing",
                         container=container, key=key)
                    await self._alert(
                        f"⚠️ *Service Frozen*: `{health.name}`\n"
                        f"Container is running but heartbeat missing.\n"
                        f"Possible stuck consumer or rate-limit loop.",
                        dedup_key=f"heartbeat_{container}",
                    )
                    await self._maybe_restart(container)

                elif health.heartbeat_ok and not prev_ok:
                    _log("info", "watchdog.heartbeat_recovered", container=container)

            except Exception:
                pass

    # ── Infrastructure connectivity ────────────────────────────────────────────

    async def _check_infra_connectivity(self) -> None:
        """Probe Postgres (SELECT 1) and Redis (PING) directly."""
        # Redis
        try:
            pong = await self._redis.ping()
            redis_ok = pong is True or pong == b"PONG" or str(pong).upper() == "PONG"
        except Exception as e:
            redis_ok = False
            _log("error", "watchdog.redis_probe_failed", error=str(e))
            await self._alert(
                "🔴 *Redis unreachable*\nWatchdog cannot ping Redis.",
                dedup_key="redis_down",
            )

        self._snapshot.infra_health["redis"] = redis_ok

        # Postgres — strip SQLAlchemy driver suffix if present
        # e.g. "postgresql+asyncpg://..." → "postgresql://..."
        try:
            import asyncpg
            db_url = settings.database_url.replace("+asyncpg", "").replace("+psycopg2", "")
            conn = await asyncpg.connect(db_url, timeout=5)
            await conn.fetchval("SELECT 1")
            await conn.close()
            pg_ok = True
        except Exception as e:
            pg_ok = False
            _log("error", "watchdog.postgres_probe_failed", error=str(e))
            await self._alert(
                "🔴 *Postgres unreachable*\n`SELECT 1` failed.",
                dedup_key="postgres_down",
            )

        self._snapshot.infra_health["postgres"] = pg_ok

    # ── Kafka consumer lag ────────────────────────────────────────────────────

    async def _check_kafka_lag(self) -> None:
        """
        Check consumer group lag via Redpanda Admin API.
        Uses plain HTTP to the container hostname (no unix socket transport).
        Correct endpoint: GET /v1/consumer_groups then
                          GET /v1/consumer_groups/{id}/offsets for lag details.
        """
        try:
            async with httpx.AsyncClient(
                base_url="http://trading_redpanda:9644",
                timeout=5.0,
            ) as admin:
                resp = await admin.get("/v1/consumer_groups")
                if resp.status_code != 200:
                    return
                groups_data = resp.json()

            # groups_data is {"groups": [...]} or a list depending on Redpanda version
            groups = groups_data if isinstance(groups_data, list) \
                     else groups_data.get("groups", [])

            for group in groups:
                group_id = group.get("group_id", "")
                if not group_id:
                    continue

                # Fetch per-partition offsets for accurate lag
                try:
                    async with httpx.AsyncClient(
                        base_url="http://trading_redpanda:9644",
                        timeout=5.0,
                    ) as admin:
                        offset_resp = await admin.get(
                            f"/v1/consumer_groups/{group_id}/offsets"
                        )
                    if offset_resp.status_code != 200:
                        continue
                    offset_data = offset_resp.json()
                except Exception:
                    continue

                # Each partition: {"partition_id": N, "committed_offset": X,
                #                   "high_watermark": Y, "lag": Z}
                partitions = offset_data if isinstance(offset_data, list) \
                             else offset_data.get("partitions", [])

                total_lag = sum(
                    max(0, p.get("lag", 0) or
                        p.get("high_watermark", 0) - p.get("committed_offset", 0))
                    for p in partitions
                )

                self._snapshot.kafka_lag[group_id] = total_lag

                if total_lag > KAFKA_LAG_ALERT:
                    _log("warning", "watchdog.high_kafka_lag",
                         group=group_id, lag=total_lag)
                    await self._alert(
                        f"⚠️ *High Kafka Lag*\n"
                        f"Group `{group_id}`: {total_lag:,} messages behind",
                        dedup_key=f"kafka_lag_{group_id}",
                    )

        except Exception as e:
            _log("debug", "watchdog.kafka_lag_error", error=str(e))

    # ── LLM Budget ────────────────────────────────────────────────────────────

    async def _check_budget(self) -> None:
        """
        Check LLM daily budget from Redis.
        Alerts once per day per threshold crossing (80% and 95%).
        """
        try:
            today = datetime.now(timezone.utc).date().isoformat()
            spent = await self._redis.get(f"llm:budget:{today}")
            if not spent:
                return

            self._snapshot.llm_budget_used  = float(spent)
            limit = float(settings.llm_daily_budget_usd)
            self._snapshot.llm_budget_limit = limit
            pct = self._snapshot.llm_budget_used / limit

            for threshold, label in [(0.95, "95%"), (BUDGET_WARN_PCT, "80%")]:
                if pct >= threshold:
                    dedup_key = f"budget_{today}_{label}"
                    already = await self._redis.get(
                        f"{_REDIS_BUDGET_ALERTED}{dedup_key}"
                    )
                    if not already:
                        await self._redis.setex(
                            f"{_REDIS_BUDGET_ALERTED}{dedup_key}",
                            86400, "1"
                        )
                        emoji = "🚨" if threshold >= 0.95 else "⚠️"
                        _log("warning", "watchdog.budget_warning",
                             used=self._snapshot.llm_budget_used,
                             limit=limit, pct=round(pct * 100))
                        await self._alert(
                            f"{emoji} *LLM Budget {label}*\n"
                            f"Used: ${self._snapshot.llm_budget_used:.2f} "
                            f"/ ${limit:.2f} ({pct*100:.0f}%)"
                        )
                    break  # only fire highest threshold

        except Exception as e:
            _log("debug", "watchdog.budget_check_error", error=str(e))

    # ── Signal block rate ─────────────────────────────────────────────────────

    async def _check_signal_block_rate(self) -> None:
        """
        Alert if the pre-trade filter is blocking an abnormally high
        fraction of signals (e.g., regime filter misconfigured).
        Uses Redis counters written by the pre-trade filter.
        """
        try:
            now_ts   = int(time.time())
            hour_ago = now_ts - 3600
            window   = str(hour_ago // 60)  # bucket by minute

            # Expect keys: signals:actionable:count  signals:blocked:count
            # (rolling hourly counters reset at midnight or via INCRBY with TTL)
            actionable = await self._redis.get("signals:actionable:count:1h") or "0"
            blocked    = await self._redis.get("signals:blocked:count:1h") or "0"

            a = int(actionable)
            b = int(blocked)
            total = a + b

            if total < 10:
                return  # not enough data

            block_rate = b / total
            self._snapshot.block_rate_1h = round(block_rate, 3)

            if block_rate >= BLOCK_RATE_ALERT:
                _log("warning", "watchdog.high_block_rate",
                     actionable=a, blocked=b, rate=round(block_rate, 2))
                await self._alert(
                    f"⚠️ *Pre-Trade Filter Blocking {block_rate*100:.0f}% of Signals*\n"
                    f"Last hour: {b} blocked / {a} actionable\n"
                    f"Check regime filter, technical threshold, or filter config.",
                    dedup_key="high_block_rate",
                )

        except Exception as e:
            _log("debug", "watchdog.block_rate_error", error=str(e))

    # ── Restart logic ─────────────────────────────────────────────────────────

    async def _maybe_restart(self, container_name: str) -> None:
        """Auto-restart with cooldown, hourly limit, and market-hours guard."""
        if container_name not in AUTO_RESTART_SERVICES:
            return

        # Suppress restarts outside market hours for connector services only
        if container_name in CONNECTOR_SERVICES and not _is_market_hours():
            _log("debug", "watchdog.restart_suppressed_outside_hours",
                 container=container_name)
            return

        health = self._health[container_name]
        now    = time.time()

        # Cooldown check
        if now - health.last_restart < RESTART_COOLDOWN:
            remaining = int(RESTART_COOLDOWN - (now - health.last_restart))
            _log("debug", "watchdog.restart_cooldown",
                 container=container_name, seconds_remaining=remaining)
            return

        # Prune in-memory hourly window
        hour_ago = now - 3600
        self._restart_history[container_name] = [
            t for t in self._restart_history[container_name] if t > hour_ago
        ]

        if len(self._restart_history[container_name]) >= MAX_AUTO_RESTARTS:
            _log("error", "watchdog.restart_limit_reached",
                 container=container_name,
                 count=len(self._restart_history[container_name]))
            await self._alert(
                f"🚨 *Restart limit reached*\n"
                f"`{MONITORED_SERVICES.get(container_name, container_name)}`\n"
                f"Failed {MAX_AUTO_RESTARTS}× in last hour — manual intervention needed",
                dedup_key=f"restart_limit_{container_name}",
            )
            return

        _log("info", "watchdog.auto_restart", container=container_name)
        try:
            transport = httpx.AsyncHTTPTransport(uds=self._docker_socket)
            async with httpx.AsyncClient(transport=transport, timeout=10.0) as docker:
                resp = await docker.post(
                    f"http://docker/containers/{container_name}/restart"
                )
                if resp.status_code in (200, 204):
                    health.restart_count += 1
                    health.last_restart   = now
                    self._restart_history[container_name].append(now)
                    await self._persist_restart_count(container_name)
                    _log("info", "watchdog.restarted", container=container_name,
                         total_today=health.restart_count)
                    await self._alert(
                        f"🔄 *Auto-restarted*: "
                        f"`{MONITORED_SERVICES.get(container_name, container_name)}`\n"
                        f"Restart #{health.restart_count} today",
                        dedup_key=None,   # always send restart confirmations
                    )
                else:
                    _log("error", "watchdog.restart_failed",
                         container=container_name, status=resp.status_code)
        except Exception as e:
            _log("error", "watchdog.restart_error",
                 container=container_name, error=str(e))

    # ── Events ────────────────────────────────────────────────────────────────

    async def _on_service_down(self, container_name: str, reason: str) -> None:
        name = MONITORED_SERVICES.get(container_name, container_name)
        _log("error", "watchdog.service_down", service=name, reason=reason)
        await self._alert(
            f"🔴 *Service Down*: `{name}`\n"
            f"Reason: {reason}\n"
            f"Time: {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC",
            dedup_key=f"down_{container_name}",
        )

    # ── Snapshot ──────────────────────────────────────────────────────────────

    async def _save_snapshot(self) -> None:
        """Save current health snapshot to Redis for admin bot to query."""
        self._snapshot.timestamp    = datetime.now(timezone.utc).isoformat()
        self._snapshot.market_hours = _is_market_hours()
        self._snapshot.services     = {k: asdict(v) for k, v in self._health.items()}
        self._snapshot.overall_healthy = all(
            h.healthy and h.heartbeat_ok
            for h in self._health.values()
            if h.container in AUTO_RESTART_SERVICES
        )

        # Pull today's signal/news counters from Redis for snapshot enrichment
        try:
            today = datetime.now(timezone.utc).date().isoformat()
            sigs    = await self._redis.get(f"signals:count:{today}")
            blocked = await self._redis.get(f"signals:blocked:count:{today}")
            news    = await self._redis.get(f"news:processed:{today}")
            if sigs:    self._snapshot.signals_today          = int(sigs)
            if blocked: self._snapshot.signals_blocked_today  = int(blocked)
            if news:    self._snapshot.news_processed_today   = int(news)
        except Exception:
            pass

        try:
            await self._redis.setex(
                _REDIS_SNAPSHOT_KEY,
                120,
                json.dumps(asdict(self._snapshot)),
            )
        except Exception as e:
            _log("warning", "watchdog.snapshot_save_error", error=str(e))

    # ── Telegram with deduplication ────────────────────────────────────────────

    async def _alert(
        self,
        message: str,
        dedup_key: str | None = None,
        dedup_ttl: int = ALERT_DEDUP_TTL,
    ) -> None:
        """
        Send alert to admin Telegram chat.

        If dedup_key is provided, the alert is suppressed if the same key was
        already sent within dedup_ttl seconds. Pass dedup_key=None to always send
        (e.g., restart confirmations).
        """
        if not self._bot_token or not self._admin_chat_id:
            _log("debug", "watchdog.alert_no_config", message=message[:60])
            return

        # Deduplication via Redis
        if dedup_key is not None:
            redis_key = f"{_REDIS_ALERT_DEDUP_PFX}{dedup_key}"
            try:
                already = await self._redis.get(redis_key)
                if already:
                    _log("debug", "watchdog.alert_suppressed", dedup_key=dedup_key)
                    return
                await self._redis.setex(redis_key, dedup_ttl, "1")
            except Exception:
                pass  # Redis failure — fall through and send anyway

        try:
            await self._http.post(
                f"https://api.telegram.org/bot{self._bot_token}/sendMessage",
                json={
                    "chat_id": self._admin_chat_id,
                    "text": message,
                    "parse_mode": "Markdown",
                },
            )
        except Exception as e:
            _log("warning", "watchdog.alert_error", error=str(e))


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    import signal
    watchdog = WatchdogService()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, watchdog.stop)
    loop.add_signal_handler(signal.SIGINT,  watchdog.stop)
    await watchdog.start()


if __name__ == "__main__":
    asyncio.run(main())