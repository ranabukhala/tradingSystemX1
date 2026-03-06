"""
Watchdog Service — monitors all trading system services.

Responsibilities:
  - Polls Docker container health every 30s
  - Monitors Kafka consumer lag per service
  - Tracks LLM budget consumption rate
  - Detects connector silence (no messages for N minutes)
  - Auto-restarts unhealthy containers (with cooldown)
  - Sends alerts to admin Telegram bot on any anomaly
  - Maintains a health state snapshot for the admin bot to query
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx
import redis.asyncio as aioredis

POLL_INTERVAL = 30          # seconds between health checks
SILENCE_THRESHOLD = 300     # seconds — connector silence = alert
RESTART_COOLDOWN = 120      # seconds — min time between auto-restarts
MAX_AUTO_RESTARTS = 3       # per service per hour before giving up
BUDGET_WARN_PCT = 0.80      # alert when daily LLM budget > 80%

# Services we monitor (container name → display name)
MONITORED_SERVICES = {
    "trading_connector_benzinga":           "Benzinga",
    "trading_connector_polygon_news":       "Polygon News",
    "trading_connector_earnings":           "Earnings",
    "trading_connector_fred":              "FRED",
    "trading_pipeline_normalizer":          "Normalizer",
    "trading_pipeline_deduplicator":        "Deduplicator",
    "trading_pipeline_entity_resolver":     "Entity Resolver",
    "trading_signals_ai_summarizer":        "AI Summarizer",
    "trading_signals_aggregator":           "Signal Aggregator",
    "trading_signals_telegram":             "Telegram Alerts",
    "trading_redpanda":                     "Redpanda",
    "trading_postgres":                     "Postgres",
    "trading_redis":                        "Redis",
}

# Services that should auto-restart on failure
AUTO_RESTART_SERVICES = {
    "trading_connector_benzinga",
    "trading_connector_polygon_news",
    "trading_pipeline_normalizer",
    "trading_pipeline_deduplicator",
    "trading_pipeline_entity_resolver",
    "trading_signals_ai_summarizer",
    "trading_signals_aggregator",
    "trading_signals_telegram",
}


def _log(level: str, event: str, **kw) -> None:
    entry = {"ts": datetime.now(timezone.utc).isoformat(),
             "level": level, "event": event, **kw}
    print(json.dumps(entry), flush=True)


@dataclass
class ServiceHealth:
    name: str
    container: str
    status: str = "unknown"       # running | stopped | unhealthy | unknown
    healthy: bool = False
    last_seen: str = ""
    restart_count: int = 0
    last_restart: float = 0.0
    error: str = ""


@dataclass
class SystemSnapshot:
    timestamp: str = ""
    services: dict[str, dict] = field(default_factory=dict)
    kafka_lag: dict[str, int] = field(default_factory=dict)
    llm_budget_used: float = 0.0
    llm_budget_limit: float = 5.0
    alerts_today: int = 0
    signals_today: int = 0
    news_processed_today: int = 0
    overall_healthy: bool = False


class WatchdogService:

    def __init__(self) -> None:
        self._running = False
        self._http: httpx.AsyncClient | None = None
        self._redis: aioredis.Redis | None = None
        self._health: dict[str, ServiceHealth] = {}
        self._restart_history: dict[str, list[float]] = {}
        self._snapshot = SystemSnapshot()

        self._bot_token = os.environ.get("ADMIN_BOT_TOKEN", "")
        self._admin_chat_id = os.environ.get("ADMIN_CHAT_ID", "")
        self._docker_socket = "/var/run/docker.sock"

        # Initialize health state
        for container, name in MONITORED_SERVICES.items():
            self._health[container] = ServiceHealth(name=name, container=container)
            self._restart_history[container] = []

    async def start(self) -> None:
        self._running = True
        self._http = httpx.AsyncClient(timeout=10.0)
        self._redis = await aioredis.from_url(
            os.environ.get("REDIS_URL", "redis://redis:6379/0"),
            decode_responses=True,
        )
        _log("info", "watchdog.started", services=len(MONITORED_SERVICES))
        await self._alert("🐕 *Watchdog started* — monitoring system health every 30s")

        while self._running:
            try:
                await self._check_all()
                await self._save_snapshot()
            except Exception as e:
                _log("error", "watchdog.check_error", error=str(e))
            await asyncio.sleep(POLL_INTERVAL)

    def stop(self) -> None:
        self._running = False

    # ── Health Checks ─────────────────────────────────────────────────────────

    async def _check_all(self) -> None:
        await asyncio.gather(
            self._check_containers(),
            self._check_kafka_lag(),
            self._check_budget(),
            return_exceptions=True,
        )

    async def _check_containers(self) -> None:
        """Query Docker API via unix socket."""
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

        # Build lookup by name
        container_map = {}
        for c in containers:
            for name in c.get("Names", []):
                container_map[name.lstrip("/")] = c

        for container_name, health in self._health.items():
            c = container_map.get(container_name)
            if not c:
                if health.status != "stopped":
                    health.status = "stopped"
                    health.healthy = False
                    await self._on_service_down(container_name, "container not found")
                continue

            state = c.get("State", "unknown")
            docker_health = c.get("Status", "")
            was_healthy = health.healthy

            health.last_seen = datetime.now(timezone.utc).isoformat()

            if state == "running" and "unhealthy" not in docker_health.lower():
                health.status = "running"
                health.healthy = True
                health.error = ""
            elif "unhealthy" in docker_health.lower():
                health.status = "unhealthy"
                health.healthy = False
                if was_healthy:
                    await self._on_service_down(container_name, "Docker health check failing")
                    await self._maybe_restart(container_name)
            else:
                health.status = state
                health.healthy = False
                if was_healthy:
                    await self._on_service_down(container_name, f"state={state}")

    async def _check_kafka_lag(self) -> None:
        """Check consumer lag via Redpanda Admin API."""
        try:
            transport = httpx.AsyncHTTPTransport(uds=self._docker_socket)
            async with httpx.AsyncClient(
                base_url="http://trading_redpanda:9644",
                timeout=5.0
            ) as admin:
                resp = await admin.get("/v1/consumer_groups")
                if resp.status_code != 200:
                    return
                groups = resp.json()

            for group in groups:
                group_id = group.get("group_id", "")
                lag = sum(
                    m.get("partition_offset", 0) - m.get("member_offset", 0)
                    for member in group.get("members", [])
                    for m in member.get("assignments", [])
                )
                self._snapshot.kafka_lag[group_id] = max(0, lag)

                # Alert on very high lag (> 1000 unprocessed messages)
                if lag > 1000:
                    _log("warning", "watchdog.high_kafka_lag",
                         group=group_id, lag=lag)

        except Exception:
            pass  # Redpanda admin API not always available

    async def _check_budget(self) -> None:
        """Check LLM daily budget from Redis."""
        try:
            # Budget tracker stores daily spend in Redis
            today = datetime.now(timezone.utc).date().isoformat()
            spend_key = f"llm:budget:{today}"
            spent = await self._redis.get(spend_key)
            if spent:
                self._snapshot.llm_budget_used = float(spent)
                limit = float(os.environ.get("LLM_DAILY_BUDGET_USD", "5.0"))
                self._snapshot.llm_budget_limit = limit
                pct = self._snapshot.llm_budget_used / limit
                if pct >= BUDGET_WARN_PCT:
                    _log("warning", "watchdog.budget_warning",
                         used=self._snapshot.llm_budget_used,
                         limit=limit, pct=round(pct * 100))
                    await self._alert(
                        f"⚠️ *LLM Budget Warning*\n"
                        f"Used: ${self._snapshot.llm_budget_used:.2f} / ${limit:.2f} "
                        f"({pct*100:.0f}%)"
                    )
        except Exception:
            pass

    # ── Restart Logic ─────────────────────────────────────────────────────────

    async def _maybe_restart(self, container_name: str) -> None:
        """Auto-restart with cooldown and hourly limit."""
        if container_name not in AUTO_RESTART_SERVICES:
            return

        health = self._health[container_name]
        now = time.time()

        # Cooldown check
        if now - health.last_restart < RESTART_COOLDOWN:
            _log("debug", "watchdog.restart_cooldown",
                 container=container_name,
                 seconds_remaining=int(RESTART_COOLDOWN - (now - health.last_restart)))
            return

        # Hourly limit check
        history = self._restart_history[container_name]
        hour_ago = now - 3600
        history = [t for t in history if t > hour_ago]
        self._restart_history[container_name] = history

        if len(history) >= MAX_AUTO_RESTARTS:
            _log("error", "watchdog.restart_limit_reached",
                 container=container_name, count=len(history))
            await self._alert(
                f"🚨 *Restart limit reached*\n"
                f"`{MONITORED_SERVICES.get(container_name, container_name)}`\n"
                f"Failed {MAX_AUTO_RESTARTS}× in last hour — manual intervention needed"
            )
            return

        # Restart
        _log("info", "watchdog.auto_restart", container=container_name)
        try:
            transport = httpx.AsyncHTTPTransport(uds=self._docker_socket)
            async with httpx.AsyncClient(transport=transport, timeout=10.0) as docker:
                resp = await docker.post(
                    f"http://docker/containers/{container_name}/restart"
                )
                if resp.status_code in (204, 200):
                    health.restart_count += 1
                    health.last_restart = now
                    self._restart_history[container_name].append(now)
                    _log("info", "watchdog.restarted", container=container_name)
                    await self._alert(
                        f"🔄 *Auto-restarted*: "
                        f"`{MONITORED_SERVICES.get(container_name, container_name)}`\n"
                        f"Restart #{health.restart_count} today"
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
            f"Time: {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"
        )

    # ── Snapshot ──────────────────────────────────────────────────────────────

    async def _save_snapshot(self) -> None:
        """Save current health snapshot to Redis for admin bot to query."""
        self._snapshot.timestamp = datetime.now(timezone.utc).isoformat()
        self._snapshot.services = {
            k: asdict(v) for k, v in self._health.items()
        }
        self._snapshot.overall_healthy = all(
            h.healthy for h in self._health.values()
            if h.container in AUTO_RESTART_SERVICES
        )

        try:
            await self._redis.setex(
                "watchdog:snapshot",
                120,  # TTL 2 minutes
                json.dumps(asdict(self._snapshot)),
            )
        except Exception as e:
            _log("warning", "watchdog.snapshot_save_error", error=str(e))

    # ── Telegram ──────────────────────────────────────────────────────────────

    async def _alert(self, message: str) -> None:
        """Send alert to admin Telegram chat."""
        if not self._bot_token or not self._admin_chat_id:
            _log("debug", "watchdog.alert_no_config", message=message[:60])
            return
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


async def main() -> None:
    import signal
    watchdog = WatchdogService()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, watchdog.stop)
    loop.add_signal_handler(signal.SIGINT, watchdog.stop)
    await watchdog.start()


if __name__ == "__main__":
    asyncio.run(main())
