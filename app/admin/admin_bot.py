"""
Admin Bot — Telegram bot for remote system management.

Commands:
  /status    — full system health dashboard
  /ps        — list all containers with status
  /stop      — stop all trading services (keeps infra running)
  /start     — start all trading services
  /restart   — restart a specific service
  /logs      — get last 20 lines of a service
  /budget    — LLM daily spend summary
  /signals   — recent signals summary
  /lag       — Kafka consumer lag
  /help      — command list

Security: only responds to ADMIN_CHAT_ID — ignores all other chats.
"""
from __future__ import annotations

import asyncio
import json
import os
import signal as signal_module
from datetime import datetime, timezone
from typing import Any

import httpx
import redis.asyncio as aioredis

POLL_INTERVAL = 1.0   # Telegram long-poll timeout seconds


def _log(level: str, event: str, **kw) -> None:
    entry = {"ts": datetime.now(timezone.utc).isoformat(),
             "level": level, "event": event, **kw}
    print(json.dumps(entry), flush=True)


# All trading services (not infra) that can be stopped/started
TRADING_SERVICES = [
    "trading_connector_benzinga",
    "trading_connector_polygon_news",
    "trading_connector_earnings",
    "trading_connector_fred",
    "trading_pipeline_normalizer",
    "trading_pipeline_deduplicator",
    "trading_pipeline_entity_resolver",
    "trading_signals_ai_summarizer",
    "trading_signals_aggregator",
    "trading_signals_telegram",
]

SERVICE_DISPLAY = {
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
    "trading_minio":                        "MinIO",
    "trading_grafana":                      "Grafana",
    "trading_prometheus":                   "Prometheus",
    "trading_adminer":                      "Adminer",
    "trading_redpanda_console":             "Redpanda Console",
}

STATUS_EMOJI = {
    "running":   "🟢",
    "stopped":   "🔴",
    "unhealthy": "🟠",
    "exited":    "⚫",
    "unknown":   "⚪",
}


class AdminBot:

    def __init__(self) -> None:
        self._running = False
        self._bot_token = os.environ.get("ADMIN_BOT_TOKEN", "")
        self._admin_chat_id = str(os.environ.get("ADMIN_CHAT_ID", ""))
        self._docker_socket = "/var/run/docker.sock"
        self._last_update_id = 0
        self._http: httpx.AsyncClient | None = None
        self._redis: aioredis.Redis | None = None

    async def start(self) -> None:
        if not self._bot_token:
            _log("error", "admin_bot.no_token",
                 msg="ADMIN_BOT_TOKEN not set — admin bot disabled")
            return

        self._running = True
        self._http = httpx.AsyncClient(timeout=35.0)
        self._redis = await aioredis.from_url(
            os.environ.get("REDIS_URL", "redis://redis:6379/0"),
            decode_responses=True,
        )

        # Get bot info
        me = await self._api("getMe")
        bot_name = me["result"]["username"] if me else "unknown"
        _log("info", "admin_bot.started", bot=bot_name,
             admin_chat=self._admin_chat_id)

        await self._send(
            "🤖 *Admin Bot Online*\n"
            f"Trading system monitor ready.\n"
            f"Type /help for commands."
        )

        while self._running:
            try:
                await self._poll()
            except Exception as e:
                _log("error", "admin_bot.poll_error", error=str(e))
                await asyncio.sleep(5)

    def stop(self) -> None:
        self._running = False

    # ── Telegram polling ──────────────────────────────────────────────────────

    async def _poll(self) -> None:
        """Long-poll for new messages."""
        resp = await self._api("getUpdates", {
            "offset": self._last_update_id + 1,
            "timeout": 30,
            "allowed_updates": ["message"],
        })
        if not resp or not resp.get("result"):
            return

        for update in resp["result"]:
            self._last_update_id = update["update_id"]
            msg = update.get("message", {})
            chat_id = str(msg.get("chat", {}).get("id", ""))
            text = msg.get("text", "").strip()

            # Security: ignore messages not from admin
            if chat_id != self._admin_chat_id:
                _log("warning", "admin_bot.unauthorized",
                     chat_id=chat_id, text=text[:30])
                continue

            if text:
                await self._handle_command(text)

    async def _handle_command(self, text: str) -> None:
        """Route command to handler."""
        parts = text.split()
        cmd = parts[0].lower().lstrip("/")
        args = parts[1:] if len(parts) > 1 else []

        _log("info", "admin_bot.command", cmd=cmd, args=args)

        handlers = {
            "start":   self._cmd_help,
            "help":    self._cmd_help,
            "status":  self._cmd_status,
            "ps":      self._cmd_ps,
            "stop":    self._cmd_stop,
            "go":      self._cmd_start,
            "restart": self._cmd_restart,
            "logs":    self._cmd_logs,
            "budget":  self._cmd_budget,
            "signals": self._cmd_signals,
            "lag":     self._cmd_lag,
        }

        handler = handlers.get(cmd)
        if handler:
            await handler(args)
        else:
            await self._send(f"Unknown command: `{cmd}`\nType /help for list.")

    # ── Commands ──────────────────────────────────────────────────────────────

    async def _cmd_help(self, args: list) -> None:
        await self._send(
            "🤖 *Trading System Admin*\n\n"
            "/status — full health dashboard\n"
            "/ps — all containers status\n"
            "/stop — stop all trading services\n"
            "/go — start all trading services\n"
            "/restart `<service>` — restart one service\n"
            "/logs `<service>` — last 20 log lines\n"
            "/budget — LLM daily spend\n"
            "/signals — recent signals summary\n"
            "/lag — Kafka consumer lag\n\n"
            "Service shortcuts: `benzinga`, `normalizer`, `summarizer`, `aggregator`, `telegram`"
        )

    async def _cmd_status(self, args: list) -> None:
        """Full system health dashboard from watchdog snapshot."""
        snapshot_raw = await self._redis.get("watchdog:snapshot")
        if not snapshot_raw:
            await self._send("⚠️ Watchdog snapshot not available yet.")
            return

        snap = json.loads(snapshot_raw)
        ts = snap.get("timestamp", "")[:19].replace("T", " ")

        overall = "✅ All systems operational" if snap.get("overall_healthy") else "⚠️ Issues detected"

        lines = [f"📊 *System Status* — {ts} UTC", f"", overall, ""]

        services = snap.get("services", {})
        # Group: connectors, pipeline, signals, infra
        groups = {
            "📡 Connectors": ["trading_connector_benzinga", "trading_connector_polygon_news",
                               "trading_connector_earnings", "trading_connector_fred"],
            "⚙️ Pipeline":   ["trading_pipeline_normalizer", "trading_pipeline_deduplicator",
                               "trading_pipeline_entity_resolver"],
            "🧠 Signals":    ["trading_signals_ai_summarizer", "trading_signals_aggregator",
                               "trading_signals_telegram"],
            "🏗️ Infra":      ["trading_redpanda", "trading_postgres", "trading_redis"],
        }

        for group_name, containers in groups.items():
            lines.append(f"*{group_name}*")
            for c in containers:
                svc = services.get(c, {})
                status = svc.get("status", "unknown")
                emoji = STATUS_EMOJI.get(status, "⚪")
                name = SERVICE_DISPLAY.get(c, c)
                restarts = svc.get("restart_count", 0)
                restart_str = f" (↺{restarts})" if restarts else ""
                lines.append(f"  {emoji} {name}{restart_str}")
            lines.append("")

        # Budget
        used = snap.get("llm_budget_used", 0)
        limit = snap.get("llm_budget_limit", 5)
        pct = int(used / limit * 100) if limit else 0
        budget_bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
        lines.append(f"💰 *LLM Budget*: {budget_bar} {pct}% (${used:.2f}/${limit:.2f})")

        await self._send("\n".join(lines))

    async def _cmd_ps(self, args: list) -> None:
        """List all containers with status."""
        containers = await self._docker_containers()
        if not containers:
            await self._send("❌ Cannot reach Docker API")
            return

        lines = ["🐳 *Container Status*\n"]
        for c in sorted(containers, key=lambda x: x.get("Names", [""])[0]):
            name = c.get("Names", ["/unknown"])[0].lstrip("/")
            display = SERVICE_DISPLAY.get(name, name.replace("trading_", ""))
            state = c.get("State", "unknown")
            status = c.get("Status", "")
            emoji = STATUS_EMOJI.get(state, "⚪")
            if "unhealthy" in status.lower():
                emoji = "🟠"
            lines.append(f"{emoji} `{display}`")

        await self._send("\n".join(lines))

    async def _cmd_stop(self, args: list) -> None:
        """Stop all trading services (keep infra running)."""
        await self._send("⏸ Stopping all trading services...")
        stopped = []
        failed = []

        for container in TRADING_SERVICES:
            ok = await self._docker_action(container, "stop")
            name = SERVICE_DISPLAY.get(container, container)
            if ok:
                stopped.append(name)
            else:
                failed.append(name)

        msg = f"⏸ *Stopped {len(stopped)} services*\n"
        if stopped:
            msg += "\n".join(f"  ⚫ {s}" for s in stopped)
        if failed:
            msg += f"\n\n❌ Failed: {', '.join(failed)}"
        await self._send(msg)

    async def _cmd_start(self, args: list) -> None:
        """Start all trading services."""
        await self._send("▶️ Starting all trading services...")
        started = []
        failed = []

        for container in TRADING_SERVICES:
            ok = await self._docker_action(container, "start")
            name = SERVICE_DISPLAY.get(container, container)
            if ok:
                started.append(name)
            else:
                failed.append(name)

        msg = f"▶️ *Started {len(started)} services*\n"
        if started:
            msg += "\n".join(f"  🟢 {s}" for s in started)
        if failed:
            msg += f"\n\n❌ Failed: {', '.join(failed)}"
        await self._send(msg)

    async def _cmd_restart(self, args: list) -> None:
        """Restart a specific service by shortname."""
        if not args:
            await self._send(
                "Usage: /restart `<service>`\n\n"
                "Shortcuts: `benzinga`, `polygon`, `normalizer`, "
                "`deduplicator`, `resolver`, `summarizer`, `aggregator`, `telegram`"
            )
            return

        shortname = args[0].lower()
        container = self._resolve_service(shortname)
        if not container:
            await self._send(f"❌ Unknown service: `{shortname}`")
            return

        name = SERVICE_DISPLAY.get(container, container)
        await self._send(f"🔄 Restarting `{name}`...")
        ok = await self._docker_action(container, "restart")
        if ok:
            await self._send(f"✅ `{name}` restarted")
        else:
            await self._send(f"❌ Failed to restart `{name}`")

    async def _cmd_logs(self, args: list) -> None:
        """Get last 30 lines of logs for a service."""
        if not args:
            await self._send("Usage: /logs `<service>`")
            return

        shortname = args[0].lower()
        container = self._resolve_service(shortname)
        if not container:
            await self._send(f"❌ Unknown service: `{shortname}`")
            return

        name = SERVICE_DISPLAY.get(container, container)
        try:
            transport = httpx.AsyncHTTPTransport(uds=self._docker_socket)
            async with httpx.AsyncClient(transport=transport, timeout=10.0) as docker:
                resp = await docker.get(
                    f"http://docker/containers/{container}/logs"
                    f"?tail=30&stdout=true&stderr=true&timestamps=false"
                )
                raw = resp.content.decode("utf-8", errors="replace")

            # Strip Docker log multiplexing headers (8-byte prefix per line)
            lines = []
            for line in raw.split("\n"):
                if len(line) > 8:
                    lines.append(line[8:] if line[0] in "\x01\x02" else line)
                elif line:
                    lines.append(line)

            log_text = "\n".join(lines[-25:])
            if len(log_text) > 3500:
                log_text = "..." + log_text[-3500:]

            await self._send(f"📋 *Logs: {name}*\n```\n{log_text}\n```")

        except Exception as e:
            await self._send(f"❌ Error fetching logs: {e}")

    async def _cmd_budget(self, args: list) -> None:
        """LLM daily budget summary."""
        snapshot_raw = await self._redis.get("watchdog:snapshot")
        if snapshot_raw:
            snap = json.loads(snapshot_raw)
            used = snap.get("llm_budget_used", 0)
            limit = snap.get("llm_budget_limit", 5)
        else:
            used, limit = 0.0, 5.0

        pct = used / limit * 100 if limit else 0
        bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
        remaining = limit - used
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        await self._send(
            f"💰 *LLM Budget — {today}*\n\n"
            f"`{bar}` {pct:.1f}%\n\n"
            f"Spent:     ${used:.4f}\n"
            f"Remaining: ${remaining:.4f}\n"
            f"Limit:     ${limit:.2f}/day\n\n"
            f"T1 (Haiku):  ~$0.0008/item\n"
            f"T2 (Sonnet): ~$0.011/item"
        )

    async def _cmd_signals(self, args: list) -> None:
        """Recent signals from Redis."""
        try:
            keys = await self._redis.keys("signal:*")
            if not keys:
                await self._send("📭 No recent signals in Redis")
                return

            lines = [f"📡 *Recent Signals* ({len(keys)} total)\n"]
            # Get last 10
            for key in sorted(keys)[-10:]:
                raw = await self._redis.get(key)
                if raw:
                    sig = json.loads(raw)
                    emoji = "🟢" if sig.get("direction") == "long" else \
                            "🔴" if sig.get("direction") == "short" else "🟡"
                    ticker = sig.get("ticker", "?")
                    conviction = int(sig.get("conviction", 0) * 100)
                    signal_type = sig.get("signal_type", "")
                    lines.append(f"{emoji} *{ticker}* {conviction}% — {signal_type}")

            await self._send("\n".join(lines))
        except Exception as e:
            await self._send(f"❌ Error: {e}")

    async def _cmd_lag(self, args: list) -> None:
        """Kafka consumer lag."""
        snapshot_raw = await self._redis.get("watchdog:snapshot")
        if not snapshot_raw:
            await self._send("⚠️ No watchdog data available")
            return

        snap = json.loads(snapshot_raw)
        lag = snap.get("kafka_lag", {})

        if not lag:
            await self._send("✅ No consumer lag detected (or data not available)")
            return

        lines = ["📊 *Kafka Consumer Lag*\n"]
        for group, count in sorted(lag.items(), key=lambda x: -x[1]):
            emoji = "🔴" if count > 1000 else "🟠" if count > 100 else "🟢"
            lines.append(f"{emoji} `{group}`: {count:,} messages")

        await self._send("\n".join(lines))

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _resolve_service(self, shortname: str) -> str | None:
        """Map shortname to full container name."""
        mapping = {
            "benzinga":    "trading_connector_benzinga",
            "polygon":     "trading_connector_polygon_news",
            "earnings":    "trading_connector_earnings",
            "fred":        "trading_connector_fred",
            "normalizer":  "trading_pipeline_normalizer",
            "deduplicator": "trading_pipeline_deduplicator",
            "resolver":    "trading_pipeline_entity_resolver",
            "summarizer":  "trading_signals_ai_summarizer",
            "aggregator":  "trading_signals_aggregator",
            "telegram":    "trading_signals_telegram",
            "redpanda":    "trading_redpanda",
            "postgres":    "trading_postgres",
            "redis":       "trading_redis",
        }
        return mapping.get(shortname)

    async def _docker_containers(self) -> list[dict]:
        try:
            transport = httpx.AsyncHTTPTransport(uds=self._docker_socket)
            async with httpx.AsyncClient(transport=transport, timeout=5.0) as docker:
                resp = await docker.get("http://docker/containers/json?all=true")
                return resp.json() if resp.status_code == 200 else []
        except Exception:
            return []

    async def _docker_action(self, container: str, action: str) -> bool:
        """stop | start | restart a container."""
        try:
            transport = httpx.AsyncHTTPTransport(uds=self._docker_socket)
            async with httpx.AsyncClient(transport=transport, timeout=30.0) as docker:
                resp = await docker.post(
                    f"http://docker/containers/{container}/{action}"
                )
                return resp.status_code in (200, 204, 304)
        except Exception as e:
            _log("error", "admin_bot.docker_action_error",
                 container=container, action=action, error=str(e))
            return False

    async def _api(self, method: str, params: dict | None = None) -> dict | None:
        """Call Telegram Bot API."""
        try:
            url = f"https://api.telegram.org/bot{self._bot_token}/{method}"
            if params:
                resp = await self._http.post(url, json=params)
            else:
                resp = await self._http.get(url)
            return resp.json()
        except Exception as e:
            _log("error", "admin_bot.api_error", method=method, error=str(e))
            return None

    async def _send(self, text: str) -> None:
        """Send message to admin chat."""
        if not self._admin_chat_id:
            return
        await self._api("sendMessage", {
            "chat_id": self._admin_chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        })


async def main() -> None:
    bot = AdminBot()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal_module.SIGTERM, bot.stop)
    loop.add_signal_handler(signal_module.SIGINT, bot.stop)
    await bot.start()


if __name__ == "__main__":
    asyncio.run(main())
