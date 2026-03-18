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
  /apistatus — test all external API connections live
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
        self._db_url = os.environ.get("DATABASE_URL", "postgresql://trading:tradingpass@postgres:5432/trading_db")

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
            "signals":      self._cmd_signals,
            "lag":          self._cmd_lag,
            "performance":  self._cmd_performance,
            "outcomes":     self._cmd_outcomes,
            "run_outcomes": self._cmd_run_outcomes,
            "trades":       self._cmd_trades,
            "apistatus":    self._cmd_apistatus,
            "logreport":    self._cmd_logreport,
        }

        handler = handlers.get(cmd)
        if handler:
            await handler(args)
        else:
            await self._send(f"Unknown command: `{cmd}`\nType /help for list.")

    # ── Commands ──────────────────────────────────────────────────────────────

    async def _cmd_help(self, args: list) -> None:
        lines = [
            "Trading System Admin",
            "",
            "/status - full health dashboard",
            "/ps - all containers status",
            "/stop - stop all trading services",
            "/go - start all trading services",
            "/restart NAME - restart one service",
            "/logs NAME - last 20 log lines",
            "/budget - LLM daily spend",
            "/signals - recent signals summary",
            "/lag - Kafka consumer lag",
            "/performance - signal accuracy stats",
            "/outcomes - yesterday trade outcomes",
            "/run_outcomes - fetch outcomes from Polygon",
            "/trades - show recent executed trades",
            "/apistatus - test all external API connections",
            "/logreport [mins] - AI diagnosis of errors & warnings (default 30m)",
            "",
            "Shortcuts: benzinga, normalizer, summarizer, aggregator",
        ]
        await self._send("\n".join(lines), markdown=False)


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


    async def _cmd_performance(self, args: list) -> None:
        """Signal accuracy and trade stats."""
        try:
            import asyncpg
            conn = await asyncpg.connect(self._db_url)

            # Check if signal_log exists
            has_signal_log = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'signal_log'
                )
            """)

            # Check if trade table exists
            has_trade = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'trade'
                )
            """)

            lines = ["=== Performance Summary ===\n"]

            if has_trade:
                trade_stats = await conn.fetchrow("""
                    SELECT
                        COUNT(*)                            AS total,
                        COUNT(DISTINCT ticker)              AS tickers,
                        SUM(CASE WHEN direction='long' THEN 1 ELSE 0 END)  AS longs,
                        SUM(CASE WHEN direction='short' THEN 1 ELSE 0 END) AS shorts,
                        ROUND(AVG(conviction)::numeric, 2)  AS avg_conviction,
                        MIN(created_at AT TIME ZONE 'America/New_York')    AS first_trade,
                        MAX(created_at AT TIME ZONE 'America/New_York')    AS last_trade
                    FROM trade
                    WHERE created_at >= now() - interval '30 days'
                """)
                t = trade_stats
                first = str(t["first_trade"])[:16] if t["first_trade"] else "n/a"
                last  = str(t["last_trade"])[:16]  if t["last_trade"]  else "n/a"
                lines += [
                    f"--- Trades (last 30d) ---",
                    f"Total trades:    {t['total']}",
                    f"Unique tickers:  {t['tickers']}",
                    f"Longs/Shorts:    {t['longs']}/{t['shorts']}",
                    f"Avg conviction:  {t['avg_conviction']}",
                    f"First trade:     {first}",
                    f"Last trade:      {last}",
                ]

                # Top tickers
                top = await conn.fetch("""
                    SELECT ticker, COUNT(*) as n, direction
                    FROM trade
                    WHERE created_at >= now() - interval '30 days'
                    GROUP BY ticker, direction
                    ORDER BY n DESC LIMIT 5
                """)
                if top:
                    lines.append("\nMost traded:")
                    for r in top:
                        arrow = "↑" if r["direction"] == "long" else "↓"
                        lines.append(f"  {arrow} {r['ticker']} x{r['n']}")
            else:
                lines.append("trade table not found — run migration 004")

            if has_signal_log:
                sig_stats = await conn.fetchrow("""
                    SELECT COUNT(*) AS total,
                           SUM(passed_gate::int) AS passed,
                           SUM(was_executed::int) AS executed,
                           COUNT(outcome_correct) AS evaluated,
                           ROUND(100.0 * SUM(outcome_correct::int)
                                 / NULLIF(COUNT(outcome_correct),0), 1) AS accuracy
                    FROM signal_log
                    WHERE created_at >= now() - interval '30 days'
                """)
                s = sig_stats
                if s["total"] > 0:
                    acc = s["accuracy"] or 0
                    emoji = "[OK]" if acc >= 60 else "[WARN]" if acc >= 50 else "[LOW]"
                    lines += [
                        f"\n--- Signal Log (last 30d) ---",
                        f"Total evaluated: {s['total']}",
                        f"Passed gate:     {s['passed']}",
                        f"Executed:        {s['executed']}",
                        f"Accuracy:        {emoji} {acc}%  ({s['evaluated']} outcomes)",
                    ]
                else:
                    lines.append("\nsignal_log empty — aggregator not writing yet")
            else:
                lines.append("\nsignal_log not found — run migration 005")

            await conn.close()
            await self._send("\n".join(lines), markdown=False)

        except Exception as e:
            await self._send(f"DB error: {e}")

    async def _cmd_outcomes(self, args: list) -> None:
        """Yesterday's signal outcomes — ✓/✗ per ticker."""
        try:
            import asyncpg
            conn = await asyncpg.connect(self._db_url)

            # Support /outcomes 2026-03-07 for specific date
            if args:
                try:
                    from datetime import date
                    target = f"DATE(created_at AT TIME ZONE \'America/New_York\') = \'{args[0]}\'"
                except Exception:
                    target = "created_at >= now() - interval \'24 hours\'"
            else:
                target = "DATE(created_at AT TIME ZONE \'America/New_York\') = CURRENT_DATE - 1"

            rows = await conn.fetch(f"""
                SELECT
                    ticker, direction, conviction,
                    catalyst_type, outcome_correct,
                    outcome_price_1h, outcome_price_1d,
                    passed_gate, was_executed
                FROM signal_log
                WHERE {target}
                  AND passed_gate = TRUE
                ORDER BY conviction DESC
                LIMIT 20
            """)

            await conn.close()

            if not rows:
                await self._send("📭 No passed signals found for that date.")
                return

            correct   = sum(1 for r in rows if r["outcome_correct"] is True)
            incorrect = sum(1 for r in rows if r["outcome_correct"] is False)
            pending   = sum(1 for r in rows if r["outcome_correct"] is None)
            evaluated = correct + incorrect
            acc = int(correct / evaluated * 100) if evaluated else 0

            lines = [f"📊 Outcomes — {correct}/{evaluated} correct ({acc}%)\n"]

            for r in rows:
                if r["outcome_correct"] is True:
                    mark = "✅"
                elif r["outcome_correct"] is False:
                    mark = "❌"
                else:
                    mark = "⏳"

                direction = "↑" if r["direction"] == "long" else "↓"
                move = f"{r['outcome_price_1d']:+.1f}%" if r["outcome_price_1d"] is not None else "pending"
                exec_mark = "🔫" if r["was_executed"] else ""
                lines.append(
                    f"{mark} *{r['ticker']}* {direction} {int(r['conviction']*100)}%  "
                    f"`{move}` {exec_mark}"
                )

            if pending:
                lines.append(f"\n⏳ {pending} signals still pending outcomes — run /run_outcomes")

            await self._send("\n".join(lines))

        except Exception as e:
            await self._send(f"❌ DB error: {e}")

    async def _cmd_run_outcomes(self, args: list) -> None:
        """Trigger outcome tracker to fetch prices from Polygon now."""
        await self._send("⏳ Fetching outcomes from Polygon...")
        try:
            import asyncio
            import sys
            import subprocess
            result = subprocess.run(
                ["python", "-m", "app.scripts.outcome_tracker"] + args,
                capture_output=True, text=True, timeout=120,
            )
            output = result.stdout[-1500:] if result.stdout else ""
            stderr = result.stderr[-500:] if result.stderr else ""

            if result.returncode == 0:
                # Extract just the summary lines
                lines = [l for l in output.split("\n") if any(
                    k in l for k in ["Correct:", "Evaluated:", "Accuracy", "correct", "✓", "✗", "==="]
                )]
                summary = "\n".join(lines[-20:]) if lines else output[-800:]
                await self._send(f"✅ *Outcomes updated*\n\n```\n{summary}\n```")
            else:
                await self._send(f"❌ Outcome tracker failed:\n```\n{stderr or output}\n```")
        except subprocess.TimeoutExpired:
            await self._send("⏰ Timed out — try again after market close")
        except Exception as e:
            await self._send(f"❌ Error: {e}")


    async def _cmd_apistatus(self, args: list) -> None:
        """Test all external API connections and report results."""
        await self._send("🔍 Testing all API connections...")

        keys = {k: v for k, v in os.environ.items()}

        results: list[dict] = []

        async def check(api: str, check_name: str, coro) -> None:
            try:
                status, detail, latency = await coro
            except Exception as e:
                status, detail, latency = "FAIL", str(e)[:80], None
            results.append({
                "api": api, "check": check_name,
                "status": status, "detail": detail, "latency": latency,
            })

        async with httpx.AsyncClient(follow_redirects=True, timeout=10.0) as http:
            await asyncio.gather(
                check("Polygon",       "SPY Snapshot",         self._api_polygon_snapshot(keys, http)),
                check("Polygon",       "NVDA SMA-20",          self._api_polygon_sma(keys, http)),
                check("Polygon",       "NVDA Options",         self._api_polygon_options(keys, http)),
                check("Unusual Whales","NVDA Flow Alerts",     self._api_unusual_whales(keys, http)),
                check("Tradier",       "NVDA Options Chain",   self._api_tradier(keys, http)),
                check("IEX Cloud",     "NVDA Short Interest",  self._api_iex(keys, http)),
                check("Finviz",        "NVDA Short Float",     self._api_finviz(http)),
                check("FMP",           "NVDA Quote",           self._api_fmp_quote(keys, http)),
                check("FMP",           "NVDA RSI",             self._api_fmp_rsi(keys, http)),
                check("Finnhub",       "NVDA News",            self._api_finnhub_news(keys, http)),
                check("Finnhub",       "Earnings Calendar",    self._api_finnhub_earnings(keys, http)),
                check("Alpaca",        "Paper Account",        self._api_alpaca(keys, http)),
                return_exceptions=True,
            )

        # ── Format Telegram message ────────────────────────────────────────────
        ok   = [r for r in results if r["status"] == "OK"]
        fail = [r for r in results if r["status"] == "FAIL"]
        warn = [r for r in results if r["status"] == "WARN"]
        skip = [r for r in results if r["status"] == "SKIP"]

        icon = "✅" if not fail else "❌"
        ts = datetime.now(timezone.utc).strftime("%H:%M UTC")
        lines = [f"{icon} *API Status* — {ts}",
                 f"✅ {len(ok)} OK  ❌ {len(fail)} FAIL  ⚠️ {len(warn)} WARN  ⏭ {len(skip)} SKIP",
                 ""]

        STATUS_ICON = {"OK": "✅", "FAIL": "❌", "WARN": "⚠️", "SKIP": "⏭"}

        if fail:
            lines.append("*Failing:*")
            for r in fail:
                lat = f" ({r['latency']:.0f}ms)" if r["latency"] else ""
                lines.append(f"  ❌ {r['api']} / {r['check']}{lat}")
                lines.append(f"      _{r['detail'][:70]}_")
            lines.append("")

        if warn:
            lines.append("*Warnings:*")
            for r in warn:
                lines.append(f"  ⚠️ {r['api']} / {r['check']}: _{r['detail'][:70]}_")
            lines.append("")

        if ok:
            lines.append("*OK:*")
            for r in ok:
                lat = f" {r['latency']:.0f}ms" if r["latency"] else ""
                lines.append(f"  ✅ {r['api']} / {r['check']}{lat} — {r['detail'][:50]}")

        if skip:
            skipped = ", ".join(f"{r['api']}" for r in skip)
            lines.append(f"\n_No key: {skipped}_")

        await self._send("\n".join(lines))

    # ── API check helpers (used by _cmd_apistatus) ────────────────────────────

    async def _api_polygon_snapshot(self, keys: dict, http: httpx.AsyncClient):
        api_key = keys.get("POLYGON_API_KEY", "")
        if not api_key:
            return "SKIP", "POLYGON_API_KEY not set", None
        t0 = asyncio.get_event_loop().time()
        resp = await http.get(
            "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/SPY",
            params={"apiKey": api_key})
        lat = (asyncio.get_event_loop().time() - t0) * 1000
        if resp.status_code == 200:
            price = resp.json().get("ticker", {}).get("day", {}).get("c")
            return "OK", f"SPY=${price}", lat
        return "FAIL", f"HTTP {resp.status_code}", lat

    async def _api_polygon_sma(self, keys: dict, http: httpx.AsyncClient):
        api_key = keys.get("POLYGON_API_KEY", "")
        if not api_key:
            return "SKIP", "POLYGON_API_KEY not set", None
        t0 = asyncio.get_event_loop().time()
        resp = await http.get(
            "https://api.polygon.io/v1/indicators/sma/NVDA",
            params={"timespan": "day", "window": 20, "series_type": "close",
                    "limit": 1, "apiKey": api_key})
        lat = (asyncio.get_event_loop().time() - t0) * 1000
        if resp.status_code == 200:
            vals = resp.json().get("results", {}).get("values", [])
            if vals:
                return "OK", f"SMA20={vals[0]['value']:.2f}", lat
            return "WARN", "No SMA values returned", lat
        return "FAIL", f"HTTP {resp.status_code}", lat

    async def _api_polygon_options(self, keys: dict, http: httpx.AsyncClient):
        api_key = keys.get("POLYGON_API_KEY", "")
        if not api_key:
            return "SKIP", "POLYGON_API_KEY not set", None
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        t0 = asyncio.get_event_loop().time()
        resp = await http.get(
            "https://api.polygon.io/v3/snapshot/options/NVDA",
            params={"apiKey": api_key, "limit": 10, "expiration_date.gte": today})
        lat = (asyncio.get_event_loop().time() - t0) * 1000
        if resp.status_code == 200:
            n = len(resp.json().get("results", []))
            return ("OK" if n else "WARN"), f"{n} contracts", lat
        return "FAIL", f"HTTP {resp.status_code}", lat

    async def _api_unusual_whales(self, keys: dict, http: httpx.AsyncClient):
        api_key = keys.get("UNUSUAL_WHALES_API_KEY", "")
        if not api_key:
            return "SKIP", "UNUSUAL_WHALES_API_KEY not set", None
        t0 = asyncio.get_event_loop().time()
        resp = await http.get(
            "https://api.unusualwhales.com/api/stock/NVDA/flow-alerts",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"limit": 5})
        lat = (asyncio.get_event_loop().time() - t0) * 1000
        if resp.status_code == 200:
            n = len(resp.json().get("data", []))
            return "OK", f"{n} flow alerts", lat
        return "FAIL", f"HTTP {resp.status_code}", lat

    async def _api_tradier(self, keys: dict, http: httpx.AsyncClient):
        api_key = keys.get("TRADIER_API_KEY", "")
        if not api_key:
            return "SKIP", "TRADIER_API_KEY not set", None
        from datetime import timedelta
        days_ahead = (4 - datetime.now(timezone.utc).weekday()) % 7 or 7
        expiry = (datetime.now(timezone.utc) + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        t0 = asyncio.get_event_loop().time()
        resp = await http.get(
            "https://api.tradier.com/v1/markets/options/chains",
            headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
            params={"symbol": "NVDA", "expiration": expiry, "greeks": "false"})
        lat = (asyncio.get_event_loop().time() - t0) * 1000
        if resp.status_code == 200:
            opts = resp.json().get("options") or {}
            n = len(opts.get("option") or [])
            return ("OK" if n else "WARN"), f"{n} contracts for {expiry}", lat
        return "FAIL", f"HTTP {resp.status_code}", lat

    async def _api_iex(self, keys: dict, http: httpx.AsyncClient):
        api_key = keys.get("IEX_API_KEY", "")
        if not api_key:
            return "SKIP", "IEX_API_KEY not set", None
        t0 = asyncio.get_event_loop().time()
        resp = await http.get(
            "https://cloud.iexapis.com/stable/stock/NVDA/short-interest",
            params={"token": api_key})
        lat = (asyncio.get_event_loop().time() - t0) * 1000
        if resp.status_code == 200:
            data = resp.json()
            rec = data[0] if isinstance(data, list) and data else data
            si = rec.get("shortPercent") or rec.get("shortInterestPercent", "n/a")
            return "OK", f"Short interest: {si}", lat
        return "FAIL", f"HTTP {resp.status_code}", lat

    async def _api_finviz(self, http: httpx.AsyncClient):
        import csv, io
        api_key = os.environ.get("FINVIZ_API_KEY", "")
        if not api_key:
            return "SKIP", "FINVIZ_API_KEY not set", None
        t0 = asyncio.get_event_loop().time()
        resp = await http.get(
            "https://elite.finviz.com/export.ashx",
            params={"v": "111", "f": "ticker_NVDA", "c": "1,72,75", "auth": api_key},
        )
        lat = (asyncio.get_event_loop().time() - t0) * 1000
        if resp.status_code == 200:
            rows = list(csv.DictReader(io.StringIO(resp.text)))
            if rows:
                row = rows[0]
                sf = row.get("Short Float") or row.get("Short Float %", "?")
                return "OK", f"NVDA short float: {sf}", lat
            return "WARN", "CSV empty — check filter params", lat
        elif resp.status_code == 401:
            return "FAIL", "401 Unauthorized — key invalid or expired", lat
        return "FAIL", f"HTTP {resp.status_code}", lat

    async def _api_fmp_quote(self, keys: dict, http: httpx.AsyncClient):
        api_key = keys.get("FMP_API_KEY", "")
        if not api_key:
            return "SKIP", "FMP_API_KEY not set", None
        t0 = asyncio.get_event_loop().time()
        resp = await http.get(
            "https://financialmodelingprep.com/stable/quote",
            params={"symbol": "NVDA", "apikey": api_key})
        lat = (asyncio.get_event_loop().time() - t0) * 1000
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and data:
                return "OK", f"NVDA=${data[0].get('price')}", lat
            if isinstance(data, dict) and "Error Message" in data:
                return "FAIL", data["Error Message"][:60], lat
        return "FAIL", f"HTTP {resp.status_code}", lat

    async def _api_fmp_rsi(self, keys: dict, http: httpx.AsyncClient):
        api_key = keys.get("FMP_API_KEY", "")
        if not api_key:
            return "SKIP", "FMP_API_KEY not set", None
        t0 = asyncio.get_event_loop().time()
        resp = await http.get(
            "https://financialmodelingprep.com/stable/technical-indicators/rsi",
            params={"symbol": "NVDA", "periodLength": 14, "timeframe": "1day", "apikey": api_key})
        lat = (asyncio.get_event_loop().time() - t0) * 1000
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and data:
                rsi = data[0].get("value")
                return "OK", f"RSI(14)={float(rsi):.2f}" if rsi is not None else "RSI null", lat
            if isinstance(data, dict) and "Error Message" in data:
                return "FAIL", data["Error Message"][:60], lat
            return "WARN", "No RSI data — check FMP plan", lat
        return "FAIL", f"HTTP {resp.status_code}", lat

    async def _api_finnhub_news(self, keys: dict, http: httpx.AsyncClient):
        api_key = keys.get("FINNHUB_API_KEY", "")
        if not api_key:
            return "SKIP", "FINNHUB_API_KEY not set", None
        from datetime import timedelta
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
        t0 = asyncio.get_event_loop().time()
        resp = await http.get(
            "https://finnhub.io/api/v1/company-news",
            params={"symbol": "NVDA", "from": week_ago, "to": today, "token": api_key})
        lat = (asyncio.get_event_loop().time() - t0) * 1000
        if resp.status_code == 200:
            return "OK", f"{len(resp.json())} articles (7d)", lat
        return "FAIL", f"HTTP {resp.status_code}", lat

    async def _api_finnhub_earnings(self, keys: dict, http: httpx.AsyncClient):
        api_key = keys.get("FINNHUB_API_KEY", "")
        if not api_key:
            return "SKIP", "FINNHUB_API_KEY not set", None
        from datetime import timedelta
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        two_weeks = (datetime.now(timezone.utc) + timedelta(days=14)).strftime("%Y-%m-%d")
        t0 = asyncio.get_event_loop().time()
        resp = await http.get(
            "https://finnhub.io/api/v1/calendar/earnings",
            params={"from": today, "to": two_weeks, "token": api_key})
        lat = (asyncio.get_event_loop().time() - t0) * 1000
        if resp.status_code == 200:
            n = len(resp.json().get("earningsCalendar", []))
            return "OK", f"{n} events in next 14d", lat
        return "FAIL", f"HTTP {resp.status_code}", lat

    async def _api_alpaca(self, keys: dict, http: httpx.AsyncClient):
        api_key    = keys.get("ALPACA_API_KEY", "")
        secret_key = keys.get("ALPACA_SECRET_KEY", "")
        if not api_key or not secret_key:
            return "SKIP", "ALPACA_API_KEY or ALPACA_SECRET_KEY not set", None
        t0 = asyncio.get_event_loop().time()
        resp = await http.get(
            "https://paper-api.alpaca.markets/v2/account",
            headers={"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": secret_key})
        lat = (asyncio.get_event_loop().time() - t0) * 1000
        if resp.status_code == 200:
            data = resp.json()
            equity = float(data.get("equity", 0))
            status = data.get("status", "?")
            return "OK", f"status={status} equity=${equity:,.0f}", lat
        return "FAIL", f"HTTP {resp.status_code}", lat

    async def _cmd_trades(self, args: list) -> None:
        """Show recent trades from the trade table."""
        try:
            import asyncpg
            conn = await asyncpg.connect(self._db_url)

            limit = int(args[0]) if args else 10
            rows = await conn.fetch(f"""
                SELECT ticker, direction, qty, entry_price,
                       conviction, status, catalyst_type,
                       created_at AT TIME ZONE 'America/New_York' AS time_et
                FROM trade
                ORDER BY created_at DESC
                LIMIT {min(limit, 20)}
            """)
            await conn.close()

            if not rows:
                await self._send("No trades found in DB.")
                return

            lines = [f"Last {len(rows)} Trades\n"]
            for r in rows:
                arrow = "↑" if r["direction"] == "long" else "↓"
                t = str(r["time_et"])[:16]
                lines.append(
                    f"{arrow} {r['ticker']} x{r['qty']} @ ${r['entry_price']:.2f}  "
                    f"[{int(r['conviction']*100)}%]  {t}"
                )
            await self._send("\n".join(lines))
        except Exception as e:
            await self._send(f"DB error: {e}")


    async def _cmd_logreport(self, args: list) -> None:
        """
        Collect errors & warnings from all containers for the last N minutes,
        feed them to Claude, and return an AI-generated system health summary.

        Usage: /logreport [minutes]   default = 30
        """
        minutes = 30
        if args:
            try:
                minutes = max(5, min(int(args[0]), 180))
            except ValueError:
                pass

        await self._send(
            f"🔍 Collecting logs from all containers (last {minutes}m)...\n"
            f"_This may take 15–20 seconds_"
        )

        reporter = LogReporter(self._docker_socket, self._http, redis=self._redis)
        log_bundle = await reporter.collect(minutes=minutes)

        if not log_bundle["entries"]:
            await self._send(
                f"✅ No errors or warnings found in the last {minutes} minutes.\n"
                f"Scanned {log_bundle['containers_scanned']} containers, "
                f"{log_bundle['lines_scanned']:,} log lines."
            )
            return

        await self._send(
            f"📦 Collected {log_bundle['total_issues']} issues from "
            f"{log_bundle['containers_with_issues']} containers — analysing with Claude..."
        )

        anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not anthropic_key:
            # Fallback: send raw summary without AI
            await self._send_raw_log_summary(log_bundle)
            return

        summary = await reporter.analyse_with_claude(log_bundle, anthropic_key)

        # Send as plain text — AI output uses ## headers and **bold** which
        # are not valid Telegram Markdown v1 and cause parse failures.
        # _send() also has an auto-fallback, but better to be explicit.
        for chunk in _split_message(summary, limit=4000):
            await self._send(chunk, markdown=False)

        _log("info", "admin_bot.logreport_sent",
             containers=log_bundle["containers_with_issues"],
             issues=log_bundle["total_issues"],
             minutes=minutes)

    async def _send_raw_log_summary(self, log_bundle: dict) -> None:
        """Fallback: send a plain-text error summary (no Claude key)."""
        lines = [
            f"⚠️ *ANTHROPIC_API_KEY not set — raw summary*\n",
            f"Scanned: {log_bundle['containers_scanned']} containers, "
            f"{log_bundle['lines_scanned']:,} lines",
            f"Issues: {log_bundle['total_issues']} across "
            f"{log_bundle['containers_with_issues']} containers\n",
        ]
        for container, entries in log_bundle["entries"].items():
            display = SERVICE_DISPLAY.get(container, container.replace("trading_", ""))
            errors   = [e for e in entries if e["level"] == "error"]
            warnings = [e for e in entries if e["level"] == "warning"]
            lines.append(
                f"*{display}*: {len(errors)} errors, {len(warnings)} warnings"
            )
            for e in errors[:3]:
                msg = e["message"][:120]
                lines.append(f"  🔴 `{msg}`")
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

    async def _send(self, text: str, markdown: bool = True) -> None:
        """
        Send message to admin chat.
        If Markdown parse fails (e.g. unmatched backticks from log content),
        automatically retries as plain text so the message is never silently lost.
        """
        if not self._admin_chat_id:
            _log("warning", "admin_bot.send_no_chat_id")
            return
        params: dict = {
            "chat_id": self._admin_chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        if markdown:
            params["parse_mode"] = "Markdown"
        result = await self._api("sendMessage", params)
        if not result:
            _log("warning", "admin_bot.send_no_response", text_preview=text[:60])
            return
        if not result.get("ok"):
            error_desc = result.get("description", "unknown")
            error_code = result.get("error_code")
            # Telegram returns 400 when Markdown is malformed (unmatched ` * _ etc.)
            # Retry as plain text so the message always gets through.
            if markdown and error_code == 400 and "parse" in error_desc.lower():
                _log("warning", "admin_bot.markdown_parse_failed_retrying",
                     error=error_desc, text_preview=text[:60])
                plain_params = {
                    "chat_id": self._admin_chat_id,
                    "text": _strip_markdown(text),
                    "disable_web_page_preview": True,
                }
                retry = await self._api("sendMessage", plain_params)
                if retry and retry.get("ok"):
                    _log("debug", "admin_bot.send_ok_plain_fallback",
                         text_preview=text[:60])
                else:
                    _log("error", "admin_bot.send_failed_both_modes",
                         text_preview=text[:60])
            else:
                _log("warning", "admin_bot.send_failed",
                     error=error_desc, error_code=error_code,
                     text_preview=text[:60])
        else:
            _log("debug", "admin_bot.send_ok", text_preview=text[:60])



def _strip_markdown(text: str) -> str:
    """
    Remove Telegram Markdown v1 formatting characters so a message that failed
    to parse can be re-sent as plain text without losing the content.
    Strips: *bold*, _italic_, `code`, [links](url)
    """
    import re
    # Remove inline code spans first (may contain other special chars)
    text = re.sub(r"`+[^`]*`+", lambda m: m.group(0).replace("`", ""), text)
    # Remove bold/italic markers
    text = re.sub(r"[*_]", "", text)
    # Remove backticks
    text = text.replace("`", "'")
    # Remove [text](url) links — keep display text
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"", text)
    return text


def _split_message(text: str, limit: int = 4000) -> list[str]:
    """Split a long string into Telegram-safe chunks at newline boundaries."""
    if len(text) <= limit:
        return [text]
    chunks = []
    while text:
        if len(text) <= limit:
            chunks.append(text)
            break
        split_at = text.rfind("\n", 0, limit)
        if split_at == -1:
            split_at = limit
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    return chunks


class LogReporter:
    """
    Collects structured and plain log lines from Docker containers,
    filters to errors and warnings, then asks Claude for a diagnosis.
    """

    # Infra containers we still want to scan
    ALL_CONTAINERS = list({
        "trading_connector_benzinga", "trading_connector_polygon_news",
        "trading_connector_polygon_prices", "trading_connector_earnings",
        "trading_connector_fred",
        "trading_finnhub_news", "trading_finnhub_press_releases",
        "trading_finnhub_fundamentals", "trading_finnhub_websocket",
        "trading_finnhub_sentiment",
        "trading_fmp_earnings", "trading_fmp_enrichment",
        "trading_fmp_sectors", "trading_fmp_technical",
        "trading_pipeline_normalizer", "trading_pipeline_deduplicator",
        "trading_pipeline_entity_resolver",
        "trading_signals_ai_summarizer", "trading_signals_aggregator",
        "trading_pretrade_filter", "trading_signals_telegram",
        "trading_execution_engine", "trading_position_monitor",
        "trading_volatility_monitor", "trading_stock_context",
        "trading_regime_poller", "trading_scheduler",
        "trading_admin_watchdog", "trading_admin_bot",
        "trading_redpanda", "trading_postgres", "trading_redis",
    })

    # Keywords that identify an error/warning in plain-text (non-JSON) logs
    ERROR_KEYWORDS   = ("error", "exception", "traceback", "critical", "fatal",
                        "failed", "failure", "crash")
    WARNING_KEYWORDS = ("warning", "warn", "deprecated", "timeout", "retry",
                        "rate.limit", "rate_limit", "backoff")

    # High-volume benign patterns — suppressed before any classification.
    # These inflate issue counts and confuse the AI without indicating real problems.
    NOISE_PATTERNS = (
        # Redpanda: services auto-create topics on every startup;
        # already-existing topics are rejected with this warning — harmless.
        "topic_already_exists",
        # Grafana plugin update checker emits level=info lines — not errors.
        "flag evaluation succeeded",
        "pluginsAutoUpdate",
        # Prometheus: after restart it re-ingests historical metric samples
        # that arrive out of chronological order — self-resolving.
        "out-of-order samples",
        "ingesting out-of-order",
        # asyncpg pool churn during normal operation
        "connection was closed in the middle of operation",
        # Redpanda internal raft/controller heartbeat noise
        "raft - raft_log",
        "controller_backend",
    )

    def __init__(self, docker_socket: str, http: httpx.AsyncClient,
                 redis=None) -> None:
        self._socket = docker_socket
        self._http   = http
        self._redis  = redis  # optional — used for pipeline context enrichment

    async def collect(self, minutes: int = 30) -> dict:
        """
        Fetch logs from all containers and return a structured bundle.

        Returns:
            {
              "entries": { container_name: [{"level", "message", "ts", "event", ...}] },
              "containers_scanned": int,
              "containers_with_issues": int,
              "lines_scanned": int,
              "total_issues": int,
              "window_minutes": int,
            }
        """
        since_seconds = minutes * 60
        tasks = [
            self._fetch_container_issues(c, since_seconds)
            for c in self.ALL_CONTAINERS
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        entries: dict[str, list[dict]] = {}
        lines_scanned = 0
        for container, result in zip(self.ALL_CONTAINERS, results):
            if isinstance(result, Exception) or result is None:
                continue
            issues, scanned = result
            lines_scanned += scanned
            if issues:
                entries[container] = issues

        return {
            "entries":                entries,
            "containers_scanned":     len(self.ALL_CONTAINERS),
            "containers_with_issues": len(entries),
            "lines_scanned":          lines_scanned,
            "total_issues":           sum(len(v) for v in entries.values()),
            "window_minutes":         minutes,
        }

    async def _fetch_container_issues(
        self, container: str, since_seconds: int
    ) -> tuple[list[dict], int] | None:
        """Fetch and filter logs for a single container."""
        try:
            transport = httpx.AsyncHTTPTransport(uds=self._socket)
            async with httpx.AsyncClient(transport=transport, timeout=8.0) as docker:
                resp = await docker.get(
                    f"http://docker/containers/{container}/logs"
                    f"?since={since_seconds}&stdout=true&stderr=true"
                    f"&timestamps=false&tail=500"
                )
                if resp.status_code == 404:
                    return None  # container doesn't exist
                raw = resp.content.decode("utf-8", errors="replace")
        except Exception:
            return None

        # Strip Docker multiplexing 8-byte headers
        clean_lines = []
        for line in raw.split("\n"):
            if not line:
                continue
            clean_lines.append(line[8:] if len(line) > 8 and line[0] in "\x01\x02" else line)

        issues: list[dict] = []
        for line in clean_lines:
            issue = self._classify_line(line)
            if issue:
                issues.append(issue)

        return issues, len(clean_lines)

    def _classify_line(self, line: str) -> dict | None:
        """
        Return a classified issue dict if line is an error or warning,
        else return None. Handles both JSON-structured and plain-text logs.
        """
        line = line.strip()
        if not line:
            return None

        # Suppress known-noisy benign patterns before any further classification.
        # Check against the raw line (not lowercased) to preserve case-sensitive matches.
        for noise in self.NOISE_PATTERNS:
            if noise in line:
                return None

        # Try JSON first (all our services emit structured JSON logs)
        if line.startswith("{"):
            try:
                obj = json.loads(line)
                level = str(obj.get("level", "")).lower()
                if level in ("error", "critical", "fatal"):
                    return {
                        "level":   "error",
                        "event":   obj.get("event", ""),
                        "message": obj.get("error", obj.get("msg", obj.get("message", ""))),
                        "ts":      obj.get("ts", ""),
                        "extra":   {k: v for k, v in obj.items()
                                    if k not in ("ts", "level", "event", "error",
                                                 "msg", "message")},
                    }
                if level == "warning":
                    return {
                        "level":   "warning",
                        "event":   obj.get("event", ""),
                        "message": obj.get("error", obj.get("msg", obj.get("message", ""))),
                        "ts":      obj.get("ts", ""),
                        "extra":   {},
                    }
                return None
            except (json.JSONDecodeError, ValueError):
                pass

        # Plain-text fallback
        lower = line.lower()
        if any(k in lower for k in self.ERROR_KEYWORDS):
            return {"level": "error",   "event": "", "message": line[:300], "ts": "", "extra": {}}
        if any(k in lower for k in self.WARNING_KEYWORDS):
            return {"level": "warning", "event": "", "message": line[:300], "ts": "", "extra": {}}
        return None

    async def analyse_with_claude(self, log_bundle: dict, api_key: str) -> str:
        """
        Send the collected log data to Claude and return a formatted summary.
        Uses claude-haiku-4-5 for speed and low cost.
        """
        # Build a compact log digest for the prompt
        # Enrich digest with live pipeline context from watchdog snapshot
        pipeline_ctx = ""
        try:
            if self._redis:
                snap_raw = await self._redis.get("watchdog:snapshot")
                if snap_raw:
                    snap = json.loads(snap_raw)
                    sig_today    = snap.get("signals_today", 0)
                    blocked_today = snap.get("signals_blocked_today", 0)
                    block_rate   = snap.get("block_rate_1h", 0)
                    news_today   = snap.get("news_processed_today", 0)
                    budget_used  = snap.get("llm_budget_used", 0)
                    budget_limit = snap.get("llm_budget_limit", 5)
                    infra        = snap.get("infra_health", {})
                    pipeline_ctx = (
                        "Pipeline context (live):\n"
                        f"  Signals today: {sig_today} ({blocked_today} blocked, "
                        f"block rate last 1h: {block_rate*100:.0f}%)\n"
                        f"  News processed today: {news_today}\n"
                        f"  LLM budget: ${budget_used:.3f} / ${budget_limit:.2f}\n"
                        f"  Postgres reachable: {infra.get('postgres', 'unknown')}\n"
                        f"  Redis reachable: {infra.get('redis', 'unknown')}\n"
                    )
        except Exception:
            pass

        digest_lines = [
            f"Trading system log digest — last {log_bundle['window_minutes']} minutes",
            f"Containers scanned: {log_bundle['containers_scanned']}",
            f"Total issues found: {log_bundle['total_issues']} "
            f"across {log_bundle['containers_with_issues']} containers",
            pipeline_ctx,
            "",
        ]

        for container, entries in sorted(log_bundle["entries"].items()):
            display = SERVICE_DISPLAY.get(container, container.replace("trading_", ""))
            errors   = [e for e in entries if e["level"] == "error"]
            warnings = [e for e in entries if e["level"] == "warning"]
            digest_lines.append(
                f"[{display}] {len(errors)} errors, {len(warnings)} warnings"
            )

            # Deduplicate by event name — repeated identical events bloat the
            # digest and push the AI toward over-counting a single root cause.
            # Keep one representative sample + occurrence count per unique event.
            def _dedup(items: list[dict], limit: int) -> list[tuple[dict, int]]:
                """Return [(entry, count)] deduplicated by event key."""
                seen: dict[str, list] = {}
                for item in items:
                    key = item.get("event") or item.get("message", "")[:80]
                    seen.setdefault(key, []).append(item)
                # Sort by frequency desc, take top `limit` unique events
                deduped = sorted(seen.values(), key=len, reverse=True)[:limit]
                return [(group[-1], len(group)) for group in deduped]

            for e, count in _dedup(errors, limit=8):
                event = f" ({e['event']})" if e.get("event") else ""
                msg   = e.get("message", "")[:200]
                count_str = f" [x{count}]" if count > 1 else ""
                extra = ""
                if e.get("extra"):
                    kv = ", ".join(
                        f"{k}={str(v)[:60]}"
                        for k, v in list(e["extra"].items())[:4]
                        if v
                    )
                    extra = f" | {kv}" if kv else ""
                digest_lines.append(f"  ERROR{event}{count_str}: {msg}{extra}")

            for w, count in _dedup(warnings, limit=5):
                event = f" ({w['event']})" if w.get("event") else ""
                msg   = w.get("message", "")[:150]
                count_str = f" [x{count}]" if count > 1 else ""
                digest_lines.append(f"  WARN{event}{count_str}: {msg}")

            digest_lines.append("")

        digest = "\n".join(digest_lines)

        # Cap digest size to avoid huge token usage
        if len(digest) > 12_000:
            digest = digest[:12_000] + "\n... [truncated]"

        system_prompt = """You are a senior DevOps engineer and quantitative trading system expert.
You are monitoring a production algorithmic trading system built with:
- Python async microservices in Docker containers
- Redpanda (Kafka) message bus with topics: signals.raw, signals.actionable, signals.filtered, signals.blocked, news.enriched
- PostgreSQL + pgvector database
- Redis for caching and state
- News-driven signal pipeline: connectors → normalizer → deduplicator → entity resolver → AI summarizer → signal aggregator → pre-trade filter → execution engine
- External data: Polygon.io, Finnhub, FMP (Financial Modeling Prep), Benzinga
- Paper trading via Alpaca

You will receive a log digest and must produce a clear, actionable system health report.
Be direct and specific. Prioritise critical issues. Group related problems together.
Use emojis sparingly for visual scanning. Do not repeat the raw log lines back.

The following patterns are known benign operational noise — dismiss them without listing as issues:
- Redpanda "topic_already_exists": every service attempts to auto-create its topics on startup; duplicates are rejected harmlessly.
- Grafana "flag evaluation succeeded" / "pluginsAutoUpdate": routine plugin update checks, always INFO level.
- Prometheus "out-of-order samples" / "ingesting out-of-order": post-restart metric catch-up, self-resolving within minutes.
- Redpanda raft/controller_backend log lines: internal consensus heartbeats, not errors.
- asyncpg "connection was closed in the middle of operation": connection pool recycling during idle periods, harmless.

If you see these, acknowledge them in the "What's Working" section as expected behavior, not in warnings."""

        user_prompt = f"""Analyse the following log digest from the trading system.

Format your response using ONLY these section headers (plain text, no ## or ###):

OVERALL STATUS
One sentence: healthy / degraded / critical and why.

CRITICAL ISSUES
Each issue as: [SERVICE] Short title
- Impact: what breaks
- Cause: root cause
- Fix: concrete command or code change (be specific)

WARNINGS
Each as: [SERVICE] Short title — impact — recommended action

ROOT CAUSES
Group related errors across services. Identify shared causes (e.g. schema migration needed, rate limit budget shared across services).

WHAT IS WORKING
One line per healthy service group. Confirm what to ignore.

Rules:
- No markdown headers (## ###). Use CAPS section titles only.
- No ** bold. Use CAPS for emphasis if needed.
- Be concise. Max 1200 words total.
- If an error count says [x48] that means 48 identical occurrences — treat as one issue.
- Do not list Redpanda topic_already_exists, Grafana flag evaluation, or Prometheus out-of-order samples as issues.

---
{digest}"""

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key":         api_key,
                        "anthropic-version": "2023-06-01",
                        "content-type":      "application/json",
                    },
                    json={
                        "model":      "claude-haiku-4-5-20251001",
                        "max_tokens": 3000,
                        "system":     system_prompt,
                        "messages":   [{"role": "user", "content": user_prompt}],
                    },
                )

            if resp.status_code != 200:
                _log("error", "admin_bot.claude_api_error",
                     status=resp.status_code, body=resp.text[:200])
                return (
                    f"❌ Claude API error ({resp.status_code})\n"
                    f"Falling back to raw summary:\n\n"
                    + self._format_raw_summary(log_bundle)
                )

            data    = resp.json()
            content = data.get("content", [])
            text    = "\n".join(
                block.get("text", "") for block in content if block.get("type") == "text"
            ).strip()

            usage = data.get("usage", {})
            cost  = (
                usage.get("input_tokens",  0) * 0.00000025 +
                usage.get("output_tokens", 0) * 0.00000125
            )
            _log("info", "admin_bot.logreport_claude_done",
                 input_tokens=usage.get("input_tokens"),
                 output_tokens=usage.get("output_tokens"),
                 cost_usd=round(cost, 5))

            header = (
                f"🤖 *System Log Report* — "
                f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n"
                f"_{log_bundle['total_issues']} issues · "
                f"{log_bundle['containers_with_issues']} containers · "
                f"last {log_bundle['window_minutes']}m · "
                f"${cost:.4f}_\n\n"
            )
            return header + text

        except Exception as e:
            _log("error", "admin_bot.claude_call_failed", error=str(e))
            return (
                f"❌ Failed to reach Claude API: {e}\n\n"
                + self._format_raw_summary(log_bundle)
            )

    def _format_raw_summary(self, log_bundle: dict) -> str:
        """Compact plain-text summary used as fallback when Claude is unavailable."""
        lines = [
            f"📋 Raw Log Summary — last {log_bundle['window_minutes']}m",
            f"Scanned {log_bundle['containers_scanned']} containers, "
            f"{log_bundle['lines_scanned']:,} lines",
            f"Found {log_bundle['total_issues']} issues\n",
        ]
        for container, entries in sorted(log_bundle["entries"].items()):
            display = SERVICE_DISPLAY.get(container, container.replace("trading_", ""))
            errors   = [e for e in entries if e["level"] == "error"]
            warnings = [e for e in entries if e["level"] == "warning"]
            lines.append(f"*{display}* — {len(errors)}E {len(warnings)}W")
            for e in errors[:5]:
                msg = (e.get("event") or e.get("message") or "")[:120]
                lines.append(f"  🔴 {msg}")
        return "\n".join(lines)


async def main() -> None:
    bot = AdminBot()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal_module.SIGTERM, bot.stop)
    loop.add_signal_handler(signal_module.SIGINT, bot.stop)
    await bot.start()


if __name__ == "__main__":
    asyncio.run(main())