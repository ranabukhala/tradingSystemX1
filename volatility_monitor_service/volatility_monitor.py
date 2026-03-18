"""
Volatility Monitor Service.

Polls VXX and UVXY price data from Polygon REST API every 5 minutes during
market hours (9:30–16:00 ET, weekdays). Computes a volatility regime
classification and:

  1. Caches the current regime in Redis (key: vol:regime, TTL 600 s)
  2. Maintains a 5-day rolling VXX close history (key: vol:vxx_history)
  3. Publishes a regime_change event to Redpanda topic signals.volatility-regime
     ONLY when the regime transitions — not on every poll.

HTTP endpoints (port 8083):
  GET /health   — service status, last poll time, Polygon reachability
  GET /regime   — full cached regime JSON from Redis

Regime classification
─────────────────────
All thresholds are named constants at the top of this file for easy tuning.

  HIGH_VOL_BACKWARDATION  vxx_5d_roc > 0.15  AND leverage_ratio > 1.7
  ELEVATED_VOL_CONTANGO   vxx_5d_roc > 0.05  AND leverage_ratio >= 1.4
  VOL_MEAN_REVERSION      vxx_5d_roc < -0.05 AND previous was HIGH or ELEVATED
  LOW_VOL_STABLE          (default / catch-all)

leverage_ratio = uvxy_daily_return / vxx_daily_return
  Expected ~1.5.  > 1.7 = futures backwardation; < 1.3 = roll decay / calm.
  Division-by-zero guarded — if |vxx_return| < LEVERAGE_ZERO_GUARD, classify
  immediately as LOW_VOL_STABLE and skip the ratio calculation.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as aioredis
from aiohttp import web, ClientSession, ClientTimeout
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ── Aiokafka import (graceful degradation if missing) ─────────────────────────
try:
    from aiokafka import AIOKafkaProducer
    _KAFKA_AVAILABLE = True
except ImportError:
    _KAFKA_AVAILABLE = False
    AIOKafkaProducer = None  # type: ignore[assignment,misc]

# ══════════════════════════════════════════════════════════════════════════════
# Tunable thresholds — adjust here, not inline in logic
# ══════════════════════════════════════════════════════════════════════════════

# Regime classification
VOL_ROC_HIGH_THRESHOLD       = 0.15   # vxx_5d_roc above → HIGH_VOL_BACKWARDATION candidate
VOL_ROC_ELEVATED_THRESHOLD   = 0.05   # vxx_5d_roc above → ELEVATED_VOL_CONTANGO candidate
VOL_ROC_REVERSION_THRESHOLD  = -0.05  # vxx_5d_roc below + prior HIGH/ELEVATED → MEAN_REVERSION
LEVERAGE_BACKWARDATION_MIN   = 1.7    # leverage_ratio above → backwardation confirmed
LEVERAGE_ELEVATED_MIN        = 1.4    # leverage_ratio above → elevated contango confirmed
LEVERAGE_ZERO_GUARD          = 0.001  # |vxx_return| below this → skip ratio, classify LOW_VOL

# Rolling history
VXX_HISTORY_DAYS   = 5    # number of VXX closes kept in vol:vxx_history
REGIME_CACHE_TTL   = 600  # seconds (10 min — overlaps 5-min poll interval)

# Polygon API
POLYGON_TIMEOUT_S  = 10.0  # per-request timeout

# ══════════════════════════════════════════════════════════════════════════════
# Redis keys
# ══════════════════════════════════════════════════════════════════════════════

REDIS_KEY_REGIME      = "vol:regime"
REDIS_KEY_VXX_HISTORY = "vol:vxx_history"

# ══════════════════════════════════════════════════════════════════════════════
# Regimes that count as "high fear" for the mean-reversion check
# ══════════════════════════════════════════════════════════════════════════════

_FEAR_REGIMES = frozenset({"HIGH_VOL_BACKWARDATION", "ELEVATED_VOL_CONTANGO"})

# ══════════════════════════════════════════════════════════════════════════════
# Logging
# ══════════════════════════════════════════════════════════════════════════════

class _JsonFormatter(logging.Formatter):
    """Minimal JSON log formatter matching the project stack."""

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        extra = {
            k: v for k, v in record.__dict__.items()
            if k not in logging.LogRecord.__dict__ and k not in (
                "message", "asctime", "exc_info", "exc_text", "stack_info",
                "lineno", "pathname", "filename", "module", "funcName",
                "created", "msecs", "relativeCreated", "thread", "threadName",
                "processName", "process", "args", "msg", "levelname", "name",
            )
        }
        payload: dict = {
            "ts":      datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level":   record.levelname.lower(),
            "event":   record.getMessage(),
            "service": "volatility_monitor",
            **extra,
        }
        return json.dumps(payload, default=str)


def _setup_logging() -> None:
    log_level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
    handler   = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(log_level)


logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# Polygon data fetching
# ══════════════════════════════════════════════════════════════════════════════

_POLYGON_BASE = "https://api.polygon.io"


async def _fetch_polygon(
    session: ClientSession,
    path: str,
    api_key: str,
    params: dict[str, str] | None = None,
) -> dict | None:
    """
    GET a Polygon endpoint.  Returns parsed JSON on 200, None on any error.
    Logs warnings for 403/429; errors for everything else.
    """
    url = f"{_POLYGON_BASE}{path}"
    p   = {"apiKey": api_key, **(params or {})}
    try:
        async with session.get(url, params=p) as resp:
            if resp.status == 200:
                return await resp.json()
            if resp.status in (403, 429):
                logger.warning(
                    "polygon.http_error",
                    extra={"path": path, "status": resp.status},
                )
            else:
                logger.error(
                    "polygon.http_error",
                    extra={"path": path, "status": resp.status},
                )
            return None
    except Exception as exc:
        logger.error(
            "polygon.request_failed",
            extra={"path": path, "error": str(exc)},
        )
        return None


async def _get_prev_close(
    session: ClientSession,
    ticker: str,
    api_key: str,
) -> float | None:
    """Return the previous day's close for *ticker* via /v2/aggs/ticker/{t}/prev."""
    data = await _fetch_polygon(
        session,
        f"/v2/aggs/ticker/{ticker}/prev",
        api_key,
        {"adjusted": "true"},
    )
    if not data:
        return None
    results = data.get("results") or []
    if not results:
        logger.warning("polygon.empty_prev", extra={"ticker": ticker})
        return None
    return float(results[0].get("c", 0)) or None


async def _get_snapshot_close(
    session: ClientSession,
    ticker: str,
    api_key: str,
) -> float | None:
    """
    Return the most recent close from the snapshot endpoint.
    Falls back to prevDay close if today's close is absent.
    """
    data = await _fetch_polygon(
        session,
        f"/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}",
        api_key,
    )
    if not data:
        return None
    tick = (data.get("ticker") or {})
    # Prefer today's day close if available (populated after 4 PM ET)
    day_close = (tick.get("day") or {}).get("c")
    if day_close:
        return float(day_close)
    prev_close = (tick.get("prevDay") or {}).get("c")
    if prev_close:
        return float(prev_close)
    logger.warning("polygon.no_close_in_snapshot", extra={"ticker": ticker})
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Regime classification
# ══════════════════════════════════════════════════════════════════════════════

def _classify_regime(
    vxx_return: float,
    uvxy_return: float,
    vxx_5d_roc: float,
    previous_regime: str,
) -> tuple[str, float | None]:
    """
    Return (regime_name, leverage_ratio | None).

    leverage_ratio is None when |vxx_return| < LEVERAGE_ZERO_GUARD (division
    guarded).  In that case the regime defaults to LOW_VOL_STABLE.
    """
    if abs(vxx_return) < LEVERAGE_ZERO_GUARD:
        logger.debug(
            "regime.zero_guard_triggered",
            extra={"vxx_return": vxx_return},
        )
        return "LOW_VOL_STABLE", None

    leverage_ratio = uvxy_return / vxx_return

    if vxx_5d_roc > VOL_ROC_HIGH_THRESHOLD and leverage_ratio > LEVERAGE_BACKWARDATION_MIN:
        regime = "HIGH_VOL_BACKWARDATION"
    elif vxx_5d_roc > VOL_ROC_ELEVATED_THRESHOLD and leverage_ratio >= LEVERAGE_ELEVATED_MIN:
        regime = "ELEVATED_VOL_CONTANGO"
    elif vxx_5d_roc < VOL_ROC_REVERSION_THRESHOLD and previous_regime in _FEAR_REGIMES:
        regime = "VOL_MEAN_REVERSION"
    else:
        regime = "LOW_VOL_STABLE"

    return regime, leverage_ratio


# ══════════════════════════════════════════════════════════════════════════════
# Core poll cycle
# ══════════════════════════════════════════════════════════════════════════════

async def _poll_cycle(app: web.Application) -> None:
    """
    Fetch VXX + UVXY, classify regime, update Redis, publish to Redpanda
    if regime changed.

    Called by APScheduler every 5 minutes during 9:30–16:00 ET weekdays.
    Never raises — all errors are caught and logged so the scheduler keeps
    running on the next tick.
    """
    api_key: str            = app["polygon_api_key"]
    redis:   aioredis.Redis = app["redis"]
    kafka_producer          = app.get("kafka_producer")
    state: dict             = app["state"]

    now_utc = datetime.now(timezone.utc)
    state["last_poll_at"] = now_utc

    try:
        timeout = ClientTimeout(total=POLYGON_TIMEOUT_S)
        async with ClientSession(timeout=timeout) as session:
            # Fetch previous-day closes (stable baseline for daily return)
            vxx_prev  = await _get_prev_close(session, "VXX",  api_key)
            uvxy_prev = await _get_prev_close(session, "UVXY", api_key)

            if vxx_prev is None or uvxy_prev is None:
                logger.warning(
                    "poll.prev_close_unavailable",
                    extra={"vxx_prev": vxx_prev, "uvxy_prev": uvxy_prev},
                )
                state["polygon_reachable"] = False
                return

            # Fetch real-time-ish snapshot closes (intraday or prev day)
            vxx_close  = await _get_snapshot_close(session, "VXX",  api_key)
            uvxy_close = await _get_snapshot_close(session, "UVXY", api_key)

            if vxx_close is None or uvxy_close is None:
                logger.warning(
                    "poll.snapshot_unavailable",
                    extra={"vxx_close": vxx_close, "uvxy_close": uvxy_close},
                )
                state["polygon_reachable"] = False
                return

        state["polygon_reachable"] = True

        # ── Daily returns ──────────────────────────────────────────────────
        vxx_return  = (vxx_close  - vxx_prev)  / vxx_prev  if vxx_prev  else 0.0
        uvxy_return = (uvxy_close - uvxy_prev) / uvxy_prev if uvxy_prev else 0.0

        # ── 5-day rolling VXX close → vxx_5d_roc ─────────────────────────
        await redis.rpush(REDIS_KEY_VXX_HISTORY, str(vxx_close))
        await redis.ltrim(REDIS_KEY_VXX_HISTORY, -VXX_HISTORY_DAYS, -1)
        history_raw = await redis.lrange(REDIS_KEY_VXX_HISTORY, 0, -1)
        history     = [float(v) for v in history_raw if v]

        if len(history) >= 2:
            vxx_5d_roc = (history[-1] - history[0]) / history[0]
        else:
            vxx_5d_roc = 0.0  # not enough history yet

        # ── Previous regime (for mean-reversion check) ─────────────────────
        prev_cached = await redis.get(REDIS_KEY_REGIME)
        previous_regime = "LOW_VOL_STABLE"
        if prev_cached:
            try:
                previous_regime = json.loads(prev_cached).get("regime", "LOW_VOL_STABLE")
            except Exception:
                pass

        # ── Classify ───────────────────────────────────────────────────────
        regime, leverage_ratio = _classify_regime(
            vxx_return, uvxy_return, vxx_5d_roc, previous_regime
        )

        # ── Build payload ──────────────────────────────────────────────────
        payload: dict[str, Any] = {
            "regime":         regime,
            "vxx_close":      round(vxx_close,       4),
            "uvxy_close":     round(uvxy_close,      4),
            "vxx_return":     round(vxx_return,      6),
            "uvxy_return":    round(uvxy_return,     6),
            "leverage_ratio": round(leverage_ratio,  4) if leverage_ratio is not None else None,
            "vxx_5d_roc":     round(vxx_5d_roc,      6),
            "timestamp":      now_utc.isoformat(),
        }

        # ── Write to Redis ─────────────────────────────────────────────────
        await redis.setex(REDIS_KEY_REGIME, REGIME_CACHE_TTL, json.dumps(payload))

        logger.info(
            "poll.regime_updated",
            extra={
                "regime":         regime,
                "vxx_return":     round(vxx_return,  4),
                "uvxy_return":    round(uvxy_return, 4),
                "leverage_ratio": round(leverage_ratio, 4) if leverage_ratio is not None else None,
                "vxx_5d_roc":     round(vxx_5d_roc,  4),
            },
        )

        # ── Publish to Redpanda only on regime change ──────────────────────
        if regime != previous_regime:
            logger.info(
                "poll.regime_change",
                extra={"previous": previous_regime, "new": regime},
            )
            if kafka_producer is not None:
                event: dict[str, Any] = {
                    "event":            "regime_change",
                    "previous_regime":  previous_regime,
                    "new_regime":       regime,
                    "vxx_close":        payload["vxx_close"],
                    "uvxy_close":       payload["uvxy_close"],
                    "leverage_ratio":   payload["leverage_ratio"],
                    "vxx_5d_roc":       payload["vxx_5d_roc"],
                    "timestamp":        payload["timestamp"],
                }
                try:
                    await kafka_producer.send_and_wait(
                        "signals.volatility-regime",
                        json.dumps(event).encode(),
                    )
                    logger.info(
                        "kafka.regime_change_published",
                        extra={"topic": "signals.volatility-regime", "regime": regime},
                    )
                except Exception as exc:
                    logger.error(
                        "kafka.publish_failed", extra={"error": str(exc)}
                    )

    except Exception as exc:
        logger.error("poll.unexpected_error", extra={"error": str(exc)})
        state["polygon_reachable"] = False


# ══════════════════════════════════════════════════════════════════════════════
# HTTP handlers
# ══════════════════════════════════════════════════════════════════════════════

async def _handle_health(request: web.Request) -> web.Response:
    """GET /health — service status."""
    state: dict = request.app["state"]
    last_poll = state.get("last_poll_at")
    body = {
        "status":            "ok",
        "polygon_reachable": state.get("polygon_reachable", False),
        "last_poll_at":      last_poll.isoformat() if last_poll else None,
    }
    return web.Response(
        text=json.dumps(body),
        content_type="application/json",
        status=200,
    )


async def _handle_regime(request: web.Request) -> web.Response:
    """GET /regime — full cached regime from Redis."""
    redis: aioredis.Redis = request.app["redis"]
    raw = await redis.get(REDIS_KEY_REGIME)
    if raw is None:
        return web.Response(
            text=json.dumps({"error": "No regime data cached yet"}),
            content_type="application/json",
            status=503,
        )
    return web.Response(
        text=raw,  # already JSON string
        content_type="application/json",
        status=200,
    )


# ══════════════════════════════════════════════════════════════════════════════
# Application factory + lifecycle
# ══════════════════════════════════════════════════════════════════════════════

_HTTP_PORT = int(os.environ.get("VOLATILITY_MONITOR_PORT", "8083"))


async def create_app() -> web.Application:
    app = web.Application()
    app.on_startup.append(_on_startup)
    app.on_shutdown.append(_on_shutdown)
    app.router.add_get("/health", _handle_health)
    app.router.add_get("/regime", _handle_regime)
    return app


async def _on_startup(app: web.Application) -> None:
    api_key   = os.environ.get("POLYGON_API_KEY", "")
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
    kafka_url = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")

    app["polygon_api_key"] = api_key
    # Mutable state dict — avoids aiohttp DeprecationWarning about mutating
    # app after startup (triggered when background tasks update keys directly)
    app["state"] = {
        "polygon_reachable": False,
        "last_poll_at":      None,
    }

    if not api_key:
        logger.warning("startup.no_polygon_api_key")

    # ── Redis ──────────────────────────────────────────────────────────────────
    app["redis"] = await aioredis.from_url(redis_url, decode_responses=True)
    logger.info("startup.redis_connected", extra={"url": redis_url})

    # ── Kafka producer ─────────────────────────────────────────────────────────
    kafka_producer = None
    if _KAFKA_AVAILABLE:
        try:
            kafka_producer = AIOKafkaProducer(bootstrap_servers=kafka_url)
            await kafka_producer.start()
            logger.info("startup.kafka_connected", extra={"servers": kafka_url})
        except Exception as exc:
            logger.error(
                "startup.kafka_failed",
                extra={"error": str(exc), "note": "Regime change events will not be published"},
            )
            kafka_producer = None
    else:
        logger.warning("startup.aiokafka_not_installed",
                       extra={"note": "Install aiokafka to enable Kafka publishing"})
    app["kafka_producer"] = kafka_producer

    # ── APScheduler — market hours only ───────────────────────────────────────
    sched = AsyncIOScheduler(timezone="America/New_York")

    # Every 5 minutes from 9:30 to 16:00 ET, Mon–Fri
    sched.add_job(
        _poll_cycle,
        CronTrigger(
            day_of_week="mon-fri",
            hour="9-15",
            minute="*/5",
            timezone="America/New_York",
        ),
        args=[app],
        id="poll_market_hours",
        replace_existing=True,
    )
    # Catch 9:30 AM exactly with a separate trigger (hour=9, minute=30)
    sched.add_job(
        _poll_cycle,
        CronTrigger(
            day_of_week="mon-fri",
            hour=9,
            minute=30,
            timezone="America/New_York",
        ),
        args=[app],
        id="poll_open",
        replace_existing=True,
    )
    # Final poll at 16:00 ET (closing snapshot)
    sched.add_job(
        _poll_cycle,
        CronTrigger(
            day_of_week="mon-fri",
            hour=16,
            minute=0,
            timezone="America/New_York",
        ),
        args=[app],
        id="poll_close",
        replace_existing=True,
    )

    sched.start()
    app["scheduler"] = sched
    logger.info("startup.scheduler_started")

    # Immediate startup poll (non-blocking — don't delay HTTP server)
    asyncio.create_task(_poll_cycle(app), name="startup_poll")
    logger.info("startup.ready", extra={"port": _HTTP_PORT})


async def _on_shutdown(app: web.Application) -> None:
    sched = app.get("scheduler")
    if sched:
        sched.shutdown(wait=False)
        logger.info("shutdown.scheduler_stopped")

    kafka_producer = app.get("kafka_producer")
    if kafka_producer is not None:
        try:
            await kafka_producer.stop()
        except Exception:
            pass
        logger.info("shutdown.kafka_stopped")

    redis: aioredis.Redis | None = app.get("redis")
    if redis:
        await redis.aclose()
        logger.info("shutdown.redis_closed")

    logger.info("shutdown.complete")


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    _setup_logging()
    logger.info("main.starting", extra={"port": _HTTP_PORT})

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    stop_event = asyncio.Event()

    def _handle_signal(*_: object) -> None:
        logger.info("main.shutdown_signal_received")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except (NotImplementedError, OSError):
            signal.signal(sig, _handle_signal)   # type: ignore[arg-type]

    async def _run() -> None:
        app    = await create_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site   = web.TCPSite(runner, "0.0.0.0", _HTTP_PORT)
        await site.start()
        logger.info("main.http_server_started", extra={"port": _HTTP_PORT})
        await stop_event.wait()
        await runner.cleanup()

    try:
        loop.run_until_complete(_run())
    finally:
        loop.close()
        logger.info("main.exited")


if __name__ == "__main__":
    main()
