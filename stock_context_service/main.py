"""
Stock Context Classifier — service entry point.

Startup sequence
----------------
1. Configure structured logging.
2. Open Redis and PostgreSQL connection pools.
3. Inject the Redis client into redis_cache module.
4. Start the APScheduler (triggers an immediate watchlist refresh).
5. Start the aiohttp HTTP server on port 8082.
6. Block until SIGTERM / SIGINT is received.
7. Drain the scheduler and close connection pools on shutdown.

HTTP endpoints
--------------
GET /health
    {"status": "ok", "cached_tickers": N, "last_refresh": "<ISO>"}

GET /context/{ticker}
    Cached StockContext as JSON.  404 if not in cache.

GET /context/{ticker}/refresh
    Immediately reclassifies *ticker* and returns the new StockContext.
    Useful for forced invalidation before a signal arrives.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone

import asyncpg
import redis.asyncio as aioredis
from aiohttp import web

from . import redis_cache
from .scheduler import ContextScheduler

logger = logging.getLogger(__name__)

_HTTP_PORT = int(os.environ.get("CONTEXT_SERVICE_PORT", "8082"))


# ── Logging setup ─────────────────────────────────────────────────────────────

def _setup_logging() -> None:
    """Configure JSON-formatted structured logging to stdout."""
    log_level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
    handler   = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(log_level)


class _JsonFormatter(logging.Formatter):
    """Minimal JSON log formatter compatible with the existing stack."""

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        extra = {k: v for k, v in record.__dict__.items()
                 if k not in logging.LogRecord.__dict__ and k not in (
                     "message", "asctime", "exc_info", "exc_text", "stack_info",
                     "lineno", "pathname", "filename", "module", "funcName",
                     "created", "msecs", "relativeCreated", "thread", "threadName",
                     "processName", "process", "args", "msg", "levelname", "name",
                 )}
        payload: dict = {
            "ts":      datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level":   record.levelname.lower(),
            "event":   record.getMessage(),
            "service": "stock_context",
            **extra,
        }
        return json.dumps(payload, default=str)


# ── Application factory ───────────────────────────────────────────────────────

async def create_app() -> web.Application:
    """Build the aiohttp application with all routes and lifecycle hooks."""
    app = web.Application()

    # ── Startup / shutdown ────────────────────────────────────────────────────
    app.on_startup.append(_on_startup)
    app.on_shutdown.append(_on_shutdown)

    # ── Routes ────────────────────────────────────────────────────────────────
    app.router.add_get("/health",                    _handle_health)
    app.router.add_get("/context/{ticker}",          _handle_get_context)
    app.router.add_get("/context/{ticker}/refresh",  _handle_refresh_context)

    return app


# ── Lifecycle hooks ───────────────────────────────────────────────────────────

async def _on_startup(app: web.Application) -> None:
    """Initialise all shared resources."""
    redis_url  = os.environ.get("REDIS_URL",    "redis://redis:6379/0")
    db_url     = os.environ.get("DATABASE_URL",
                    "postgresql://trading:trading@postgres:5432/trading_db")

    # ── Redis ─────────────────────────────────────────────────────────────────
    app["redis"] = await aioredis.from_url(redis_url, decode_responses=True)
    redis_cache.set_redis_client(app["redis"])
    logger.info("main.redis_connected", extra={"url": redis_url})

    # ── PostgreSQL ────────────────────────────────────────────────────────────
    try:
        app["db"] = await asyncpg.create_pool(
            db_url,
            min_size=1,
            max_size=5,
            command_timeout=30,
        )
        logger.info("main.db_connected")
    except Exception as exc:
        logger.error("main.db_connect_failed", extra={"error": str(exc)})
        app["db"] = None

    # ── Scheduler ────────────────────────────────────────────────────────────
    sched = ContextScheduler(db_pool=app["db"])
    app["scheduler"] = sched
    await sched.start()
    logger.info("main.ready", extra={"port": _HTTP_PORT})


async def _on_shutdown(app: web.Application) -> None:
    """Drain scheduler and close connection pools."""
    sched: ContextScheduler | None = app.get("scheduler")
    if sched:
        await sched.stop()

    db: asyncpg.Pool | None = app.get("db")
    if db:
        await db.close()

    redis: aioredis.Redis | None = app.get("redis")
    if redis:
        await redis.aclose()

    logger.info("main.shutdown_complete")


# ── HTTP handlers ─────────────────────────────────────────────────────────────

async def _handle_health(request: web.Request) -> web.Response:
    """
    GET /health

    Returns service status, number of cached tickers, and last refresh time.
    """
    sched: ContextScheduler = request.app["scheduler"]
    cached = await redis_cache.get_all_cached_tickers()
    body = {
        "status":         "ok",
        "cached_tickers": len(cached),
        "last_refresh":   sched.last_refresh.isoformat() if sched.last_refresh else None,
    }
    return web.Response(
        text=json.dumps(body),
        content_type="application/json",
        status=200,
    )


async def _handle_get_context(request: web.Request) -> web.Response:
    """
    GET /context/{ticker}

    Returns the cached StockContext for *ticker* as JSON.
    Returns 404 if the ticker is not cached.
    """
    ticker = request.match_info["ticker"].upper()
    ctx = await redis_cache.get_context(ticker)
    if ctx is None:
        return web.Response(
            text=json.dumps({"error": f"No cached context for {ticker}"}),
            content_type="application/json",
            status=404,
        )
    return web.Response(
        text=json.dumps(ctx.to_dict()),
        content_type="application/json",
        status=200,
    )


async def _handle_refresh_context(request: web.Request) -> web.Response:
    """
    GET /context/{ticker}/refresh

    Force-reclassifies *ticker* immediately and returns the new StockContext.
    Returns 503 if the underlying Polygon fetch fails.
    """
    ticker = request.match_info["ticker"].upper()
    sched: ContextScheduler = request.app["scheduler"]

    logger.info("main.forced_refresh", extra={"ticker": ticker})
    ctx = await sched.refresh_ticker(ticker)
    if ctx is None:
        return web.Response(
            text=json.dumps({"error": f"Could not fetch data for {ticker}"}),
            content_type="application/json",
            status=503,
        )
    return web.Response(
        text=json.dumps(ctx.to_dict()),
        content_type="application/json",
        status=200,
    )


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    """Run the service.  Called by ``python -m stock_context_service.main``."""
    _setup_logging()
    logger.info("main.starting", extra={"port": _HTTP_PORT})

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Graceful SIGTERM / SIGINT shutdown
    stop_event = asyncio.Event()

    def _handle_signal(*_: object) -> None:
        logger.info("main.shutdown_signal_received")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except (NotImplementedError, OSError):
            # Windows doesn't support add_signal_handler
            signal.signal(sig, _handle_signal)   # type: ignore[arg-type]

    async def _run() -> None:
        app     = await create_app()
        runner  = web.AppRunner(app)
        await runner.setup()
        site    = web.TCPSite(runner, "0.0.0.0", _HTTP_PORT)
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
