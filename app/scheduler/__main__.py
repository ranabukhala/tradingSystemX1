"""Market scheduler entry point.

Usage (via docker-compose):
    entrypoint: ["python", "-m", "app.scheduler"]
"""
import asyncio
import signal as _signal

from app.logging import setup_logging, get_logger

setup_logging()
log = get_logger("scheduler")


async def _run() -> None:
    from app.scheduler.market_scheduler import MarketScheduler

    svc = MarketScheduler()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(_signal.SIGTERM, svc.stop)
    loop.add_signal_handler(_signal.SIGINT,  svc.stop)

    await svc.run()


if __name__ == "__main__":
    asyncio.run(_run())
