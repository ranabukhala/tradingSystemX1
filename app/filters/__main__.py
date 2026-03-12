"""Filters module dispatcher."""
import asyncio
import signal
import sys
import json
from datetime import datetime, timezone


def _log(level, event, **kw):
    print(json.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                      "level": level, "event": event, **kw}), flush=True)


async def run_pretrade_filter():
    from app.filters.pretrade_filter import PreTradeFilterService
    svc = PreTradeFilterService()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, svc.stop)
    loop.add_signal_handler(signal.SIGINT, svc.stop)
    _log("info", "service.starting", service="pretrade_filter")
    await svc.run()


async def run_regime_poller():
    from app.filters.regime import RegimeFilter
    svc = RegimeFilter()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, svc.stop)
    loop.add_signal_handler(signal.SIGINT, svc.stop)
    _log("info", "service.starting", service="regime_poller")
    await svc.run()


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else ""
    runners = {
        "pretrade": run_pretrade_filter,
        "regime":   run_regime_poller,
    }
    runner = runners.get(name)
    if not runner:
        _log("error", "unknown_service", name=name,
             usage="python -m app.filters [pretrade|regime]")
        sys.exit(1)
    asyncio.run(runner())
