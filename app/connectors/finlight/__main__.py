"""Finlight connector dispatcher."""
import asyncio
import json
import signal
import sys
from datetime import datetime, timezone


def _log(level, event, **kw):
    print(json.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                      "level": level, "event": event, **kw}), flush=True)


async def run_connector():
    from app.connectors.finlight.connector import FinlightConnector
    svc = FinlightConnector()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, svc.stop)
    loop.add_signal_handler(signal.SIGINT, svc.stop)
    _log("info", "service.starting", service="finlight_connector")
    await svc.run()


if __name__ == "__main__":
    asyncio.run(run_connector())
