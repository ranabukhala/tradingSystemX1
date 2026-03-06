"""Connector runner dispatcher."""
import asyncio
import sys
import signal
import json
from datetime import datetime, timezone


def _log(level, event, **kw):
    print(json.dumps({"ts": datetime.now(timezone.utc).isoformat(), "level": level, "event": event, **kw}), flush=True)


async def run_connector(name: str) -> None:
    if name == "benzinga":
        from app.connectors.benzinga import BenzingaConnector
        connector = BenzingaConnector()
    elif name == "polygon_news":
        from app.connectors.polygon_news import PolygonNewsConnector
        connector = PolygonNewsConnector()
    elif name == "polygon_prices":
        from app.connectors.polygon_prices import PolygonPriceConnector
        connector = PolygonPriceConnector()
    elif name == "earnings_whispers":
        from app.connectors.earnings_whispers import EarningsWhispersConnector
        connector = EarningsWhispersConnector()
    elif name == "fred_calendar":
        from app.connectors.fred_calendar import FredCalendarConnector
        connector = FredCalendarConnector()
    else:
        _log("error", "unknown_connector", name=name)
        sys.exit(1)

    loop = asyncio.get_running_loop()

    def shutdown():
        _log("info", "connector.shutdown_signal", connector=name)
        connector.stop()

    loop.add_signal_handler(signal.SIGTERM, shutdown)
    loop.add_signal_handler(signal.SIGINT, shutdown)

    _log("info", "connector.starting", connector=name)
    await connector.run()


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "benzinga"
    asyncio.run(run_connector(name))
