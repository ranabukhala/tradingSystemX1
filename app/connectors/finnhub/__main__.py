"""Finnhub connector services dispatcher."""
import asyncio
import sys
import signal
import json
from datetime import datetime, timezone


def _log(level, event, **kw):
    print(json.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                      "level": level, "event": event, **kw}), flush=True)


async def run_news():
    """REST polling news connector."""
    import os
    from app.connectors.finnhub.news import FinnhubNewsConnector
    svc = FinnhubNewsConnector()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, svc.stop)
    loop.add_signal_handler(signal.SIGINT, svc.stop)
    _log("info", "service.starting", service="finnhub_news")
    await svc.run()


async def run_websocket():
    """Real-time WebSocket news stream."""
    from app.connectors.finnhub.websocket import FinnhubWebSocketConnector
    svc = FinnhubWebSocketConnector()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, svc.stop)
    loop.add_signal_handler(signal.SIGINT, svc.stop)
    _log("info", "service.starting", service="finnhub_websocket")
    await svc.run()


async def run_sentiment():
    """Sentiment enrichment pipeline service."""
    from app.connectors.finnhub.sentiment import FinnhubSentimentService
    svc = FinnhubSentimentService()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, svc.stop)
    loop.add_signal_handler(signal.SIGINT, svc.stop)
    _log("info", "service.starting", service="finnhub_sentiment")
    await svc.run()


async def run_fundamentals():
    """Earnings + analyst data background poller."""
    from app.connectors.finnhub.fundamentals import FinnhubFundamentalsConnector
    svc = FinnhubFundamentalsConnector()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, svc.stop)
    loop.add_signal_handler(signal.SIGINT, svc.stop)
    _log("info", "service.starting", service="finnhub_fundamentals")
    await svc.run()


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else ""
    runners = {
        "news":         run_news,
        "websocket":    run_websocket,
        "sentiment":    run_sentiment,
        "fundamentals": run_fundamentals,
    }
    runner = runners.get(name)
    if not runner:
        _log("error", "unknown_service", name=name,
             usage=f"python -m app.connectors.finnhub [{'|'.join(runners)}]")
        sys.exit(1)
    asyncio.run(runner())
