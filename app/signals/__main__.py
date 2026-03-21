"""Signals service runner dispatcher."""
import asyncio
import sys
import signal
import json
from datetime import datetime, timezone


def _log(level, event, **kw):
    print(json.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                      "level": level, "event": event, **kw}), flush=True)


async def run_service(name: str) -> None:
    if name == "ai_summarizer":
        from app.signals.ai_summarizer import AISummarizerService
        service = AISummarizerService()
    elif name == "signal_aggregator":
        from app.signals.signal_aggregator import SignalAggregatorService
        service = SignalAggregatorService()
    elif name == "telegram_alerts":
        from app.signals.telegram_alerts import TelegramAlertsService
        service = TelegramAlertsService()
    elif name == "diagnostics_reporter":
        from app.signals.diagnostics_reporter import DiagnosticsReporter
        service = DiagnosticsReporter()
    else:
        _log("error", "unknown_service", name=name)
        sys.exit(1)

    loop = asyncio.get_running_loop()

    def shutdown():
        _log("info", "service.shutdown_signal", service=name)
        service.stop()

    loop.add_signal_handler(signal.SIGTERM, shutdown)
    loop.add_signal_handler(signal.SIGINT, shutdown)

    _log("info", "service.starting", service=name)
    await service.run()


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "ai_summarizer"
    asyncio.run(run_service(name))
