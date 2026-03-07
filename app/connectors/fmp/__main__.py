"""FMP connector services dispatcher."""
import asyncio
import sys
import signal
import json
from datetime import datetime, timezone


def _log(level, event, **kw):
    print(json.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                      "level": level, "event": event, **kw}), flush=True)


async def run_enrichment():
    from app.connectors.fmp.enrichment import FMPEnrichmentService
    svc = FMPEnrichmentService()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, svc.stop)
    loop.add_signal_handler(signal.SIGINT, svc.stop)
    await svc.run()


async def run_earnings():
    from app.connectors.fmp.earnings import FMPEarningsConnector
    svc = FMPEarningsConnector()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, svc.stop)
    loop.add_signal_handler(signal.SIGINT, svc.stop)
    await svc.run()


async def run_technical():
    from app.connectors.fmp.market_context import FMPTechnicalConnector
    svc = FMPTechnicalConnector()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, svc.stop)
    loop.add_signal_handler(signal.SIGINT, svc.stop)
    await svc.run()


async def run_sectors():
    from app.connectors.fmp.market_context import FMPSectorConnector
    svc = FMPSectorConnector()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, svc.stop)
    loop.add_signal_handler(signal.SIGINT, svc.stop)
    await svc.run()


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else ""
    runners = {
        "enrichment": run_enrichment,
        "earnings":   run_earnings,
        "technical":  run_technical,
        "sectors":    run_sectors,
    }
    runner = runners.get(name)
    if not runner:
        _log("error", "unknown_service", name=name,
             usage=f"python -m app.connectors.fmp [{'|'.join(runners)}]")
        sys.exit(1)
    asyncio.run(runner())
