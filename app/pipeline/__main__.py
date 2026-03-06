"""Pipeline service runner dispatcher."""
import asyncio
import sys
import signal
import json
from datetime import datetime, timezone


def _log(level, event, **kw):
    print(json.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                      "level": level, "event": event, **kw}), flush=True)


async def run_service(name: str) -> None:
    if name == "normalizer":
        from app.pipeline.normalizer import NormalizerService
        service = NormalizerService()
    elif name == "deduplicator":
        from app.pipeline.deduplicator import DeduplicatorService
        service = DeduplicatorService()
    elif name == "entity_resolver":
        from app.pipeline.entity_resolver import EntityResolverService
        service = EntityResolverService()
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
    name = sys.argv[1] if len(sys.argv) > 1 else "normalizer"
    asyncio.run(run_service(name))
