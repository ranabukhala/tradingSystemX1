"""Execution services dispatcher."""
import asyncio
import sys
import signal
import json
from datetime import datetime, timezone


def _log(level, event, **kw):
    print(json.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                      "level": level, "event": event, **kw}), flush=True)


async def run_execution_engine():
    from app.execution.execution_engine import ExecutionEngine
    svc = ExecutionEngine()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, svc.stop)
    loop.add_signal_handler(signal.SIGINT, svc.stop)
    _log("info", "service.starting", service="execution_engine")
    await svc.run()


async def run_position_monitor():
    import os
    from app.execution.execution_engine import _get_broker
    from app.execution.position_monitor import PositionMonitor

    broker = _get_broker()
    await broker.connect()

    monitor = PositionMonitor(broker)
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, monitor.stop)
    loop.add_signal_handler(signal.SIGINT, monitor.stop)

    _log("info", "service.starting", service="position_monitor")
    await monitor.start()
    await broker.disconnect()


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else ""

    if name == "execution_engine":
        asyncio.run(run_execution_engine())
    elif name == "position_monitor":
        asyncio.run(run_position_monitor())
    else:
        _log("error", "unknown_service", name=name,
             usage="python -m app.execution.__main__ [execution_engine|position_monitor]")
        sys.exit(1)
