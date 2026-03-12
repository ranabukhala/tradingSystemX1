"""Execution services dispatcher."""
import asyncio
import sys
import signal as sig

from app.logging import get_logger, setup_logging

setup_logging()
log = get_logger("execution")


async def run_execution_engine():
    from app.execution.execution_engine import ExecutionEngine
    svc = ExecutionEngine()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(sig.SIGTERM, svc.stop)
    loop.add_signal_handler(sig.SIGINT, svc.stop)
    log.info("service.starting", service="execution_engine")
    await svc.run()


async def run_position_monitor():
    from app.execution.execution_engine import _get_broker
    from app.execution.position_monitor import PositionMonitor

    broker = _get_broker()
    await broker.connect()

    monitor = PositionMonitor(broker)
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(sig.SIGTERM, monitor.stop)
    loop.add_signal_handler(sig.SIGINT, monitor.stop)

    log.info("service.starting", service="position_monitor")
    await monitor.start()
    await broker.disconnect()


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else ""

    if name == "execution_engine":
        asyncio.run(run_execution_engine())
    elif name == "position_monitor":
        asyncio.run(run_position_monitor())
    else:
        log.error("unknown_service", name=name,
                  usage="python -m app.execution.__main__ [execution_engine|position_monitor]")
        sys.exit(1)
