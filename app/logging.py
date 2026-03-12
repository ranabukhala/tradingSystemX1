"""
Structured logging setup using structlog.
All services call get_logger() to get a bound logger.

Every log line is JSON with: service, timestamp, level, event_id.
Correlation IDs propagated via structlog contextvars.

Usage:
    from app.logging import get_logger
    log = get_logger(__name__)
    log.info("connector.started", source="benzinga", poll_interval=30)
"""
from __future__ import annotations

import logging
import sys
import uuid

import structlog

from app.config import settings


def setup_logging() -> None:
    """Configure structlog for structured JSON output in Docker."""
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    """Get a named structlog logger, pre-bound with service name."""
    return structlog.get_logger(name).bind(service=settings.service_name)


def bind_event_context(event_id: str, **kwargs) -> None:
    """Bind correlation IDs to structlog context for current async task."""
    structlog.contextvars.bind_contextvars(event_id=event_id, **kwargs)


def clear_event_context() -> None:
    """Clear correlation context after processing."""
    structlog.contextvars.clear_contextvars()


def new_event_id() -> str:
    """Generate a new event/correlation ID."""
    return str(uuid.uuid4())
