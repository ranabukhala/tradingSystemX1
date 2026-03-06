"""
Structured logging setup using structlog.
All services call setup_logging() at startup.
Usage:
    from app.logging import get_logger
    log = get_logger(__name__)
    log.info("connector.started", source="benzinga", poll_interval=30)
"""
from __future__ import annotations

import logging
import sys

import structlog

from app.config import settings


def setup_logging() -> None:
    """Configure structlog — compatible with PrintLoggerFactory."""

    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    # NOTE: add_logger_name requires stdlib logger (.name attribute).
    # We use PrintLoggerFactory so we omit that processor.
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.dev.ConsoleRenderer(colors=False),  # Clean output in Docker
    ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    """Get a named structlog logger."""
    return structlog.get_logger(name)
