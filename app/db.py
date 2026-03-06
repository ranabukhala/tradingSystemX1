"""
Async database connection pool and session factory.
Uses SQLAlchemy 2.0 async style with asyncpg driver.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from app.config import settings
from app.logging import get_logger

log = get_logger(__name__)


class Base(DeclarativeBase):
    """Base class for all ORM models."""
    pass


# ── Engine ────────────────────────────────────────────────────────────────────

def create_engine() -> AsyncEngine:
    return create_async_engine(
        settings.database_url,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,          # Verify connections before using
        pool_recycle=3600,           # Recycle connections every hour
        echo=settings.environment == "development",
    )


engine: AsyncEngine = create_engine()

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


# ── Session context manager ───────────────────────────────────────────────────

@asynccontextmanager
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency-injection style session factory.

    Usage:
        async with get_db() as db:
            result = await db.execute(select(NewsItem))
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


# ── Health check ──────────────────────────────────────────────────────────────

async def check_db_health() -> bool:
    """Returns True if database is reachable."""
    try:
        async with get_db() as db:
            await db.execute(__import__("sqlalchemy").text("SELECT 1"))
        return True
    except Exception as e:
        log.error("db.health_check_failed", error=str(e))
        return False


def get_engine() -> AsyncEngine:
    """Return the shared async engine instance."""
    return engine
