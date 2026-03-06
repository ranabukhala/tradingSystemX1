"""
Data quality checks — run after connectors to validate data is flowing.
Called by: make status, scheduled health check, CI.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass

import aiohttp
import redis.asyncio as aioredis

from app.config import settings
from app.logging import get_logger

log = get_logger(__name__)


@dataclass
class CheckResult:
    name: str
    passed: bool
    message: str
    value: float | int | None = None


async def check_kafka_topic_has_messages(topic: str, min_count: int = 1) -> CheckResult:
    """Verify a Kafka topic has recent messages via Redpanda REST API."""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"http://localhost:18082/topics/{topic}/partitions"
            async with session.get(url) as resp:
                if resp.status != 200:
                    return CheckResult(topic, False, f"HTTP {resp.status}")
                data = await resp.json()

        # Check if any partition has messages
        total = sum(p.get("high_watermark", 0) for p in data)
        passed = total >= min_count
        return CheckResult(
            name=f"topic.{topic}",
            passed=passed,
            message=f"{total} messages" if passed else f"Only {total} messages (need {min_count})",
            value=total,
        )
    except Exception as e:
        return CheckResult(topic, False, str(e))


async def check_postgres_table_rows(table: str, min_rows: int = 1) -> CheckResult:
    """Check that a Postgres table has minimum rows."""
    try:
        import asyncpg
        conn = await asyncpg.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            user=settings.postgres_user,
            password=settings.postgres_password,
            database=settings.postgres_db,
        )
        count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
        await conn.close()
        passed = count >= min_rows
        return CheckResult(
            name=f"table.{table}",
            passed=passed,
            message=f"{count} rows" if passed else f"Only {count} rows (need {min_rows})",
            value=count,
        )
    except Exception as e:
        return CheckResult(table, False, str(e))


async def check_recent_news(max_age_minutes: int = 10) -> CheckResult:
    """Check that news arrived in the last N minutes (only during market hours)."""
    try:
        import asyncpg
        conn = await asyncpg.connect(
            host=settings.postgres_host, port=settings.postgres_port,
            user=settings.postgres_user, password=settings.postgres_password,
            database=settings.postgres_db,
        )
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM news_item WHERE received_at > $1", cutoff
        )
        await conn.close()

        now_et = datetime.now(timezone.utc)  # Simplified; use pytz for proper ET
        is_market_hours = True  # Simplified check

        if count > 0:
            return CheckResult("recent_news", True, f"{count} items in last {max_age_minutes}min", count)
        elif not is_market_hours:
            return CheckResult("recent_news", True, "Market closed — no news expected", 0)
        else:
            return CheckResult("recent_news", False, f"No news in last {max_age_minutes}min", 0)
    except Exception as e:
        return CheckResult("recent_news", False, str(e))


async def check_earnings_events_loaded(min_events: int = 5) -> CheckResult:
    """Check that upcoming earnings events are in the DB."""
    try:
        import asyncpg
        conn = await asyncpg.connect(
            host=settings.postgres_host, port=settings.postgres_port,
            user=settings.postgres_user, password=settings.postgres_password,
            database=settings.postgres_db,
        )
        cutoff = datetime.now(timezone.utc) + timedelta(days=7)
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM event WHERE event_type='earnings' AND event_date <= $1 AND event_date >= NOW()",
            cutoff,
        )
        await conn.close()
        passed = count >= min_events
        return CheckResult(
            "earnings_events",
            passed,
            f"{count} earnings events in next 7 days" if passed else f"Only {count} events (need {min_events})",
            count,
        )
    except Exception as e:
        return CheckResult("earnings_events", False, str(e))


async def run_all_checks() -> list[CheckResult]:
    """Run all data quality checks and return results."""
    checks = await asyncio.gather(
        check_kafka_topic_has_messages("news.raw", min_count=1),
        check_kafka_topic_has_messages("prices.bars", min_count=1),
        check_kafka_topic_has_messages("events.calendar", min_count=1),
        check_postgres_table_rows("news_item", min_rows=0),
        check_postgres_table_rows("event", min_rows=0),
        check_recent_news(max_age_minutes=15),
        check_earnings_events_loaded(min_events=1),
        return_exceptions=True,
    )

    results = []
    for r in checks:
        if isinstance(r, Exception):
            results.append(CheckResult("unknown", False, str(r)))
        else:
            results.append(r)

    return results


async def print_check_report() -> None:
    """Print a formatted check report to stdout."""
    results = await run_all_checks()

    passed = sum(1 for r in results if r.passed)
    total = len(results)

    print(f"\n{'═'*50}")
    print(f"  Data Quality Checks — {passed}/{total} passed")
    print(f"{'═'*50}")

    for r in results:
        icon = "✓" if r.passed else "✗"
        status = "\033[92m" if r.passed else "\033[91m"
        print(f"  {status}{icon}\033[0m  {r.name:<30} {r.message}")

    print(f"{'─'*50}\n")

    if passed < total:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(print_check_report())
