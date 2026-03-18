"""
Shared LLM budget tracker — persists spend to Redis.

Tracks daily and monthly spend per provider (Anthropic, OpenAI).
All Redis writes use INCRBYFLOAT for atomic concurrent updates.

Usage::

    tracker = LLMBudgetTracker(redis_client)
    await tracker.record("anthropic", cost_usd=0.0012)
    await tracker.record("openai", cost_usd=0.000002)

    summary = await tracker.get_summary()
    # {
    #   "daily":   {"anthropic": 0.42, "openai": 0.003, "total": 0.423},
    #   "monthly": {"anthropic": 8.20, "openai": 0.09,  "total": 8.29},
    #   "daily_limit":       25.0,
    #   "monthly_limit":     500.0,
    #   "daily_remaining":   24.577,
    #   "monthly_remaining": 491.71,
    #   "date":  "2026-03-18",
    #   "month": "2026-03",
    # }
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# TTLs
_DAILY_TTL_S   = 8 * 86400   # 8 days — keep last week for trend analysis
_MONTHLY_TTL_S = 95 * 86400  # ~3 months of history

VALID_PROVIDERS = ("anthropic", "openai")


class LLMBudgetTracker:
    """
    Async Redis-backed LLM spend tracker.

    All writes are INCRBYFLOAT — safe for concurrent coroutines.
    Falls back to in-memory tracking if Redis is unavailable so the
    summarizer / embedding service never crashes due to a Redis blip.
    """

    def __init__(self, redis_client, daily_limit: float = 25.0,
                 monthly_limit: float = 500.0) -> None:
        self._redis        = redis_client
        self.daily_limit   = daily_limit
        self.monthly_limit = monthly_limit
        # In-memory fallback (used when Redis is unreachable)
        self._fallback_daily: dict[str, float]   = {}
        self._fallback_monthly: dict[str, float] = {}

    # ── Internal helpers ──────────────────────────────────────────────────

    @staticmethod
    def _today() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    @staticmethod
    def _month() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m")

    def _daily_key(self, provider: str | None = None) -> str:
        base = f"llm:budget:{self._today()}"
        return f"{base}:{provider}" if provider else base

    def _monthly_key(self, provider: str | None = None) -> str:
        base = f"llm:budget:monthly:{self._month()}"
        return f"{base}:{provider}" if provider else base

    def _call_key(self, provider: str) -> str:
        return f"llm:calls:{self._today()}:{provider}"

    async def _incr(self, key: str, amount: float, ttl: int) -> float:
        """INCRBYFLOAT key by amount; set TTL on first write. Returns new value."""
        try:
            new_val = await self._redis.incrbyfloat(key, amount)
            # Set TTL only on first write (when value ≈ amount)
            if abs(float(new_val) - amount) < 1e-9:
                await self._redis.expire(key, ttl)
            return float(new_val)
        except Exception as e:
            logger.debug("budget.redis_write_failed key=%s error=%s", key, e)
            return 0.0

    # ── Public API ────────────────────────────────────────────────────────

    async def record(self, provider: str, cost_usd: float) -> None:
        """
        Record spend for a provider.

        Args:
            provider: "anthropic" or "openai"
            cost_usd: spend in USD (e.g. 0.0012)
        """
        if provider not in VALID_PROVIDERS:
            logger.warning("budget.unknown_provider provider=%s", provider)
            return
        if cost_usd <= 0:
            return

        try:
            # Daily — combined and per-provider
            await self._incr(self._daily_key(),           cost_usd, _DAILY_TTL_S)
            await self._incr(self._daily_key(provider),   cost_usd, _DAILY_TTL_S)
            # Monthly — combined and per-provider
            await self._incr(self._monthly_key(),         cost_usd, _MONTHLY_TTL_S)
            await self._incr(self._monthly_key(provider), cost_usd, _MONTHLY_TTL_S)
            # Call count
            await self._redis.incr(self._call_key(provider))
            # Ensure call count key has TTL
            await self._redis.expire(self._call_key(provider), _DAILY_TTL_S)

            logger.debug(
                "budget.recorded provider=%s cost=%.6f", provider, cost_usd
            )
        except Exception as e:
            # Redis failure — update in-memory fallback so can_spend still works
            logger.debug("budget.redis_failed_fallback error=%s", e)
            self._fallback_daily[provider]   = self._fallback_daily.get(provider, 0) + cost_usd
            self._fallback_monthly[provider] = self._fallback_monthly.get(provider, 0) + cost_usd

    async def get_daily_total(self) -> float:
        """Total combined spend today (all providers)."""
        try:
            val = await self._redis.get(self._daily_key())
            return float(val) if val else 0.0
        except Exception:
            return sum(self._fallback_daily.values())

    async def get_monthly_total(self) -> float:
        """Total combined spend this calendar month."""
        try:
            val = await self._redis.get(self._monthly_key())
            return float(val) if val else 0.0
        except Exception:
            return sum(self._fallback_monthly.values())

    async def can_spend(self, provider: str, estimated: float) -> bool:
        """
        Returns True if spending `estimated` USD would stay within both
        daily and monthly limits.
        """
        daily   = await self.get_daily_total()
        monthly = await self.get_monthly_total()
        return (
            (daily   + estimated) <= self.daily_limit and
            (monthly + estimated) <= self.monthly_limit
        )

    async def get_summary(self) -> dict:
        """
        Return a full budget summary dict for the watchdog snapshot
        and admin bot /budget command.
        """
        try:
            keys = [
                self._daily_key(),
                self._daily_key("anthropic"),
                self._daily_key("openai"),
                self._monthly_key(),
                self._monthly_key("anthropic"),
                self._monthly_key("openai"),
                self._call_key("anthropic"),
                self._call_key("openai"),
            ]
            values = await self._redis.mget(*keys)

            def _f(v) -> float:
                return float(v) if v else 0.0

            daily_total     = _f(values[0])
            daily_anthropic = _f(values[1])
            daily_openai    = _f(values[2])
            monthly_total   = _f(values[3])
            monthly_anthro  = _f(values[4])
            monthly_openai  = _f(values[5])
            calls_anthropic = int(values[6] or 0)
            calls_openai    = int(values[7] or 0)

        except Exception as e:
            logger.debug("budget.get_summary_failed error=%s", e)
            daily_total = daily_anthropic = daily_openai = 0.0
            monthly_total = monthly_anthro = monthly_openai = 0.0
            calls_anthropic = calls_openai = 0

        return {
            "daily": {
                "anthropic": round(daily_anthropic, 6),
                "openai":    round(daily_openai,    6),
                "total":     round(daily_total,     6),
            },
            "monthly": {
                "anthropic": round(monthly_anthro,  4),
                "openai":    round(monthly_openai,  4),
                "total":     round(monthly_total,   4),
            },
            "calls_today": {
                "anthropic": calls_anthropic,
                "openai":    calls_openai,
            },
            "daily_limit":       self.daily_limit,
            "monthly_limit":     self.monthly_limit,
            "daily_remaining":   round(max(0.0, self.daily_limit   - daily_total),   4),
            "monthly_remaining": round(max(0.0, self.monthly_limit - monthly_total), 4),
            "daily_pct":         round(daily_total   / self.daily_limit   * 100, 1) if self.daily_limit   else 0,
            "monthly_pct":       round(monthly_total / self.monthly_limit * 100, 1) if self.monthly_limit else 0,
            "date":  self._today(),
            "month": self._month(),
        }
