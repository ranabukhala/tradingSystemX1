"""
Portfolio Risk Manager — Redis-backed state store (v1.10).

All transient risk state lives in Redis (fast, survives container restarts,
consistent across scaled instances). The broker API is the source of truth for
account / position data; results are cached here for 30 seconds.

Redis key schema
----------------
risk:kill_switch                 → "1" | absent
risk:auto_halt:drawdown          → reason string | absent
risk:auto_halt:loss_streak       → reason string | absent
risk:auto_halt:order_rate        → reason string | absent
risk:consecutive_losses          → int string
risk:last_loss_at                → ISO-8601 UTC timestamp string
risk:hourly_orders               → ZSET of float timestamps (sliding 1 h window)
risk:daily_orders:{YYYY-MM-DD}   → int string (TTL 25 h)
risk:event_trades:{cluster_id}   → int string (TTL cluster_ttl_seconds)
risk:positions_cache             → JSON string {ticker: {...}}
risk:positions_cache_at          → ISO-8601 UTC timestamp string
risk:equity_hwm                  → float string
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import redis.asyncio as aioredis
    from app.config import Settings

# Redis key constants
_KS_MANUAL       = "risk:kill_switch"
_AH_PREFIX       = "risk:auto_halt:"
_CONSEC_LOSSES   = "risk:consecutive_losses"
_LAST_LOSS_AT    = "risk:last_loss_at"
_HOURLY_ORDERS   = "risk:hourly_orders"
_DAILY_ORDERS    = "risk:daily_orders:{date}"
_EVENT_TRADES    = "risk:event_trades:{cluster_id}"
_POS_CACHE       = "risk:positions_cache"
_POS_CACHE_AT    = "risk:positions_cache_at"
_EQUITY_HWM      = "risk:equity_hwm"


class RiskStateStore:
    """
    Thin async wrapper around Redis for portfolio risk hot state.

    All methods degrade gracefully: if Redis is unavailable, they return
    safe defaults (0 counts, False flags, None caches) and log a warning.
    The risk service will then proceed to check constraints directly against
    the broker API, preserving fail-open behaviour.
    """

    def __init__(self, redis: "aioredis.Redis", settings: "Settings") -> None:
        self._r = redis
        self._s = settings

    # ── Kill switches ─────────────────────────────────────────────────────────

    async def is_kill_switch_active(self) -> tuple[bool, str]:
        """
        Returns (True, reason_code) if ANY kill switch is active.
        Manual switch takes priority over auto-halts.
        """
        try:
            manual = await self._r.get(_KS_MANUAL)
            if manual:
                return True, "RISK:KILL_SWITCH_MANUAL"
            # Check all auto-halt keys
            for suffix in ("drawdown", "loss_streak", "order_rate"):
                val = await self._r.get(f"{_AH_PREFIX}{suffix}")
                if val:
                    return True, val if isinstance(val, str) else val.decode()
        except Exception:
            pass
        return False, ""

    async def is_auto_halt_active(self) -> tuple[bool, str]:
        """Returns (True, reason) if any auto-halt key is set (ignores manual)."""
        try:
            for suffix in ("drawdown", "loss_streak", "order_rate"):
                val = await self._r.get(f"{_AH_PREFIX}{suffix}")
                if val:
                    return True, val if isinstance(val, str) else val.decode()
        except Exception:
            pass
        return False, ""

    async def set_auto_halt(self, suffix: str, reason: str, ttl: int | None = None) -> None:
        """Set auto-halt key.  suffix ∈ {drawdown, loss_streak, order_rate}."""
        try:
            key = f"{_AH_PREFIX}{suffix}"
            if ttl:
                await self._r.setex(key, ttl, reason)
            else:
                await self._r.set(key, reason)
        except Exception:
            pass

    async def clear_auto_halt(self, suffix: str) -> None:
        """Clear a specific auto-halt key."""
        try:
            await self._r.delete(f"{_AH_PREFIX}{suffix}")
        except Exception:
            pass

    # ── Consecutive loss tracking ─────────────────────────────────────────────

    async def get_consecutive_losses(self) -> int:
        try:
            val = await self._r.get(_CONSEC_LOSSES)
            return int(val) if val else 0
        except Exception:
            return 0

    async def record_loss(self) -> int:
        """Increment consecutive loss counter, record timestamp. Returns new count."""
        try:
            count = await self._r.incr(_CONSEC_LOSSES)
            now_iso = datetime.now(timezone.utc).isoformat()
            await self._r.set(_LAST_LOSS_AT, now_iso)
            return count
        except Exception:
            return 0

    async def reset_consecutive_losses(self) -> None:
        """Call on a winning trade to reset the streak."""
        try:
            await self._r.delete(_CONSEC_LOSSES)
            await self._r.delete(_LAST_LOSS_AT)
        except Exception:
            pass

    async def is_in_cooldown(self, cooldown_minutes: int) -> bool:
        """
        True if the last loss was within `cooldown_minutes` ago AND consecutive
        losses ≥ halt_consecutive_losses threshold (checked by caller).
        """
        try:
            last = await self._r.get(_LAST_LOSS_AT)
            if not last:
                return False
            last_str = last if isinstance(last, str) else last.decode()
            last_dt = datetime.fromisoformat(last_str)
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=cooldown_minutes)
            return last_dt > cutoff
        except Exception:
            return False

    # ── Order rate tracking ───────────────────────────────────────────────────

    async def get_hourly_order_count(self) -> int:
        """Count orders submitted in the last 3600 seconds."""
        try:
            now_ts = time.time()
            window_start = now_ts - 3600.0
            # Remove expired entries, then count remaining
            await self._r.zremrangebyscore(_HOURLY_ORDERS, "-inf", window_start)
            count = await self._r.zcard(_HOURLY_ORDERS)
            return count
        except Exception:
            return 0

    async def record_order_submitted(self) -> None:
        """Record order submission timestamp in sliding-window ZSET."""
        try:
            now_ts = time.time()
            await self._r.zadd(_HOURLY_ORDERS, {str(now_ts): now_ts})
            # Expire the whole key after 2 hours (cleanup)
            await self._r.expire(_HOURLY_ORDERS, 7200)
        except Exception:
            pass

    async def get_daily_order_count(self, date_str: str) -> int:
        """Count orders for a given date string (YYYY-MM-DD)."""
        try:
            key = _DAILY_ORDERS.format(date=date_str)
            val = await self._r.get(key)
            return int(val) if val else 0
        except Exception:
            return 0

    async def increment_daily_orders(self, date_str: str) -> None:
        """Increment daily order counter, set 25h TTL."""
        try:
            key = _DAILY_ORDERS.format(date=date_str)
            await self._r.incr(key)
            await self._r.expire(key, 90000)  # 25h
        except Exception:
            pass

    # ── Per-event cluster tracking ────────────────────────────────────────────

    async def get_event_trade_count(self, cluster_id: str) -> int:
        try:
            key = _EVENT_TRADES.format(cluster_id=cluster_id)
            val = await self._r.get(key)
            return int(val) if val else 0
        except Exception:
            return 0

    async def record_event_trade(self, cluster_id: str, ttl: int) -> None:
        """Increment event trade counter with TTL."""
        try:
            key = _EVENT_TRADES.format(cluster_id=cluster_id)
            await self._r.incr(key)
            await self._r.expire(key, ttl)
        except Exception:
            pass

    # ── Position cache ────────────────────────────────────────────────────────

    async def get_positions_cache(self, ttl_seconds: int) -> dict | None:
        """
        Returns cached position dict {ticker: {...}} if fresh, else None.
        Caller should re-fetch from broker and call set_positions_cache().
        """
        try:
            cache_at = await self._r.get(_POS_CACHE_AT)
            if not cache_at:
                return None
            cache_at_str = cache_at if isinstance(cache_at, str) else cache_at.decode()
            cached_dt = datetime.fromisoformat(cache_at_str)
            age = (datetime.now(timezone.utc) - cached_dt).total_seconds()
            if age > ttl_seconds:
                return None
            raw = await self._r.get(_POS_CACHE)
            return json.loads(raw) if raw else None
        except Exception:
            return None

    async def set_positions_cache(self, positions: dict) -> None:
        """Cache position dict with current timestamp."""
        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            await self._r.set(_POS_CACHE, json.dumps(positions))
            await self._r.set(_POS_CACHE_AT, now_iso)
        except Exception:
            pass

    # ── Equity high-water mark ────────────────────────────────────────────────

    async def get_equity_hwm(self) -> float:
        try:
            val = await self._r.get(_EQUITY_HWM)
            return float(val) if val else 0.0
        except Exception:
            return 0.0

    async def update_equity_hwm(self, equity: float) -> None:
        """Update HWM only if equity is higher than current stored value."""
        try:
            current = await self.get_equity_hwm()
            if equity > current:
                await self._r.set(_EQUITY_HWM, str(equity))
        except Exception:
            pass
