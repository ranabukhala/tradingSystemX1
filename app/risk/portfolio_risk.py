"""
Portfolio Risk Manager — Stage 6b of the pipeline (v1.10).

Sits between pretrade_filter and execution_engine.
Consumes:  signals.filtered
Emits:     signals.risk_approved   (cleared — proceed to execution)
           signals.blocked         (risk violation — for review)

Constraint evaluation order (fail-fast, cheapest checks first):
  1.  Kill switch — manual (Redis:risk:kill_switch) or automatic halt
  2.  Daily loss cap — account.daily_pnl_pct ≤ −max_daily_loss_pct
  3.  Consecutive-loss cooldown — N losses within cooldown window
  4.  Hourly order rate — sliding 1-hour window counter
  5.  Daily order cap — calendar-day counter
  6.  Max open positions — broker positions count
  7.  Per-ticker exposure — existing USD in same ticker
  8.  Sector concentration — (sector_positions_usd / portfolio_value) ≥ max_sector_pct
  9.  Catalyst concentration — count of open positions with same catalyst_type
  10. Correlation proxy — count of (sector + direction) pairs among open positions
  11. Per-event cluster dedup — cluster already generated a trade (Redis TTL counter)

All blocks emit machine-readable reason codes (app.risk.risk_codes) and write
an immutable audit row to portfolio_risk_log.

Kill-switch design
------------------
Manual:   redis-cli SET risk:kill_switch 1      → halt all signals
          redis-cli DEL risk:kill_switch         → resume
Automatic:
  Drawdown    equity < HWM × (1 − risk_halt_drawdown_pct) → risk:auto_halt:drawdown
  Loss streak consecutive_losses ≥ halt_consecutive_losses → risk:auto_halt:loss_streak
  Order rate  hourly_orders > risk_max_orders_per_hour    → risk:auto_halt:order_rate

Auto-halts are written by this service; they persist until cleared manually or
at day-roll (00:00 ET).  The cooldown check (constraint 3) is distinct from the
auto-halt: cooldown blocks are time-bounded; auto-halts require manual action.
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone

import redis.asyncio as aioredis

from app.config import settings as _settings
from app.pipeline.base_consumer import BaseConsumer, _log
from app.risk.risk_codes import (
    KILL_SWITCH_MANUAL,
    AUTO_HALT_DRAWDOWN,
    AUTO_HALT_LOSS_STREAK,
    AUTO_HALT_ORDER_RATE,
    DAILY_LOSS_HALT,
    CONSECUTIVE_LOSS_COOLDOWN,
    ORDER_RATE_LIMIT,
    DAILY_ORDER_CAP,
    MAX_POSITIONS,
    TICKER_EXPOSURE,
    SECTOR_CONCENTRATION,
    CATALYST_CONCENTRATION,
    CORRELATION_RISK,
    EVENT_ALREADY_TRADED,
)
from app.risk.risk_state import RiskStateStore


def _catalyst_max(catalyst_type: str) -> int:
    """Return the max-concurrent-positions limit for a catalyst family."""
    mapping = {
        "earnings":   _settings.risk_max_catalyst_earnings_positions,
        "analyst":    _settings.risk_max_catalyst_analyst_positions,
        "regulatory": _settings.risk_max_catalyst_regulatory_positions,
        "filing":     _settings.risk_max_catalyst_regulatory_positions,
        "ma":         _settings.risk_max_catalyst_ma_positions,
        "macro":      _settings.risk_max_catalyst_macro_positions,
    }
    return mapping.get(catalyst_type, _settings.risk_max_catalyst_default_positions)


class PortfolioRiskService(BaseConsumer):
    """
    Kafka consumer: signals.filtered → signals.risk_approved / signals.blocked
    """

    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None
        self._state: RiskStateStore | None = None
        self._broker = None           # BaseBroker instance, set in on_start
        super().__init__()

    @property
    def service_name(self) -> str:
        return "portfolio_risk"

    @property
    def input_topic(self) -> str:
        return "signals.filtered"

    @property
    def output_topic(self) -> str:
        return "signals.risk_approved"

    async def on_start(self) -> None:
        self._redis = await aioredis.from_url(
            os.environ.get("REDIS_URL", "redis://redis:6379/0"),
            decode_responses=True,
        )
        self._state = RiskStateStore(self._redis, _settings)

        # Lazy-import broker to avoid circular deps; same pattern as execution_engine
        from app.execution.execution_engine import _get_broker
        self._broker = _get_broker()
        await self._broker.connect()

        _log("info", "portfolio_risk.ready",
             max_positions=_settings.risk_max_open_positions,
             max_daily_loss_pct=_settings.risk_max_daily_loss_pct,
             kill_switch_key=_settings.risk_kill_switch_redis_key)

    async def on_stop(self) -> None:
        if self._broker:
            await self._broker.disconnect()
        if self._redis:
            await self._redis.aclose()

    # ── Main process ──────────────────────────────────────────────────────────

    async def process(self, record: dict) -> dict | None:
        direction = record.get("direction", "neutral")
        if direction == "neutral":
            return None

        ticker        = record.get("ticker", "")
        conviction    = float(record.get("conviction", 0.0))
        catalyst_type = record.get("catalyst_type", "other")
        cluster_id    = record.get("cluster_id")
        sectors: list[str] = record.get("sectors") or []

        # ── 1. Kill switch (manual + auto-halt) — Redis only, no broker call ──
        ks_active, ks_reason = await self._state.is_kill_switch_active()
        if ks_active:
            return await self._block(record, ks_reason,
                                     f"Trading halted: {ks_reason}",
                                     conviction=conviction)

        # ── Fetch account + positions (cached 30 s) ───────────────────────────
        account, open_positions = await self._get_account_and_positions()

        # ── 2. Daily loss cap + drawdown auto-halt ─────────────────────────────
        if account is not None and account.equity > 0:
            daily_loss_pct = abs(min(account.daily_pnl, 0)) / account.equity
            if daily_loss_pct >= _settings.risk_max_daily_loss_pct:
                return await self._block(
                    record, DAILY_LOSS_HALT,
                    f"Daily loss {daily_loss_pct*100:.1f}% ≥ limit "
                    f"{_settings.risk_max_daily_loss_pct*100:.1f}%",
                    conviction=conviction,
                    daily_pnl_pct=-daily_loss_pct,
                )
            # Drawdown auto-halt check
            hwm = await self._state.get_equity_hwm()
            if hwm > 0:
                drawdown = (hwm - account.equity) / hwm
                if drawdown >= _settings.risk_halt_drawdown_pct:
                    reason = (f"Equity drawdown {drawdown*100:.1f}% ≥ "
                              f"{_settings.risk_halt_drawdown_pct*100:.1f}% from HWM "
                              f"${hwm:,.0f}")
                    await self._state.set_auto_halt("drawdown", AUTO_HALT_DRAWDOWN)
                    return await self._block(record, AUTO_HALT_DRAWDOWN, reason,
                                             conviction=conviction)

        # ── 3. Consecutive-loss cooldown ───────────────────────────────────────
        consecutive = await self._state.get_consecutive_losses()
        if consecutive >= _settings.risk_halt_consecutive_losses:
            in_cooldown = await self._state.is_in_cooldown(
                _settings.risk_cooldown_after_loss_minutes
            )
            if in_cooldown:
                return await self._block(
                    record, CONSECUTIVE_LOSS_COOLDOWN,
                    f"Cooldown active after {consecutive} consecutive losses",
                    conviction=conviction,
                )
            else:
                # Cooldown expired — check if auto-halt still set and clear it
                await self._state.clear_auto_halt("loss_streak")

        # ── 4. Hourly order rate ───────────────────────────────────────────────
        hourly = await self._state.get_hourly_order_count()
        if hourly >= _settings.risk_max_orders_per_hour:
            await self._state.set_auto_halt(
                "order_rate",
                AUTO_HALT_ORDER_RATE,
                ttl=3600,
            )
            return await self._block(
                record, ORDER_RATE_LIMIT,
                f"Hourly order rate {hourly} ≥ limit {_settings.risk_max_orders_per_hour}",
                conviction=conviction,
                hourly_orders=hourly,
            )

        # ── 5. Daily order cap ─────────────────────────────────────────────────
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        daily = await self._state.get_daily_order_count(today)
        if daily >= _settings.risk_max_orders_per_day:
            return await self._block(
                record, DAILY_ORDER_CAP,
                f"Daily order cap {_settings.risk_max_orders_per_day} reached",
                conviction=conviction,
            )

        # ── Position-level checks (require broker state) ──────────────────────
        n_positions = len(open_positions) if open_positions is not None else 0

        # ── 6. Max open positions ─────────────────────────────────────────────
        if n_positions >= _settings.risk_max_open_positions:
            return await self._block(
                record, MAX_POSITIONS,
                f"Max open positions {_settings.risk_max_open_positions} reached",
                conviction=conviction,
                open_positions=n_positions,
            )

        if open_positions and account:
            portfolio_value = account.equity

            # ── 7. Per-ticker exposure ─────────────────────────────────────────
            existing = next(
                (p for p in open_positions if p.ticker == ticker), None
            )
            if existing:
                ticker_value = abs(existing.market_value)
                max_usd = min(
                    _settings.risk_max_ticker_exposure_usd,
                    portfolio_value * _settings.risk_max_ticker_exposure_pct,
                )
                if ticker_value >= max_usd:
                    return await self._block(
                        record, TICKER_EXPOSURE,
                        f"Ticker {ticker} exposure ${ticker_value:,.0f} ≥ limit ${max_usd:,.0f}",
                        conviction=conviction,
                        open_positions=n_positions,
                    )

            # ── 8. Sector concentration ────────────────────────────────────────
            if sectors and portfolio_value > 0:
                # primary sector is the first in the list (set by entity resolver)
                primary_sector = sectors[0]
                sector_value = sum(
                    abs(p.market_value)
                    for p in open_positions
                    if _position_sector(p) == primary_sector
                )
                sector_pct = sector_value / portfolio_value
                if sector_pct >= _settings.risk_max_sector_pct:
                    return await self._block(
                        record, SECTOR_CONCENTRATION,
                        f"Sector '{primary_sector}' at {sector_pct*100:.1f}% ≥ "
                        f"limit {_settings.risk_max_sector_pct*100:.0f}%",
                        conviction=conviction,
                        open_positions=n_positions,
                        sector_pct=sector_pct,
                    )

            # ── 9. Catalyst concentration ──────────────────────────────────────
            cat_count = _catalyst_positions_count(open_positions, catalyst_type)
            cat_max   = _catalyst_max(catalyst_type)
            if cat_count >= cat_max:
                return await self._block(
                    record, CATALYST_CONCENTRATION,
                    f"Catalyst '{catalyst_type}': {cat_count} open positions ≥ limit {cat_max}",
                    conviction=conviction,
                    open_positions=n_positions,
                    catalyst_count=cat_count,
                )

            # ── 10. Correlation proxy (same sector + direction) ────────────────
            if sectors:
                primary_sector = sectors[0]
                corr_count = sum(
                    1 for p in open_positions
                    if _position_sector(p) == primary_sector
                    and _position_direction(p) == direction
                )
                if corr_count >= _settings.risk_max_correlated_positions:
                    return await self._block(
                        record, CORRELATION_RISK,
                        f"Correlation proxy: {corr_count} open "
                        f"'{primary_sector}/{direction}' positions ≥ limit "
                        f"{_settings.risk_max_correlated_positions}",
                        conviction=conviction,
                        open_positions=n_positions,
                    )

        # ── 11. Per-event cluster dedup ────────────────────────────────────────
        if cluster_id:
            event_count = await self._state.get_event_trade_count(str(cluster_id))
            if event_count >= _settings.risk_max_trades_per_cluster:
                return await self._block(
                    record, EVENT_ALREADY_TRADED,
                    f"Cluster {cluster_id} already generated {event_count} trade(s)",
                    conviction=conviction,
                    open_positions=n_positions,
                )

        # ── All checks passed ─────────────────────────────────────────────────
        # Update state counters
        await self._state.record_order_submitted()
        await self._state.increment_daily_orders(today)
        if cluster_id:
            await self._state.record_event_trade(
                str(cluster_id), _settings.risk_cluster_ttl_seconds
            )
        if account:
            await self._state.update_equity_hwm(account.equity)

        # Enrich record with portfolio risk debug fields
        record["portfolio_risk_checked"] = True
        record["risk_positions_count"]   = n_positions
        record["risk_hourly_orders"]     = hourly

        _log("info", "portfolio_risk.approved",
             ticker=ticker, direction=direction,
             conviction=conviction,
             open_positions=n_positions,
             hourly_orders=hourly,
             catalyst_type=catalyst_type)

        return record

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _get_account_and_positions(self):
        """
        Fetch account info + open positions from broker, with 30s Redis cache.
        Returns (AccountInfo | None, list[Position] | None).
        """
        try:
            cached = await self._state.get_positions_cache(
                _settings.risk_position_cache_ttl_seconds
            )
            if cached is not None:
                # Reconstruct position list from cache
                from app.execution.base_broker import Position as BrokerPosition
                positions = [
                    BrokerPosition(**v) for v in cached.get("positions", {}).values()
                ]
                from app.execution.base_broker import AccountInfo as BrokerAccount
                acc_data = cached.get("account", {})
                account = BrokerAccount(**acc_data) if acc_data else None
                return account, positions

            account    = await self._broker.get_account()
            positions  = await self._broker.get_positions()

            # Cache for next call
            cache_data = {
                "account":   account.__dict__ if account else {},
                "positions": {p.ticker: p.__dict__ for p in positions},
            }
            await self._state.set_positions_cache(cache_data)
            return account, positions

        except Exception as exc:
            _log("warning", "portfolio_risk.broker_fetch_error", error=str(exc))
            return None, None

    async def _block(
        self,
        record: dict,
        code: str,
        human_reason: str,
        conviction: float = 0.0,
        open_positions: int | None = None,
        daily_pnl_pct: float | None = None,
        sector_pct: float | None = None,
        catalyst_count: int | None = None,
        hourly_orders: int | None = None,
    ) -> None:
        """Emit to signals.blocked and write audit row."""
        ticker        = record.get("ticker", "")
        catalyst_type = record.get("catalyst_type")
        cluster_id    = record.get("cluster_id")

        _log("info", "portfolio_risk.blocked",
             ticker=ticker, code=code, reason=human_reason,
             conviction=conviction, open_positions=open_positions)

        blocked_record = {
            **record,
            "blocked":      True,
            "block_reason": human_reason,
            "block_code":   code,
        }

        # Emit to signals.blocked topic
        try:
            from app.kafka import get_producer
            producer = get_producer()
            producer.produce(
                topic="signals.blocked",
                value=blocked_record,
                key=ticker,
            )
        except Exception as e:
            _log("warning", "portfolio_risk.blocked_emit_error", error=str(e))

        # Fire-and-forget audit write
        asyncio.ensure_future(
            self._write_risk_log(
                ticker=ticker,
                direction=record.get("direction"),
                catalyst_type=catalyst_type,
                cluster_id=cluster_id,
                news_id=record.get("news_id"),
                code=code,
                reason=human_reason,
                conviction=conviction,
                open_positions=open_positions,
                daily_pnl_pct=daily_pnl_pct,
                sector_pct=sector_pct,
                catalyst_count=catalyst_count,
                hourly_orders=hourly_orders,
            )
        )
        return None

    async def _write_risk_log(
        self, *, ticker: str, direction: str | None,
        catalyst_type: str | None, cluster_id, news_id,
        code: str, reason: str, conviction: float,
        open_positions: int | None, daily_pnl_pct: float | None,
        sector_pct: float | None, catalyst_count: int | None,
        hourly_orders: int | None,
    ) -> None:
        """Insert immutable audit row into portfolio_risk_log."""
        try:
            import asyncpg
            dsn = os.environ.get("DATABASE_URL",
                "postgresql://trading:trading@postgres:5432/trading_db")
            conn = await asyncpg.connect(dsn)
            await conn.execute("""
                INSERT INTO portfolio_risk_log (
                    news_id, ticker, direction, catalyst_type, cluster_id,
                    block_code, block_reason, conviction,
                    open_positions, daily_pnl_pct, sector_pct,
                    catalyst_count, hourly_orders
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            """,
                news_id, ticker, direction, catalyst_type, cluster_id,
                code, reason, conviction,
                open_positions, daily_pnl_pct, sector_pct,
                catalyst_count, hourly_orders,
            )
            await conn.close()
        except Exception as e:
            _log("warning", "portfolio_risk.log_write_error", error=str(e))


# ── Private helpers ───────────────────────────────────────────────────────────

def _position_sector(position) -> str:
    """
    Extract sector from a Position object.  Positions from the broker API
    do not carry sector metadata directly; the risk manager stores sector
    in the positions cache when a signal is approved (via enriched record).
    Falls back to empty string if unavailable.
    """
    return getattr(position, "sector", "") or ""


def _position_direction(position) -> str:
    """Return 'long' or 'short' from Position.qty sign."""
    qty = getattr(position, "qty", 0)
    return "long" if qty >= 0 else "short"


def _catalyst_positions_count(open_positions: list, catalyst_type: str) -> int:
    """Count open positions with matching catalyst_type attribute."""
    return sum(
        1 for p in open_positions
        if getattr(p, "catalyst_type", None) == catalyst_type
    )
