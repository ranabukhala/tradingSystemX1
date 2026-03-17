"""
Pre-Trade Filter — orchestrates all 4 signal quality checks.

Sits between signal aggregator and execution engine.
Consumes:  signals.actionable
Emits:     signals.filtered  (passed signals, with updated conviction)
           signals.blocked   (rejected signals, with reason — for review)

Pipeline position:
  news.raw → ... → signals.actionable → [PRE-TRADE FILTER] → signals.filtered → execution_engine

Four filters run in parallel for every signal:
  1. Regime     — is the market environment safe to trade?
  2. Technicals — does price structure confirm the news direction?
  3. Options    — is smart money positioned in the same direction?
  4. Short      — is this a squeeze setup (adjust sizing + signal type)?

Final conviction formula (v1.9 — catalyst-policy-aware):
  conviction_final = conviction_original
    × effective_regime_scale          (regime.conviction_scale × mktcap_regime_adj)
    × technicals.conviction_multiplier
    × (1 + effective_options_delta)   (options_delta × options_weight × mktcap_options_scale)
    × squeeze.conviction_multiplier
    × policy_multiplier               (interpretive_penalty × sympathy_multiplier)

Hard blocks (conviction → 0, signal dropped):
  - regime.block_longs/block_shorts active
  - technicals.blocked (score < -0.55 vs direction)
  - catalyst_policy tech_score gate / volume gate (v1.9)

Soft adjustments (conviction scaled, signal passes):
  - Everything else

All decisions written to signal_log (filter_result, final_conviction columns).
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Optional

import httpx
import redis.asyncio as aioredis

from app.config import settings as _settings
from app.filters.catalyst_policy import (
    CatalystPolicy,
    PolicyResult,
    build_policy_table,
    get_policy,
    apply_policy,
    _build_mktcap_options_scale,
    _build_mktcap_regime_adj,
)
from app.filters.regime import get_current_regime
from app.filters.technicals import score_technicals
from app.filters.options_flow import score_options_flow
from app.filters.short_interest import score_squeeze
from app.pipeline.base_consumer import BaseConsumer, _log
from app.pipeline.staleness import staleness_from_signal_dict

# Stock context service — provides per-ticker adjusted_threshold
# Import lazily inside process() to avoid hard dependency at module load time.
_CONTEXT_SERVICE_URL = os.environ.get(
    "CONTEXT_SERVICE_URL", "http://stock_context_service:8082"
)
POLYGON_BASE = "https://api.polygon.io"


class PreTradeFilterService(BaseConsumer):
    """
    Kafka consumer: signals.actionable → signals.filtered / signals.blocked
    """

    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None
        self._http: httpx.AsyncClient | None = None
        self._polygon_key = os.environ.get("POLYGON_API_KEY", "")
        # Build catalyst policy table from settings at startup (v1.9)
        self._policy_table: dict[str, CatalystPolicy] = build_policy_table(_settings)
        self._mktcap_options_scale: dict[str, float] = _build_mktcap_options_scale(_settings)
        self._mktcap_regime_adj: dict[str, float] = _build_mktcap_regime_adj(_settings)
        super().__init__()

    @property
    def service_name(self) -> str:
        return "pretrade_filter"

    @property
    def input_topic(self) -> str:
        return "signals.actionable"

    @property
    def output_topic(self) -> str:
        return "signals.filtered"

    async def on_start(self) -> None:
        self._redis = await aioredis.from_url(
            os.environ.get("REDIS_URL", "redis://redis:6379/0"),
            decode_responses=True,
        )
        self._http = httpx.AsyncClient(timeout=10.0)

        # Warn loudly if critical env vars are missing — silent failures waste signals
        if not self._polygon_key:
            _log("error", "pretrade_filter.missing_env",
                 var="POLYGON_API_KEY",
                 impact="Technicals filter will return score=0 for all signals")
        if not os.environ.get("UNUSUAL_WHALES_API_KEY"):
            _log("warning", "pretrade_filter.missing_env",
                 var="UNUSUAL_WHALES_API_KEY",
                 impact="Options flow will fall back to Tradier/Polygon")
        if not os.environ.get("TRADIER_API_KEY"):
            _log("warning", "pretrade_filter.missing_env",
                 var="TRADIER_API_KEY",
                 impact="Options flow will fall back to Polygon only")
        # IEX Cloud shut down Aug 2024. Squeeze filter uses Polygon as sole source.
        # No env var check needed.

        _log("info", "pretrade_filter.ready")

    async def on_stop(self) -> None:
        if self._http:
            await self._http.aclose()
        if self._redis:
            await self._redis.aclose()

    async def _get_stock_context(self, ticker: str) -> Optional[dict]:
        """
        Return the cached StockContext dict for *ticker* from Redis.

        Falls back to a forced HTTP refresh via the context service if
        the cache is cold.  Returns None on failure (graceful degradation).
        """
        # 1. Try Redis cache first (same key written by stock_context_service)
        try:
            raw = await self._redis.get(f"stock_context:{ticker}")
            if raw:
                return json.loads(raw)
        except Exception as exc:
            _log("warning", "pretrade_filter.context_redis_error",
                 ticker=ticker, error=str(exc))

        # 2. Cache miss — ask the context service to reclassify immediately
        try:
            url = f"{_CONTEXT_SERVICE_URL}/context/{ticker}/refresh"
            resp = await self._http.get(url, timeout=4.0)
            if resp.status_code == 200:
                ctx = resp.json()
                _log("debug", "pretrade_filter.context_refreshed",
                     ticker=ticker,
                     threshold=ctx.get("adjusted_threshold"))
                return ctx
            _log("warning", "pretrade_filter.context_refresh_failed",
                 ticker=ticker, status=resp.status_code)
        except Exception as exc:
            _log("warning", "pretrade_filter.context_service_unreachable",
                 ticker=ticker, error=str(exc))

        return None

    async def process(self, record: dict) -> dict | None:
        ticker    = record.get("ticker", "")
        direction = record.get("direction", "neutral")
        conviction_in = float(record.get("conviction", 0.0))

        if direction == "neutral":
            return None

        # ── Staleness guard — reject old signals before any I/O ───────────────
        # This runs before fetching stock context / regime / technicals to avoid
        # wasting network calls on signals the market has already absorbed.
        stale = staleness_from_signal_dict(record)
        if stale.is_stale:
            _log("info", "pretrade_filter.staleness_blocked",
                 ticker=ticker,
                 direction=direction,
                 catalyst_type=stale.catalyst_type,
                 session=stale.session,
                 route_type=stale.route_type,
                 age_seconds=stale.age_seconds,
                 max_age_seconds=stale.max_age_seconds,
                 reason=stale.reason)
            await self._log_staleness_block(record, stale)
            blocked_record = {
                **record,
                "blocked": True,
                "block_reason": stale.reason,
                "signal_age_seconds": stale.age_seconds,
                "max_age_seconds": stale.max_age_seconds,
            }
            await self._emit_blocked(blocked_record)
            return None
        # ─────────────────────────────────────────────────────────────────────

        # ── Fetch stock context (regime / cleanliness / adjusted threshold) ───
        stock_ctx = await self._get_stock_context(ticker)
        if stock_ctx is None:
            _log("warning", "pretrade_filter.no_context",
                 ticker=ticker,
                 fallback="using static TECHNICAL_SCORE_THRESHOLD")
        else:
            _log("debug", "pretrade_filter.context_loaded",
                 ticker=ticker,
                 trend=stock_ctx.get("trend_regime"),
                 vol=stock_ctx.get("volatility_regime"),
                 clean=stock_ctx.get("cleanliness"),
                 threshold=stock_ctx.get("adjusted_threshold"))

        # ── Run all 4 filters in parallel ─────────────────────────────────────
        regime_task = asyncio.create_task(
            get_current_regime(self._redis)
        )
        tech_task = asyncio.create_task(
            score_technicals(ticker, direction, self._redis, self._http, self._polygon_key)
        )
        options_task = asyncio.create_task(
            score_options_flow(ticker, direction, self._redis, self._http)
        )
        squeeze_task = asyncio.create_task(
            score_squeeze(ticker, direction, self._redis, self._http)
        )

        regime, tech, options, squeeze = await asyncio.gather(
            regime_task, tech_task, options_task, squeeze_task,
            return_exceptions=True,
        )

        # Handle partial failures gracefully — never let a filter crash the pipeline
        if isinstance(regime, Exception):
            _log("warning", "pretrade_filter.regime_error", error=str(regime))
            regime = {"conviction_scale": 1.0, "block_longs": False, "block_shorts": False, "regime": "unknown"}
        if isinstance(tech, Exception):
            _log("warning", "pretrade_filter.tech_error", error=str(tech))
            from app.filters.technicals import TechnicalScore
            tech = TechnicalScore(ticker=ticker, direction=direction)
        if isinstance(options, Exception):
            _log("warning", "pretrade_filter.options_error", error=str(options))
            from app.filters.options_flow import OptionsFlowResult
            options = OptionsFlowResult(ticker=ticker, direction=direction)
        if isinstance(squeeze, Exception):
            _log("warning", "pretrade_filter.squeeze_error", error=str(squeeze))
            from app.filters.short_interest import SqueezeResult
            squeeze = SqueezeResult(ticker=ticker, direction=direction)

        # ── Hard block checks ──────────────────────────────────────────────────
        block_reason = _check_hard_blocks(direction, regime, tech)
        if block_reason:
            _log("info", "pretrade_filter.blocked",
                 ticker=ticker, direction=direction,
                 conviction_in=conviction_in,
                 reason=block_reason)
            await self._update_signal_log(record, conviction_in, 0.0,
                                          blocked=True, reason=block_reason,
                                          regime=regime, tech=tech,
                                          options=options, squeeze=squeeze)
            # Emit to blocked topic for review
            blocked_record = {**record, "blocked": True, "block_reason": block_reason}
            await self._emit_blocked(blocked_record)
            return None

        # ── Context-adjusted technical score threshold ────────────────────────
        # The static TECHNICAL_SCORE_THRESHOLD env var is the system default.
        # When stock_context_service is available it supplies a ticker-specific
        # threshold (7, 8, or 9) based on trend regime and price cleanliness.
        # We re-evaluate the block decision here — without touching technicals.py.
        tech_score_val = getattr(tech, "technical_score", None)
        if stock_ctx is not None and tech_score_val is not None:
            ctx_threshold = int(stock_ctx.get("adjusted_threshold", 8))
            if tech_score_val < ctx_threshold and not getattr(tech, "blocked", False):
                # Context requires a higher bar than the static env var
                ctx_block_reason = (
                    f"Context threshold {ctx_threshold}/10 not met "
                    f"(score={tech_score_val}, "
                    f"trend={stock_ctx.get('trend_regime')}, "
                    f"cleanliness={stock_ctx.get('cleanliness')})"
                )
                _log("info", "pretrade_filter.context_threshold_block",
                     ticker=ticker, direction=direction,
                     tech_score=tech_score_val,
                     ctx_threshold=ctx_threshold,
                     trend=stock_ctx.get("trend_regime"),
                     cleanliness=stock_ctx.get("cleanliness"))
                await self._update_signal_log(record, conviction_in, 0.0,
                                              blocked=True, reason=ctx_block_reason,
                                              regime=regime, tech=tech,
                                              options=options, squeeze=squeeze)
                blocked_record = {
                    **record,
                    "blocked": True,
                    "block_reason": ctx_block_reason,
                }
                await self._emit_blocked(blocked_record)
                return None
            elif tech_score_val >= ctx_threshold and getattr(tech, "blocked", False):
                # Context permits a lower bar than the static env var — unblock
                _log("info", "pretrade_filter.context_threshold_unblock",
                     ticker=ticker, direction=direction,
                     tech_score=tech_score_val,
                     ctx_threshold=ctx_threshold,
                     trend=stock_ctx.get("trend_regime"))
                tech.blocked      = False
                tech.block_reason = ""

        # ── Catalyst policy gate (v1.9) ───────────────────────────────────────
        # Runs AFTER regime/tech hard blocks so regime gates always fire first.
        # Determines per-catalyst threshold, multipliers, and adjusted deltas.
        catalyst_type    = record.get("catalyst_type", "other")
        is_sympathy      = bool(record.get("is_sympathy", False))
        mktcap_tier      = record.get("market_cap_tier")
        tech_raw_score   = getattr(tech, "technical_score", 0)
        raw_options_delta = getattr(options, "conviction_delta", 0.0)

        if _settings.enable_catalyst_policy:
            policy = get_policy(self._policy_table, catalyst_type, is_sympathy)
            policy_result = apply_policy(
                policy,
                signal=record,
                tech_raw_score=tech_raw_score,
                options_delta=raw_options_delta,
                regime_scale=regime.get("conviction_scale", 1.0),
                mktcap_tier=mktcap_tier,
                sympathy_threshold_bump=_settings.policy_sympathy_threshold_bump,
                mktcap_options_scale=self._mktcap_options_scale,
                mktcap_regime_adj=self._mktcap_regime_adj,
            )
            if policy_result.block:
                _log("info", "pretrade_filter.policy_blocked",
                     ticker=ticker, direction=direction,
                     catalyst_type=catalyst_type,
                     conviction_in=conviction_in,
                     reason=policy_result.block_reason)
                await self._update_signal_log(
                    record, conviction_in, 0.0,
                    blocked=True, reason=policy_result.block_reason,
                    regime=regime, tech=tech, options=options, squeeze=squeeze,
                    policy_result=policy_result,
                )
                blocked_record = {
                    **record,
                    "blocked": True,
                    "block_reason": policy_result.block_reason,
                }
                await self._emit_blocked(blocked_record)
                return None

            effective_regime_scale  = policy_result.adjusted_regime_scale
            effective_options_delta = policy_result.adjusted_options_delta
            policy_multiplier       = policy_result.multiplier
            effective_threshold     = policy_result.threshold
        else:
            # Kill-switch path — preserve pre-v1.9 behaviour exactly
            effective_regime_scale  = regime.get("conviction_scale", 1.0)
            effective_options_delta = raw_options_delta
            policy_multiplier       = 1.0
            effective_threshold     = float(os.environ.get("SIGNAL_CONVICTION_THRESHOLD", "0.55"))
            policy_result           = PolicyResult(
                adjusted_options_delta=raw_options_delta,
                adjusted_regime_scale=effective_regime_scale,
                threshold=effective_threshold,
            )

        # ── Compute final conviction ───────────────────────────────────────────
        regime_name  = regime.get("regime", "risk_on")
        is_systemic  = regime_name in ("risk_off", "high_vol")

        conviction = conviction_in
        conviction *= effective_regime_scale

        # Technical multiplier: in a systemic sell-off stocks are below their MAs
        # market-wide — that's the same information already in regime.conviction_scale.
        # For LONG signals floor the tech multiplier at 1.0 so we don't double-count.
        # For SHORT signals the sub-MA reading confirms direction, so leave it as-is.
        tech_mult = getattr(tech, "conviction_multiplier", 1.0)
        if is_systemic and direction == "long" and tech_mult < 1.0:
            tech_mult = 1.0
        conviction *= tech_mult

        # Options delta: broad bearish flow in risk_off / high_vol is systemic noise,
        # not a stock-specific signal.  Neutralise the negative delta for LONG signals
        # only; SHORT signals get to keep the confirming bearish flow.
        options_delta = effective_options_delta
        if is_systemic and direction == "long" and options_delta < 0:
            options_delta = 0.0
        conviction *= (1.0 + options_delta)

        conviction *= getattr(squeeze, "conviction_multiplier", 1.0)

        # Policy multiplier (interpretive_penalty × sympathy_multiplier) applied last.
        conviction *= policy_multiplier

        conviction = round(min(max(conviction, 0.0), 1.0), 3)

        # ── Post-formula conviction threshold gate (v1.9) ─────────────────────
        # The signal_aggregator threshold was set before policy adjustments were
        # applied.  After policy multipliers (interpretive_penalty, sympathy_mult)
        # and mktcap-adjusted options/regime scalings, the adjusted conviction may
        # fall below the per-catalyst effective threshold.  Block here so that
        # downstream execution engine never sees sub-threshold signals.
        if conviction < effective_threshold:
            threshold_block_reason = (
                f"Post-policy conviction {conviction:.3f} < "
                f"effective_threshold {effective_threshold:.3f} "
                f"(catalyst={catalyst_type}, "
                f"policy_mult={policy_multiplier:.3f})"
            )
            _log("info", "pretrade_filter.threshold_blocked",
                 ticker=ticker, direction=direction,
                 conviction_in=conviction_in,
                 conviction_out=conviction,
                 effective_threshold=effective_threshold,
                 catalyst_type=catalyst_type)
            await self._update_signal_log(
                record, conviction_in, conviction,
                blocked=True, reason=threshold_block_reason,
                regime=regime, tech=tech, options=options, squeeze=squeeze,
                policy_result=policy_result,
            )
            blocked_record = {
                **record,
                "blocked": True,
                "block_reason": threshold_block_reason,
                "conviction": conviction,
            }
            await self._emit_blocked(blocked_record)
            return None

        # ── Enrich record ──────────────────────────────────────────────────────
        record["conviction_original"] = conviction_in
        record["conviction"] = conviction
        record["conviction_delta"] = round(conviction - conviction_in, 3)

        # Policy debug fields (v1.9) — enables research queries on policy impact
        record["catalyst_policy_applied"]  = _settings.enable_catalyst_policy
        record["policy_threshold_used"]    = effective_threshold
        record["policy_block_reason"]      = None
        record["policy_multiplier"]        = round(policy_multiplier, 4)
        record["policy_options_weight"]    = round(policy_result.options_weight_effective, 4)
        record["policy_regime_adj"]        = round(policy_result.regime_adj_effective, 4)

        record["filter_regime"] = {
            "regime":      regime.get("regime"),
            "vix":         regime.get("vix"),
            "spy_chg_pct": regime.get("spy_chg_pct"),
            "scale":       regime.get("conviction_scale"),
        }
        record["filter_technicals"] = {
            "score": getattr(tech, "score", None),
            "multiplier": getattr(tech, "conviction_multiplier", 1.0),
            "checks": getattr(tech, "checks", {}),
            "rsi": getattr(tech, "rsi", None),
            # ↓ promote these so formatter can access them directly
            "technical_score": getattr(tech, "technical_score", 0),
            "technical_score_breakdown": getattr(tech, "technical_score_breakdown", {}),
        }
        record["filter_options"] = {
            "flow_bias":   getattr(options, "flow_bias", "neutral"),
            "pcr":         getattr(options, "put_call_ratio", None),
            "delta":       getattr(options, "conviction_delta", 0.0),
            "notes":       getattr(options, "notes", ""),
        }
        record["filter_squeeze"] = {
            "score":       getattr(squeeze, "squeeze_score", 0.0),
            "short_float": getattr(squeeze, "short_float_pct", None),
            "dtc":         getattr(squeeze, "days_to_cover", None),
            "multiplier":  getattr(squeeze, "conviction_multiplier", 1.0),
        }

        # Promote signal type if squeeze candidate
        if getattr(squeeze, "signal_type_override", None):
            record["signal_type"] = squeeze.signal_type_override

        # ── Attach stock context fields to the outbound record ────────────────
        # These flow through to signals.filtered → execution_engine → signal_log.
        if stock_ctx is not None:
            record["trend_regime"]       = stock_ctx.get("trend_regime")
            record["volatility_regime"]  = stock_ctx.get("volatility_regime")
            record["cleanliness"]        = stock_ctx.get("cleanliness")
            record["adjusted_threshold"] = stock_ctx.get("adjusted_threshold")
        else:
            record["trend_regime"]       = None
            record["volatility_regime"]  = None
            record["cleanliness"]        = None
            record["adjusted_threshold"] = None

        _log("info", "pretrade_filter.passed",
             ticker=ticker, direction=direction,
             conviction_in=conviction_in,
             conviction_out=conviction,
             regime=regime.get("regime"),
             tech_score=getattr(tech, "score", None),
             flow_bias=getattr(options, "flow_bias", "neutral"),
             squeeze_score=getattr(squeeze, "squeeze_score", 0.0),
             policy_multiplier=policy_multiplier,
             policy_threshold=effective_threshold)

        await self._update_signal_log(record, conviction_in, conviction,
                                      blocked=False, reason="",
                                      regime=regime, tech=tech,
                                      options=options, squeeze=squeeze,
                                      policy_result=policy_result)
        return record

    async def _emit_blocked(self, record: dict) -> None:
        """Emit blocked signals to signals.blocked topic for review."""
        try:
            from app.kafka import get_producer
            producer = get_producer()
            producer.produce(
                topic="signals.blocked",
                value=record,
                key=record.get("ticker", ""),
            )
        except Exception as e:
            _log("warning", "pretrade_filter.blocked_emit_error", error=str(e))

    async def _update_signal_log(
        self, record: dict, conviction_in: float, conviction_out: float,
        blocked: bool, reason: str, regime, tech, options, squeeze,
        policy_result: "PolicyResult | None" = None,
    ) -> None:
        """Update the signal_log row (written by signal_aggregator) with filter results."""
        try:
            import asyncpg
            dsn = os.environ.get("DATABASE_URL",
                "postgresql://trading:trading@postgres:5432/trading_db")
            conn = await asyncpg.connect(dsn)
            await conn.execute("""
                UPDATE signal_log SET
                    conviction               = $1,
                    filter_blocked           = $2,
                    filter_block_reason      = $3,
                    filter_regime            = $4,
                    filter_tech_score        = $5,
                    filter_flow_bias         = $6,
                    filter_squeeze_score     = $7,
                    catalyst_policy_applied  = $8,
                    policy_threshold_used    = $9,
                    policy_block_reason      = $10,
                    policy_multiplier        = $11,
                    policy_options_weight    = $12,
                    policy_regime_adj        = $13
                WHERE news_id = $14
                  AND ticker  = $15
                  AND created_at > now() - interval '10 minutes'
            """,
                conviction_out, blocked, reason,
                regime.get("regime") if isinstance(regime, dict) else None,
                getattr(tech, "score", None),
                getattr(options, "flow_bias", None),
                getattr(squeeze, "squeeze_score", None),
                _settings.enable_catalyst_policy,
                policy_result.threshold if policy_result else None,
                policy_result.block_reason if (policy_result and policy_result.block) else None,
                policy_result.multiplier if policy_result else None,
                policy_result.options_weight_effective if policy_result else None,
                policy_result.regime_adj_effective if policy_result else None,
                record.get("news_id"),
                record.get("ticker"),
            )
            await conn.close()
        except Exception as e:
            _log("warning", "pretrade_filter.log_update_error", error=str(e))

    async def _log_staleness_block(self, record: dict, stale) -> None:
        """Write a row to staleness_log and update signal_log for staleness blocks."""
        try:
            import asyncpg
            dsn = os.environ.get("DATABASE_URL",
                "postgresql://trading:trading@postgres:5432/trading_db")
            dsn = dsn.replace("postgresql+asyncpg://", "postgresql://")
            conn = await asyncpg.connect(dsn)

            # Immutable audit entry
            # news_published_at arrives as an ISO-8601 string from Kafka;
            # asyncpg requires a datetime object for TIMESTAMPTZ columns.
            raw_published_at = record.get("news_published_at")
            if isinstance(raw_published_at, str):
                raw_published_at = datetime.fromisoformat(
                    raw_published_at.replace("Z", "+00:00")
                )

            await conn.execute("""
                INSERT INTO staleness_log (
                    source, ticker, catalyst_type, session_context, route_type,
                    signal_age_s, max_age_s, reason, news_published_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """,
                "pretrade_filter",
                record.get("ticker", ""),
                stale.catalyst_type,
                stale.session,
                stale.route_type,
                stale.age_seconds,
                stale.max_age_seconds,
                stale.reason,
                raw_published_at,
            )

            # Update matching signal_log row so dashboards see the block
            await conn.execute("""
                UPDATE signal_log SET
                    staleness_blocked  = TRUE,
                    staleness_reason   = $1,
                    signal_age_seconds = $2,
                    max_age_seconds    = $3,
                    filter_blocked     = TRUE,
                    filter_block_reason= $1
                WHERE news_id = $4
                  AND ticker  = $5
                  AND created_at > now() - interval '10 minutes'
            """,
                stale.reason,
                stale.age_seconds,
                stale.max_age_seconds,
                record.get("news_id"),
                record.get("ticker", ""),
            )
            await conn.close()
        except Exception as e:
            _log("warning", "pretrade_filter.staleness_log_error", error=str(e))


def _check_hard_blocks(direction: str, regime: dict, tech) -> str:
    """
    Returns a non-empty reason string if the trade should be hard-blocked.
    Returns "" if trade should proceed.
    """
    # Regime hard blocks
    if direction == "long" and regime.get("block_longs"):
        vix = regime.get("vix", 0)
        spy = regime.get("spy_chg_pct", 0)
        return f"Regime block: longs disabled (VIX={vix:.0f}, SPY={spy:.1f}%)"

    if direction == "short" and regime.get("block_shorts"):
        spy = regime.get("spy_chg_pct", 0)
        return f"Regime block: shorts disabled (SPY ripping +{spy:.1f}%)"

    # Technical hard block
    if getattr(tech, "blocked", False):
        return getattr(tech, "block_reason", "Technical setup opposes direction")

    return ""
