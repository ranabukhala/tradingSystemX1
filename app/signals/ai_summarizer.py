"""
AI Summarizer Service — Stage 4 of the pipeline.

Consumes: news.enriched
Emits:    news.summarized

Two-tier summarization:
  T1 — fast facts extraction, runs on ALL items, ~100 tokens
  T2 — trader intelligence, runs only when impact_day >= threshold, ~300 tokens

Cost controls:
  - Daily budget cap (ANTHROPIC_DAILY_BUDGET_USD)
  - T2 gated by impact_day score
  - Non-representative items skipped (duplicates already summarized via cluster)
  - Rate limiting with token bucket
"""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from app.config import settings
from app.models.news import (
    CatalystType, EnrichedRecord, FactsJson,
    RegimeFlag, SummarizedRecord,
)
from app.pipeline.base_consumer import BaseConsumer, _log
from app.pipeline.route_classifier import classify_route
from app.pipeline.fast_path_builder import build_fast_path_summary
from app.signals.budget import LLMBudgetTracker
from app.signals.prompts import (
    PROMPT_VERSION, T1_SYSTEM, T1_USER, T2_SYSTEM, T2_USER,
)

CLAUDE_MODEL = "claude-haiku-4-5-20251001"   # Fast + cheap for T1
CLAUDE_MODEL_T2 = "claude-sonnet-4-6"        # Smarter for T2 trader analysis

# Approximate cost per 1K tokens (input+output blended)
COST_PER_1K_T1 = 0.001    # Haiku
COST_PER_1K_T2 = 0.015    # Sonnet

# ── Prometheus: route selection counter ──────────────────────────────────────
# Labels: route_type ("fast"|"slow"), catalyst_type (from CatalystType.value)
try:
    from prometheus_client import Counter as _PCounter
    _ROUTE_COUNTER = _PCounter(
        "ai_summarizer_route_total",
        "Count of fast vs slow path route selections by catalyst type",
        ["route_type", "catalyst_type"],
    )
except Exception:  # prometheus_client absent or registry conflict in tests
    class _NoopCounter:  # type: ignore[no-redef]
        def labels(self, **_kw):
            return self
        def inc(self) -> None:
            pass
    _ROUTE_COUNTER = _NoopCounter()  # type: ignore[assignment]


class AISummarizerService(BaseConsumer):

    def __init__(self) -> None:
        # Budget tracker initialized in on_start() once Redis is available
        self._budget: LLMBudgetTracker | None = None
        self._http: httpx.AsyncClient | None = None
        self._Session: Any | None = None
        # Simple rate limit: max 50 req/min to Claude API
        self._req_times: list[float] = []
        super().__init__()

    @property
    def service_name(self) -> str:
        return "ai_summarizer"

    @property
    def input_topic(self) -> str:
        return settings.topic_news_fmp_enriched

    @property
    def output_topic(self) -> str:
        return settings.topic_news_summarized

    async def on_start(self) -> None:
        if not settings.anthropic_api_key:
            raise ValueError("ANTHROPIC_API_KEY not set — required for Phase 3")
        self._http = httpx.AsyncClient(
            base_url="https://api.anthropic.com",
            headers={
                "x-api-key": settings.anthropic_api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            timeout=30.0,
        )
        # DB session — used to back-fill tickers on news_item rows after LLM extraction
        import os
        if os.environ.get("DATABASE_URL"):
            try:
                from app.db import get_engine
                from sqlalchemy.ext.asyncio import AsyncSession
                from sqlalchemy.orm import sessionmaker
                engine = get_engine()
                self._Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
                _log("info", "ai_summarizer.db_session_ready")
            except Exception as db_err:
                _log("warning", "ai_summarizer.db_session_failed", error=str(db_err))
        else:
            _log("warning", "ai_summarizer.no_database_url",
                 note="LLM ticker DB back-fill disabled")

        # ── Budget tracker ────────────────────────────────────────────────────
        import redis.asyncio as _budget_redis
        _redis_client = _budget_redis.from_url(
            settings.redis_url,
            decode_responses=True,
            max_connections=3,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        self._budget = LLMBudgetTracker(
            redis_client=_redis_client,
            daily_limit=settings.llm_daily_budget_usd,
            monthly_limit=getattr(settings, "llm_monthly_budget_usd", 500.0),
        )
        _log("info", "ai_summarizer.budget_tracker_ready",
             daily_limit=settings.llm_daily_budget_usd,
             monthly_limit=getattr(settings, "llm_monthly_budget_usd", 500.0))

        _log("info", "ai_summarizer.ready",
             model_t1=CLAUDE_MODEL,
             model_t2=CLAUDE_MODEL_T2,
             daily_budget=settings.llm_daily_budget_usd)

    async def on_stop(self) -> None:
        if self._http:
            await self._http.aclose()

    async def _rate_limit(self) -> None:
        """Allow max 50 requests per minute."""
        now = time.time()
        self._req_times = [t for t in self._req_times if now - t < 60]
        if len(self._req_times) >= 50:
            wait = 60 - (now - self._req_times[0])
            if wait > 0:
                _log("debug", "ai_summarizer.rate_limit_wait", seconds=round(wait, 1))
                await asyncio.sleep(wait)
        self._req_times.append(time.time())

    async def _call_claude(
        self,
        system: str,
        user: str,
        model: str,
        max_tokens: int = 500,
    ) -> tuple[str, int]:
        """
        Call Claude API. Returns (response_text, tokens_used).
        Raises on API error.
        """
        await self._rate_limit()

        payload = {
            "model": model,
            "max_tokens": max_tokens,
            "system": system,
            "messages": [{"role": "user", "content": user}],
        }

        resp = await self._http.post("/v1/messages", json=payload)

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("retry-after", 30))
            _log("warning", "ai_summarizer.rate_limited", retry_after=retry_after)
            await asyncio.sleep(retry_after)
            raise ConnectionError("Claude API rate limited")

        if resp.status_code != 200:
            raise ConnectionError(
                f"Claude API error {resp.status_code}: {resp.text[:200]}"
            )

        data = resp.json()
        text = data["content"][0]["text"]
        tokens = data["usage"]["input_tokens"] + data["usage"]["output_tokens"]
        return text, tokens

    def _parse_json_response(self, text: str) -> dict:
        """Safely parse JSON from LLM response, strip markdown if present."""
        text = text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        text = text.strip()
        return json.loads(text)

    async def _run_t1(self, record: EnrichedRecord) -> tuple[dict, int, float]:
        """
        Run T1 summarization. Returns (parsed_dict, tokens, cost_usd).
        """
        user_prompt = T1_USER.format(
            title=record.title,
            snippet=record.snippet or "",
            tickers=", ".join(record.tickers) if record.tickers else "unknown",
            catalyst_type=record.catalyst_type.value,
        )

        text, tokens = await self._call_claude(
            system=T1_SYSTEM,
            user=user_prompt,
            model=CLAUDE_MODEL,
            max_tokens=400,
        )

        parsed = self._parse_json_response(text)
        cost = (tokens / 1000) * COST_PER_1K_T1
        return parsed, tokens, cost

    async def _get_fmp_context(self, record: EnrichedRecord, raw_record: dict) -> dict:
        """Pull FMP context from record fields and Redis."""
        ctx = {
            "float_shares": raw_record.get("float_shares", "unknown"),
            "float_sensitivity": raw_record.get("float_sensitivity",
                record.float_sensitivity.value if record.float_sensitivity else "normal"),
            "beta": raw_record.get("fmp_beta", "unknown"),
            "sector": raw_record.get("fmp_sector", "unknown"),
            "market_cap_tier": raw_record.get("market_cap_tier",
                record.market_cap_tier.value if record.market_cap_tier else "unknown"),
            "sector_return": "unknown",
            "analyst_context": "",
            "insider_context": "",
            "technical_context": "",
            "quote_context": "",
        }

        try:
            import redis.asyncio as aioredis, os
            r = await aioredis.from_url(
                os.environ.get("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)

            # Sector return
            sectors_raw = await r.get("fmp:sectors")
            if sectors_raw:
                sd = json.loads(sectors_raw)
                ctx["sector_return"] = sd.get("sectors", {}).get(ctx["sector"], "unknown")

            # Technical indicators
            ticker = record.tickers[0] if record.tickers else ""
            if ticker:
                tech_raw = await r.get(f"fmp:technical:{ticker}")
                if tech_raw:
                    tech = json.loads(tech_raw)
                    rsi = tech.get("rsi", "")
                    ctx["technical_context"] = (
                        f"RSI: {rsi} ({tech.get('rsi_signal','')}) | "
                        f"MACD: {tech.get('macd_bias','')}\n"
                    )
            await r.aclose()
        except Exception:
            pass

        # Analyst context
        analyst = raw_record.get("fmp_analyst")
        if analyst:
            avg_pt = analyst.get("avg_price_target", "")
            num = analyst.get("num_analysts", "")
            sentiment = analyst.get("analyst_sentiment", 0)
            sentiment_str = "bullish" if sentiment > 0 else "bearish" if sentiment < 0 else "neutral"
            ctx["analyst_context"] = (
                f"Analyst: avg PT ${avg_pt}, {num} analysts, sentiment: {sentiment_str}\n"
            )
            # FMP composite rating (ratings-snapshot — replaced grades-latest Aug 2025)
            fmp_rating = analyst.get("fmp_rating", "")
            fmp_score  = analyst.get("fmp_overall_score", 0)
            if fmp_rating:
                ctx["analyst_context"] += (
                    f"FMP rating: {fmp_rating} (score {fmp_score}/5 — "
                    f"DCF:{analyst.get('fmp_dcf_score',0)} "
                    f"ROE:{analyst.get('fmp_roe_score',0)} "
                    f"P/E:{analyst.get('fmp_pe_score',0)})\n"
                )
            # recent_ratings: populated by old grades-latest endpoint (removed Aug 2025).
            # Kept for backward compat; will be empty list going forward.
            recent = analyst.get("recent_ratings", [])
            if recent:
                ratings_str = " | ".join(
                    f"{r['firm']} {r.get('action','')} ({r.get('from_grade','')}→{r.get('to_grade','')})"
                    for r in recent[:3]
                )
                ctx["analyst_context"] += f"Recent grades: {ratings_str}\n"

        # Insider context
        insider = raw_record.get("fmp_insider")
        if insider and insider.get("notable"):
            ctx["insider_context"] = (
                f"Insiders (30d): buys ${insider.get('total_buy_value',0):,.0f} "
                f"sells ${insider.get('total_sell_value',0):,.0f} "
                f"net: {insider.get('net_sentiment','neutral')}\n"
            )

        # Quote / technical setup context
        quote = raw_record.get("fmp_quote")
        if quote:
            gap_pct     = quote.get("gap_pct", 0)
            vol_ratio   = quote.get("vol_ratio", 0)
            w52_pos     = quote.get("week52_position", 0.5)
            price       = quote.get("price", 0)
            prev_close  = quote.get("prev_close", 0)
            week52_high = quote.get("week52_high", 0)
            week52_low  = quote.get("week52_low", 0)
            avg_vol     = quote.get("avg_volume", 0)

            gap_label = "significant (>4%)" if abs(gap_pct) > 4 else (
                        "moderate (2-4%)" if abs(gap_pct) > 2 else "small (<2%)")
            vol_label = "very strong (>3x avg)" if vol_ratio > 3 else (
                        "strong (>2x avg)" if vol_ratio > 2 else (
                        "moderate (1-2x avg)" if vol_ratio > 1 else "weak (<avg)"))
            near_52w_high = w52_pos > 0.90
            near_52w_low  = w52_pos < 0.10
            w52_flag = " NEAR 52W HIGH" if near_52w_high else (" NEAR 52W LOW" if near_52w_low else "")

            line1 = "Price: ${} | Prev close: ${} | Gap: {:+.1f}% ({})".format(
                price, prev_close, gap_pct, gap_label)
            line2 = "52w range: ${}-${} | Position: {:.0%} of range{}".format(
                week52_low, week52_high, w52_pos, w52_flag)
            line3 = "Volume: {:.1f}x avg ({}) | Avg daily vol: {:,}".format(
                vol_ratio, vol_label, avg_vol)

            ctx["quote_context"] = "\n".join([line1, line2, line3]) + "\n"

        return ctx

    async def _get_finnhub_context(self, ticker: str, catalyst_type: str) -> dict:
        """Pull Finnhub earnings history + analyst consensus from Redis."""
        ctx = {
            "earnings_history": "",
            "analyst_consensus": "",
            "sentiment_score": "",
        }
        try:
            import redis.asyncio as aioredis, os
            r = await aioredis.from_url(
                os.environ.get("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)

            # Earnings history — inject for earnings catalyst
            if catalyst_type == "earnings":
                raw = await r.get(f"finnhub:earnings:{ticker}")
                if raw:
                    d = json.loads(raw)
                    ctx["earnings_history"] = f"Earnings history: {d.get('summary', '')}\n"

            # Analyst consensus — inject for analyst catalyst
            if catalyst_type in ("analyst", "earnings"):
                raw = await r.get(f"finnhub:analyst:{ticker}")
                if raw:
                    d = json.loads(raw)
                    ctx["analyst_consensus"] = (
                        f"Analyst consensus: {d.get('consensus','').replace('_',' ')} "
                        f"({d.get('buy',0)} buy / {d.get('hold',0)} hold / {d.get('sell',0)} sell), "
                        f"trend: {d.get('trend','stable')}\n"
                    )

            # News sentiment score
            raw = await r.get(f"finnhub:sentiment:{ticker}")
            if raw:
                d = json.loads(raw)
                bias  = d.get("bias", "neutral")
                score = d.get("score", 0)
                ctx["sentiment_score"] = f"Finnhub NLP sentiment: {bias} (score {score:.2f})\n"

            await r.aclose()
        except Exception:
            pass
        return ctx

    async def _run_t2(self, record: EnrichedRecord, t1: dict, raw_record: dict | None = None) -> tuple[dict, int, float]:
        """
        Run T2 trader analysis with FMP fundamental context.
        Returns (parsed_dict, tokens, cost_usd).
        """
        raw = raw_record or {}
        ticker = record.tickers[0] if record.tickers else ""
        catalyst = record.catalyst_type.value

        # Gather FMP + Finnhub context in parallel
        fmp_ctx, finnhub_ctx = await asyncio.gather(
            self._get_fmp_context(record, raw),
            self._get_finnhub_context(ticker, catalyst),
        )

        float_str = (f"{fmp_ctx['float_shares']:,}" if isinstance(fmp_ctx["float_shares"], (int, float))
                     else str(fmp_ctx["float_shares"]))

        user_prompt = T2_USER.format(
            title=record.title,
            snippet=record.snippet or "",
            tickers=", ".join(record.tickers) if record.tickers else "unknown",
            catalyst_type=catalyst,
            session_context=record.session_context.value,
            t1_summary=t1.get("t1_summary", ""),
            impact_day=round(t1.get("impact_day", 0.0), 2),
            impact_swing=round(t1.get("impact_swing", 0.0), 2),
            float_shares=float_str,
            float_sensitivity=fmp_ctx["float_sensitivity"],
            market_cap_tier=fmp_ctx["market_cap_tier"],
            beta=fmp_ctx["beta"],
            sector=fmp_ctx["sector"],
            sector_return=fmp_ctx["sector_return"],
            quote_context=fmp_ctx["quote_context"],
            analyst_context=fmp_ctx["analyst_context"],
            insider_context=fmp_ctx["insider_context"],
            technical_context=fmp_ctx["technical_context"],
            earnings_history=finnhub_ctx["earnings_history"],
            analyst_consensus=finnhub_ctx["analyst_consensus"],
            sentiment_score=finnhub_ctx["sentiment_score"],
        )

        text, tokens = await self._call_claude(
            system=T2_SYSTEM,
            user=user_prompt,
            model=CLAUDE_MODEL_T2,
            max_tokens=600,
        )

        parsed = self._parse_json_response(text)
        cost = (tokens / 1000) * COST_PER_1K_T2
        return parsed, tokens, cost

    async def process(self, record: dict) -> dict | None:
        try:
            enriched = EnrichedRecord.from_kafka_dict(record)
        except Exception as e:
            _log("error", "ai_summarizer.parse_error", error=str(e))
            raise

        # Skip non-representative duplicates — they share a cluster with
        # an already-summarized item
        if not enriched.is_representative:
            _log("debug", "ai_summarizer.skip_duplicate",
                 vendor_id=enriched.vendor_id,
                 cluster_id=str(enriched.cluster_id))
            # Still emit but without AI fields — downstream can join on cluster_id
            summarized = SummarizedRecord(**enriched.model_dump())
            return summarized.to_kafka_dict()

        # ── Fast-path routing ─────────────────────────────────────────────────
        # Check whether structured vendor data is sufficient to bypass T1/T2 LLM.
        # route_classifier inspects raw_payload and known vendor fields.
        route = classify_route(enriched, raw_record=record)
        _log(
            "info", "ai_summarizer.route_selected",
            vendor_id=enriched.vendor_id,
            catalyst=enriched.catalyst_type.value,
            route_type=route.route_type,
            reason=route.reason,
            confidence=round(route.confidence, 2),
        )
        if route.is_fast:
            _ROUTE_COUNTER.labels(
                route_type="fast",
                catalyst_type=enriched.catalyst_type.value,
            ).inc()
            summarized = build_fast_path_summary(enriched, route)
            _log(
                "info", "ai_summarizer.fast_path_complete",
                vendor_id=enriched.vendor_id,
                tickers=enriched.tickers,
                reason=route.reason,
                direction=summarized.signal_bias,
                impact_day=summarized.impact_day,
            )
            return summarized.to_kafka_dict()
        # ─────────────────────────────────────────────────────────────────────

        # Budget check
        if self._budget and not await self._budget.can_spend("anthropic", 0.01):
            _log("warning", "ai_summarizer.budget_exhausted",
                 daily_limit=settings.llm_daily_budget_usd,
                 vendor_id=enriched.vendor_id)
            summarized = SummarizedRecord(**enriched.model_dump())
            return summarized.to_kafka_dict()

        total_tokens = 0
        total_cost = 0.0
        t1_result = {}
        t2_result = {}

        # ── T1: Always run ────────────────────────────────────────────────────
        try:
            t1_result, t1_tokens, t1_cost = await self._run_t1(enriched)
            total_tokens += t1_tokens
            total_cost += t1_cost
            if self._budget:
                await self._budget.record("anthropic", t1_cost)

            _log("info", "ai_summarizer.t1_complete",
                 vendor_id=enriched.vendor_id,
                 tickers=enriched.tickers,
                 impact_day=t1_result.get("impact_day"),
                 impact_swing=t1_result.get("impact_swing"),
                 tokens=t1_tokens,
                 cost_usd=round(t1_cost, 5))

            # ── Merge LLM-extracted tickers if entity resolver found none ──────
            llm_tickers = t1_result.get("tickers_extracted", [])
            if llm_tickers and not enriched.tickers:
                valid_llm_tickers = [
                    t.upper().strip() for t in llm_tickers
                    if isinstance(t, str) and 1 <= len(t.strip()) <= 5
                       and t.strip().isalpha()
                ][:3]  # Cap at 3

                if valid_llm_tickers:
                    enriched.tickers = valid_llm_tickers
                    _log("info", "ai_summarizer.llm_tickers_merged",
                         vendor_id=enriched.vendor_id,
                         llm_tickers=valid_llm_tickers,
                         title=enriched.title[:80])

                    # Back-fill the news_item row so dedup + future queries are consistent
                    if self._Session:
                        try:
                            from sqlalchemy import text
                            async with self._Session() as session:
                                await session.execute(text("""
                                    UPDATE news_item SET
                                        tickers = :tickers,
                                        ticker_confidence = :confidence
                                    WHERE source = :source AND vendor_id = :vendor_id
                                      AND (tickers = '{}' OR tickers IS NULL)
                                """), {
                                    "tickers": valid_llm_tickers,
                                    "confidence": json.dumps(
                                        {t: 0.80 for t in valid_llm_tickers}
                                    ),
                                    "source": enriched.source.value,
                                    "vendor_id": enriched.vendor_id,
                                })
                                await session.commit()
                        except Exception as db_err:
                            _log("warning", "ai_summarizer.llm_ticker_db_update_error",
                                 error=str(db_err))
            # ──────────────────────────────────────────────────────────────────

        except Exception as e:
            _log("error", "ai_summarizer.t1_error",
                 vendor_id=enriched.vendor_id, error=str(e))
            # Emit without AI fields rather than crash
            summarized = SummarizedRecord(**enriched.model_dump())
            return summarized.to_kafka_dict()

        impact_day = float(t1_result.get("impact_day", 0.0))
        impact_swing = float(t1_result.get("impact_swing", 0.0))

        # ── T2: High-impact items only ────────────────────────────────────────
        if impact_day >= settings.llm_t2_impact_threshold:
            if not self._budget or await self._budget.can_spend("anthropic", 0.05):
                try:
                    t2_result, t2_tokens, t2_cost = await self._run_t2(enriched, t1_result, raw_record=record)
                    total_tokens += t2_tokens
                    total_cost += t2_cost
                    if self._budget:
                        await self._budget.record("anthropic", t2_cost)

                    _log("info", "ai_summarizer.t2_complete",
                         vendor_id=enriched.vendor_id,
                         signal_bias=t2_result.get("signal_bias"),
                         regime=t2_result.get("regime_flag"),
                         tokens=t2_tokens,
                         cost_usd=round(t2_cost, 5))

                except Exception as e:
                    _log("warning", "ai_summarizer.t2_error",
                         vendor_id=enriched.vendor_id, error=str(e))

        # ── Build FactsJson ───────────────────────────────────────────────────
        raw_facts = t1_result.get("facts", {})
        try:
            facts = FactsJson(**{k: v for k, v in raw_facts.items()
                                  if k in FactsJson.model_fields})
        except Exception:
            facts = None

        # ── Regime flag ───────────────────────────────────────────────────────
        regime_str = t2_result.get("regime_flag") or t1_result.get("regime_flag")
        regime_flag = None
        if regime_str:
            try:
                regime_flag = RegimeFlag(regime_str)
            except ValueError:
                pass

        # ── Parse sympathy plays — validate they look like real tickers ─────
        raw_sympathy = t2_result.get("sympathy_plays", []) or []
        sympathy_plays = [
            t.upper().strip() for t in raw_sympathy
            if isinstance(t, str) and 1 <= len(t.strip()) <= 5
               and t.strip().isalpha()
        ][:3]  # Cap at 3

        # ── Assemble final record ─────────────────────────────────────────────
        _ROUTE_COUNTER.labels(
            route_type="slow",
            catalyst_type=enriched.catalyst_type.value,
        ).inc()
        summarized = SummarizedRecord(
            **enriched.model_dump(),
            t1_summary=t1_result.get("t1_summary"),
            t2_summary=t2_result.get("t2_summary"),
            facts_json=facts,
            impact_day=impact_day,
            impact_swing=impact_swing,
            regime_flag=regime_flag,
            source_credibility=t2_result.get("source_credibility"),
            signal_bias=t2_result.get("signal_bias"),
            priced_in=t2_result.get("priced_in"),
            priced_in_reason=t2_result.get("priced_in_reason"),
            sympathy_plays=sympathy_plays,
            route_type="slow",
            prompt_version=PROMPT_VERSION,
            llm_tokens_used=total_tokens,
            llm_cost_usd=round(total_cost, 6),
        )

        return summarized.to_kafka_dict()
