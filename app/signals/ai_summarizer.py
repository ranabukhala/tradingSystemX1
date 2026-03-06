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
from datetime import datetime, timezone, date
from typing import Any

import httpx

from app.config import settings
from app.models.news import (
    CatalystType, EnrichedRecord, FactsJson,
    RegimeFlag, SummarizedRecord,
)
from app.pipeline.base_consumer import BaseConsumer, _log
from app.signals.prompts import (
    PROMPT_VERSION, T1_SYSTEM, T1_USER, T2_SYSTEM, T2_USER,
)

CLAUDE_MODEL = "claude-haiku-4-5-20251001"   # Fast + cheap for T1
CLAUDE_MODEL_T2 = "claude-sonnet-4-6"        # Smarter for T2 trader analysis

# Approximate cost per 1K tokens (input+output blended)
COST_PER_1K_T1 = 0.001    # Haiku
COST_PER_1K_T2 = 0.015    # Sonnet


class BudgetTracker:
    """Simple daily spend tracker. Resets at midnight UTC."""

    def __init__(self, daily_limit: float) -> None:
        self.daily_limit = daily_limit
        self._spend: float = 0.0
        self._day: date = date.today()

    def _maybe_reset(self) -> None:
        today = date.today()
        if today != self._day:
            self._spend = 0.0
            self._day = today

    def can_spend(self, estimated: float) -> bool:
        self._maybe_reset()
        return (self._spend + estimated) <= self.daily_limit

    def record(self, cost: float) -> None:
        self._maybe_reset()
        self._spend += cost
        _log("debug", "budget.recorded",
             cost_usd=round(cost, 5),
             daily_total=round(self._spend, 4),
             daily_limit=self.daily_limit)

    @property
    def remaining(self) -> float:
        self._maybe_reset()
        return max(0.0, self.daily_limit - self._spend)


class AISummarizerService(BaseConsumer):

    def __init__(self) -> None:
        self._budget = BudgetTracker(settings.llm_daily_budget_usd)
        self._http: httpx.AsyncClient | None = None
        # Simple rate limit: max 50 req/min to Claude API
        self._req_times: list[float] = []
        super().__init__()

    @property
    def service_name(self) -> str:
        return "ai_summarizer"

    @property
    def input_topic(self) -> str:
        return settings.topic_news_enriched

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

    async def _run_t2(self, record: EnrichedRecord, t1: dict) -> tuple[dict, int, float]:
        """
        Run T2 trader analysis. Returns (parsed_dict, tokens, cost_usd).
        """
        user_prompt = T2_USER.format(
            title=record.title,
            snippet=record.snippet or "",
            tickers=", ".join(record.tickers) if record.tickers else "unknown",
            catalyst_type=record.catalyst_type.value,
            session_context=record.session_context.value,
            t1_summary=t1.get("t1_summary", ""),
            impact_day=round(t1.get("impact_day", 0.0), 2),
            impact_swing=round(t1.get("impact_swing", 0.0), 2),
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

        # Budget check
        if not self._budget.can_spend(0.01):  # Minimum T1 cost estimate
            _log("warning", "ai_summarizer.budget_exhausted",
                 remaining=self._budget.remaining,
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
            self._budget.record(t1_cost)

            _log("info", "ai_summarizer.t1_complete",
                 vendor_id=enriched.vendor_id,
                 tickers=enriched.tickers,
                 impact_day=t1_result.get("impact_day"),
                 impact_swing=t1_result.get("impact_swing"),
                 tokens=t1_tokens,
                 cost_usd=round(t1_cost, 5))

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
            if self._budget.can_spend(0.05):  # T2 cost estimate
                try:
                    t2_result, t2_tokens, t2_cost = await self._run_t2(enriched, t1_result)
                    total_tokens += t2_tokens
                    total_cost += t2_cost
                    self._budget.record(t2_cost)

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

        # ── Assemble final record ─────────────────────────────────────────────
        summarized = SummarizedRecord(
            **enriched.model_dump(),
            t1_summary=t1_result.get("t1_summary"),
            t2_summary=t2_result.get("t2_summary"),
            facts_json=facts,
            impact_day=impact_day,
            impact_swing=impact_swing,
            regime_flag=regime_flag,
            source_credibility=t2_result.get("source_credibility"),
            prompt_version=PROMPT_VERSION,
            llm_tokens_used=total_tokens,
            llm_cost_usd=round(total_cost, 6),
        )

        return summarized.to_kafka_dict()
