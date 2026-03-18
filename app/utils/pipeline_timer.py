"""
Pipeline Latency Timer
======================
Lightweight utility for tracking ISO-8601 timestamps at each stage of the
news → signal → execution pipeline.  Zero external dependencies.

Usage (in every BaseConsumer subclass via the base hook — no direct call needed):
    from app.utils.pipeline_timer import PipelineTimer

    # Stamp a stage on the outgoing dict
    msg["stage_timestamps"] = PipelineTimer.stamp(
        msg.get("stage_timestamps"), "ai_summarized"
    )

    # Compute end-to-end latency (first → last recorded stage)
    e2e_ms = PipelineTimer.calculate_e2e_latency_ms(msg.get("stage_timestamps", {}))

    # Break down time between consecutive stages
    durations = PipelineTimer.stage_durations_ms(msg.get("stage_timestamps", {}))
    # → {"news_ingested→normalized": 12.3, "normalized→entity_resolved": 45.7, ...}
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional


class PipelineTimer:
    """Static utility — instantiate-free, thread/coroutine-safe."""

    # Canonical stage order.  Stages not in this list still get recorded;
    # they just sort after known stages in duration calculations.
    STAGE_ORDER: list[str] = [
        "news_ingested",
        "normalized",
        "entity_resolved",
        "ai_summarized",
        "signal_aggregated",
        "pretrade_filtered",
        "execution_submitted",
        "execution_filled",
    ]

    @staticmethod
    def now_iso() -> str:
        """UTC timestamp with millisecond precision, e.g. '2026-03-18T14:22:03.417Z'."""
        return (
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        )

    @staticmethod
    def stamp(existing: Optional[dict | str], stage: str) -> dict:
        """
        Return a copy of *existing* with *stage* timestamped at now.

        Accepts None, a dict, or a JSON string (deserialized transparently).
        Always returns a plain dict — safe to assign back to msg['stage_timestamps'].
        """
        if existing is None:
            ts: dict = {}
        elif isinstance(existing, str):
            try:
                ts = json.loads(existing)
            except Exception:
                ts = {}
        else:
            ts = dict(existing)   # shallow copy — don't mutate the original

        ts[stage] = PipelineTimer.now_iso()
        return ts

    @staticmethod
    def calculate_e2e_latency_ms(timestamps: dict | None) -> Optional[float]:
        """
        End-to-end latency from the first recorded stage to the last, in ms.
        Returns None if fewer than two stages are recorded.
        """
        if not timestamps or len(timestamps) < 2:
            return None

        ordered = PipelineTimer._ordered_values(timestamps)
        if len(ordered) < 2:
            return None

        try:
            first = PipelineTimer._parse(ordered[0])
            last  = PipelineTimer._parse(ordered[-1])
            return round((last - first).total_seconds() * 1000, 1)
        except Exception:
            return None

    @staticmethod
    def stage_durations_ms(timestamps: dict | None) -> dict[str, float]:
        """
        Duration between each consecutive stage in milliseconds.

        Returns e.g.:
            {
              "news_ingested→normalized":       12.3,
              "normalized→entity_resolved":     45.7,
              "entity_resolved→ai_summarized": 2814.0,
              ...
            }
        """
        if not timestamps or len(timestamps) < 2:
            return {}

        ordered_pairs = PipelineTimer._ordered_pairs(timestamps)
        durations: dict[str, float] = {}

        for (name_a, ts_a), (name_b, ts_b) in zip(ordered_pairs, ordered_pairs[1:]):
            try:
                dt_a = PipelineTimer._parse(ts_a)
                dt_b = PipelineTimer._parse(ts_b)
                durations[f"{name_a}→{name_b}"] = round(
                    (dt_b - dt_a).total_seconds() * 1000, 1
                )
            except Exception:
                continue

        return durations

    @staticmethod
    def slowest_stage(timestamps: dict | None) -> tuple[str, float] | None:
        """
        Return (stage_transition, duration_ms) for the single slowest stage.
        Returns None if fewer than two stages are recorded.
        """
        durations = PipelineTimer.stage_durations_ms(timestamps)
        if not durations:
            return None
        return max(durations.items(), key=lambda kv: kv[1])

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _ordered_pairs(ts: dict) -> list[tuple[str, str]]:
        """Return (stage_name, iso_str) pairs sorted by canonical stage order."""
        known   = [(s, ts[s]) for s in PipelineTimer.STAGE_ORDER if s in ts]
        unknown = [(s, ts[s]) for s in ts if s not in PipelineTimer.STAGE_ORDER]
        return known + unknown

    @staticmethod
    def _ordered_values(ts: dict) -> list[str]:
        return [v for _, v in PipelineTimer._ordered_pairs(ts)]

    @staticmethod
    def _parse(iso: str) -> datetime:
        """Parse an ISO-8601 string produced by now_iso()."""
        return datetime.strptime(iso, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc
        )
