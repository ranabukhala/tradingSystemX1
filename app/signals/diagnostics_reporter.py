"""
Signal Diagnostics Reporter v2 — focused Telegram alerts for signal tuning.

Reports ONLY:
  1. BLOCKED signals (passed aggregator but blocked by pretrade filter)
  2. PASSED signals (reached execution engine)
  3. Daily summary at market close (16:05 ET / 20:05 UTC)

Does NOT report low-conviction signals, neutral direction, or no-ticker articles.

Consumes:
  - signals.actionable    (signals that passed the aggregator)
  - signals.filtered      (signals that passed pretrade filter → execution)
  - signals.blocked       (signals blocked by pretrade filter)

Uses event_id as the correlation key across topics.
Read-only service — never writes to Kafka or modifies the database.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx
from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, start_http_server

from app.config import settings
from app.logging import get_logger

log = get_logger("diagnostics_reporter")


def _log(level: str, event: str, **kw) -> None:
    getattr(log, level, log.info)(event, **kw)


# ── Configuration ────────────────────────────────────────────────────────────

_BOT_TOKEN = os.environ.get("TELEGRAM_TRAINING_BOT_TOKEN", "")
_CHAT_ID = os.environ.get("TELEGRAM_TRAINING_CHAT_ID", "-1003763136763")
_MAX_MSG_PER_MIN = int(os.environ.get("DIAG_MAX_MSG_PER_MIN", "30"))
_CORRELATION_TTL_S = int(os.environ.get("DIAG_CORRELATION_TTL_S", "600"))
_DAILY_SUMMARY_UTC_HOUR = int(os.environ.get("DIAG_SUMMARY_HOUR_UTC", "20"))
_DAILY_SUMMARY_UTC_MIN = int(os.environ.get("DIAG_SUMMARY_MIN_UTC", "5"))

TOPICS = [
    "signals.actionable",
    "signals.filtered",
    "signals.blocked",
]

_DIR_EMOJI = {"long": "🟢", "short": "🔴"}


def _de(direction: str) -> str:
    return _DIR_EMOJI.get(direction, "⚪")


# ── Rate limiter ─────────────────────────────────────────────────────────────

class _RateLimiter:
    def __init__(self, max_per_min: int):
        self._max = max_per_min
        self._timestamps: list[float] = []

    def can_send(self) -> bool:
        now = time.monotonic()
        self._timestamps = [t for t in self._timestamps if now - t < 60]
        return len(self._timestamps) < self._max

    def record(self) -> None:
        self._timestamps.append(time.monotonic())

    @property
    def wait_seconds(self) -> float:
        if not self._timestamps:
            return 0
        return max(0, 60 - (time.monotonic() - self._timestamps[0]))


# ── Bounded TTL dict ─────────────────────────────────────────────────────────

class _TTLDict:
    def __init__(self, ttl_s: int, max_size: int = 5000):
        self._data: OrderedDict[str, tuple[float, dict]] = OrderedDict()
        self._ttl = ttl_s
        self._max = max_size

    def set(self, key: str, value: dict) -> None:
        self._evict()
        if key in self._data:
            self._data.move_to_end(key)
        self._data[key] = (time.monotonic(), value)
        if len(self._data) > self._max:
            self._data.popitem(last=False)

    def get(self, key: str) -> dict | None:
        item = self._data.get(key)
        if item is None:
            return None
        ts, val = item
        if time.monotonic() - ts > self._ttl:
            del self._data[key]
            return None
        return val

    def pop(self, key: str) -> dict | None:
        item = self._data.pop(key, None)
        return item[1] if item else None

    def _evict(self) -> None:
        now = time.monotonic()
        while self._data:
            k, (ts, _) = next(iter(self._data.items()))
            if now - ts > self._ttl:
                self._data.popitem(last=False)
            else:
                break


# ── Daily stats tracker ──────────────────────────────────────────────────────

class _DailyStats:
    """Accumulates daily signal statistics for the end-of-day summary."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.signals_generated = 0
        self.blocked_count = 0
        self.executed_count = 0
        self.blocked_technical = 0
        self.blocked_staleness = 0
        self.blocked_policy = 0
        self.blocked_regime = 0
        self.blocked_context = 0
        self.blocked_threshold = 0
        self.sub_filter_fails: dict[str, int] = {
            "volume": 0, "multi_timeframe": 0, "key_levels": 0,
            "vwap": 0, "atr": 0, "rsi_divergence": 0, "relative_strength": 0,
        }
        self.sub_filter_totals = 0  # signals with technical data
        self.vol_ratios: list[float] = []
        self.closest_signals: list[dict] = []  # top N closest to passing
        self.facts_count = 0
        self.all_blocked: list[dict] = []  # store blocked signal summaries

    def record_actionable(self, record: dict):
        self.signals_generated += 1
        ds = record.get("direction_source", "")
        if ds == "facts":
            self.facts_count += 1

    def record_blocked(self, record: dict):
        self.blocked_count += 1
        reason = (record.get("block_reason") or "").lower()

        if "stale" in reason or "signal_stale" in reason:
            self.blocked_staleness += 1
        elif "context threshold" in reason:
            self.blocked_context += 1
        elif "policy" in reason:
            self.blocked_policy += 1
        elif "regime" in reason or "opposes" in reason:
            self.blocked_regime += 1
        elif "post-policy conviction" in reason:
            self.blocked_threshold += 1
        else:
            self.blocked_technical += 1

        # Track technical sub-filter failures
        ft = record.get("filter_technicals", {})
        bd = ft.get("technical_score_breakdown", {})
        if bd:
            self.sub_filter_totals += 1
            for comp in self.sub_filter_fails:
                comp_data = bd.get(comp, {})
                pts = comp_data.get("pts", 0)
                if pts == 0:
                    self.sub_filter_fails[comp] += 1

            # Volume ratios
            vol = bd.get("volume", {})
            vr = vol.get("vol_ratio")
            if vr is not None:
                try:
                    self.vol_ratios.append(float(vr))
                except (ValueError, TypeError):
                    pass

            # Closest to passing
            score = ft.get("technical_score", 0)
            threshold = bd.get("threshold", 7)
            if score is not None and threshold is not None:
                self.closest_signals.append({
                    "ticker": record.get("ticker", "?"),
                    "direction": record.get("direction", "?"),
                    "score": score,
                    "threshold": threshold,
                    "gap": threshold - score,
                    "failed": [c for c, d in bd.items()
                               if isinstance(d, dict) and d.get("pts", 1) == 0
                               and c not in ("threshold", "passed", "total")],
                })

        self.all_blocked.append({
            "ticker": record.get("ticker", "?"),
            "direction": record.get("direction", "?"),
            "reason": record.get("block_reason", "?"),
            "conviction": record.get("conviction"),
        })

    def record_executed(self, record: dict):
        self.executed_count += 1


# ── Helpers ──────────────────────────────────────────────────────────────────

def _safe(val: Any, default: str = "n/a") -> str:
    if val is None:
        return default
    return str(val)


def _safe_f(val: Any, digits: int = 3) -> str:
    if val is None:
        return "n/a"
    try:
        return f"{float(val):.{digits}f}"
    except (ValueError, TypeError):
        return str(val)


def _trunc(s: str | None, n: int = 120) -> str:
    if not s:
        return ""
    return s[:n] + ("…" if len(s) > n else "")


def _escape_html(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _latency_str(published_at: str | None) -> str:
    if not published_at:
        return "n/a"
    try:
        if isinstance(published_at, str):
            pub = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        else:
            pub = published_at
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
        delta = (datetime.now(timezone.utc) - pub).total_seconds()
        if delta < 120:
            return f"{int(delta)}s"
        return f"{int(delta / 60)}m"
    except Exception:
        return "n/a"


def _bar(pts: int, max_pts: int) -> str:
    return "█" * pts + "░" * (max_pts - pts)


def _check_or_x(pts: int) -> str:
    return "✅" if pts > 0 else "❌"


# ── Message formatting ───────────────────────────────────────────────────────

def _fmt_tech_block(ft: dict) -> str:
    """Format technical breakdown with ✅/❌ per sub-filter."""
    bd = ft.get("technical_score_breakdown") or {}
    score = ft.get("technical_score", "?")
    threshold = bd.get("threshold", 7)

    lines = [f"🔧 <b>Technical: {score}/10 (need {threshold})</b>"]

    # Volume
    v = bd.get("volume", {})
    vpts = v.get("pts", 0)
    vr = v.get("vol_ratio")
    obv = v.get("obv_trending_with_direction")
    detail = ""
    if vr is not None:
        detail += f"ratio={float(vr):.1f}x"
    if obv is not None:
        detail += f" OBV={'✓' if obv else '✗'}"
    lines.append(f"  {_check_or_x(vpts)} Volume    {vpts}/2  {detail}")

    # Multi-TF
    m = bd.get("multi_timeframe", {})
    mpts = m.get("pts", 0)
    d50 = m.get("daily_50ma_aligned")
    h20 = m.get("hourly_20ma_aligned")
    detail = ""
    if d50 is not None:
        detail += f"50MA={'✓' if d50 else '✗'}"
    if h20 is not None:
        detail += f" 1hr={'✓' if h20 else '✗'}"
    lines.append(f"  {_check_or_x(mpts)} Multi-TF  {mpts}/2  {detail}")

    # Key Levels
    k = bd.get("key_levels", {})
    kpts = k.get("pts", 0)
    detail = ""
    if k.get("pct_from_swing_high") is not None:
        detail += f"{k['pct_from_swing_high']:.1f}% to high"
    elif k.get("pct_from_swing_low") is not None:
        detail += f"{k['pct_from_swing_low']:.1f}% to low"
    lines.append(f"  {_check_or_x(kpts)} Key Lvl   {kpts}/2  {detail}")

    # VWAP
    vw = bd.get("vwap", {})
    vwpts = vw.get("pts", 0)
    detail = ""
    pvw = vw.get("price_vs_vwap", "")
    vval = vw.get("vwap")
    if pvw:
        detail += pvw
        if vval:
            detail += f" ${float(vval):.2f}"
    lines.append(f"  {_check_or_x(vwpts)} VWAP      {vwpts}/1  {detail}")

    # ATR
    a = bd.get("atr", {})
    apts = a.get("pts", 0)
    ratio = a.get("move_atr_ratio")
    detail = f"move={float(ratio):.1f}x ATR" if ratio is not None else ""
    lines.append(f"  {_check_or_x(apts)} ATR       {apts}/1  {detail}")

    # RSI
    r = bd.get("rsi_divergence", {})
    rpts = r.get("pts", 0)
    rsi_val = r.get("rsi")
    div = r.get("opposing_divergence")
    detail = ""
    if rsi_val is not None:
        detail += f"RSI={float(rsi_val):.0f}"
    if div is not None:
        detail += f" div={'yes' if div else 'no'}"
    lines.append(f"  {_check_or_x(rpts)} RSI       {rpts}/1  {detail}")

    # Relative Strength
    rs = bd.get("relative_strength", {})
    rspts = rs.get("pts", 0)
    s5d = rs.get("stock_5d_pct")
    spy5d = rs.get("spy_5d_pct")
    detail = ""
    if s5d is not None:
        detail += f"stock={float(s5d):.1f}%"
    if spy5d is not None:
        detail += f" vs SPY={float(spy5d):.1f}%"
    lines.append(f"  {_check_or_x(rspts)} RelStr    {rspts}/1  {detail}")

    return "\n".join(lines)


def format_blocked(record: dict) -> str:
    """Format a signal blocked by pretrade filter."""
    ticker = record.get("ticker", "?")
    direction = record.get("direction", "neutral")
    block_reason = record.get("block_reason", "unknown")
    block_lower = block_reason.lower() if isinstance(block_reason, str) else ""

    msg = (
        f"🚫 <b>{_escape_html(str(ticker))} {_de(direction)} {direction}"
        f" — BLOCKED</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
    )

    # Catalyst info
    catalyst = _safe(record.get("catalyst_type"))
    source = _safe(record.get("source"))
    title = _escape_html(_trunc(record.get("news_title") or record.get("title"), 120))
    tw_label = _safe(record.get("time_window_label"))
    msg += f"📰 {title}\n   {catalyst} | {source} | {tw_label}\n\n"

    # Signal scores
    conviction = _safe_f(record.get("conviction") or record.get("conviction_original"))
    impact_d = _safe_f(record.get("impact_day"))
    impact_s = _safe_f(record.get("impact_swing"))
    dir_source = _safe(record.get("direction_source"))
    regime = _safe(record.get("regime_flag"))
    msg += (
        f"📊 Conviction: {conviction} | Impact: {impact_d}/{impact_s}\n"
        f"   Direction: {dir_source} | Regime: {regime}\n\n"
    )

    # Block reason — format depends on type
    if "stale" in block_lower or "signal_stale" in block_lower:
        age_s = record.get("signal_age_seconds")
        max_s = record.get("max_age_seconds")
        route = _safe(record.get("route_type"))
        session = _safe(record.get("session_context"))
        msg += (
            f"❌ <b>STALE:</b> {_safe(age_s)}s old (max {_safe(max_s)}s)\n"
            f"   Catalyst: {catalyst} | Route: {route} | Session: {session}\n"
        )
    elif "policy" in block_lower:
        msg += f"❌ <b>POLICY:</b> {_escape_html(str(block_reason))}\n"
    else:
        msg += f"❌ <b>BLOCKED:</b> {_escape_html(str(block_reason))}\n\n"
        # Technical breakdown
        ft = record.get("filter_technicals", {})
        if ft and ft.get("technical_score_breakdown"):
            msg += f"{_fmt_tech_block(ft)}\n"

    # Context info
    if record.get("trend_regime") or record.get("cleanliness"):
        msg += (
            f"\n📈 Trend: {_safe(record.get('trend_regime'))} | "
            f"Vol: {_safe(record.get('volatility_regime'))} | "
            f"Clean: {_safe(record.get('cleanliness'))}\n"
        )

    # Timing
    pub = record.get("news_published_at") or record.get("published_at")
    msg += f"\n⏱ Published {_safe(pub)} → Processed {_latency_str(pub)}"

    return msg


def format_passed(record: dict) -> str:
    """Format a signal that passed all filters → execution."""
    ticker = record.get("ticker", "?")
    direction = record.get("direction", "neutral")

    msg = (
        f"✅ <b>{_escape_html(str(ticker))} {_de(direction)} {direction}"
        f" — SENT TO EXECUTION</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
    )

    catalyst = _safe(record.get("catalyst_type"))
    title = _escape_html(_trunc(record.get("news_title") or record.get("title"), 120))
    conviction = _safe_f(record.get("conviction"))
    msg += f"📰 {title}\n   {catalyst} | Conviction: {conviction}\n\n"

    # Technical breakdown
    ft = record.get("filter_technicals", {})
    if ft and ft.get("technical_score_breakdown"):
        msg += f"{_fmt_tech_block(ft)}\n\n"

    # Options & Squeeze
    fo = record.get("filter_options", {})
    fs = record.get("filter_squeeze", {})
    if fo or fs:
        msg += (
            f"📋 Options: bias={_safe(fo.get('flow_bias'))} "
            f"pcr={_safe_f(fo.get('pcr'))} | "
            f"Squeeze: {_safe_f(fs.get('score'))} "
            f"short={_safe_f(fs.get('short_float'))}%\n\n"
        )

    msg += f"🎯 <b>Sent to execution engine</b>\n"

    pub = record.get("news_published_at") or record.get("published_at")
    msg += f"\n⏱ Published {_safe(pub)} → Processed {_latency_str(pub)}"

    return msg


def format_daily_summary(stats: _DailyStats) -> str:
    """Format the end-of-day summary with tuning suggestions."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    total_blocked = stats.blocked_count

    msg = (
        f"📋 <b>Daily Signal Report — {today}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
    )

    # Pipeline stats
    msg += (
        f"📊 <b>Pipeline Stats:</b>\n"
        f"   Signals generated: {stats.signals_generated}\n"
        f"   Signals blocked: {stats.blocked_count}\n"
        f"   Signals executed: {stats.executed_count}\n\n"
    )

    # Block reasons
    if total_blocked > 0:
        msg += (
            f"❌ <b>Block Reasons:</b>\n"
            f"   Staleness:     {stats.blocked_staleness}\n"
            f"   Technical:     {stats.blocked_technical}\n"
            f"   Context thresh:{stats.blocked_context}\n"
            f"   Policy:        {stats.blocked_policy}\n"
            f"   Regime:        {stats.blocked_regime}\n"
            f"   Post-policy:   {stats.blocked_threshold}\n\n"
        )

    # Sub-filter failure rates
    n = stats.sub_filter_totals
    if n > 0:
        msg += f"🔧 <b>Technical Sub-Filter Failure Rates:</b>\n"
        for comp, label in [
            ("volume", "Volume     "),
            ("multi_timeframe", "Multi-TF   "),
            ("key_levels", "Key Levels "),
            ("vwap", "VWAP       "),
            ("atr", "ATR        "),
            ("rsi_divergence", "RSI        "),
            ("relative_strength", "RelStrength"),
        ]:
            fails = stats.sub_filter_fails[comp]
            pct = round(100 * fails / n) if n else 0
            msg += f"   {label}: {fails}/{n} ({pct}%)\n"
        msg += "\n"

    # Market context
    if stats.signals_generated > 0:
        avg_vr = (
            f"{sum(stats.vol_ratios) / len(stats.vol_ratios):.1f}x"
            if stats.vol_ratios else "n/a"
        )
        msg += (
            f"📈 <b>Market Context:</b>\n"
            f"   Avg volume ratio: {avg_vr}\n"
            f"   Direction=facts: {stats.facts_count}/{stats.signals_generated}\n\n"
        )

    # Closest to passing (top 3)
    closest = sorted(stats.closest_signals, key=lambda x: x["gap"])[:3]
    if closest:
        msg += f"🏆 <b>Closest to Passing:</b>\n"
        for sig in closest:
            failed_str = ", ".join(sig["failed"][:4]) if sig["failed"] else "none"
            msg += (
                f"   • {sig['ticker']} {_de(sig['direction'])} "
                f"— {sig['score']}/{sig['threshold']} technical\n"
                f"     Missing: {failed_str}\n"
            )
        msg += "\n"

    # Tuning suggestions
    suggestions = _generate_suggestions(stats)
    msg += f"💡 <b>Tuning Suggestions:</b>\n"
    for s in suggestions:
        msg += f"   {s}\n"

    return msg


def _generate_suggestions(stats: _DailyStats) -> list[str]:
    suggestions = []
    n = stats.sub_filter_totals

    if n > 0:
        rs_fail_rate = stats.sub_filter_fails["relative_strength"] / n
        if rs_fail_rate > 0.8:
            suggestions.append(
                "📉 Most signals fail on SPY relative strength — "
                "broad market weakness, not signal quality."
            )

        vol_fail_rate = stats.sub_filter_fails["volume"] / n
        if vol_fail_rate > 0.7:
            suggestions.append(
                "📉 Volume consistently failing — "
                "market participation is thin."
            )

    if stats.vol_ratios:
        avg_vr = sum(stats.vol_ratios) / len(stats.vol_ratios)
        if avg_vr < 1.0:
            suggestions.append(
                f"📉 Avg volume ratio {avg_vr:.1f}x — "
                "below average, volume filters will be strict."
            )

    total_blocked = stats.blocked_count
    if total_blocked > 0:
        stale_rate = stats.blocked_staleness / total_blocked
        if stale_rate > 0.5:
            suggestions.append(
                "⏱ Over half blocked by staleness — "
                "consider relaxing STALENESS_*_MAX_AGE."
            )

    # Closest signals gap
    closest = sorted(stats.closest_signals, key=lambda x: x["gap"])[:5]
    if closest:
        avg_gap = sum(s["gap"] for s in closest) / len(closest)
        if avg_gap <= 1:
            suggestions.append(
                f"🎯 Signals avg {avg_gap:.1f} pts below threshold — "
                "very close. Consider lowering threshold by 1."
            )

    if stats.signals_generated > 0:
        facts_rate = stats.facts_count / stats.signals_generated
        if facts_rate < 0.3:
            suggestions.append(
                "📰 Few signals have factual direction — "
                "news quality may be too generic."
            )

    if not suggestions:
        suggestions.append("✅ System filtering appropriately for current conditions.")

    return suggestions


# ── Main service ─────────────────────────────────────────────────────────────

class DiagnosticsReporter:
    """
    Multi-topic Kafka consumer that reports blocked and passed signals
    to Telegram, plus a daily summary at market close.

    Does NOT inherit BaseConsumer (which supports only a single topic).
    Manages its own Kafka consumer, rate limiter, and HTTP client.
    """

    def __init__(self) -> None:
        self._running = False
        self._http: httpx.AsyncClient | None = None
        self._rate_limiter = _RateLimiter(_MAX_MSG_PER_MIN)

        # Correlation store for enriching blocked records with actionable data
        self._events = _TTLDict(_CORRELATION_TTL_S)

        # Daily stats
        self._daily = _DailyStats()
        self._summary_sent_today = False
        self._last_reset_date: str | None = None

        # Metrics
        self._msg_sent = Counter(
            "diag_messages_sent_total", "Telegram messages sent", ["type"])
        self._msg_failed = Counter(
            "diag_messages_failed_total", "Telegram send failures")
        self._events_consumed = Counter(
            "diag_events_consumed_total", "Kafka messages consumed", ["topic"])

        try:
            port = int(os.environ.get("METRICS_PORT", "8025"))
            start_http_server(port)
            _log("info", "diagnostics.metrics_started", port=port)
        except OSError:
            pass

    async def run(self) -> None:
        self._running = True
        _log("info", "diagnostics_v2.starting",
             topics=TOPICS, chat_id=_CHAT_ID,
             bot_token_set=bool(_BOT_TOKEN))

        if not _BOT_TOKEN:
            _log("error", "diagnostics_v2.no_bot_token",
                 msg="TELEGRAM_TRAINING_BOT_TOKEN not set — exiting")
            return

        self._http = httpx.AsyncClient(
            base_url=f"https://api.telegram.org/bot{_BOT_TOKEN}",
            timeout=15.0,
        )

        # Verify bot
        try:
            resp = await self._http.get("/getMe")
            if resp.status_code == 200:
                bot_name = resp.json()["result"]["username"]
                _log("info", "diagnostics_v2.bot_connected", bot=bot_name)
            else:
                _log("warning", "diagnostics_v2.bot_check_failed",
                     status=resp.status_code)
        except Exception as e:
            _log("warning", "diagnostics_v2.bot_check_error", error=str(e))

        consumer = Consumer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "diagnostics_reporter_v2",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
            "session.timeout.ms": 30000,
        })
        consumer.subscribe(TOPICS)
        _log("info", "diagnostics_v2.subscribed", topics=TOPICS)

        loop = asyncio.get_running_loop()

        try:
            while self._running:
                msg = await loop.run_in_executor(
                    None, lambda: consumer.poll(timeout=1.0))

                # Check daily summary schedule
                await self._check_daily_summary()

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    _log("warning", "diagnostics_v2.kafka_error",
                         error=str(msg.error()))
                    continue

                topic = msg.topic()
                self._events_consumed.labels(topic=topic).inc()

                try:
                    record = json.loads(msg.value().decode("utf-8"))
                except Exception as e:
                    _log("warning", "diagnostics_v2.decode_error",
                         error=str(e), topic=topic)
                    continue

                await self._handle_record(topic, record)

        except Exception as e:
            _log("error", "diagnostics_v2.fatal", error=str(e))
            raise
        finally:
            consumer.close()
            if self._http:
                await self._http.aclose()
            _log("info", "diagnostics_v2.stopped")

    def stop(self) -> None:
        self._running = False

    # ── Record handling ──────────────────────────────────────────────────────

    async def _handle_record(self, topic: str, record: dict) -> None:
        event_id = record.get("event_id")
        if not event_id:
            return

        eid = str(event_id)

        if topic == "signals.actionable":
            # Store for correlation and track stats
            self._events.set(eid, {"actionable_data": record})
            self._daily.record_actionable(record)

        elif topic == "signals.blocked":
            # Merge with actionable data if available
            ctx = self._events.pop(eid) or {}
            actionable = ctx.get("actionable_data", {})
            # The blocked record is {**actionable_record, blocked: True, ...}
            # so it should already contain most fields
            merged = {**actionable, **record}
            self._daily.record_blocked(merged)
            await self._emit_blocked(merged)

        elif topic == "signals.filtered":
            # Signal passed all filters — heading to execution
            ctx = self._events.pop(eid) or {}
            actionable = ctx.get("actionable_data", {})
            merged = {**actionable, **record}
            self._daily.record_executed(merged)
            await self._emit_passed(merged)

    async def _emit_blocked(self, record: dict) -> None:
        text = format_blocked(record)
        await self._send_telegram(text, msg_type="blocked")

    async def _emit_passed(self, record: dict) -> None:
        text = format_passed(record)
        await self._send_telegram(text, msg_type="passed")

    # ── Daily summary ────────────────────────────────────────────────────────

    async def _check_daily_summary(self) -> None:
        """Send daily summary at configured time (default 20:05 UTC = 16:05 ET)."""
        now = datetime.now(timezone.utc)
        today_str = now.strftime("%Y-%m-%d")

        # Reset counters at market open (13:25 UTC = 9:25 ET)
        if (self._last_reset_date != today_str
                and now.hour >= 13 and now.minute >= 25):
            self._daily.reset()
            self._summary_sent_today = False
            self._last_reset_date = today_str
            _log("info", "diagnostics_v2.daily_reset", date=today_str)

        # Send summary at configured time
        if (not self._summary_sent_today
                and now.hour == _DAILY_SUMMARY_UTC_HOUR
                and now.minute >= _DAILY_SUMMARY_UTC_MIN):
            # Only send if there's data
            if (self._daily.signals_generated > 0
                    or self._daily.blocked_count > 0
                    or self._daily.executed_count > 0):
                text = format_daily_summary(self._daily)
                await self._send_telegram(text, msg_type="daily_summary")
                self._summary_sent_today = True
                _log("info", "diagnostics_v2.daily_summary_sent",
                     signals=self._daily.signals_generated,
                     blocked=self._daily.blocked_count,
                     executed=self._daily.executed_count)

    # ── Telegram sender ──────────────────────────────────────────────────────

    async def _send_telegram(self, text: str, msg_type: str = "unknown") -> None:
        if not self._http:
            return

        while not self._rate_limiter.can_send():
            wait = self._rate_limiter.wait_seconds
            _log("debug", "diagnostics_v2.rate_limited", wait_s=round(wait, 1))
            await asyncio.sleep(min(wait + 0.5, 10))
            if not self._running:
                return

        # Telegram 4096 char limit
        if len(text) > 4090:
            text = text[:4087] + "…"

        try:
            resp = await self._http.post("/sendMessage", json={
                "chat_id": _CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            })

            if resp.status_code == 200:
                self._rate_limiter.record()
                self._msg_sent.labels(type=msg_type).inc()
            elif resp.status_code == 429:
                retry_after = resp.json().get("parameters", {}).get(
                    "retry_after", 30)
                _log("warning", "diagnostics_v2.telegram_rate_limited",
                     retry_after=retry_after)
                await asyncio.sleep(retry_after)
                resp2 = await self._http.post("/sendMessage", json={
                    "chat_id": _CHAT_ID,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                })
                if resp2.status_code == 200:
                    self._rate_limiter.record()
                    self._msg_sent.labels(type=msg_type).inc()
                else:
                    self._msg_failed.inc()
            else:
                _log("warning", "diagnostics_v2.telegram_error",
                     status=resp.status_code,
                     body=resp.text[:300])
                self._msg_failed.inc()

        except Exception as e:
            _log("warning", "diagnostics_v2.telegram_send_error",
                 error=str(e))
            self._msg_failed.inc()
