"""
Telegram Alert Service — Stage 6 of the pipeline.

Consumes: signals.filtered
Pushes:   Formatted alerts to Telegram bot

Message format (v2 — enriched):
  🟢 LONG · AAPL · event_driven

  ~$191.40  ·  Stop ~$188.20 (-1.7%)  ·  Target ~$198.00 (+3.4%)  ·  R:R 2.0×

  Conviction: ████████░░ 82%
  Impact: day=0.74 swing=0.51
  Edge decay: ~3h

  📊 Technicals: 7/10 ✅
  `Vol  ` ██ 2/2 · 3.2× avg · OBV ✓
  `RSI  ` ░ 0/1 · RSI 71 (FMP 69 ✓) · div ⚠️
  `Ctx  ` · ADX 34.2 ↑ trend · BB expanding

  📰 _Apple beats Q1 EPS by $0.14_ · Benzinga · 4m ago

  ...

  Regime: 🚀 Risk On · avg +0.8% · 8/11 up

  🆔 `a3f9c12b`  🔗 Event `c7d2a1`
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone

import httpx
import redis.asyncio as aioredis

# Override via SIGNAL_ALERT_THRESHOLD env var
ALERT_CONVICTION_THRESHOLD = float(os.environ.get("SIGNAL_ALERT_THRESHOLD", "0.60"))

from app.config import settings
from app.pipeline.base_consumer import BaseConsumer, _log
from app.signals.signal_aggregator import TradingSignal


# ── Display maps ──────────────────────────────────────────────────────────────

DIRECTION_EMOJI = {
    "long":    "🟢",
    "short":   "🔴",
    "neutral": "🟡",
}

SESSION_LABELS = {
    "premarket":  "🌅 Pre-market",
    "open":       "🔔 Market open",
    "intraday":   "📈 Intraday",
    "afterhours": "🌙 After-hours",
    "overnight":  "🌃 Overnight",
}

CATALYST_LABELS = {
    "earnings":   "📊 Earnings",
    "analyst":    "🎯 Analyst",
    "ma":         "🤝 M&A",
    "regulatory": "💊 Regulatory",
    "filing":     "📋 SEC Filing",
    "macro":      "🌍 Macro",
    "legal":      "⚖️ Legal/Lawsuit ⚠️",
    "other":      "📰 News",
}

# Human-readable source names (matches NewsSource enum values)
_SOURCE_NAMES: dict[str, str] = {
    "benzinga":       "Benzinga",
    "reuters":        "Reuters",
    "pr_newswire":    "PR Newswire",
    "businesswire":   "BusinessWire",
    "globe_newswire": "GlobeNewswire",
    "marketwatch":    "MarketWatch",
    "seekingalpha":   "Seeking Alpha",
    "thestreet":      "TheStreet",
    "yahoo_finance":  "Yahoo Finance",
    "finnhub":        "Finnhub",
    "polygon":        "Polygon",
    "alpaca":         "Alpaca",
}


# ── Public formatter ──────────────────────────────────────────────────────────

def format_signal_message(
    signal: TradingSignal,
    tech_checks: dict | None = None,
    context: dict | None = None,
) -> str:
    """
    Format a trading signal into a Telegram message.

    Parameters
    ----------
    signal      : Validated TradingSignal record.
    tech_checks : Pre-trade filter technicals dict (filter_technicals.checks).
    context     : Redis-fetched enrichment data (stock_context, fmp_sectors,
                  fmp_technical, current_price).  All keys optional.
    """
    ctx             = context or {}
    direction_emoji = DIRECTION_EMOJI.get(signal.direction, "⚪")
    direction_label = signal.direction.upper()
    session         = SESSION_LABELS.get(signal.session_context, signal.session_context)
    catalyst        = CATALYST_LABELS.get(signal.catalyst_type, signal.catalyst_type)
    conviction_pct  = int(signal.conviction * 100)
    conviction_bar  = _conviction_bar(signal.conviction)

    decay_str = ""
    if signal.decay_minutes:
        decay_str = (
            f"{signal.decay_minutes}m"
            if signal.decay_minutes < 60
            else f"{signal.decay_minutes // 60}h"
        )

    # ── Header ────────────────────────────────────────────────────────────────
    lines = [
        f"{direction_emoji} *{direction_label} · {signal.ticker} · {signal.signal_type}*",
        "",
    ]

    # ── Item 6: Current price + ATR-based stop/target ─────────────────────────
    price_line = _format_price_line(signal, ctx)
    if price_line:
        lines.append(price_line)
        lines.append("")

    # ── Conviction / impact / decay ───────────────────────────────────────────
    lines += [
        f"Conviction: {conviction_bar} {conviction_pct}%",
        f"Impact: day={signal.impact_day:.2f} swing={signal.impact_swing:.2f}",
    ]
    if decay_str:
        lines.append(f"Edge decay: ~{decay_str}")

    # ── Technical scoring block (Items 2 + 5 woven in) ────────────────────────
    stock_ctx  = ctx.get("stock_context") or {}
    fmp_tech   = ctx.get("fmp_technical") or {}
    fmp_rsi    = fmp_tech.get("rsi")
    tech_block = _format_technicals_block(
        tech_checks or {},
        fmp_rsi=fmp_rsi,
        stock_ctx=stock_ctx,
    )
    if tech_block:
        lines.append("")
        lines.extend(tech_block.splitlines())

    # ── Item 1: News headline + source + age ──────────────────────────────────
    source_age = _format_source_age(signal)
    lines.append("")
    lines.append(f"📰 _{signal.news_title}_{source_age}")

    # ── T1 / T2 summaries ─────────────────────────────────────────────────────
    if signal.t1_summary:
        lines.append("")
        lines.append(f"*Facts:* {signal.t1_summary}")

    if signal.t2_summary:
        lines.append("")
        if getattr(signal, "is_sympathy", False):
            lines.append(f"↗️ {signal.t2_summary}")
        else:
            lines.append(f"💡 {signal.t2_summary}")

    # ── Priced-in status ──────────────────────────────────────────────────────
    if getattr(signal, "priced_in", None) and not getattr(signal, "is_sympathy", False):
        priced_emoji = {"yes": "🔴", "partially": "🟡", "no": "🟢"}.get(signal.priced_in, "⚪")
        lines.append("")
        lines.append(f"{priced_emoji} Priced in: {signal.priced_in.title()}")
        if getattr(signal, "priced_in_reason", None):
            lines.append(f"   _{signal.priced_in_reason}_")

    # ── Sympathy plays ────────────────────────────────────────────────────────
    if getattr(signal, "sympathy_plays", None) and not getattr(signal, "is_sympathy", False):
        plays = " · ".join(signal.sympathy_plays)
        lines.append("")
        lines.append(f"🔗 Sympathy: {plays}")

    # ── Session / catalyst / regime ───────────────────────────────────────────
    lines.append("")
    lines.append(f"{session} · {catalyst}")

    if signal.regime_flag:
        regime_emoji = {
            "risk_on":     "🚀",
            "risk_off":    "🛡️",
            "high_vol":    "⚡",
            "compression": "🔄",
        }.get(signal.regime_flag, "")
        regime_label   = signal.regime_flag.replace("_", " ").title()
        # Item 3: append sector breadth from fmp:sectors
        sector_suffix  = _format_sector_breadth(ctx.get("fmp_sectors"))
        lines.append(f"Regime: {regime_emoji} {regime_label}{sector_suffix}")

    if signal.market_cap_tier:
        tier_emoji = {
            "mega":  "🏔️",
            "large": "🏢",
            "mid":   "🏬",
            "small": "🏪",
            "micro": "🏠",
        }.get(signal.market_cap_tier, "")
        lines.append(f"Cap tier: {tier_emoji} {signal.market_cap_tier.title()}")

    # ── Timing window ─────────────────────────────────────────────────────────
    tw_label   = getattr(signal, "time_window_label", None)
    tw_emoji   = getattr(signal, "time_window_emoji", "")
    tw_mult    = getattr(signal, "time_window_mult", None)
    tw_quality = getattr(signal, "time_window_quality", None)
    if tw_label:
        tw_warn  = " ⚠️" if tw_quality == "poor" else ""
        mult_str = ""
        if tw_mult is not None:
            delta    = (tw_mult - 1) * 100
            sign     = "+" if delta >= 0 else ""
            mult_str = f" ({sign}{delta:.0f}% if enabled)"
        lines.append("")
        lines.append(f"🕐 _Timing:_ {tw_emoji} {tw_label}{tw_warn}{mult_str}")

    # ── Slow-pipeline warning ─────────────────────────────────────────────────
    stage_ts = ctx.get("stage_timestamps") or {}
    if stage_ts:
        from app.utils.pipeline_timer import PipelineTimer
        e2e = PipelineTimer.calculate_e2e_latency_ms(stage_ts)
        if e2e and e2e > 10_000:   # 10 s threshold
            slowest = PipelineTimer.slowest_stage(stage_ts)
            bottleneck = (
                f"{slowest[0]} @ {slowest[1] / 1000:.1f}s"
                if slowest else "unknown"
            )
            lines.append(
                f"⚠️ _Slow pipeline: {e2e / 1000:.1f}s total · bottleneck: {bottleneck}_"
            )

    # ── Footer: signal ID + Item 4 event dedup indicator ─────────────────────
    lines.append("")
    id_parts = [f"🆔 `{str(signal.id)[:8]}`"]
    if getattr(signal, "event_id", None):
        id_parts.append(f"🔗 Event `{str(signal.event_id)[:6]}`")
    lines.append("  ".join(id_parts))

    return "\n".join(lines)


# ── Item 1 helper: news source + age ─────────────────────────────────────────

def _format_source_age(signal: TradingSignal) -> str:
    """
    Return ' · Benzinga · 4m ago' to append to the news headline, or ''.

    Handles NewsSource enum repr like 'NewsSource.benzinga' by splitting on '.'.
    """
    parts: list[str] = []

    if signal.source:
        raw = str(signal.source).lower().rsplit(".", 1)[-1]
        parts.append(_SOURCE_NAMES.get(raw, raw.replace("_", " ").title()))

    if getattr(signal, "news_published_at", None):
        try:
            now = datetime.now(timezone.utc)
            pub = signal.news_published_at
            if pub.tzinfo is None:
                pub = pub.replace(tzinfo=timezone.utc)
            age_s = max(0.0, (now - pub).total_seconds())
            if age_s < 60:
                parts.append(f"{int(age_s)}s ago")
            elif age_s < 3_600:
                parts.append(f"{int(age_s // 60)}m ago")
            elif age_s < 86_400:
                parts.append(f"{int(age_s // 3_600)}h ago")
            else:
                parts.append(f"{int(age_s // 86_400)}d ago")
        except Exception:
            pass

    return (" · " + " · ".join(parts)) if parts else ""


# ── Item 6 helper: price + ATR stop/target ───────────────────────────────────

def _format_price_line(signal: TradingSignal, ctx: dict) -> str:
    """
    Return a one-line price / stop / target summary, or '' if price unknown.

    Uses ATR(14) from stock_context.raw_metrics when available; falls back to
    a market-cap-tier percentage so the line is always shown when price exists.
    """
    price = ctx.get("current_price")
    if not price or price <= 0:
        return ""

    raw_metrics = (ctx.get("stock_context") or {}).get("raw_metrics") or {}
    atr         = raw_metrics.get("atr14")
    atr_label   = ""

    if atr and atr > 0:
        stop_dist   = atr * 1.5
        target_dist = atr * 2.5
    else:
        # Percentage fallback by market cap tier (no ATR data available)
        tier_pcts = {
            "mega":  0.012,
            "large": 0.015,
            "mid":   0.020,
            "small": 0.025,
            "micro": 0.030,
        }
        pct       = tier_pcts.get(signal.market_cap_tier or "", 0.018)
        stop_dist   = price * pct
        target_dist = price * pct * (5 / 3)   # ~1.67× → R:R ≈ 1.7
        atr_label   = " ~est"

    rr = round(target_dist / stop_dist, 1) if stop_dist > 0 else 0.0

    if signal.direction == "long":
        stop   = price - stop_dist
        target = price + target_dist
    elif signal.direction == "short":
        stop   = price + stop_dist
        target = price - target_dist
    else:
        return f"~${price:.2f}"

    stop_pct   = abs(stop   - price) / price * 100
    target_pct = abs(target - price) / price * 100

    return (
        f"~${price:.2f}"
        f"  ·  Stop ~${stop:.2f} (-{stop_pct:.1f}%){atr_label}"
        f"  ·  Target ~${target:.2f} (+{target_pct:.1f}%)"
        f"  ·  R:R {rr}×"
    )


# ── Item 3 helper: sector breadth suffix ─────────────────────────────────────

def _format_sector_breadth(fmp_sectors: dict | None) -> str:
    """
    Return ' · avg +0.8% · 8/11 up' to append to the Regime line, or ''.
    """
    if not fmp_sectors:
        return ""
    try:
        avg   = fmp_sectors.get("avg_change", 0)
        pos   = fmp_sectors.get("positive_sectors", 0)
        neg   = fmp_sectors.get("negative_sectors", 0)
        total = pos + neg
        if total == 0:
            return ""
        sign = "+" if avg >= 0 else ""
        return f" · avg {sign}{avg:.1f}% · {pos}/{total} up"
    except Exception:
        return ""


# ── Technicals block (Items 2 + 5 added) ─────────────────────────────────────

def _format_technicals_block(
    checks: dict,
    fmp_rsi: float | None = None,
    stock_ctx: dict | None = None,
) -> str:
    """
    Format the 0-10 technical score breakdown as a compact Telegram block.

    v2 additions:
      Item 5 — FMP RSI cross-reference on the RSI row
      Item 2 — ADX / BB / vol regime on a new `Ctx` row (from stock_context)
    """
    score = checks.get("_technical_score")
    bd    = checks.get("_technical_score_breakdown")

    if score is None or not bd:
        return ""

    threshold    = bd.get("threshold", 7)
    passed       = bd.get("passed", score >= threshold)
    status_emoji = "✅" if passed else "❌"

    def bar(pts: int, max_pts: int) -> str:
        return "█" * pts + "░" * (max_pts - pts)

    lines = [f"📊 *Technicals: {score}/10* {status_emoji}"]

    # 1. Volume (2pts)
    v      = bd.get("volume", {})
    vpts   = v.get("pts", 0)
    detail = ""
    if v.get("vol_ratio") is not None:
        detail += f" · {v['vol_ratio']:.1f}× avg"
    obv = v.get("obv_trending_with_direction")
    if obv is not None:
        detail += f" · OBV {'✓' if obv else '✗'}"
    lines.append(f"`Vol  ` {bar(vpts, 2)} {vpts}/2{detail}")

    # 2. Multi-Timeframe (2pts)
    m    = bd.get("multi_timeframe", {})
    mpts = m.get("pts", 0)
    detail = ""
    d50 = m.get("daily_50ma_aligned")
    h20 = m.get("hourly_20ma_aligned")
    if d50 is not None:
        detail += f" · 50MA {'✓' if d50 else '✗'}"
    if h20 is not None:
        detail += f" · 1h-20 {'✓' if h20 else '✗'}"
    lines.append(f"`MTF  ` {bar(mpts, 2)} {mpts}/2{detail}")

    # 3. Key Levels (2pts)
    k    = bd.get("key_levels", {})
    kpts = k.get("pts", 0)
    detail = ""
    if k.get("pct_from_swing_high") is not None:
        detail += f" · {k['pct_from_swing_high']:.1f}% to high"
    elif k.get("pct_from_swing_low") is not None:
        detail += f" · {k['pct_from_swing_low']:.1f}% to low"
    if k.get("near_round_number"):
        detail += " · round#"
    lines.append(f"`Lvl  ` {bar(kpts, 2)} {kpts}/2{detail}")

    # 4. VWAP (1pt)
    vw    = bd.get("vwap", {})
    vwpts = vw.get("pts", 0)
    detail = ""
    pvw  = vw.get("price_vs_vwap")
    vval = vw.get("vwap")
    if pvw:
        detail += f" · {pvw}"
        if vval:
            detail += f" ${vval:.2f}"
    lines.append(f"`VWAP ` {bar(vwpts, 1)} {vwpts}/1{detail}")

    # 5. ATR (1pt)
    a    = bd.get("atr", {})
    apts = a.get("pts", 0)
    detail = ""
    ratio = a.get("move_atr_ratio")
    if ratio is not None:
        detail += f" · {ratio:.1f}× ATR"
        if not a.get("in_sweet_spot", True):
            detail += " ⚠️"
    lines.append(f"`ATR  ` {bar(apts, 1)} {apts}/1{detail}")

    # 6. RSI Divergence (1pt) — Item 5: FMP RSI cross-reference
    r       = bd.get("rsi_divergence", {})
    rpts    = r.get("pts", 0)
    detail  = ""
    rsi_val = r.get("current_rsi")
    div     = r.get("opposing_divergence")
    if rsi_val is not None:
        detail += f" · RSI {rsi_val:.0f}"
        if fmp_rsi is not None:
            # ≤5 point divergence between Polygon and FMP = confirming
            agree   = abs(fmp_rsi - rsi_val) <= 5
            detail += f" (FMP {fmp_rsi:.0f} {'✓' if agree else '≠'})"
    if div is not None:
        detail += f" · {'div ⚠️' if div else 'clean'}"
    lines.append(f"`RSI  ` {bar(rpts, 1)} {rpts}/1{detail}")

    # 7. Relative Strength (1pt)
    rs    = bd.get("relative_strength", {})
    rspts = rs.get("pts", 0)
    detail = ""
    s5   = rs.get("stock_5d_pct")
    spy5 = rs.get("spy_5d_pct")
    if s5 is not None and spy5 is not None:
        s_sign   = "+" if s5   >= 0 else ""
        spy_sign = "+" if spy5 >= 0 else ""
        detail += f" · {s_sign}{s5:.1f}% vs SPY {spy_sign}{spy5:.1f}%"
    lines.append(f"`RS   ` {bar(rspts, 1)} {rspts}/1{detail}")

    # ── Item 2: ADX + BB context row (from stock_context:{ticker}) ────────────
    if stock_ctx:
        adx_val  = stock_ctx.get("adx")
        trend    = stock_ctx.get("trend_regime", "")
        bb_trend = stock_ctx.get("bb_width_trend", "")
        vol_reg  = stock_ctx.get("volatility_regime", "")

        ctx_parts: list[str] = []
        if adx_val is not None:
            trend_label = {
                "TRENDING_UP":   "↑ trend",
                "TRENDING_DOWN": "↓ trend",
                "RANGING":       "ranging",
            }.get(trend, trend.lower())
            ctx_parts.append(f"ADX {adx_val:.1f} {trend_label}")
        if bb_trend:
            bb_label = {
                "EXPANDING":   "BB expanding",
                "CONTRACTING": "BB contracting",
                "STABLE":      "BB stable",
            }.get(bb_trend, bb_trend.lower())
            ctx_parts.append(bb_label)
        if vol_reg and vol_reg != "NORMAL":
            vol_label = {"EXPANDING": "vol ↑", "CONTRACTING": "vol ↓"}.get(
                vol_reg, vol_reg.lower()
            )
            ctx_parts.append(vol_label)

        if ctx_parts:
            lines.append(f"`Ctx  ` " + " · ".join(ctx_parts))

    return "\n".join(lines)


# ── Conviction bar ─────────────────────────────────────────────────────────────

def _conviction_bar(conviction: float) -> str:
    """Visual conviction bar: ████░░ 65%"""
    filled = int(conviction * 10)
    empty  = 10 - filled
    return "█" * filled + "░" * empty


# ── Service class ─────────────────────────────────────────────────────────────

class TelegramAlertsService(BaseConsumer):

    def __init__(self) -> None:
        self._http:       httpx.AsyncClient | None   = None
        self._redis:      aioredis.Redis   | None   = None   # context fetches
        self._bot_token:  str = ""
        self._chat_id:    str = ""
        super().__init__()

    @property
    def service_name(self) -> str:
        return "telegram_alerts"

    @property
    def input_topic(self) -> str:
        return "signals.filtered"

    @property
    def output_topic(self) -> str:
        return "signals.filtered"   # pass-through — no downstream topic

    async def on_start(self) -> None:
        self._bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        self._chat_id   = os.environ.get("TELEGRAM_CHAT_ID", "")

        if not self._bot_token or not self._chat_id:
            _log("warning", "telegram_alerts.not_configured",
                 msg="TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set — alerts disabled")
            return

        self._http = httpx.AsyncClient(
            base_url=f"https://api.telegram.org/bot{self._bot_token}",
            timeout=10.0,
        )

        # Redis client — used to enrich alerts with stock_context, fmp:sectors,
        # fmp:technical, and current price.  Failure is non-fatal.
        try:
            self._redis = aioredis.from_url(
                settings.redis_url,
                decode_responses=True,
                max_connections=4,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
        except Exception as e:
            _log("warning", "telegram_alerts.redis_unavailable", error=str(e))

        # Verify bot connectivity
        try:
            resp = await self._http.get("/getMe")
            if resp.status_code == 200:
                bot_name = resp.json()["result"]["username"]
                _log("info", "telegram_alerts.connected", bot=bot_name)
            else:
                _log("warning", "telegram_alerts.connection_failed",
                     status=resp.status_code)
        except Exception as e:
            _log("warning", "telegram_alerts.connection_error", error=str(e))

    async def on_stop(self) -> None:
        if self._http:
            await self._http.aclose()
        if self._redis:
            await self._redis.aclose()

    async def process(self, record: dict) -> dict | None:
        try:
            signal = TradingSignal.model_validate(record)
        except Exception as e:
            _log("error", "telegram_alerts.parse_error", error=str(e))
            raise

        # Skip neutral signals unless extremely high conviction + impact.
        if signal.direction.lower() == "neutral":
            if signal.conviction < 0.85 or signal.impact_day < 0.70:
                return None

        # Filter by conviction threshold
        if signal.conviction < ALERT_CONVICTION_THRESHOLD:
            return None

        if self._http and self._bot_token and self._chat_id:
            tech_checks = record.get("filter_technicals", {}).get("checks", {})
            await self._send_alert(signal, tech_checks, record=record)

        return None   # pass-through; don't re-emit to Kafka

    async def _fetch_context(self, signal: TradingSignal) -> dict:
        """
        Fetch enrichment data from Redis in a single mget call.

        Keys (all optional — missing data is silently skipped):
          stock_context:{ticker}  → ADX, BB, ATR, vol regime  (Items 2, 6)
          fmp:sectors             → sector breadth / avg       (Item 3)
          fmp:technical:{ticker}  → FMP RSI cross-reference    (Item 5)
          price:{ticker}:last     → last trade price            (Item 6)
        """
        if not self._redis:
            return {}

        t = signal.ticker
        keys = [
            f"stock_context:{t}",
            "fmp:sectors",
            f"fmp:technical:{t}",
            f"price:{t}:last",
        ]

        try:
            values = await self._redis.mget(*keys)
        except Exception as e:
            _log("debug", "telegram_alerts.context_fetch_error",
                 ticker=t, error=str(e))
            return {}

        ctx: dict = {}

        if values[0]:
            try:
                ctx["stock_context"] = json.loads(values[0])
            except Exception:
                pass

        if values[1]:
            try:
                ctx["fmp_sectors"] = json.loads(values[1])
            except Exception:
                pass

        if values[2]:
            try:
                ctx["fmp_technical"] = json.loads(values[2])
            except Exception:
                pass

        if values[3]:
            try:
                ctx["current_price"] = float(values[3])
            except Exception:
                pass

        return ctx

    def _enrich_context_with_record(self, ctx: dict, record: dict) -> dict:
        """
        Merge stage_timestamps from the raw Kafka record into the context dict.
        Called in _send_alert() so the slow-pipeline warning has timing data.
        """
        stage_ts = record.get("stage_timestamps")
        if stage_ts:
            ctx["stage_timestamps"] = stage_ts
        return ctx

    async def _send_alert(
        self,
        signal:      TradingSignal,
        tech_checks: dict | None = None,
        record:      dict | None = None,
    ) -> None:
        """Fetch context, format, and send the Telegram alert."""
        context = await self._fetch_context(signal)
        # Merge stage_timestamps from the raw Kafka record so the slow-pipeline
        # warning in format_signal_message() has the full timing chain.
        if record:
            self._enrich_context_with_record(context, record)
        message = format_signal_message(signal, tech_checks=tech_checks, context=context)

        try:
            resp = await self._http.post("/sendMessage", json={
                "chat_id":                self._chat_id,
                "text":                   message,
                "parse_mode":             "Markdown",
                "disable_web_page_preview": True,
            })

            if resp.status_code == 200:
                _log("info", "telegram_alerts.sent",
                     ticker=signal.ticker,
                     direction=signal.direction,
                     conviction=signal.conviction,
                     signal_type=signal.signal_type)

            elif resp.status_code == 429:
                retry_after = resp.json().get("parameters", {}).get("retry_after", 30)
                _log("warning", "telegram_alerts.rate_limited",
                     retry_after=retry_after)
                await asyncio.sleep(retry_after)
                # Retry once after backoff
                await self._http.post("/sendMessage", json={
                    "chat_id":    self._chat_id,
                    "text":       message,
                    "parse_mode": "Markdown",
                })

            else:
                _log("error", "telegram_alerts.send_failed",
                     status=resp.status_code,
                     body=resp.text[:200])

        except Exception as e:
            _log("error", "telegram_alerts.send_error",
                 ticker=signal.ticker, error=str(e))
