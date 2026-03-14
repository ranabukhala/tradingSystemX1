"""
Telegram Alert Service — Stage 6 of the pipeline.

Consumes: signals.actionable
Pushes:   Formatted alerts to Telegram bot

Message format:
  🟢 LONG · AAPL · event_driven
  Conviction: 87% | Impact: 0.82 | Decay: 4h
  📰 Apple beats EPS by $0.12, raises FY guidance
  💡 Gap up expected at open. Watch $195 resistance.
  Session: premarket | Catalyst: earnings
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone

import httpx

# Override via SIGNAL_ALERT_THRESHOLD env var
ALERT_CONVICTION_THRESHOLD = float(os.environ.get("SIGNAL_ALERT_THRESHOLD", "0.60"))

from app.config import settings
from app.pipeline.base_consumer import BaseConsumer, _log
from app.signals.signal_aggregator import TradingSignal



# Direction emoji
DIRECTION_EMOJI = {
    "long":    "🟢",
    "short":   "🔴",
    "neutral": "🟡",
}

# Session labels
SESSION_LABELS = {
    "premarket":  "🌅 Pre-market",
    "open":       "🔔 Market open",
    "intraday":   "📈 Intraday",
    "afterhours": "🌙 After-hours",
    "overnight":  "🌃 Overnight",
}

# Catalyst labels
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


def format_signal_message(signal: TradingSignal, tech_checks: dict | None = None) -> str:
    """Format a trading signal into a Telegram message."""
    direction_emoji = DIRECTION_EMOJI.get(signal.direction, "⚪")
    direction_label = signal.direction.upper()
    session = SESSION_LABELS.get(signal.session_context, signal.session_context)
    catalyst = CATALYST_LABELS.get(signal.catalyst_type, signal.catalyst_type)

    conviction_pct = int(signal.conviction * 100)
    conviction_bar = _conviction_bar(signal.conviction)

    decay_str = ""
    if signal.decay_minutes:
        if signal.decay_minutes < 60:
            decay_str = f"{signal.decay_minutes}m"
        else:
            decay_str = f"{signal.decay_minutes // 60}h"

    # Header
    lines = [
        f"{direction_emoji} *{direction_label} · {signal.ticker} · {signal.signal_type}*",
        f"",
        f"Conviction: {conviction_bar} {conviction_pct}%",
        f"Impact: day={signal.impact_day:.2f} swing={signal.impact_swing:.2f}",
    ]

    if decay_str:
        lines.append(f"Edge decay: ~{decay_str}")

    # ── Technical scoring block ────────────────────────────────────────────────
    tech_block = _format_technicals_block(tech_checks or {})
    if tech_block:
        lines.append(f"")
        lines.extend(tech_block.splitlines())

    lines.append(f"")
    lines.append(f"📰 _{signal.news_title}_")

    if signal.t1_summary:
        lines.append(f"")
        lines.append(f"*Facts:* {signal.t1_summary}")

    if signal.t2_summary:
        lines.append(f"")
        # Sympathy signals get a different header
        if getattr(signal, "is_sympathy", False):
            lines.append(f"↗️ {signal.t2_summary}")
        else:
            lines.append(f"💡 {signal.t2_summary}")

    # Priced-in status
    if getattr(signal, "priced_in", None) and not getattr(signal, "is_sympathy", False):
        priced_emoji = {"yes": "🔴", "partially": "🟡", "no": "🟢"}.get(signal.priced_in, "⚪")
        lines.append(f"")
        lines.append(f"{priced_emoji} Priced in: {signal.priced_in.title()}")
        if getattr(signal, "priced_in_reason", None):
            lines.append(f"   _{signal.priced_in_reason}_")

    # Sympathy plays
    if getattr(signal, "sympathy_plays", None) and not getattr(signal, "is_sympathy", False):
        plays = " · ".join(signal.sympathy_plays)
        lines.append(f"")
        lines.append(f"🔗 Sympathy: {plays}")

    lines.append(f"")
    lines.append(f"{session} · {catalyst}")

    if signal.regime_flag:
        regime_emoji = {"risk_on": "🚀", "risk_off": "🛡️",
                        "high_vol": "⚡", "compression": "🔄"}.get(signal.regime_flag, "")
        lines.append(f"Regime: {regime_emoji} {signal.regime_flag.replace('_', ' ').title()}")

    if signal.market_cap_tier:
        tier_emoji = {"mega": "🏔️", "large": "🏢", "mid": "🏬",
                      "small": "🏪", "micro": "🏠"}.get(signal.market_cap_tier, "")
        lines.append(f"Cap tier: {tier_emoji} {signal.market_cap_tier.title()}")

    # Timing window (observe only)
    tw_label   = getattr(signal, "time_window_label", None)
    tw_emoji   = getattr(signal, "time_window_emoji", "")
    tw_mult    = getattr(signal, "time_window_mult", None)
    tw_quality = getattr(signal, "time_window_quality", None)
    if tw_label:
        tw_warn = " ⚠️" if tw_quality == "poor" else ""
        mult_str = ""
        if tw_mult is not None:
            delta = (tw_mult - 1) * 100
            sign = "+" if delta >= 0 else ""
            mult_str = f" ({sign}{delta:.0f}% if enabled)"
        lines.append(f"")
        lines.append(f"🕐 _Timing:_ {tw_emoji} {tw_label}{tw_warn}{mult_str}")

    lines.append(f"")
    lines.append(f"🆔 `{str(signal.id)[:8]}`")

    return "\n".join(lines)


def _format_technicals_block(checks: dict) -> str:
    """
    Format the 0-10 technical score breakdown as a compact Telegram block.

    Input: record["filter_technicals"]["checks"]
    Returns: multi-line string ready for inline inclusion, or "" if no data.
    """
    score = checks.get("_technical_score")
    bd    = checks.get("_technical_score_breakdown")

    if score is None or not bd:
        return ""

    threshold    = bd.get("threshold", 7)
    passed       = bd.get("passed", score >= threshold)
    status_emoji = "✅" if passed else "❌"

    def bar(pts: int, max_pts: int) -> str:
        """Compact filled/empty bar: ██░ for 2/3."""
        return "█" * pts + "░" * (max_pts - pts)

    lines = [f"📊 *Technicals: {score}/10* {status_emoji}"]

    # 1. Volume (2pts)
    v    = bd.get("volume", {})
    vpts = v.get("pts", 0)
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

    # 6. RSI Divergence (1pt)
    r    = bd.get("rsi_divergence", {})
    rpts = r.get("pts", 0)
    detail = ""
    rsi_val = r.get("current_rsi")
    div     = r.get("opposing_divergence")
    if rsi_val is not None:
        detail += f" · RSI {rsi_val:.0f}"
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

    return "\n".join(lines)


def _conviction_bar(conviction: float) -> str:
    """Visual conviction bar: ████░░ 65%"""
    filled = int(conviction * 10)
    empty = 10 - filled
    return "█" * filled + "░" * empty


class TelegramAlertsService(BaseConsumer):

    def __init__(self) -> None:
        self._http: httpx.AsyncClient | None = None
        self._bot_token: str = ""
        self._chat_id: str = ""
        super().__init__()

    @property
    def service_name(self) -> str:
        return "telegram_alerts"

    @property
    def input_topic(self) -> str:
        return "signals.filtered"

    @property
    def output_topic(self) -> str:
        return "signals.filtered"  # Pass-through, no downstream topic

    async def on_start(self) -> None:
        import os
        self._bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        self._chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")

        if not self._bot_token or not self._chat_id:
            _log("warning", "telegram_alerts.not_configured",
                 msg="TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set — alerts disabled")
            return

        self._http = httpx.AsyncClient(
            base_url=f"https://api.telegram.org/bot{self._bot_token}",
            timeout=10.0,
        )

        # Test connection
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

    async def process(self, record: dict) -> dict | None:
        try:
            signal = TradingSignal.model_validate(record)
        except Exception as e:
            _log("error", "telegram_alerts.parse_error", error=str(e))
            raise

        # Skip neutral signals unless extremely high conviction + impact.
        # Neutral signals are noise 99% of the time — only alert on the rare
        # case where the model is very confident AND the event is high-impact.
        # Must come before the base conviction filter so low-conviction neutrals
        # don't accidentally pass if ALERT_CONVICTION_THRESHOLD is lowered.
        if signal.direction.lower() == "neutral":
            if signal.conviction < 0.85 or signal.impact_day < 0.70:
                return None  # drop silently, no alert

        # Filter by conviction threshold
        if signal.conviction < ALERT_CONVICTION_THRESHOLD:
            return None

        # Send alert
        if self._http and self._bot_token and self._chat_id:
            # Pull the technical checks from the raw record before the Pydantic
            # model drops the filter_technicals field (it's not declared on TradingSignal)
            tech_checks = record.get("filter_technicals", {}).get("checks", {})
            await self._send_alert(signal, tech_checks)

        # Pass-through — don't re-emit to Kafka
        return None

    async def _send_alert(self, signal: TradingSignal, tech_checks: dict | None = None) -> None:
        """Send formatted alert to Telegram."""
        message = format_signal_message(signal, tech_checks=tech_checks)

        try:
            resp = await self._http.post("/sendMessage", json={
                "chat_id": self._chat_id,
                "text": message,
                "parse_mode": "Markdown",
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
                # Retry once
                await self._http.post("/sendMessage", json={
                    "chat_id": self._chat_id,
                    "text": message,
                    "parse_mode": "Markdown",
                })
            else:
                _log("error", "telegram_alerts.send_failed",
                     status=resp.status_code,
                     body=resp.text[:200])

        except Exception as e:
            _log("error", "telegram_alerts.send_error",
                 ticker=signal.ticker, error=str(e))
