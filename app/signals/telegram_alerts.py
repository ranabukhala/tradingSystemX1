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
    "other":      "📰 News",
}


def format_signal_message(signal: TradingSignal) -> str:
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

    lines.append(f"")
    lines.append(f"📰 _{signal.news_title}_")

    if signal.t1_summary:
        lines.append(f"")
        lines.append(f"*Facts:* {signal.t1_summary}")

    if signal.t2_summary:
        lines.append(f"")
        lines.append(f"💡 {signal.t2_summary}")

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

    lines.append(f"")
    lines.append(f"🆔 `{str(signal.id)[:8]}`")

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
        return "signals.actionable"

    @property
    def output_topic(self) -> str:
        return "signals.actionable"  # Pass-through, no downstream topic

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

        # Filter by conviction threshold
        if signal.conviction < ALERT_CONVICTION_THRESHOLD:
            return None

        # Skip neutral signals unless very high conviction
        if signal.direction == "neutral" and signal.conviction < 0.75:
            return None

        # Send alert
        if self._http and self._bot_token and self._chat_id:
            await self._send_alert(signal)

        # Pass-through — don't re-emit to Kafka
        return None

    async def _send_alert(self, signal: TradingSignal) -> None:
        """Send formatted alert to Telegram."""
        message = format_signal_message(signal)

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
