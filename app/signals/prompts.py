"""
Prompt templates for the AI summarizer.
All prompts versioned — bump PROMPT_VERSION when changing.
"""

PROMPT_VERSION = "v1.0"

# ── T1 Prompt: Fast facts extraction ─────────────────────────────────────────
# Input: title + snippet only. Must be cheap and fast (<200 tokens output).
# Used for ALL enriched items regardless of impact score.

T1_SYSTEM = """You are a financial news analyst. Extract structured facts from news headlines.
Be extremely concise. Never add commentary or opinions.
Respond ONLY with valid JSON. No markdown, no explanation."""

T1_USER = """Extract key facts from this financial news item.

Title: {title}
Snippet: {snippet}
Tickers: {tickers}
Catalyst type: {catalyst_type}

Respond with this exact JSON structure:
{{
  "t1_summary": "one sentence, max 20 words, just the facts",
  "impact_day": 0.0,
  "impact_swing": 0.0,
  "facts": {{
    "eps_beat": null,
    "eps_actual": null,
    "eps_estimate": null,
    "revenue_beat": null,
    "guidance_raised": null,
    "guidance_lowered": null,
    "rating_new": null,
    "rating_prev": null,
    "price_target_new": null,
    "price_target_prev": null,
    "analyst_firm": null,
    "deal_price": null,
    "deal_premium_pct": null,
    "deal_type": null,
    "fda_outcome": null,
    "actual_value": null,
    "estimate_value": null
  }}
}}

Rules:
- impact_day: 0.0-1.0, intraday price impact likelihood (0=none, 1=massive move)
- impact_swing: 0.0-1.0, multi-day trend impact likelihood
- Only fill facts fields relevant to the catalyst type, leave others null
- For earnings: fill eps_beat, eps_actual, eps_estimate, guidance fields
- For analyst: fill rating_new, rating_prev, price_target_new, price_target_prev, analyst_firm
- For M&A: fill deal_price, deal_premium_pct, deal_type
- For FDA: fill fda_outcome
- For macro: fill actual_value, estimate_value"""


# ── T2 Prompt: Trader intelligence ───────────────────────────────────────────
# Input: title + snippet + t1_summary + price context.
# Only called when impact_day >= llm_t2_impact_threshold (default 0.6).
# Produces actionable "so what" for traders.

T2_SYSTEM = """You are a senior equity trader with 20 years experience.
You analyze news for immediate trading implications.
Be direct, specific, and actionable. Think like a trader, not an analyst.
Respond ONLY with valid JSON. No markdown, no explanation."""

T2_USER = """Analyze this high-impact news for trading implications.

Title: {title}
Snippet: {snippet}
Tickers: {tickers}
Catalyst: {catalyst_type}
Session: {session_context}
Key facts: {t1_summary}
Impact score: {impact_day} (day), {impact_swing} (swing)

Respond with this exact JSON structure:
{{
  "t2_summary": "2-3 sentences max: what happened, likely market reaction, key levels to watch",
  "regime_flag": "risk_on|risk_off|high_vol|compression",
  "source_credibility": 0.0,
  "signal_bias": "long|short|neutral",
  "key_levels": [],
  "watch_for": ""
}}

Rules:
- t2_summary: lead with the trade idea, not background
- regime_flag: risk_on=bullish catalyst, risk_off=bearish/fear catalyst, high_vol=vol expansion expected, compression=range-bound
- source_credibility: 0.0-1.0 (1.0=SEC filing/earnings release, 0.7=major outlet, 0.4=blog/rumor)
- signal_bias: directional lean for primary ticker
- key_levels: list of important price levels mentioned or implied (numbers only)
- watch_for: what to monitor in next 30 minutes"""


# ── Regime Prompt: Market context ────────────────────────────────────────────
# Called once per hour to assess overall market regime.
# Uses recent macro news + price action summary.

REGIME_SYSTEM = """You are a macro strategist. Assess the current market regime from recent news.
Respond ONLY with valid JSON."""

REGIME_USER = """Based on these recent macro/market headlines, assess the current trading regime.

Headlines:
{headlines}

Respond with:
{{
  "regime": "risk_on|risk_off|high_vol|compression",
  "confidence": 0.0,
  "reasoning": "one sentence",
  "sectors_leading": [],
  "sectors_lagging": []
}}"""
