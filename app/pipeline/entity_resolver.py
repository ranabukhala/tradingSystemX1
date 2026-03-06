"""
Entity Resolver Service — Stage 3 of the processing pipeline.

Consumes: news.deduped
Emits:    news.enriched

Responsibilities:
  1. Ticker resolution     — validate raw_tickers against known universe,
                             score confidence, add missed tickers from title scan
  2. Catalyst classification — map raw_categories to CatalystType enum
  3. Mode detection        — stock_specific vs general_market
  4. Session context       — premarket / open / afterhours / overnight
  5. Market cap tier       — lookup from entity table or known universe
  6. Earnings proximity    — hours to nearest earnings event ± window
  7. Decay estimation      — how long the edge likely lasts
"""
from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from typing import Any

import pytz

from app.config import settings
from app.models.news import (
    CatalystType, DedupedRecord, EnrichedRecord,
    FloatSensitivity, MarketCapTier, NewsMode, SessionContext,
)
from app.pipeline.base_consumer import BaseConsumer, _log

ET = pytz.timezone("America/New_York")

# ── Known ticker universe (S&P 500 subset + high-volume small caps) ──────────
# This is a static seed. In production you'd load from a DB table or vendor API.
KNOWN_TICKERS: set[str] = {
    # Mega cap
    "AAPL","MSFT","NVDA","GOOGL","GOOG","META","AMZN","TSLA","BRK.B","LLY",
    "V","JPM","UNH","XOM","JNJ","WMT","MA","PG","AVGO","HD",
    # Large cap tech
    "AMD","INTC","QCOM","TXN","MU","AMAT","LRCX","KLAC","MRVL","ORCL",
    "CRM","NOW","SNOW","DDOG","NET","CRWD","ZS","OKTA","PANW","FTNT",
    # Financials
    "GS","MS","BAC","WFC","C","BLK","SCHW","AXP","COF","USB",
    # Healthcare/Biotech
    "PFE","MRNA","ABBV","BMY","GILD","REGN","BIIB","VRTX","ISRG","MDT",
    # Consumer
    "COST","TGT","SBUX","MCD","NKE","LULU","DECK","TPR",
    # Energy
    "CVX","SLB","COP","EOG","PXD","MPC","VLO","PSX",
    # EV/Auto
    "GM","F","RIVN","LCID","NIO","LI","XPEV",
    # ETFs
    "SPY","QQQ","IWM","DIA","GLD","SLV","USO","TLT","HYG","XLF",
    "XLK","XLE","XLV","XLI","ARKK","SOXS","SOXL","TQQQ","SQQQ",
    # High-volume meme/momentum
    "GME","AMC","BBBY","PLTR","SOFI","HOOD","COIN","MSTR","RIOT","MARA",
    "CLSK","HUT","BITF","CIFR","WULF",
    # Crypto proxies
    "IBIT","FBTC","GBTC","ETHE",
}

# Tickers that appear in common English words — never auto-extract these
TICKER_BLACKLIST = {
    "A","I","IT","AT","BE","BY","DO","GO","IF","IN","IS","ME","MY",
    "NO","OF","OK","ON","OR","SO","TO","UP","US","WE","AI","AR",
    "HE","HI","AN","AS","AT","BY","HE","HI",
}

# Market cap tiers (approximate, for known large names)
MARKET_CAP_TIER_MAP: dict[str, MarketCapTier] = {
    **{t: MarketCapTier.MEGA for t in [
        "AAPL","MSFT","NVDA","GOOGL","GOOG","META","AMZN","TSLA","BRK.B",
        "LLY","V","JPM","UNH","XOM","JNJ","WMT","MA","PG","AVGO","HD",
    ]},
    **{t: MarketCapTier.LARGE for t in [
        "AMD","INTC","QCOM","ORCL","CRM","NOW","SNOW","GS","MS","BAC",
        "WFC","PFE","MRNA","ABBV","COST","TGT","CVX","SLB","GM","F",
    ]},
    **{t: MarketCapTier.MID for t in [
        "RIVN","LCID","PLTR","SOFI","HOOD","COIN","MSTR","RIOT","MARA",
    ]},
    **{t: MarketCapTier.SMALL for t in [
        "GME","AMC","CLSK","HUT","BITF","CIFR","WULF",
    ]},
}

# Low-float / high-sensitivity tickers
HIGH_FLOAT_SENSITIVITY: set[str] = {
    "GME","AMC","BBBY","MSTR","COIN","RIOT","MARA","CLSK","HUT",
    "BITF","CIFR","WULF","RIVN","LCID","SOFI","HOOD",
}

# Decay estimates by catalyst type (minutes)
DECAY_MINUTES: dict[CatalystType, int] = {
    CatalystType.EARNINGS:    240,   # 4h — earnings fades after initial move
    CatalystType.ANALYST:     120,   # 2h — analyst moves fade quickly
    CatalystType.MA:          1440,  # 24h — M&A is sticky
    CatalystType.REGULATORY:  480,   # 8h — FDA etc lasts longer
    CatalystType.FILING:       60,   # 1h — SEC filings mostly noise
    CatalystType.MACRO:       180,   # 3h — macro context shifts
    CatalystType.OTHER:        60,
}

# Catalyst keyword patterns for title-based classification
CATALYST_PATTERNS: list[tuple[re.Pattern, CatalystType]] = [
    (re.compile(r'\b(eps|earnings|revenue|beat|miss|guidance|profit|loss|q[1-4]\b)', re.I), CatalystType.EARNINGS),
    (re.compile(r'\b(upgrade|downgrade|price target|overweight|underweight|buy|sell|hold|outperform|neutral)\b', re.I), CatalystType.ANALYST),
    (re.compile(r'\b(acqui|merger|buyout|takeover|deal|bid)\w*\b', re.I), CatalystType.MA),
    (re.compile(r'\b(fda|approval|trial|phase [123]|nda|bla|anda|pdufa)\b', re.I), CatalystType.REGULATORY),
    (re.compile(r'\b(sec|8-k|10-k|10-q|proxy|insider|filing)\b', re.I), CatalystType.FILING),
    (re.compile(r'\b(fed|fomc|cpi|inflation|gdp|jobs|payroll|interest rate|powell|yellen)\b', re.I), CatalystType.MACRO),
]

# Category string → CatalystType
CATEGORY_TO_CATALYST: dict[str, CatalystType] = {
    "earnings": CatalystType.EARNINGS,
    "analyst":  CatalystType.ANALYST,
    "analyst-ratings": CatalystType.ANALYST,
    "ma":       CatalystType.MA,
    "regulatory": CatalystType.REGULATORY,
    "filing":   CatalystType.FILING,
    "sec":      CatalystType.FILING,
    "macro":    CatalystType.MACRO,
    "economics": CatalystType.MACRO,
}


def get_session_context(dt: datetime) -> SessionContext:
    """Classify market session from UTC datetime."""
    et_time = dt.astimezone(ET).time()
    if et_time < _t("04:00") or et_time >= _t("20:00"):
        return SessionContext.OVERNIGHT
    elif et_time < _t("09:30"):
        return SessionContext.PREMARKET
    elif et_time < _t("16:00"):
        return SessionContext.INTRADAY
    else:
        return SessionContext.AFTERHOURS


def _t(s: str):
    from datetime import time
    h, m = s.split(":")
    return __import__("datetime").time(int(h), int(m))


def classify_catalyst(raw_categories: list[str], title: str) -> CatalystType:
    """Determine catalyst type from categories, then fall back to title scan."""
    for cat in raw_categories:
        if result := CATEGORY_TO_CATALYST.get(cat.lower()):
            return result
    for pattern, catalyst in CATALYST_PATTERNS:
        if pattern.search(title):
            return catalyst
    return CatalystType.OTHER


def resolve_tickers(raw_tickers: list[str], title: str) -> tuple[list[str], dict[str, float]]:
    """
    Validate and score tickers.
    Returns (resolved_tickers, confidence_map).
    """
    confidence: dict[str, float] = {}

    # Vendor-provided tickers get high confidence
    for t in raw_tickers:
        t = t.upper().strip()
        if t and t not in TICKER_BLACKLIST and (t in KNOWN_TICKERS or len(t) <= 5):
            confidence[t] = 0.90

    # Scan title for $TICKER or ALL-CAPS 1-5 char words that look like tickers
    title_tickers = re.findall(r'\$([A-Z]{1,5})', title)
    for t in title_tickers:
        if t not in TICKER_BLACKLIST:
            confidence[t] = max(confidence.get(t, 0), 0.95)  # $ prefix = high confidence

    # Scan for bare uppercase words that match known universe
    words = re.findall(r'\b([A-Z]{1,5})\b', title)
    for t in words:
        if t in KNOWN_TICKERS and t not in TICKER_BLACKLIST:
            confidence[t] = max(confidence.get(t, 0), 0.75)

    # Filter to confidence >= 0.7
    resolved = [t for t, c in confidence.items() if c >= 0.70]
    return resolved, confidence


def detect_mode(tickers: list[str], catalyst: CatalystType) -> NewsMode:
    """Stock-specific if has tickers and non-macro catalyst."""
    if tickers and catalyst != CatalystType.MACRO:
        return NewsMode.STOCK_SPECIFIC
    return NewsMode.GENERAL_MARKET


def estimate_decay(catalyst: CatalystType, mode: NewsMode, session: SessionContext) -> int:
    """Estimate edge decay in minutes."""
    base = DECAY_MINUTES.get(catalyst, 60)
    # Premarket news has longer lasting impact
    if session == SessionContext.PREMARKET:
        base = int(base * 1.5)
    # Macro news affects everything longer
    if mode == NewsMode.GENERAL_MARKET:
        base = int(base * 1.2)
    return base


class EntityResolverService(BaseConsumer):

    def __init__(self) -> None:
        self._upcoming_earnings: dict[str, datetime] = {}
        self._earnings_loaded_at: datetime | None = None
        self._db_session = None
        super().__init__()

    @property
    def service_name(self) -> str:
        return "entity_resolver"

    @property
    def input_topic(self) -> str:
        return settings.topic_news_deduped

    @property
    def output_topic(self) -> str:
        return settings.topic_news_enriched

    async def on_start(self) -> None:
        from app.db import get_engine
        from sqlalchemy.ext.asyncio import AsyncSession
        from sqlalchemy.orm import sessionmaker
        engine = get_engine()
        self._Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        await self._load_earnings_calendar()
        _log("info", "entity_resolver.ready",
             earnings_loaded=len(self._upcoming_earnings))

    async def _load_earnings_calendar(self) -> None:
        """Load upcoming earnings events from DB into memory."""
        if not self._Session:
            return
        try:
            from sqlalchemy import text
            async with self._Session() as session:
                result = await session.execute(text("""
                    SELECT ticker, event_date, event_time
                    FROM event
                    WHERE event_type = 'earnings'
                      AND event_date >= CURRENT_DATE
                      AND event_date <= CURRENT_DATE + INTERVAL '14 days'
                """))
                rows = result.fetchall()

            self._upcoming_earnings = {}
            for row in rows:
                ticker = row.ticker
                # Approximate datetime: BMO = 9:30 AM ET, AMC = 4:30 PM ET
                event_dt = datetime.combine(row.event_date, __import__("datetime").time(9, 30))
                if row.event_time == "AMC":
                    event_dt = datetime.combine(row.event_date, __import__("datetime").time(16, 30))
                self._upcoming_earnings[ticker] = ET.localize(event_dt).astimezone(timezone.utc)

            self._earnings_loaded_at = datetime.now(timezone.utc)
            _log("info", "entity_resolver.earnings_loaded",
                 count=len(self._upcoming_earnings))
        except Exception as e:
            _log("warning", "entity_resolver.earnings_load_error", error=str(e))

    def _earnings_proximity_hours(self, tickers: list[str], published_at: datetime) -> int | None:
        """Return hours to nearest earnings event (negative = already happened)."""
        min_delta = None
        for ticker in tickers:
            if ticker in self._upcoming_earnings:
                delta_h = (self._upcoming_earnings[ticker] - published_at).total_seconds() / 3600
                if min_delta is None or abs(delta_h) < abs(min_delta):
                    min_delta = delta_h
        return int(min_delta) if min_delta is not None else None

    async def process(self, record: dict) -> dict | None:
        try:
            deduped = DedupedRecord.from_kafka_dict(record)
        except Exception as e:
            _log("error", "entity_resolver.parse_error", error=str(e))
            raise

        # Reload earnings calendar every 30 minutes
        if (self._earnings_loaded_at is None or
                (datetime.now(timezone.utc) - self._earnings_loaded_at).seconds > 1800):
            await self._load_earnings_calendar()

        # 1. Resolve tickers
        tickers, ticker_confidence = resolve_tickers(
            deduped.raw_tickers, deduped.title)

        # 2. Classify catalyst
        catalyst = classify_catalyst(deduped.raw_categories, deduped.title)

        # 3. Session context
        session_ctx = get_session_context(deduped.published_at)

        # 4. Mode
        mode = detect_mode(tickers, catalyst)

        # 5. Market cap tier (primary ticker)
        market_cap_tier = None
        if tickers:
            market_cap_tier = MARKET_CAP_TIER_MAP.get(tickers[0])

        # 6. Float sensitivity
        float_sensitivity = FloatSensitivity.NORMAL
        if any(t in HIGH_FLOAT_SENSITIVITY for t in tickers):
            float_sensitivity = FloatSensitivity.HIGH

        # 7. Earnings proximity
        earnings_proximity_h = self._earnings_proximity_hours(tickers, deduped.published_at)

        # 8. Decay estimate
        decay = estimate_decay(catalyst, mode, session_ctx)

        enriched = EnrichedRecord(
            id=deduped.id,
            source=deduped.source,
            vendor_id=deduped.vendor_id,
            published_at=deduped.published_at,
            received_at=deduped.received_at,
            url=deduped.url,
            canonical_url=deduped.canonical_url,
            title=deduped.title,
            snippet=deduped.snippet,
            author=deduped.author,
            content_hash=deduped.content_hash,
            cluster_id=deduped.cluster_id,
            is_representative=deduped.is_representative,
            tickers=tickers,
            ticker_confidence=ticker_confidence,
            catalyst_type=catalyst,
            mode=mode,
            session_context=session_ctx,
            market_cap_tier=market_cap_tier,
            float_sensitivity=float_sensitivity,
            short_interest_flag=any(t in HIGH_FLOAT_SENSITIVITY for t in tickers),
            earnings_proximity_h=earnings_proximity_h,
            decay_minutes=decay,
        )

        return enriched.to_kafka_dict()
