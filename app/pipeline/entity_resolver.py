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

import json
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
    # Aerospace/Defense
    "BA","LMT","RTX","NOC","GD","LHX",
    # Airlines/Transport
    "DAL","UAL","AAL","LUV","FDX","UPS","UBER","LYFT",
    # Automotive
    "TM",
    # Big Tech (gaps)
    "NFLX","SPOT","PINS","SNAP","SHOP","ABNB","DASH","RBLX","TWLO","ROKU",
    "SQ","PYPL","ADBE","CSCO","IBM",
    # Semiconductors (gaps)
    "ARM","ASML","TSM",
    # Healthcare/Pharma (gaps)
    "MRK","AMGN","NVO","AZN",
    # Energy (gaps)
    "HAL",
    # Retail/Consumer (gaps)
    "LOW","DLTR","DG","KR","CMG","YUM","DIS",
    # Telecom/Media
    "T","VZ","TMUS","CMCSA","WBD",
    # Industrials
    "CAT","DE","MMM","HON","GE",
    # AI/Cloud (gaps)
    "PATH","NBIS",
}

# Tickers that appear in common English words — never auto-extract these
TICKER_BLACKLIST = {
    "A","I","IT","AT","BE","BY","DO","GO","IF","IN","IS","ME","MY",
    "NO","OF","OK","ON","OR","SO","TO","UP","US","WE","AI","AR",
    "HE","HI","AN","AS","AT","BY","HE","HI",
}

# Company name → ticker mapping for title-based detection.
# Keys are lowercase substrings matched with word-boundary regex against title.lower().
# Catches prose mentions like "Okta reports earnings" → OKTA (score 0.90).
COMPANY_NAME_MAP: dict[str, str] = {
    # Mega cap
    "apple":              "AAPL",
    "microsoft":          "MSFT",
    "nvidia":             "NVDA",
    "alphabet":           "GOOGL",
    "google":             "GOOGL",
    "meta":               "META",
    "amazon":             "AMZN",
    "tesla":              "TSLA",
    # Large cap tech
    "intel":              "INTC",
    "qualcomm":           "QCOM",
    "broadcom":           "AVGO",
    "oracle":             "ORCL",
    "salesforce":         "CRM",
    "servicenow":         "NOW",
    "snowflake":          "SNOW",
    "datadog":            "DDOG",
    "cloudflare":         "NET",
    "crowdstrike":        "CRWD",
    "zscaler":            "ZS",
    "okta":               "OKTA",
    "palo alto":          "PANW",
    "fortinet":           "FTNT",
    "netflix":            "NFLX",
    "spotify":            "SPOT",
    "pinterest":          "PINS",
    "snapchat":           "SNAP",    # "snap" alone is too common a word
    "shopify":            "SHOP",
    "airbnb":             "ABNB",
    "doordash":           "DASH",
    "roblox":             "RBLX",
    "twilio":             "TWLO",
    "roku":               "ROKU",
    "block inc":          "SQ",
    "square":             "SQ",
    "paypal":             "PYPL",
    "adobe":              "ADBE",
    "cisco":              "CSCO",
    "ibm":                "IBM",
    # Financials
    "jpmorgan":           "JPM",
    "goldman sachs":      "GS",
    "morgan stanley":     "MS",
    "bank of america":    "BAC",
    "wells fargo":        "WFC",
    "citigroup":          "C",
    "blackrock":          "BLK",
    "robinhood":          "HOOD",
    "coinbase":           "COIN",
    "mastercard":         "MA",
    "american express":   "AXP",
    "charles schwab":     "SCHW",
    "capital one":        "COF",
    "berkshire":          "BRK.B",
    "sofi":               "SOFI",
    # Healthcare / Pharma
    "pfizer":             "PFE",
    "moderna":            "MRNA",
    "abbvie":             "ABBV",
    "gilead":             "GILD",
    "johnson & johnson":  "JNJ",
    "unitedhealth":       "UNH",
    "eli lilly":          "LLY",
    "merck":              "MRK",
    "amgen":              "AMGN",
    "regeneron":          "REGN",
    "vertex":             "VRTX",
    "intuitive surgical": "ISRG",
    "novo nordisk":       "NVO",
    "astrazeneca":        "AZN",
    # Semiconductors
    "micron":             "MU",
    "applied materials":  "AMAT",
    "lam research":       "LRCX",
    "texas instruments":  "TXN",
    "marvell":            "MRVL",
    "arm holdings":       "ARM",
    "asml":               "ASML",
    "taiwan semi":        "TSM",
    "tsmc":               "TSM",
    # Energy
    "exxon":              "XOM",
    "chevron":            "CVX",
    "conocophillips":     "COP",
    "schlumberger":       "SLB",
    "halliburton":        "HAL",
    # Retail / Consumer
    "walmart":            "WMT",
    "home depot":         "HD",
    # NOTE: "target" intentionally omitted — too many false matches on "price target"
    "lowe's":             "LOW",
    "dollar tree":        "DLTR",
    "dollar general":     "DG",
    "kroger":             "KR",
    "nike":               "NKE",
    "lululemon":          "LULU",
    "chipotle":           "CMG",
    "yum brands":         "YUM",
    "disney":             "DIS",
    "costco":             "COST",
    "starbucks":          "SBUX",
    "mcdonald":           "MCD",
    # Aerospace / Defense
    "boeing":             "BA",
    "lockheed":           "LMT",
    "raytheon":           "RTX",
    "northrop":           "NOC",
    "general dynamics":   "GD",
    "l3harris":           "LHX",
    # Airlines / Transport
    "delta air":          "DAL",
    "united airlines":    "UAL",
    "american airlines":  "AAL",
    "southwest airlines": "LUV",
    "fedex":              "FDX",
    "ups":                "UPS",   # \b word boundary enforced by regex; safe in financial titles
    "uber":               "UBER",
    "lyft":               "LYFT",
    # Automotive
    "ford motor":         "F",     # "ford" alone risks "afford"/"Stanford" etc.
    "general motors":     "GM",
    "toyota":             "TM",
    # Telecom / Media
    "at&t":               "T",     # re.escape() handles the & correctly
    "verizon":            "VZ",
    "t-mobile":           "TMUS",
    "comcast":            "CMCSA",
    "warner bros":        "WBD",
    # Industrials
    "caterpillar":        "CAT",
    "deere":              "DE",
    "john deere":         "DE",
    "3m company":         "MMM",   # "3m" alone risks "$3 million" false matches
    "honeywell":          "HON",
    "general electric":   "GE",
    # EV / Other
    "rivian":             "RIVN",
    "lucid":              "LCID",
    # AI / Cloud
    "palantir":           "PLTR",
    "c3.ai":              "AI",    # "AI" is in TICKER_BLACKLIST; kept for future if list evolves
    "uipath":             "PATH",
    "nebius":             "NBIS",
    "microstrategy":      "MSTR",
}

# Market cap tiers (approximate, for known large names)
MARKET_CAP_TIER_MAP: dict[str, MarketCapTier] = {
    **{t: MarketCapTier.MEGA for t in [
        # Original mega caps
        "AAPL","MSFT","NVDA","GOOGL","GOOG","META","AMZN","TSLA","BRK.B",
        "LLY","V","JPM","UNH","XOM","JNJ","WMT","MA","PG","AVGO","HD",
        # Newly added mega caps
        "BA","LMT","DIS","NFLX","ADBE","CSCO","IBM","PYPL",
        "TSM","ASML","NVO","AZN","MRK","AMGN",
        "LOW","CAT","DE","HON","GE",
        "T","VZ","TMUS","CMCSA",
    ]},
    **{t: MarketCapTier.LARGE for t in [
        # Original large caps
        "AMD","INTC","QCOM","ORCL","CRM","NOW","SNOW","GS","MS","BAC",
        "WFC","PFE","MRNA","ABBV","COST","TGT","CVX","SLB","GM","F",
        # Newly added large caps
        "RTX","NOC","GD","LHX",
        "FDX","UPS","UBER",
        "SHOP","ABNB","SQ","ARM",
        "HAL",
        "CMG","YUM","DLTR","DG","KR","WBD","MMM",
        "DAL","UAL",
    ]},
    **{t: MarketCapTier.MID for t in [
        # Original mid caps
        "RIVN","LCID","PLTR","SOFI","HOOD","COIN","MSTR","RIOT","MARA",
        # Newly added mid caps
        "LYFT","AAL","LUV",
        "DASH","RBLX","TWLO","ROKU","SPOT","PINS","SNAP",
        "PATH","NBIS",
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
    CatalystType.LEGAL:        30,   # 30min — lawsuits are lagging, move already priced
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
    (re.compile(r'\b(lawsuit|class action|securities fraud|litigation|rosen law|korsinsky|bronstein|shareholder rights|alleged fraud|investor loss|contact.*attorney|suffered loss)\b', re.I), CatalystType.LEGAL),
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

    Scoring logic:
      0.95 — $TICKER explicitly in title (highest signal)
      0.90 — company name matched in title via COMPANY_NAME_MAP (e.g. "Okta" → OKTA)
      0.85 — vendor tag AND ticker appears in title as bare word
      0.75 — ticker appears as bare word in title only (known universe)
      0.72 — ticker appears as bare word in title only (unknown universe)
      0.60 — vendor tag NOT in title (sympathy / related ticker) → below threshold

    Title-primacy rule: if any title-confirmed ticker (score ≥ 0.72) exists,
      all non-title tickers are explicitly capped at 0.60 so they fall below
      the acceptance threshold and are excluded from the resolved list.
      This prevents sympathy-play tickers (e.g. NVDA on an Oracle article)
      from being promoted to primary.

    Threshold: only accept tickers with confidence >= 0.70.
    """
    confidence: dict[str, float] = {}

    # Pre-compute title presence for cross-validation
    title_upper = title.upper()
    title_lower = title.lower()
    title_dollar_tickers = set(re.findall(r'\$([A-Z]{1,5})', title))
    title_bare_words = set(re.findall(r'\b([A-Z]{1,5})\b', title_upper))

    # 1. Scan title for $TICKER — highest confidence, unambiguous
    for t in title_dollar_tickers:
        if t not in TICKER_BLACKLIST:
            confidence[t] = 0.95

    # 2. Company name matching — catches "Okta", "Microsoft", etc. in prose titles
    for name, ticker in COMPANY_NAME_MAP.items():
        if re.search(r'\b' + re.escape(name) + r'\b', title_lower):
            if ticker not in TICKER_BLACKLIST:
                confidence[ticker] = max(confidence.get(ticker, 0), 0.90)

    # 3. Vendor-provided tickers — cross-validate against title
    for t in raw_tickers:
        t = t.upper().strip()
        if not t or t in TICKER_BLACKLIST:
            continue
        if len(t) > 5:
            continue
        if t in title_dollar_tickers:
            # Already scored 0.95 above
            continue
        elif t in title_bare_words and t in KNOWN_TICKERS:
            # Vendor tag confirmed by title mention — high confidence
            confidence[t] = max(confidence.get(t, 0), 0.85)
        elif t in title_bare_words:
            # In title but not known universe — moderate confidence
            confidence[t] = max(confidence.get(t, 0), 0.72)
        else:
            # Vendor tag NOT in title — likely sympathy/related ticker, not the primary subject.
            # Cap at 0.60 (below 0.70 threshold) so it is excluded from resolved list.
            if t in KNOWN_TICKERS:
                confidence[t] = max(confidence.get(t, 0), 0.60)
            # Unknown tickers not in title are discarded entirely

    # 4. Scan for bare uppercase words matching known universe (title-derived)
    for t in title_bare_words:
        if t in KNOWN_TICKERS and t not in TICKER_BLACKLIST:
            confidence[t] = max(confidence.get(t, 0), 0.75)

    # 5. Title-primacy: if any ticker is confirmed in the title (score ≥ 0.72),
    #    explicitly cap all non-title tickers at 0.60 to prevent sympathy play promotion.
    #    Handles edge cases where a vendor-only ticker otherwise gained a higher score.
    title_confirmed = {t for t, c in confidence.items() if c >= 0.72}
    if title_confirmed:
        for t in list(confidence.keys()):
            if t not in title_confirmed:
                confidence[t] = min(confidence[t], 0.60)

    # Filter to confidence >= 0.70 — this excludes vendor-only tags not in title
    resolved = [t for t, c in sorted(confidence.items(), key=lambda x: -x[1]) if c >= 0.70]
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
                # Normalize timing: Finnhub stores lowercase "bmo"/"amc";
                # legacy FMP rows may be uppercase "BMO"/"AMC". Handle both.
                event_time_lower = (row.event_time or "").lower()
                if event_time_lower == "amc":
                    # AMC: 4:30 PM ET (prime premarket setup the following morning)
                    event_dt = datetime.combine(row.event_date, __import__("datetime").time(16, 30))
                else:
                    # BMO, DMH, Unknown — default to 9:30 AM ET open
                    event_dt = datetime.combine(row.event_date, __import__("datetime").time(9, 30))
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

        # Update news_item row with resolved enrichment fields
        if self._Session:
            try:
                from sqlalchemy import text
                async with self._Session() as session:
                    await session.execute(text("""
                        UPDATE news_item SET
                            tickers             = :tickers,
                            ticker_confidence   = :ticker_confidence,
                            catalyst_type       = :catalyst_type,
                            mode                = :mode,
                            session_context     = :session_context,
                            market_cap_tier     = :market_cap_tier,
                            float_sensitivity   = :float_sensitivity,
                            short_interest_flag = :short_interest_flag,
                            earnings_proximity_h= :earnings_proximity_h,
                            decay_minutes       = :decay_minutes
                        WHERE source = :source AND vendor_id = :vendor_id
                    """), {
                        "tickers":             tickers,
                        "ticker_confidence":   json.dumps({k: float(v) for k, v in ticker_confidence.items()}),
                        "catalyst_type":       catalyst.value,
                        "mode":                mode.value,
                        "session_context":     session_ctx.value,
                        "market_cap_tier":     market_cap_tier.value if market_cap_tier else None,
                        "float_sensitivity":   float_sensitivity.value,
                        "short_interest_flag": any(t in HIGH_FLOAT_SENSITIVITY for t in tickers),
                        "earnings_proximity_h":earnings_proximity_h,
                        "decay_minutes":       decay,
                        "source":              deduped.source.value,
                        "vendor_id":           deduped.vendor_id,
                    })
                    await session.commit()
            except Exception as e:
                _log("warning", "entity_resolver.db_update_error", error=str(e))

        return enriched.to_kafka_dict()