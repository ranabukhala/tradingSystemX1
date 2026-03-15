"""
Data models for the Stock Context Classifier service.

StockContext encapsulates trend regime, volatility regime, and cleanliness
for a single ticker, along with the derived adjusted_threshold that the
pre-trade filter should use in place of the static env-var setting.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class TrendRegime(str, Enum):
    TRENDING_UP   = "TRENDING_UP"
    TRENDING_DOWN = "TRENDING_DOWN"
    RANGING       = "RANGING"


class VolatilityRegime(str, Enum):
    EXPANDING   = "EXPANDING"
    CONTRACTING = "CONTRACTING"
    NORMAL      = "NORMAL"


class Cleanliness(str, Enum):
    CLEAN             = "CLEAN"
    CHOPPY            = "CHOPPY"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


@dataclass
class StockContext:
    """
    Classified stock context for one ticker.

    Attributes
    ----------
    ticker              : The equity symbol (e.g. "AAPL").
    trend_regime        : TRENDING_UP | TRENDING_DOWN | RANGING.
    volatility_regime   : EXPANDING | CONTRACTING | NORMAL.
    cleanliness         : CLEAN | CHOPPY | INSUFFICIENT_DATA.
    adx                 : Average Directional Index (14-period).
    ma_slope_20         : 5-day slope of the 20-day SMA (pct).
    bb_width_trend      : Direction Bollinger Band width is moving.
    atr_ratio           : ATR(14) / ATR(50) — current vs baseline vol.
    adjusted_threshold  : Dynamic technical-score floor (7, 8, or 9).
    calculated_at       : UTC timestamp of last computation.
    raw_metrics         : Full intermediate metrics for logging / debug.
    """

    ticker:             str
    trend_regime:       TrendRegime
    volatility_regime:  VolatilityRegime
    cleanliness:        Cleanliness
    adx:                float
    ma_slope_20:        float
    bb_width_trend:     str    # "EXPANDING" | "CONTRACTING" | "STABLE"
    atr_ratio:          float  # ATR(14) / ATR(50)
    adjusted_threshold: int    # 7 | 8 | 9
    calculated_at:      datetime
    raw_metrics:        dict = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Serialization helpers
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable dict (enum values as strings)."""
        d = asdict(self)
        d["trend_regime"]       = self.trend_regime.value
        d["volatility_regime"]  = self.volatility_regime.value
        d["cleanliness"]        = self.cleanliness.value
        d["calculated_at"]      = self.calculated_at.isoformat()
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "StockContext":
        """Reconstruct a StockContext from a previously serialised dict."""
        return cls(
            ticker             = d["ticker"],
            trend_regime       = TrendRegime(d["trend_regime"]),
            volatility_regime  = VolatilityRegime(d["volatility_regime"]),
            cleanliness        = Cleanliness(d["cleanliness"]),
            adx                = float(d["adx"]),
            ma_slope_20        = float(d["ma_slope_20"]),
            bb_width_trend     = d["bb_width_trend"],
            atr_ratio          = float(d["atr_ratio"]),
            adjusted_threshold = int(d["adjusted_threshold"]),
            calculated_at      = datetime.fromisoformat(d["calculated_at"]),
            raw_metrics        = d.get("raw_metrics", {}),
        )

    @staticmethod
    def default(ticker: str) -> "StockContext":
        """
        Conservative fallback context — used when no data is available.
        Returns RANGING / NORMAL / INSUFFICIENT_DATA with threshold 8.
        """
        return StockContext(
            ticker             = ticker,
            trend_regime       = TrendRegime.RANGING,
            volatility_regime  = VolatilityRegime.NORMAL,
            cleanliness        = Cleanliness.INSUFFICIENT_DATA,
            adx                = 0.0,
            ma_slope_20        = 0.0,
            bb_width_trend     = "STABLE",
            atr_ratio          = 1.0,
            adjusted_threshold = 8,
            calculated_at      = datetime.now(timezone.utc),
            raw_metrics        = {"source": "default_fallback"},
        )
