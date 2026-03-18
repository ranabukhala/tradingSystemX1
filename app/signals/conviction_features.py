"""
Conviction scoring — calibration-friendly design (v1.8).

Separates raw feature extraction from the conviction formula, enabling:
  1. Verbatim logging of all raw inputs (ConvictionFeatures) for calibration data
  2. Step-by-step conviction trace (ConvictionBreakdown) for debugging
  3. Pluggable calibration layer (identity → sigmoid → linear → meta-model)
  4. Correlation-risk detection and optional mitigation (v2 mode)
  5. Replay-compatible: same inputs → same output (fully deterministic)

Scoring modes  (CONVICTION_SCORING_MODE)
─────────────────────────────────────────
  v1  (default)  Identical to the pre-v1.8 multiplicative formula.
                 All multipliers applied exactly as before. No mitigations.

  v2             Adds optional correlation-risk mitigations when
                 CONVICTION_CORRELATION_GUARD=true:
                   • Catalyst-weight compression  — reduces cat_w × impact_day compound
                   • Earnings combined-factor cap — bounds cat_w × proximity_bonus

Calibration functions  (CONVICTION_CALIBRATION_FN)
────────────────────────────────────────────────────
  identity  (default)  final = raw — preserves pre-v1.8 output exactly
  sigmoid              σ-shaped curve centered at CONVICTION_SIGMOID_CENTER (0.55)
  linear               per-catalyst slope × raw + bias (CONVICTION_LINEAR_PARAMS)

Feature logging  (CONVICTION_LOG_FEATURES)
───────────────────────────────────────────
  When true, signal_aggregator writes ConvictionBreakdown to signal_feature_log
  for every emitted signal.  Set CONVICTION_LOG_DROPPED_FEATURES=true to also
  log dropped signals (high volume — use sparingly).

Correlation-risk labels
────────────────────────
  cat_impact_day     catalyst_weight ≥ 1.4 AND impact_day ≥ 0.75.
                     Both encode event magnitude; multiplying them compounds the
                     same evidence twice.

  earnings_proximity catalyst = EARNINGS AND proximity_bonus ≥ 1.3.
                     catalyst_weight already encodes "earnings are important";
                     the proximity bonus adds another amplification for the same event.

  float_squeeze      float_sensitivity = HIGH.
                     float_weight (here in signal_aggregator) AND squeeze_multiplier
                     (later in pretrade_filter) both amplify low-float plays —
                     cross-service double-count that cannot be fixed at one layer alone.

These labels are always computed and logged regardless of scoring mode.
In v2 mode with CONVICTION_CORRELATION_GUARD=true, mitigations are applied.
"""
from __future__ import annotations

import json
import math
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any

# Pipeline model imports — conviction_features is the scoring authority so it
# owns the catalog weight dicts.  signal_aggregator imports them FROM here.
from app.models.news import CatalystType, FloatSensitivity, SessionContext
from app.pipeline.llm_validation import (
    INTERPRETIVE_MAX_CONVICTION,
    INTERPRETIVE_MAX_CONVICTION_HIGH_IMPACT,
    REGIME_CONVICTION_ADJ,
)


# ── Configuration ──────────────────────────────────────────────────────────────

_SCORING_MODE       = os.environ.get("CONVICTION_SCORING_MODE",    "v1").lower()
_CORRELATION_GUARD  = os.environ.get("CONVICTION_CORRELATION_GUARD","false").lower() == "true"
LOG_FEATURES        = os.environ.get("CONVICTION_LOG_FEATURES",    "true").lower()  == "true"
LOG_DROPPED         = os.environ.get("CONVICTION_LOG_DROPPED_FEATURES", "false").lower() == "true"

# v2 mode: compress catalyst weights toward 1.0 to reduce cat_w × impact_day compound.
# Default 0.5 → cat_w range [0.3, 1.8] compresses to [0.65, 1.40].
_CATALYST_COMPRESSION = float(os.environ.get("CONVICTION_CATALYST_COMPRESSION", "0.5"))

# v2 mode: cap the combined earnings amplification (cat_w_eff × proximity_bonus).
_EARNINGS_COMBINED_CAP = float(os.environ.get("CONVICTION_EARNINGS_CAP", "1.80"))

# Calibration
_CALIBRATION_FN    = os.environ.get("CONVICTION_CALIBRATION_FN",  "identity").lower()
_SIGMOID_CENTER    = float(os.environ.get("CONVICTION_SIGMOID_CENTER",    "0.55"))
_SIGMOID_STEEPNESS = float(os.environ.get("CONVICTION_SIGMOID_STEEPNESS", "8.0"))

# Correlation-risk thresholds (tuneable without code changes)
_CORR_CAT_WEIGHT_THRESHOLD  = float(os.environ.get("CORR_CAT_WEIGHT_THRESHOLD",  "1.4"))
_CORR_IMPACT_DAY_THRESHOLD  = float(os.environ.get("CORR_IMPACT_DAY_THRESHOLD",  "0.75"))

# Priced-in multipliers — paper trading calibration (v1.6)
# Softened from 0.60/0.85 to allow genuine catalysts to execute and be measured.
# Tighten back toward 0.60/0.85 before live trading once priced_in accuracy is known.
_PRICED_IN_YES_MULT      = 0.75   # was 0.60 — reduce from 40% to 25% haircut
_PRICED_IN_PARTIAL_MULT  = 0.90   # was 0.85 — reduce from 15% to 10% haircut


# ── Catalog weight dicts (authoritative source) ────────────────────────────────
# These were previously defined in signal_aggregator.py.  They now live here so
# that conviction_features.py is fully self-contained and testable without
# importing the full pipeline consumer stack.  signal_aggregator re-exports them.

CATALYST_WEIGHT: dict[CatalystType, float] = {
    CatalystType.EARNINGS:    1.5,
    CatalystType.MA:          1.8,   # M&A = highest impact
    CatalystType.REGULATORY:  1.4,
    CatalystType.ANALYST:     0.9,
    CatalystType.FILING:      0.7,   # was 0.6 — filings often contain material facts
    CatalystType.MACRO:       0.8,   # was 0.7 — macro moves sectors, not just stocks
    CatalystType.LEGAL:       0.3,   # Lawsuits = lagging catalyst, deeply discounted
    CatalystType.OTHER:       0.75,  # was 0.5 — reduces floor, enables direction_from_facts to pass
}

SESSION_WEIGHT: dict[SessionContext, float] = {
    SessionContext.PREMARKET:  1.4,
    SessionContext.OPEN:       1.3,
    SessionContext.INTRADAY:   1.0,
    SessionContext.AFTERHOURS: 1.1,
    SessionContext.OVERNIGHT:  0.7,
}

FLOAT_WEIGHT: dict[FloatSensitivity, float] = {
    FloatSensitivity.HIGH:   1.3,
    FloatSensitivity.NORMAL: 1.0,
}


# ── Enums ──────────────────────────────────────────────────────────────────────

class CalibrationFn(str, Enum):
    IDENTITY = "identity"
    SIGMOID  = "sigmoid"
    LINEAR   = "linear"


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class ConvictionFeatures:
    """
    All raw inputs to the conviction formula — extracted before any scoring.

    Stored verbatim so that:
      • Logs contain the exact inputs that produced a given conviction score
      • Calibration model training can replay any past signal
      • A/B tests can compare formula variants on identical feature sets

    Immutable by convention; never modified after extraction.
    """
    # ── LLM outputs (validated) ───────────────────────────────────────────
    impact_day:         float    # cleaned_impact_day [0, 1]
    source_credibility: float    # cleaned_source_credibility [0, 1], default 0.6
    cred_boost:         float    # 0.8 + (source_credibility × 0.4) → [0.8, 1.2]

    # ── Catalog features ──────────────────────────────────────────────────
    catalyst_type:             str    # earnings | ma | analyst | …
    catalyst_weight:           float  # raw CATALYST_WEIGHT lookup
    catalyst_weight_effective: float  # = catalyst_weight in v1; compressed in v2

    session_context: str    # premarket | intraday | …
    session_weight:  float  # SESSION_WEIGHT lookup

    float_sensitivity: str    # high | normal
    float_weight:      float  # FLOAT_WEIGHT lookup

    # ── Earnings proximity ────────────────────────────────────────────────
    earnings_proximity_h:                 int | None  # abs hours to earnings, or None
    earnings_proximity_bonus:             float       # 1.0, 1.1, or 1.3
    earnings_proximity_bonus_effective:   float       # ≤ cap in v2; = bonus in v1

    # ── Trust features ────────────────────────────────────────────────────
    direction_source:   str        # facts | interpretive_prior | neutral_default
    priced_in_effective: str | None # "yes" always for LEGAL; else from LLM validation
    regime_flag:        str | None  # risk_off | high_vol | compression | risk_on | None
    regime_multiplier:  float       # from REGIME_CONVICTION_ADJ; 1.0 if None

    # ── Cross-validation ──────────────────────────────────────────────────
    cross_val_status:     str | None  # confirmed | partial | mismatch | unverifiable | None
    cross_val_multiplier: float       # 0.30–1.10, or 1.0 if no cross_val

    # ── Time window ───────────────────────────────────────────────────────
    time_window_label:          str | None
    time_window_mult:           float  # raw multiplier from get_time_window()
    time_window_mult_effective: float  # 1.0 when ≥ 1.0 (boosts logged-only, not applied)

    # ── Mode flags ────────────────────────────────────────────────────────
    scoring_mode:             str   # "v1" | "v2"
    correlation_guard_active: bool  # True when v2 + CONVICTION_CORRELATION_GUARD=true

    @property
    def as_dict(self) -> dict[str, Any]:
        return {
            "impact_day":                          self.impact_day,
            "source_credibility":                  self.source_credibility,
            "cred_boost":                          self.cred_boost,
            "catalyst_type":                       self.catalyst_type,
            "catalyst_weight":                     self.catalyst_weight,
            "catalyst_weight_effective":           self.catalyst_weight_effective,
            "session_context":                     self.session_context,
            "session_weight":                      self.session_weight,
            "float_sensitivity":                   self.float_sensitivity,
            "float_weight":                        self.float_weight,
            "earnings_proximity_h":                self.earnings_proximity_h,
            "earnings_proximity_bonus":            self.earnings_proximity_bonus,
            "earnings_proximity_bonus_effective":  self.earnings_proximity_bonus_effective,
            "direction_source":                    self.direction_source,
            "priced_in":                           self.priced_in_effective,
            "regime_flag":                         self.regime_flag,
            "regime_multiplier":                   self.regime_multiplier,
            "cross_val_status":                    self.cross_val_status,
            "cross_val_multiplier":                self.cross_val_multiplier,
            "time_window_label":                   self.time_window_label,
            "time_window_mult":                    self.time_window_mult,
            "time_window_mult_effective":          self.time_window_mult_effective,
            "scoring_mode":                        self.scoring_mode,
        }


@dataclass
class ConvictionBreakdown:
    """
    Full step-by-step trace of one conviction computation.

    Intermediate values enable:
      • Root-cause analysis ("why did this signal get low conviction?")
      • Calibration training ("what raw score predicted what outcome?")
      • Replay validation ("does v2 change this specific signal?")

    final_conviction is the value used for all trading decisions.
    """
    features: ConvictionFeatures

    # ── Step-by-step trail (all values post-round to 4dp) ─────────────────
    step_base_product:      float   # impact_day × cat_w_eff × sess_w × float_w × cred_boost
    step_after_proximity:   float   # × earnings_proximity_bonus_effective
    step_after_priced_in:   float   # × priced_in mult, then clamp to [0,1]; or 0.0 if blocked
    step_after_cap:         float   # after interpretive ceiling (INTERPRETIVE_MAX_CONVICTION)
    step_after_regime:      float   # × regime_multiplier
    step_after_cross_val:   float   # × cross_val_multiplier
    step_after_time_window: float   # × time_window_mult_effective (dampening only)
    step_calibrated:        float   # output of calibration function

    # ── Gates ─────────────────────────────────────────────────────────────
    interpretive_cap_applied: bool   # True → conviction was reduced to ceiling
    priced_in_blocked:        bool   # True → interpretive + priced_in="yes" → 0.0

    # ── Correlation risk ──────────────────────────────────────────────────
    correlation_risk: list[str]      # ['cat_impact_day', 'earnings_proximity', 'float_squeeze']

    # ── Calibration ───────────────────────────────────────────────────────
    calibration_fn: str   # "identity" | "sigmoid" | "linear"

    # ── Final ─────────────────────────────────────────────────────────────
    final_conviction: float   # = step_calibrated clamped to [0, 1], rounded to 3dp

    @property
    def as_log_dict(self) -> dict[str, Any]:
        """
        Flat dict for structured logging and DB persistence.
        Contains both the raw features and the full conviction trail.
        """
        return {
            **self.features.as_dict,
            "step_base_product":      round(self.step_base_product,      4),
            "step_after_proximity":   round(self.step_after_proximity,    4),
            "step_after_priced_in":   round(self.step_after_priced_in,    4),
            "step_after_cap":         round(self.step_after_cap,          4),
            "step_after_regime":      round(self.step_after_regime,       4),
            "step_after_cross_val":   round(self.step_after_cross_val,    4),
            "step_after_time_window": round(self.step_after_time_window,  4),
            "step_calibrated":        round(self.step_calibrated,         4),
            "interpretive_cap_applied": self.interpretive_cap_applied,
            "priced_in_blocked":      self.priced_in_blocked,
            "correlation_risk":       self.correlation_risk,
            "calibration_fn":         self.calibration_fn,
            "final_conviction":       round(self.final_conviction,        4),
        }


# ── Calibration layer ───────────────────────────────────────────────────────────

class ConvictionCalibrator(ABC):
    """
    Abstract calibration layer.

    Design contract:
      • Must always return a value in [0.0, 1.0]
      • Must be deterministic (same inputs → same output)
      • Must not access network, DB, or mutable state
      • May use features for per-catalyst or per-context adjustments
    """

    @abstractmethod
    def calibrate(
        self,
        raw: float,
        features: ConvictionFeatures,
    ) -> tuple[float, str]:
        """Returns (calibrated_conviction [0,1], calibration_fn_name)."""


class IdentityCalibrator(ConvictionCalibrator):
    """
    Default: no calibration.  final = raw.

    Preserves exact pre-v1.8 numerical output.  Use when the formula is
    considered well-calibrated or when you want a clean baseline.
    """

    def calibrate(
        self, raw: float, features: ConvictionFeatures
    ) -> tuple[float, str]:
        return raw, CalibrationFn.IDENTITY.value


class SigmoidCalibrator(ConvictionCalibrator):
    """
    Sigmoid centering: passes raw conviction through a logistic curve.

    Maps values near ``center`` (default 0.55) through a compression zone;
    values far from center are pushed toward 0 or 1.  This reduces the
    population of signals that hover ambiguously near the conviction threshold.

    σ(k(x − center))  where k = steepness, center = desired curve midpoint.

    Example: center=0.55, steepness=8
      raw=0.30 → calibrated≈0.13  (reduced)
      raw=0.55 → calibrated=0.50  (midpoint)
      raw=0.75 → calibrated≈0.86  (amplified)
    """

    def __init__(
        self,
        center:    float = _SIGMOID_CENTER,
        steepness: float = _SIGMOID_STEEPNESS,
    ) -> None:
        self._center    = center
        self._steepness = steepness

    def calibrate(
        self, raw: float, features: ConvictionFeatures
    ) -> tuple[float, str]:
        z = self._steepness * (raw - self._center)
        cal = 1.0 / (1.0 + math.exp(-z))
        return round(max(0.0, min(1.0, cal)), 4), CalibrationFn.SIGMOID.value


class LinearCalibrator(ConvictionCalibrator):
    """
    Per-catalyst linear adjustment: calibrated = slope × raw + bias.

    Use when back-testing reveals systematic over/under-estimation for a
    catalyst type.  Defaults to identity (slope=1.0, bias=0.0) for all types.

    Override via env:
        CONVICTION_LINEAR_PARAMS='{"earnings":[0.95,0.02],"analyst":[1.10,-0.02]}'
    """

    _DEFAULT_PARAMS: dict[str, tuple[float, float]] = {
        "earnings":   (1.0, 0.0),
        "analyst":    (1.0, 0.0),
        "ma":         (1.0, 0.0),
        "regulatory": (1.0, 0.0),
    }

    def __init__(self, params: dict[str, tuple[float, float]] | None = None) -> None:
        raw_env = os.environ.get("CONVICTION_LINEAR_PARAMS", "")
        if raw_env:
            try:
                env = {k: tuple(v) for k, v in json.loads(raw_env).items()}
                self._params = {**self._DEFAULT_PARAMS, **env}
            except Exception:
                self._params = params or self._DEFAULT_PARAMS
        else:
            self._params = params or self._DEFAULT_PARAMS

    def calibrate(
        self, raw: float, features: ConvictionFeatures
    ) -> tuple[float, str]:
        slope, bias = self._params.get(features.catalyst_type, (1.0, 0.0))
        cal = max(0.0, min(1.0, slope * raw + bias))
        return round(cal, 4), CalibrationFn.LINEAR.value


def get_default_calibrator() -> ConvictionCalibrator:
    """Return the calibrator configured via CONVICTION_CALIBRATION_FN."""
    if _CALIBRATION_FN == "sigmoid":
        return SigmoidCalibrator()
    if _CALIBRATION_FN == "linear":
        return LinearCalibrator()
    return IdentityCalibrator()


# ── Feature extraction ──────────────────────────────────────────────────────────

def extract_conviction_features(
    record,                           # SummarizedRecord
    direction_source: str,
    validation,                       # LLMValidationResult
    cross_val,                        # FactCrossValidationResult | None
    time_window_mult: float   = 1.0,
    time_window_label: str | None = None,
) -> ConvictionFeatures:
    """
    Extract all raw conviction inputs from a record and its validation results.

    Must be called BEFORE compute_conviction_breakdown().  The returned
    ConvictionFeatures object is passed to the breakdown function AND stored
    for logging — extraction is side-effect free.

    Lazy imports break the circular dependency with signal_aggregator.
    """
    # ── impact_day ────────────────────────────────────────────────────────
    impact_day = validation.cleaned_impact_day or 0.0

    # ── source_credibility + cred_boost ───────────────────────────────────
    cred       = validation.cleaned_source_credibility or 0.6
    cred_boost = round(0.8 + (cred * 0.4), 4)

    # ── catalyst ──────────────────────────────────────────────────────────
    catalyst    = record.catalyst_type
    cat_str     = catalyst.value if hasattr(catalyst, "value") else str(catalyst)
    cat_w_raw   = CATALYST_WEIGHT.get(catalyst, 0.5)

    # v2 + guard: compress catalyst weights toward 1.0
    if _SCORING_MODE == "v2" and _CORRELATION_GUARD:
        cat_w_eff = round(1.0 + (cat_w_raw - 1.0) * _CATALYST_COMPRESSION, 4)
    else:
        cat_w_eff = cat_w_raw

    # ── session ───────────────────────────────────────────────────────────
    sess_w   = SESSION_WEIGHT.get(record.session_context, 1.0)
    sess_str = (
        record.session_context.value
        if hasattr(record.session_context, "value")
        else str(record.session_context)
    )

    # ── float sensitivity ─────────────────────────────────────────────────
    float_w   = FLOAT_WEIGHT.get(record.float_sensitivity, 1.0)
    float_str = (
        record.float_sensitivity.value
        if hasattr(record.float_sensitivity, "value")
        else str(record.float_sensitivity)
    )

    # ── earnings proximity ────────────────────────────────────────────────
    proximity_bonus = 1.0
    if record.earnings_proximity_h is not None:
        h = abs(record.earnings_proximity_h)
        if h <= 2:
            proximity_bonus = 1.3
        elif h <= 24:
            proximity_bonus = 1.1

    # v2 + guard: cap combined earnings amplification
    if _SCORING_MODE == "v2" and _CORRELATION_GUARD and catalyst == CatalystType.EARNINGS:
        combined = cat_w_eff * proximity_bonus
        if combined > _EARNINGS_COMBINED_CAP:
            proximity_bonus_eff = round(_EARNINGS_COMBINED_CAP / cat_w_eff, 4)
        else:
            proximity_bonus_eff = proximity_bonus
    else:
        proximity_bonus_eff = proximity_bonus

    # ── priced_in (LEGAL always treated as fully priced in) ───────────────
    if catalyst == CatalystType.LEGAL:
        priced_in_eff = "yes"
    else:
        priced_in_eff = validation.cleaned_priced_in  # None = no penalty

    # ── regime ────────────────────────────────────────────────────────────
    regime_flag = validation.cleaned_regime_flag
    regime_mult = (
        REGIME_CONVICTION_ADJ.get(regime_flag, 1.00)
        if regime_flag else 1.00
    )

    # ── cross-validation ──────────────────────────────────────────────────
    if cross_val is not None:
        cv_status = cross_val.validation_status.value
        cv_mult   = cross_val.conviction_multiplier
    else:
        cv_status = None
        cv_mult   = 1.0

    # ── time window: only apply dampening (< 1.0) ─────────────────────────
    tw_mult_eff = time_window_mult if time_window_mult < 1.0 else 1.0

    return ConvictionFeatures(
        impact_day                         = impact_day,
        source_credibility                 = round(cred, 4),
        cred_boost                         = cred_boost,
        catalyst_type                      = cat_str,
        catalyst_weight                    = cat_w_raw,
        catalyst_weight_effective          = cat_w_eff,
        session_context                    = sess_str,
        session_weight                     = sess_w,
        float_sensitivity                  = float_str,
        float_weight                       = float_w,
        earnings_proximity_h               = (
            abs(record.earnings_proximity_h)
            if record.earnings_proximity_h is not None else None
        ),
        earnings_proximity_bonus           = proximity_bonus,
        earnings_proximity_bonus_effective = proximity_bonus_eff,
        direction_source                   = direction_source,
        priced_in_effective                = priced_in_eff,
        regime_flag                        = regime_flag,
        regime_multiplier                  = regime_mult,
        cross_val_status                   = cv_status,
        cross_val_multiplier               = cv_mult,
        time_window_label                  = time_window_label,
        time_window_mult                   = time_window_mult,
        time_window_mult_effective         = tw_mult_eff,
        scoring_mode                       = _SCORING_MODE,
        correlation_guard_active           = (_SCORING_MODE == "v2" and _CORRELATION_GUARD),
    )


# ── Conviction breakdown computation ───────────────────────────────────────────

def compute_conviction_breakdown(
    features:   ConvictionFeatures,
    calibrator: ConvictionCalibrator | None = None,
) -> ConvictionBreakdown:
    """
    Apply the conviction formula to pre-extracted features and return a full
    step-by-step breakdown.

    Formula (v1 mode — identical to pre-v1.8 compute_conviction):
      1. base_product  = impact_day × cat_w_eff × sess_w × float_w × cred_boost
      2. × earnings_proximity_bonus_effective
      3. × priced_in multiplier   (or 0.0 if interpretive + priced_in="yes")
         clamp to [0, 1]
      4. interpretive ceiling     (cap if direction_source="interpretive_prior")
      5. × regime_multiplier
      6. × cross_val_multiplier
      7. × time_window_mult_effective  (dampening only; boosts inactive)
      8. calibration function
      9. clamp final to [0, 1]

    In v2 mode with CONVICTION_CORRELATION_GUARD=true, steps 1–2 use the
    adjusted cat_w_eff and capped proximity_bonus_effective (already in features).
    """
    if calibrator is None:
        calibrator = get_default_calibrator()

    # ── Step 1: base product ──────────────────────────────────────────────
    step_base = round(
        features.impact_day
        * features.catalyst_weight_effective
        * features.session_weight
        * features.float_weight
        * features.cred_boost,
        4,
    )

    # ── Step 2: earnings proximity bonus ──────────────────────────────────
    step_prox = round(step_base * features.earnings_proximity_bonus_effective, 4)

    # ── Step 3: priced-in ─────────────────────────────────────────────────
    pi = features.priced_in_effective

    # BLOCK: interpretive direction + market fully priced in → 0.0 conviction
    if features.direction_source == "interpretive_prior" and pi == "yes":
        return ConvictionBreakdown(
            features              = features,
            step_base_product     = step_base,
            step_after_proximity  = step_prox,
            step_after_priced_in  = 0.0,
            step_after_cap        = 0.0,
            step_after_regime     = 0.0,
            step_after_cross_val  = 0.0,
            step_after_time_window= 0.0,
            step_calibrated       = 0.0,
            interpretive_cap_applied = False,
            priced_in_blocked     = True,
            correlation_risk      = detect_correlation_risks(features),
            calibration_fn        = CalibrationFn.IDENTITY.value,
            final_conviction      = 0.0,
        )

    if pi == "yes":
        step_pi = round(step_prox * _PRICED_IN_YES_MULT, 4)
    elif pi == "partially":
        step_pi = round(step_prox * _PRICED_IN_PARTIAL_MULT, 4)
    else:
        step_pi = step_prox

    # Clamp to [0, 1] — matches min(1.0, ...) in original formula
    step_pi = min(1.0, step_pi)

    # ── Step 4: interpretive ceiling ──────────────────────────────────────
    # Use a slightly higher cap (0.65) when impact_day >= 0.9 to avoid
    # hard-dropping extremely high-impact interpretive signals (Option B).
    cap_applied = False
    if features.direction_source == "interpretive_prior":
        _cap = (INTERPRETIVE_MAX_CONVICTION_HIGH_IMPACT
                if features.impact_day >= 0.9
                else INTERPRETIVE_MAX_CONVICTION)
        if step_pi > _cap:
            step_cap    = round(_cap, 4)
            cap_applied = True
        else:
            step_cap = round(step_pi, 4)
    else:
        step_cap = round(step_pi, 4)

    # ── Step 5: regime adjustment ─────────────────────────────────────────
    step_regime = round(step_cap * features.regime_multiplier, 4)

    # ── Step 6: cross-validation multiplier ───────────────────────────────
    if features.cross_val_multiplier != 1.0:
        step_cv = round(step_regime * features.cross_val_multiplier, 4)
    else:
        step_cv = step_regime

    # ── Step 7: time window dampening (active only when < 1.0) ───────────
    if features.time_window_mult_effective < 1.0:
        step_tw = round(step_cv * features.time_window_mult_effective, 4)
    else:
        step_tw = step_cv

    # ── Step 8: calibration ───────────────────────────────────────────────
    calibrated, cal_fn = calibrator.calibrate(step_tw, features)

    # ── Step 9: final clamp ───────────────────────────────────────────────
    final = round(max(0.0, min(1.0, calibrated)), 3)

    return ConvictionBreakdown(
        features               = features,
        step_base_product      = step_base,
        step_after_proximity   = step_prox,
        step_after_priced_in   = step_pi,
        step_after_cap         = step_cap,
        step_after_regime      = step_regime,
        step_after_cross_val   = step_cv,
        step_after_time_window = step_tw,
        step_calibrated        = round(calibrated, 4),
        interpretive_cap_applied = cap_applied,
        priced_in_blocked      = False,
        correlation_risk       = detect_correlation_risks(features),
        calibration_fn         = cal_fn,
        final_conviction       = final,
    )


# ── Correlation risk detection ──────────────────────────────────────────────────

def detect_correlation_risks(features: ConvictionFeatures) -> list[str]:
    """
    Detect pairs of correlated factors that may double-count the same evidence.

    Always computed for logging; mitigations are applied separately in v2 mode.
    Labels are stable strings safe to store in Postgres TEXT[] columns.
    """
    risks: list[str] = []

    # cat_impact_day: both catalog weight and LLM impact_day encode event magnitude
    if (features.catalyst_weight  >= _CORR_CAT_WEIGHT_THRESHOLD
            and features.impact_day >= _CORR_IMPACT_DAY_THRESHOLD):
        risks.append("cat_impact_day")

    # earnings_proximity: catalyst_weight already encodes "earnings matter"
    if (features.catalyst_type == "earnings"
            and features.earnings_proximity_bonus >= 1.3):
        risks.append("earnings_proximity")

    # float_squeeze: float_weight (here) + squeeze_multiplier (pretrade_filter)
    if features.float_sensitivity == "high":
        risks.append("float_squeeze")

    return risks
