"""
Execution safety tests — verify kill-switch and risk guards.
"""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest


class TestKillSwitch:
    """CONFIRM_LIVE_TRADING must be true for live brokers."""

    def test_live_broker_without_confirmation_exits(self):
        """Live broker + no confirmation = fail fast."""
        from app.config import Settings
        s = Settings(broker="alpaca_live", confirm_live_trading=False)
        assert s.is_live_broker() is True
        with pytest.raises(SystemExit):
            s.require_live_trading_confirmation()

    def test_live_broker_with_confirmation_passes(self):
        from app.config import Settings
        s = Settings(broker="alpaca_live", confirm_live_trading=True)
        # Should not raise
        s.require_live_trading_confirmation()

    def test_paper_broker_no_confirmation_needed(self):
        from app.config import Settings
        s = Settings(broker="alpaca_paper", confirm_live_trading=False)
        assert s.is_live_broker() is False
        # Should not raise
        s.require_live_trading_confirmation()

    def test_ibkr_live_requires_confirmation(self):
        from app.config import Settings
        s = Settings(broker="ibkr_live", confirm_live_trading=False)
        assert s.is_live_broker() is True
        with pytest.raises(SystemExit):
            s.require_live_trading_confirmation()


class TestRiskConfig:
    """Risk config loads correctly from settings."""

    def test_risk_config_from_settings(self):
        from app.config import Settings
        s = Settings(
            risk_max_position_pct=0.03,
            risk_min_conviction=0.50,
            risk_max_orders_per_hour=15,
        )
        assert s.risk_max_position_pct == 0.03
        assert s.risk_min_conviction == 0.50

    def test_risk_manager_creates_from_settings(self):
        from app.execution.risk_manager import RiskConfig
        with patch("app.execution.risk_manager.settings") as mock:
            mock.risk_max_position_pct = 0.05
            mock.risk_max_position_usd = 10000
            mock.risk_max_daily_loss_pct = 0.03
            mock.risk_max_open_positions = 5
            mock.risk_min_conviction = 0.60
            mock.risk_allow_premarket = False
            mock.risk_allow_afterhours = False
            mock.risk_take_profit_pct = 0.15
            mock.risk_stop_loss_pct = 0.05
            mock.risk_use_bracket_orders = True

            config = RiskConfig.from_settings()
            assert config.max_position_pct == 0.05
            assert config.min_conviction == 0.60
            assert config.allow_premarket is False
