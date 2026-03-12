"""
Idempotency / dedup tests — verify SQLite-backed at-least-once safety.
"""
from __future__ import annotations

import os
import tempfile

import pytest

from app.idempotency import IdempotencyStore


@pytest.fixture
def store(tmp_path):
    """Create a temporary IdempotencyStore."""
    db_path = str(tmp_path / "test_state.db")
    s = IdempotencyStore(db_path)
    yield s
    s.close()


class TestIdempotencyStore:

    def test_new_event_returns_false(self, store):
        """First time seeing an event should return False (= process it)."""
        result = store.check_and_mark("normalizer", "event-001")
        assert result is False

    def test_duplicate_event_returns_true(self, store):
        """Second time seeing same event should return True (= skip it)."""
        store.check_and_mark("normalizer", "event-001")
        result = store.check_and_mark("normalizer", "event-001")
        assert result is True

    def test_same_event_different_stage(self, store):
        """Same event_id in different stages should both be new."""
        r1 = store.check_and_mark("normalizer", "event-001")
        r2 = store.check_and_mark("deduplicator", "event-001")
        assert r1 is False
        assert r2 is False

    def test_different_events_both_new(self, store):
        """Different event_ids should both be new."""
        r1 = store.check_and_mark("normalizer", "event-001")
        r2 = store.check_and_mark("normalizer", "event-002")
        assert r1 is False
        assert r2 is False

    def test_replay_same_event_twice_only_processes_once(self, store):
        """Core test: replaying the same event should not produce duplicates."""
        processed = []

        for _ in range(3):  # Simulate 3 replays
            if not store.check_and_mark("execution", "signal-abc"):
                processed.append("signal-abc")

        assert len(processed) == 1


class TestOrderIdempotency:

    def test_no_prior_order(self, store):
        result = store.was_order_submitted("event-001")
        assert result is None

    def test_record_and_check_order(self, store):
        store.record_order("event-001", "broker-order-xyz", "AAPL", "long", 10)
        result = store.was_order_submitted("event-001")
        assert result == "broker-order-xyz"

    def test_duplicate_order_recording(self, store):
        """Recording same event_id twice should not raise."""
        store.record_order("event-001", "broker-1", "AAPL", "long", 10)
        store.record_order("event-001", "broker-2", "AAPL", "long", 10)
        # First one wins
        assert store.was_order_submitted("event-001") == "broker-1"

    def test_execution_idempotency_flow(self, store):
        """Full flow: check -> record -> check again = skip."""
        event_id = "signal-xyz-123"

        # First attempt: no prior order
        existing = store.was_order_submitted(event_id)
        assert existing is None

        # Submit order and record
        store.record_order(event_id, "alpaca-order-456", "TSLA", "short", 5)

        # Replay: should find existing order
        existing = store.was_order_submitted(event_id)
        assert existing == "alpaca-order-456"


class TestDailyLossTracker:

    def test_initial_stats(self, store):
        stats = store.get_daily_stats("2024-01-15")
        assert stats["order_count"] == 0
        assert stats["realized_loss"] == 0.0
        assert stats["consecutive_losses"] == 0

    def test_record_win(self, store):
        stats = store.record_order_outcome("2024-01-15", is_loss=False)
        assert stats["order_count"] == 1
        assert stats["consecutive_losses"] == 0

    def test_record_loss(self, store):
        stats = store.record_order_outcome("2024-01-15", is_loss=True, loss_amount=100.0)
        assert stats["order_count"] == 1
        assert stats["realized_loss"] == 100.0
        assert stats["consecutive_losses"] == 1

    def test_consecutive_losses_reset_on_win(self, store):
        store.record_order_outcome("2024-01-15", is_loss=True, loss_amount=50)
        store.record_order_outcome("2024-01-15", is_loss=True, loss_amount=75)
        stats = store.record_order_outcome("2024-01-15", is_loss=False)
        assert stats["consecutive_losses"] == 0
        assert stats["realized_loss"] == 125.0  # Losses still tracked
        assert stats["order_count"] == 3
