"""
SQLite-based idempotency store for at-least-once safety.

Each pipeline stage checks event_id before processing.
Replays of the same event_id are skipped, preventing duplicates.

Tables:
  processed_events  — tracks which events each stage has processed
  order_idempotency — tracks broker order submissions by event_id (execution only)

Usage:
    store = IdempotencyStore("/data/state.db")
    if store.check_and_mark("normalizer", event_id):
        # Already processed — skip
        return
    # Process normally...
"""
from __future__ import annotations

import os
import sqlite3
import threading
from datetime import datetime, timezone


class IdempotencyStore:
    """Thread-safe SQLite idempotency store."""

    def __init__(self, db_path: str | None = None) -> None:
        if db_path is None:
            from app.config import settings
            db_path = settings.sqlite_db_path

        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

        self._db_path = db_path
        self._local = threading.local()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(
                self._db_path,
                timeout=10,
                check_same_thread=False,
            )
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA synchronous=NORMAL")
        return self._local.conn

    def _init_db(self) -> None:
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS processed_events (
                stage TEXT NOT NULL,
                event_id TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                payload_hash TEXT,
                PRIMARY KEY (stage, event_id)
            );

            CREATE TABLE IF NOT EXISTS order_idempotency (
                event_id TEXT PRIMARY KEY,
                broker_order_id TEXT NOT NULL,
                ticker TEXT,
                direction TEXT,
                qty REAL,
                submitted_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS daily_loss_tracker (
                trade_date TEXT PRIMARY KEY,
                realized_loss REAL DEFAULT 0.0,
                order_count INTEGER DEFAULT 0,
                consecutive_losses INTEGER DEFAULT 0,
                last_loss_at TEXT
            );
        """)
        conn.commit()

    def check_and_mark(
        self,
        stage: str,
        event_id: str,
        payload_hash: str | None = None,
    ) -> bool:
        """
        Check if event was already processed by this stage.
        If not, mark it as processed and return False (= new, process it).
        If yes, return True (= duplicate, skip it).
        """
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        try:
            conn.execute(
                "INSERT INTO processed_events (stage, event_id, processed_at, payload_hash) "
                "VALUES (?, ?, ?, ?)",
                (stage, event_id, now, payload_hash),
            )
            conn.commit()
            return False  # New event
        except sqlite3.IntegrityError:
            return True  # Already processed

    def was_order_submitted(self, event_id: str) -> str | None:
        """Check if an order was already submitted for this event_id.
        Returns broker_order_id if exists, None otherwise."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT broker_order_id FROM order_idempotency WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        return row[0] if row else None

    def record_order(
        self,
        event_id: str,
        broker_order_id: str,
        ticker: str = "",
        direction: str = "",
        qty: float = 0.0,
    ) -> None:
        """Record a broker order submission for idempotency."""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        try:
            conn.execute(
                "INSERT INTO order_idempotency "
                "(event_id, broker_order_id, ticker, direction, qty, submitted_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (event_id, broker_order_id, ticker, direction, qty, now),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            pass  # Already recorded

    def record_order_outcome(self, trade_date: str, is_loss: bool, loss_amount: float = 0.0) -> dict:
        """Track daily order count and consecutive losses. Returns current stats."""
        conn = self._get_conn()
        conn.execute(
            "INSERT INTO daily_loss_tracker (trade_date, realized_loss, order_count, consecutive_losses) "
            "VALUES (?, 0.0, 0, 0) ON CONFLICT(trade_date) DO NOTHING",
            (trade_date,),
        )
        if is_loss:
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "UPDATE daily_loss_tracker SET "
                "realized_loss = realized_loss + ?, "
                "order_count = order_count + 1, "
                "consecutive_losses = consecutive_losses + 1, "
                "last_loss_at = ? "
                "WHERE trade_date = ?",
                (abs(loss_amount), now, trade_date),
            )
        else:
            conn.execute(
                "UPDATE daily_loss_tracker SET "
                "order_count = order_count + 1, "
                "consecutive_losses = 0 "
                "WHERE trade_date = ?",
                (trade_date,),
            )
        conn.commit()
        row = conn.execute(
            "SELECT realized_loss, order_count, consecutive_losses, last_loss_at "
            "FROM daily_loss_tracker WHERE trade_date = ?",
            (trade_date,),
        ).fetchone()
        return {
            "realized_loss": row[0],
            "order_count": row[1],
            "consecutive_losses": row[2],
            "last_loss_at": row[3],
        }

    def get_daily_stats(self, trade_date: str) -> dict:
        """Get current daily trading stats."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT realized_loss, order_count, consecutive_losses, last_loss_at "
            "FROM daily_loss_tracker WHERE trade_date = ?",
            (trade_date,),
        ).fetchone()
        if not row:
            return {"realized_loss": 0.0, "order_count": 0, "consecutive_losses": 0, "last_loss_at": None}
        return {
            "realized_loss": row[0],
            "order_count": row[1],
            "consecutive_losses": row[2],
            "last_loss_at": row[3],
        }

    def close(self) -> None:
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
