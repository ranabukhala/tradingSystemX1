"""
Market Hours Scheduler.

Starts and stops the trading pipeline containers automatically based on
US Eastern market hours.  Uses the Docker SDK via a mounted Docker socket
so no separate process or cron job is required.

Behaviour
---------
* Polls every POLL_INTERVAL seconds (default 60).
* On the first tick after market open  → starts services in order.
* On the first tick after market close → stops  services in reverse order.
* Weekends and NYSE 2026 holidays are treated as non-trading days.
* Start/stop times are configurable via env vars (see below).

Env vars
--------
  MARKET_OPEN_HOUR    int  default 8   (8:00 AM ET)
  MARKET_OPEN_MIN     int  default 0
  MARKET_CLOSE_HOUR   int  default 16  (4:30 PM ET)
  MARKET_CLOSE_MIN    int  default 30

Services (in start order; reversed on stop)
-------------------------------------------
  trading_signals_ai_summarizer
  trading_signals_signal_aggregator
  trading_pretrade_filter
  trading_signals_telegram_alerts
  trading_execution_engine
  trading_position_monitor
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import date, datetime, timedelta
from typing import Optional

import docker
import docker.errors
import psycopg2
import pytz

# ── Timezone ───────────────────────────────────────────────────────────────────

EASTERN = pytz.timezone("US/Eastern")

# ── Tunable constants ──────────────────────────────────────────────────────────

POLL_INTERVAL = 60   # seconds between state checks
STOP_TIMEOUT  = 30   # seconds Docker waits before SIGKILL on stop

OPEN_HOUR  = int(os.environ.get("MARKET_OPEN_HOUR",  "8"))
OPEN_MIN   = int(os.environ.get("MARKET_OPEN_MIN",   "0"))
CLOSE_HOUR = int(os.environ.get("MARKET_CLOSE_HOUR", "16"))
CLOSE_MIN  = int(os.environ.get("MARKET_CLOSE_MIN",  "30"))

# ── Earnings-window constants ───────────────────────────────────────────────────
# Keep services running for BEFORE minutes before the expected report time
# and AFTER minutes after it, even outside regular market hours.

EARNINGS_WINDOW_BEFORE_MIN = int(os.environ.get("EARNINGS_WINDOW_BEFORE_MIN", "90"))
EARNINGS_WINDOW_AFTER_MIN  = int(os.environ.get("EARNINGS_WINDOW_AFTER_MIN",  "120"))

# ── Services under management ──────────────────────────────────────────────────
# Listed in start order; stop order is the reverse.

MANAGED_SERVICES: list[str] = [
    "trading_signals_ai_summarizer",
    "trading_signals_signal_aggregator",
    "trading_pretrade_filter",
    "trading_signals_telegram_alerts",
    "trading_execution_engine",
    "trading_position_monitor",
]

# ── NYSE holidays 2026 ─────────────────────────────────────────────────────────
# Source: NYSE holiday calendar.  Update this set each year.

NYSE_HOLIDAYS_2026: frozenset[date] = frozenset({
    date(2026,  1,  1),   # New Year's Day
    date(2026,  1, 19),   # Martin Luther King Jr. Day
    date(2026,  2, 16),   # Presidents' Day
    date(2026,  4,  3),   # Good Friday
    date(2026,  5, 25),   # Memorial Day
    date(2026,  6, 19),   # Juneteenth National Independence Day
    date(2026,  7,  3),   # Independence Day (observed; Jul 4 falls on Saturday)
    date(2026,  9,  7),   # Labor Day
    date(2026, 11, 26),   # Thanksgiving Day
    date(2026, 12, 25),   # Christmas Day
})

# ── DB helper ──────────────────────────────────────────────────────────────────

def _get_db_connection():
    """
    Synchronous psycopg2 connection for the scheduler.
    Uses POSTGRES_* env vars; defaults match the project's docker-compose values.
    Note: DATABASE_URL uses the asyncpg dialect (postgresql+asyncpg://) which
    psycopg2 cannot parse — so we use separate params with sensible defaults.
    """
    host = os.environ.get("POSTGRES_HOST", "postgres")
    return psycopg2.connect(
        host=host,
        port=int(os.environ.get("POSTGRES_PORT",     "5432")),
        user=os.environ.get("POSTGRES_USER",     "trading"),
        password=os.environ.get("POSTGRES_PASSWORD", "tradingpass"),
        dbname=os.environ.get("POSTGRES_DB",       "trading_db"),
    )


# ── Earnings-window helper ──────────────────────────────────────────────────────

def _in_earnings_window(dt_et: datetime) -> bool:
    """
    Return True if the current Eastern time falls within the configured window
    around a scheduled earnings report.

    Window logic:
      - BMO reports: expected at 5:00 AM ET → window [5:00 - BEFORE, 5:00 + AFTER]
      - AMC reports: expected at 4:10 PM ET → window [4:10 - BEFORE, 4:10 + AFTER]
      - Unknown timing: skipped (can't compute window without timing)

    Queries today + tomorrow to cover:
      - AMC reports today that extend past 4:30 PM
      - BMO reports tomorrow whose window starts tonight

    Result is cached for 5 minutes to avoid a DB hit every 60-second tick.
    Fails closed on any DB error (returns False — don't keep services up indefinitely).
    """
    # ── 5-minute in-process cache ────────────────────────────────────────────
    cached_result: bool | None = getattr(_in_earnings_window, "_cache_result", None)
    cached_ts: datetime | None = getattr(_in_earnings_window, "_cache_ts",    None)
    if cached_result is not None and cached_ts is not None:
        age = abs((dt_et.replace(tzinfo=None) - cached_ts.replace(tzinfo=None)).total_seconds())
        if age < 300:
            return cached_result

    def _set_cache(value: bool) -> bool:
        _in_earnings_window._cache_result = value
        _in_earnings_window._cache_ts     = dt_et
        return value

    try:
        conn = _get_db_connection()
        cur  = conn.cursor()
        today    = dt_et.date()
        tomorrow = today + timedelta(days=1)

        # Pull today's AND tomorrow's earnings with known timing
        cur.execute("""
            SELECT ticker, event_date, event_time
            FROM event
            WHERE event_type = 'earnings'
              AND event_date::date IN (%s, %s)
              AND lower(event_time) IN ('bmo', 'amc')
        """, (today, tomorrow))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        if not rows:
            return _set_cache(False)

        for ticker, event_date, event_time in rows:
            event_date_d = event_date.date() if hasattr(event_date, "date") else event_date
            time_lower   = (event_time or "").lower()

            if time_lower == "bmo":
                # Conservative: expect report around 5:00 AM ET
                report_dt = EASTERN.localize(
                    datetime.combine(event_date_d, datetime.min.time().replace(hour=5, minute=0))
                )
            elif time_lower == "amc":
                # Most AMC reports land 4:05–4:15 PM ET
                report_dt = EASTERN.localize(
                    datetime.combine(event_date_d, datetime.min.time().replace(hour=16, minute=10))
                )
            else:
                continue

            window_start = report_dt - timedelta(minutes=EARNINGS_WINDOW_BEFORE_MIN)
            window_end   = report_dt + timedelta(minutes=EARNINGS_WINDOW_AFTER_MIN)

            # Ensure dt_et is tz-aware for comparison
            dt_et_aware = EASTERN.localize(dt_et) if dt_et.tzinfo is None else dt_et

            if window_start <= dt_et_aware < window_end:
                _log("info", "scheduler.earnings_window_active",
                     ticker=ticker,
                     event_time=event_time,
                     report_et=report_dt.strftime("%Y-%m-%d %H:%M ET"),
                     window_start=window_start.strftime("%H:%M ET"),
                     window_end=window_end.strftime("%H:%M ET"))
                return _set_cache(True)

        return _set_cache(False)

    except Exception as exc:
        _log("warning", "scheduler.earnings_window_error", error=str(exc))
        return False   # Fail closed — don't keep services running on DB errors


# ── Logging ────────────────────────────────────────────────────────────────────

def _log(level: str, event: str, **kw) -> None:
    print(json.dumps({
        "ts":    datetime.now(pytz.utc).isoformat(),
        "level": level,
        "event": event,
        **kw,
    }), flush=True)


# ── Schedule helpers ───────────────────────────────────────────────────────────

def _is_trading_day(dt_et: datetime) -> bool:
    """True when dt_et (Eastern) is a weekday that is not a NYSE holiday."""
    d = dt_et.date()
    return d.weekday() < 5 and d not in NYSE_HOLIDAYS_2026


def _should_be_running(dt_et: datetime) -> bool:
    """
    True when services should be active at the given Eastern timestamp.

    Services run when ANY of the following is true:
      1. Normal market hours on a trading day (8:00 AM – 4:30 PM ET by default)
      2. Within the earnings window for a scheduled earnings report
         (covers BMO pre-open and AMC post-close opportunities)
    """
    # ── Normal market hours ───────────────────────────────────────────────────
    if _is_trading_day(dt_et):
        open_dt  = dt_et.replace(hour=OPEN_HOUR,  minute=OPEN_MIN,  second=0, microsecond=0)
        close_dt = dt_et.replace(hour=CLOSE_HOUR, minute=CLOSE_MIN, second=0, microsecond=0)
        if open_dt <= dt_et < close_dt:
            return True

    # ── Earnings window (any day — covers weekend BMO surprises too) ──────────
    return _in_earnings_window(dt_et)


# ── Scheduler ──────────────────────────────────────────────────────────────────

class MarketScheduler:
    """
    Watches the clock and starts/stops Docker containers on market open/close.

    State machine (3 states):
      None  — first tick; we don't know what the containers are doing yet
      True  — services should be running; we issued start_all this session
      False — services should be stopped; we issued stop_all this session

    On restart the scheduler re-evaluates the desired state on the very first
    tick and acts accordingly.  Starting an already-running container or stopping
    an already-stopped container is a safe no-op.
    """

    def __init__(self) -> None:
        try:
            self._client = docker.from_env()
            _log("info", "scheduler.docker_connected",
                 version=self._client.version().get("Version", "unknown"))
        except Exception as exc:
            _log("error", "scheduler.docker_connect_failed", error=str(exc))
            raise

        self._state:   Optional[bool] = None   # None = undetermined
        self._running: bool           = True

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def stop(self) -> None:
        """Signal the main loop to exit cleanly."""
        _log("info", "scheduler.stop_requested")
        self._running = False

    # ── Docker helpers ─────────────────────────────────────────────────────────

    def _get_container(self, name: str):
        try:
            return self._client.containers.get(name)
        except docker.errors.NotFound:
            _log("warning", "scheduler.container_not_found", container=name)
            return None
        except Exception as exc:
            _log("error", "scheduler.container_lookup_error",
                 container=name, error=str(exc))
            return None

    def _start_one(self, name: str) -> None:
        c = self._get_container(name)
        if c is None:
            return
        c.reload()                  # refresh status from daemon
        if c.status == "running":
            _log("debug", "scheduler.container_already_running", container=name)
            return
        try:
            c.start()
            _log("info", "scheduler.container_started", container=name)
        except Exception as exc:
            _log("error", "scheduler.container_start_failed",
                 container=name, error=str(exc))

    def _stop_one(self, name: str) -> None:
        c = self._get_container(name)
        if c is None:
            return
        c.reload()
        if c.status != "running":
            _log("debug", "scheduler.container_already_stopped",
                 container=name, status=c.status)
            return
        try:
            c.stop(timeout=STOP_TIMEOUT)
            _log("info", "scheduler.container_stopped",
                 container=name, timeout=STOP_TIMEOUT)
        except Exception as exc:
            _log("error", "scheduler.container_stop_failed",
                 container=name, error=str(exc))

    # ── Bulk operations ────────────────────────────────────────────────────────

    def _start_all(self) -> None:
        _log("info", "scheduler.starting_services",
             count=len(MANAGED_SERVICES),
             services=MANAGED_SERVICES)
        for name in MANAGED_SERVICES:
            self._start_one(name)
        _log("info", "scheduler.start_all_done")

    def _stop_all(self) -> None:
        stop_order = list(reversed(MANAGED_SERVICES))
        _log("info", "scheduler.stopping_services",
             count=len(stop_order),
             services=stop_order,
             stop_timeout_s=STOP_TIMEOUT)
        for name in stop_order:
            self._stop_one(name)
        _log("info", "scheduler.stop_all_done")

    # ── Main loop ──────────────────────────────────────────────────────────────

    async def run(self) -> None:
        _log("info", "scheduler.started",
             open_et=f"{OPEN_HOUR:02d}:{OPEN_MIN:02d}",
             close_et=f"{CLOSE_HOUR:02d}:{CLOSE_MIN:02d}",
             poll_interval_s=POLL_INTERVAL,
             managed_services=MANAGED_SERVICES)

        while self._running:
            try:
                now_et = datetime.now(pytz.utc).astimezone(EASTERN)
                target = _should_be_running(now_et)

                if target and self._state is not True:
                    # Market just opened (or scheduler restarted during hours)
                    _log("info", "scheduler.action",
                         action="start_all",
                         time_et=now_et.strftime("%H:%M:%S"),
                         day=now_et.strftime("%A %Y-%m-%d"))
                    await asyncio.to_thread(self._start_all)
                    self._state = True

                elif not target and self._state is not False:
                    # Market just closed (or scheduler restarted outside hours)
                    _log("info", "scheduler.action",
                         action="stop_all",
                         time_et=now_et.strftime("%H:%M:%S"),
                         day=now_et.strftime("%A %Y-%m-%d"))
                    await asyncio.to_thread(self._stop_all)
                    self._state = False

                else:
                    # No state change — log a heartbeat at debug level
                    _log("debug", "scheduler.tick",
                         time_et=now_et.strftime("%H:%M:%S"),
                         day=now_et.strftime("%A %Y-%m-%d"),
                         trading_day=_is_trading_day(now_et),
                         should_run=target,
                         state=self._state)

            except Exception as exc:
                _log("error", "scheduler.tick_error", error=str(exc))

            await asyncio.sleep(POLL_INTERVAL)

        _log("info", "scheduler.stopped")
