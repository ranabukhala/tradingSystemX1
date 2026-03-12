"""
Outcome Tracker — runs nightly to fill in signal_log outcome columns.

For every signal from yesterday that hasn't been evaluated yet, fetches
closing prices from Polygon and computes:
  - outcome_price_1h   % move 1h after signal time
  - outcome_price_4h   % move 4h after signal time
  - outcome_price_1d   % move at end of trading day
  - outcome_correct    did price move in the predicted direction?

Run manually:
  docker exec trading_signals_ai_summarizer python -m app.scripts.outcome_tracker

Or add to crontab (runs at 6pm ET after market close):
  0 18 * * 1-5 docker exec trading_signals_ai_summarizer python -m app.scripts.outcome_tracker

Usage:
  python -m app.scripts.outcome_tracker            # yesterday's signals
  python -m app.scripts.outcome_tracker --date 2026-03-07   # specific date
  python -m app.scripts.outcome_tracker --days 7            # last 7 days
  python -m app.scripts.outcome_tracker --dry-run           # print only, no DB write
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone, timedelta, date
from typing import Optional

import asyncpg
import httpx

POLYGON_BASE = "https://api.polygon.io/v2"
DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://trading:trading@localhost:5432/trading_db"
)
POLYGON_KEY = os.environ.get("POLYGON_API_KEY", "")

# Min price move to count as "correct" — avoids noise on flat days
MIN_MOVE_PCT = 0.003   # 0.3%


async def run(target_date: date, dry_run: bool = False) -> None:
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Outcome tracker — {target_date}")
    print("=" * 60)

    conn = await asyncpg.connect(DB_URL)
    http = httpx.AsyncClient(timeout=15.0)

    try:
        # ── Fetch signals needing outcomes ─────────────────────────────────────
        rows = await conn.fetch("""
            SELECT id, ticker, direction, created_at, catalyst_type, conviction
            FROM signal_log
            WHERE outcome_correct IS NULL
              AND passed_gate = TRUE
              AND DATE(created_at AT TIME ZONE 'America/New_York') = $1
            ORDER BY created_at
        """, target_date)

        if not rows:
            print(f"No pending signals for {target_date}")
            return

        print(f"Found {len(rows)} signals to evaluate\n")

        # ── Group by ticker to minimise API calls ──────────────────────────────
        tickers = list({r["ticker"] for r in rows})
        price_cache: dict[str, dict] = {}

        for ticker in tickers:
            bars = await fetch_minute_bars(http, ticker, target_date)
            if bars:
                price_cache[ticker] = bars
            await asyncio.sleep(0.15)   # Polygon rate limit

        # ── Evaluate each signal ───────────────────────────────────────────────
        updated = 0
        results = []

        for row in rows:
            ticker    = row["ticker"]
            direction = row["direction"]
            signal_ts = row["created_at"]

            bars = price_cache.get(ticker)
            if not bars:
                print(f"  {ticker:6s} — no price data available")
                continue

            entry_price = get_price_at(bars, signal_ts)
            if not entry_price:
                print(f"  {ticker:6s} — no price at signal time {signal_ts.strftime('%H:%M')}")
                continue

            p1h  = get_price_at(bars, signal_ts + timedelta(hours=1))
            p4h  = get_price_at(bars, signal_ts + timedelta(hours=4))
            peod = get_eod_price(bars)

            chg_1h  = pct(entry_price, p1h)
            chg_4h  = pct(entry_price, p4h)
            chg_1d  = pct(entry_price, peod)

            # Correct = price moved in signal direction by at least MIN_MOVE_PCT
            if chg_1d is None:
                correct = None
            elif direction == "long":
                correct = chg_1d > MIN_MOVE_PCT
            elif direction == "short":
                correct = chg_1d < -MIN_MOVE_PCT
            else:
                correct = None

            results.append({
                "id":       row["id"],
                "ticker":   ticker,
                "direction": direction,
                "conviction": row["conviction"],
                "catalyst": row["catalyst_type"],
                "entry":    entry_price,
                "chg_1h":   chg_1h,
                "chg_4h":   chg_4h,
                "chg_1d":   chg_1d,
                "correct":  correct,
            })

            # Print result
            arrow = "✓" if correct else ("✗" if correct is False else "?")
            chg_str = f"{chg_1d:+.1f}%" if chg_1d is not None else "  n/a"
            print(
                f"  {arrow} {ticker:6s} {direction:5s} "
                f"conviction={row['conviction']:.2f}  "
                f"1d={chg_str:7s}  "
                f"1h={f'{chg_1h:+.1f}%' if chg_1h else 'n/a':6s}  "
                f"[{row['catalyst_type'] or 'other'}]"
            )

            if not dry_run:
                await conn.execute("""
                    UPDATE signal_log SET
                        outcome_price_1h  = $1,
                        outcome_price_4h  = $2,
                        outcome_price_1d  = $3,
                        outcome_correct   = $4,
                        outcome_filled_at = now()
                    WHERE id = $5
                """, chg_1h, chg_4h, chg_1d, correct, row["id"])
                updated += 1

        # ── Summary ────────────────────────────────────────────────────────────
        evaluated = [r for r in results if r["correct"] is not None]
        if evaluated:
            correct_n = sum(1 for r in evaluated if r["correct"])
            accuracy  = correct_n / len(evaluated) * 100
            avg_move  = sum(r["chg_1d"] for r in evaluated if r["chg_1d"]) / len(evaluated)

            print(f"\n{'=' * 60}")
            print(f"  Date:        {target_date}")
            print(f"  Evaluated:   {len(evaluated)} signals")
            print(f"  Correct:     {correct_n} / {len(evaluated)}  ({accuracy:.0f}%)")
            print(f"  Avg 1d move: {avg_move:+.2f}%")

            # Break down by catalyst type
            catalysts: dict[str, list] = {}
            for r in evaluated:
                cat = r["catalyst"] or "other"
                catalysts.setdefault(cat, []).append(r)

            if len(catalysts) > 1:
                print(f"\n  By catalyst type:")
                for cat, items in sorted(catalysts.items()):
                    c = sum(1 for x in items if x["correct"])
                    print(f"    {cat:15s} {c}/{len(items)} correct  ({c/len(items)*100:.0f}%)")

            # Conviction buckets
            print(f"\n  By conviction:")
            for lo, hi in [(0.0, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 1.01)]:
                bucket = [r for r in evaluated if lo <= r["conviction"] < hi]
                if bucket:
                    c = sum(1 for r in bucket if r["correct"])
                    label = f"{lo:.1f}–{hi:.1f}" if hi < 1.01 else f"{lo:.1f}+"
                    print(f"    conviction {label}:  {c}/{len(bucket)} correct  ({c/len(bucket)*100:.0f}%)")

            if not dry_run:
                print(f"\n  ✓ Updated {updated} rows in signal_log")
            else:
                print(f"\n  [DRY RUN] Would update {len(evaluated)} rows")
        else:
            print("\n  No evaluatable signals (no price data or all neutral)")

    finally:
        await http.aclose()
        await conn.close()


async def fetch_minute_bars(
    http: httpx.AsyncClient,
    ticker: str,
    day: date,
) -> dict[int, float] | None:
    """
    Fetch 1-minute OHLC bars for ticker on day.
    Returns dict of {unix_ms: close_price}.
    """
    if not POLYGON_KEY:
        print(f"  WARNING: POLYGON_API_KEY not set — cannot fetch prices for {ticker}")
        return None

    date_str = day.strftime("%Y-%m-%d")
    try:
        resp = await http.get(
            f"{POLYGON_BASE}/aggs/ticker/{ticker}/range/1/minute/{date_str}/{date_str}",
            params={"apiKey": POLYGON_KEY, "adjusted": "true", "limit": 500},
        )
        if resp.status_code != 200:
            return None
        results = resp.json().get("results", [])
        if not results:
            return None
        # Map unix_ms → close price
        return {bar["t"]: bar["c"] for bar in results}
    except Exception as e:
        print(f"  WARNING: price fetch failed for {ticker}: {e}")
        return None


def get_price_at(
    bars: dict[int, float],
    target_ts: datetime,
) -> float | None:
    """Find the closest bar price at or after target_ts."""
    target_ms = int(target_ts.timestamp() * 1000)
    # Find bar closest to target time (within 5 minutes)
    candidates = [
        (abs(ts - target_ms), price)
        for ts, price in bars.items()
        if abs(ts - target_ms) <= 5 * 60 * 1000
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda x: x[0])[1]


def get_eod_price(bars: dict[int, float]) -> float | None:
    """Get the last bar of the day (closest to 4pm ET close)."""
    if not bars:
        return None
    return bars[max(bars.keys())]


def pct(entry: float, exit_: Optional[float]) -> Optional[float]:
    if exit_ is None or entry == 0:
        return None
    return round((exit_ - entry) / entry * 100, 3)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fill signal outcome data from Polygon prices")
    parser.add_argument("--date",    help="Specific date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--days",    type=int, help="Process last N days")
    parser.add_argument("--dry-run", action="store_true", help="Print results, don't write to DB")
    args = parser.parse_args()

    if args.date:
        dates = [date.fromisoformat(args.date)]
    elif args.days:
        today = date.today()
        dates = [today - timedelta(days=i) for i in range(1, args.days + 1)]
        dates = [d for d in dates if d.weekday() < 5]   # Skip weekends
    else:
        yesterday = date.today() - timedelta(days=1)
        # If today is Monday, use Friday
        if yesterday.weekday() == 6:  # Sunday
            yesterday -= timedelta(days=1)
        elif yesterday.weekday() == 5:  # Saturday
            yesterday -= timedelta(days=1)
        dates = [yesterday]

    for d in dates:
        asyncio.run(run(d, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
