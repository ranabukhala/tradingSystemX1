# EODHD Backtesting Data — Evaluation Report

**Date:** 2026-03-17
**Evaluated by:** Claude (automated API testing)
**Status: GO — with caveats noted below**

---

## API Test Results (demo token)

| Endpoint | Status | Notes |
|---|---|---|
| `GET /api/eod/AAPL.US` (daily bars) | ✅ 200 | Full OHLCV + adjusted_close |
| `GET /api/intraday/AAPL.US?interval=1m` | ✅ 200 | timestamp, OHLCV, gmtoffset |
| `GET /api/div/AAPL.US` (dividends) | ✅ 200 | Declaration / record / payment dates |
| `GET /api/eod/SPCE.US` (delisted ticker) | ❌ 403 | Requires paid plan |
| `GET /api/eod-bulk-last-day/US` (bulk) | ❌ 403 | Requires paid plan |
| `GET /api/exchanges-list` | ❌ 403 | Requires paid plan |

### Field Coverage

**Daily bars** (`date, open, high, low, close, adjusted_close, volume`):
```json
{
  "date": "2026-03-16",
  "open": 252.105, "high": 253.885, "low": 249.88,
  "close": 252.82, "adjusted_close": 252.82,
  "volume": 30416398
}
```
`adjusted_close` is pre-calculated and accounts for splits and dividends — critical for correct backtesting P&L.

**Intraday 1m** (`timestamp, gmtoffset, datetime, open, high, low, close, volume`):
- `datetime` is local exchange time — no timezone guessing needed
- No adjusted close on intraday (expected — adjustments only applied to daily)

**Dividends** — full dividend history with declaration/record/payment dates available. Useful for corporate action filtering during backtesting (avoid entering positions on ex-div dates).

### Latency

| Request | Latency |
|---|---|
| 250 days of daily bars (MSFT) | ~1,850ms |
| 5 days of daily bars (AAPL) | ~280ms |
| 3 bars of intraday 1m | ~350ms |

**Assessment:** Acceptable for batch backtesting jobs. Not suitable for real-time use. Use parallelism (asyncio.gather) for bulk ticker downloads.

---

## Pricing (as of March 2026)

| Plan | Price | Includes |
|---|---|---|
| Free / Demo | $0 | 20 calls/day, current year only |
| EOD Historical Data | ~$19.99/mo | Daily bars, all history, 100K req/day |
| EOD + Intraday Extended | ~$39.99/mo | Daily + intraday, 100K req/day |
| All-In-One | ~$79.99/mo | Everything including delisted tickers, bulk endpoints |

**Recommendation:** Start with **EOD + Intraday Extended (~$40/mo)**. Upgrade to All-In-One if delisted ticker data is needed for survivorship-bias analysis.

---

## Go/No-Go Recommendation

### ✅ GO

**Rationale:**
1. Complete OHLCV + adjusted close on daily bars — essential for split-adjusted P&L
2. 1-minute intraday bars — enables intraday signal simulation
3. 100K req/day — sufficient to download and refresh 200 tickers × 5 years of daily bars in a single batch run (~200 API calls)
4. Dividends API — enables ex-div filtering
5. Simple REST API, easily wrapped in an async client

**Caveats:**
- **Delisted tickers require All-In-One plan** — the survivorship-bias benefit requires the more expensive tier. Evaluate whether it's needed for your backtesting goals before upgrading.
- **~1.85s per bulk request** — initial backfill of 200 tickers × 5 years takes ~6 minutes (200 calls × ~1.8s ÷ 60s, parallelized with concurrency=10 → ~37s). Acceptable.
- **No API key yet** — add `EODHD_API_KEY` to `.env` before implementing the connector.

---

## Implementation Plan

### 1. Add API Key

```bash
# .env
EODHD_API_KEY=your_key_here
```

### 2. `app/connectors/eodhd/client.py`

Async httpx client with:
- Base URL: `https://eodhd.com/api`
- Rate limiting: asyncio.Semaphore(10) for max 10 concurrent requests
- Retry: 3 attempts with exponential backoff for 5xx errors
- Symbol format: `{TICKER}.US` for US equities

```python
class EODHDClient:
    BASE = "https://eodhd.com/api"

    async def fetch_daily_bars(
        self, ticker: str, start: str, end: str
    ) -> list[dict]:
        """Returns list of {date, open, high, low, close, adjusted_close, volume}."""

    async def fetch_intraday_bars(
        self, ticker: str, date: str, interval: str = "1m"
    ) -> list[dict]:
        """Returns list of {timestamp, datetime, open, high, low, close, volume}."""

    async def fetch_dividends(
        self, ticker: str, start: str
    ) -> list[dict]:
        """Returns list of {date, declarationDate, recordDate, paymentDate, value}."""
```

### 3. `app/connectors/eodhd/historical.py`

Batch backfill logic:
- Accept a list of tickers + date range
- Fetch daily bars in parallel (asyncio.gather with semaphore)
- Upsert into `historical_bars` table
- Log progress: tickers fetched, rows inserted, latency

### 4. Database Schema (new migration `018_historical_bars.sql`)

```sql
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS historical_bars (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    ticker         VARCHAR(16) NOT NULL,
    bar_date       DATE        NOT NULL,      -- for daily bars; NULL for intraday
    bar_ts         TIMESTAMPTZ,               -- for intraday bars; NULL for daily
    timeframe      VARCHAR(8)  NOT NULL,      -- '1d' | '1m' | '5m'
    open           FLOAT       NOT NULL,
    high           FLOAT       NOT NULL,
    low            FLOAT       NOT NULL,
    close          FLOAT       NOT NULL,
    adjusted_close FLOAT,                     -- daily only; NULL for intraday
    volume         BIGINT      NOT NULL,
    source         VARCHAR(16) DEFAULT 'eodhd',
    created_at     TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_bar_ticker_ts_tf UNIQUE (ticker, bar_date, bar_ts, timeframe)
);

CREATE INDEX IF NOT EXISTS ix_hbars_ticker_date
    ON historical_bars (ticker, bar_date DESC, timeframe);
CREATE INDEX IF NOT EXISTS ix_hbars_ticker_ts
    ON historical_bars (ticker, bar_ts DESC)
    WHERE bar_ts IS NOT NULL;
```

### 5. Backtesting Usage Pattern

```python
# Replay a signal against historical data
async def backtest_signal(signal: TradingSignal, days_lookback: int = 5) -> dict:
    bars = await db.fetch(
        "SELECT bar_date, open, high, low, close, adjusted_close, volume "
        "FROM historical_bars "
        "WHERE ticker = $1 AND timeframe = '1d' "
        "  AND bar_date BETWEEN $2 AND $3 "
        "ORDER BY bar_date",
        signal.ticker,
        signal.news_published_at.date() - timedelta(days=days_lookback),
        signal.news_published_at.date() + timedelta(days=5),
    )
    # Compare signal direction vs next-day move (adjusted close)
```

---

## Next Steps

1. **Get an API key** at https://eodhd.com — start with EOD + Intraday Extended (~$40/mo)
2. **Add `EODHD_API_KEY` to `.env`**
3. **Create migration `018_historical_bars.sql`**
4. **Implement `app/connectors/eodhd/client.py`** (~100 lines)
5. **Implement `app/connectors/eodhd/historical.py`** (~80 lines)
6. **Run initial backfill** for all tickers in `WATCHLIST_TICKERS` (17 tickers × 5 years = ~17 API calls, <30s)
7. **Build `backtest_signal()` utility** for outcome tracking in `signal_log`

The `outcome_price_1d`, `outcome_price_1h`, `outcome_price_4h`, and `outcome_correct` columns already exist in `signal_log` — a nightly backfill job can populate them from `historical_bars` once the data is loaded.
