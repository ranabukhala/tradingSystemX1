# Telegram Signal Alert — Deferred Improvements

Items 1–6 (news source/age, ADX/BB, sector breadth, event dedup ID, FMP RSI
cross-reference, price + ATR stop/target) are implemented in
`app/signals/telegram_alerts.py` as of 2026-03-18.

The following items were evaluated and deferred.  Each has a clear scope,
a known blocker, and an effort estimate so they can be picked up independently.

---

## Task T1 — Pre-market gap % and volume on pre-market signals

**What it adds**
```
🌅 Pre-market gap: +2.3%  ·  Vol 4.2M (2.1× avg)
```
Shown only when `session_context == "premarket"` or `"open"`.

**Why it matters**
Gap direction and early volume are the two fastest reads on whether a pre-market
catalyst has real follow-through.  Currently missing entirely.

**Blocker**
`stock_context:{ticker}` does not store previous close or intraday volume.
The stock context service fetches Polygon daily bars but discards the last
close price after computing indicators.

**Work required**
1. In `stock_context_service/classifier.py` → persist `prev_close` and
   `avg_daily_volume` into the `raw_metrics` dict already written to Redis.
2. In `telegram_alerts._format_price_line()` → when session is pre/open,
   compute `gap_pct = (price / prev_close - 1) * 100` and append.

**Effort:** ~2–3 h (one classifier change + one formatter change).

---

## Task T2 — Inline [✅ Trade] / [⏭ Skip] approval buttons

**What it adds**
Each Telegram alert gets two inline keyboard buttons.  Tapping ✅ queues the
signal for execution; tapping ⏭ suppresses it and writes a skip record to
Postgres for outcome tracking.

**Why it matters**
Eliminates the need to run the system in fully automated mode during early
testing.  One-tap approval from the phone without opening a terminal.

**Blocker**
Requires a persistent HTTP server (webhook) that Telegram POSTs callback
events to.  The current architecture is pure outbound — no incoming HTTP.

**Work required**
1. New service `app/admin/telegram_gateway.py` — FastAPI or aiohttp webhook
   endpoint registered via `setWebhook` on bot startup.
2. Callback router: parse `callback_query.data` (e.g. `"approve:signal_id"`),
   publish a command event to a new Kafka topic `signals.commands`.
3. Execution engine subscribes to `signals.commands` and processes approvals.
4. `_send_alert()` in `telegram_alerts.py` → use `sendMessage` with
   `reply_markup.inline_keyboard` instead of plain text.
5. Docker: expose the gateway port, configure Telegram webhook URL.

**Effort:** ~2–3 days.  Significant new service + Kafka topic.

---

## Task T3 — TradingView chart link (text) and optional chart image

### T3a — TradingView link (trivial, ~5 min)
Append to every alert footer:
```
📈 https://www.tradingview.com/chart/?symbol=NASDAQ:AAPL
```
Requires a ticker → exchange mapping (NASDAQ/NYSE) stored in Redis or
hardcoded for common tickers.  Minimal effort; no new services.

### T3b — Inline chart image (high effort)
Send a candlestick screenshot via Telegram `sendPhoto` instead of / alongside
the text message.

**Blocker**
No chart renderer in the pipeline.  Options:
- `mplfinance` + Polygon OHLCV bars → PNG → `sendPhoto`  (feasible, ~1 day)
- Headless browser screenshot of TradingView (violates ToS, not recommended)

**Work required (T3b)**
1. New `app/signals/chart_renderer.py` — fetch 5-day hourly OHLCV from Polygon,
   render with mplfinance, return PNG bytes.
2. `_send_alert()` → call `sendPhoto` with the PNG, include the text as caption.
3. Add `mplfinance` + `pillow` to `requirements.txt`.

**Effort T3a:** ~30 min.  **Effort T3b:** ~1 day.

---

## Task T4 — Options IV rank and implied move %

**What it adds**
```
IV rank: 82  ·  Implied move: ±3.4%
```
Shown on earnings and analyst catalyst signals.  IV rank > 80 = options are
expensive; consider whether the trade thesis is already priced into options.

**Why it matters**
The `priced_in` field from the LLM mentions implied moves in its reasoning but
doesn't surface the actual number.  For earnings plays especially, IV crush
post-event is the primary execution risk.

**Blocker**
No options data source in the pipeline.  FMP options endpoint requires
Professional plan ($).  Free/cheap alternatives:
- Tradier free paper-trading API (options chains, IV)
- CBOE delayed data (IV indices only, not per-stock rank)
- Alpaca has no options data on paper accounts

**Work required**
1. New connector `app/connectors/options/iv_fetcher.py` — polls IV rank and
   ATM implied move for active signal tickers; writes to Redis
   `options:iv:{ticker}` with 1 h TTL.
2. `telegram_alerts._fetch_context()` → add `options:iv:{ticker}` to mget.
3. `format_signal_message()` → append IV line for earnings/analyst catalysts.

**Effort:** ~3–5 days depending on data source availability and plan costs.

---

## Task T5 — Historical win rate for catalyst × ticker

**What it adds**
```
📈 History: 4/6 wins on AAPL earnings (67%)  ·  avg +2.8%
```

**Why it matters**
The system has no way to convey whether a similar setup has worked before.
A 67% win rate on AAPL earnings longs gives a trader real confidence context;
a 25% win rate should trigger skepticism regardless of current conviction score.

**Blockers**
1. Requires enough trade history (meaningful at 20+ trades per bucket).
2. Requires a Postgres aggregation query over `trades.executed` grouped by
   `(ticker, catalyst_type, direction)`.
3. At cold start there is zero history — the field would be empty or misleading.

**Work required**
1. New Postgres view or function: `signal_win_rates(ticker, catalyst, direction)`
   → returns `(total, wins, avg_pnl_pct)`.
2. Cache result in Redis `signal:winrate:{ticker}:{catalyst}:{direction}` with
   24 h TTL to avoid blocking the alert path with a DB query.
3. `telegram_alerts._fetch_context()` → read the cache key.
4. `format_signal_message()` → append win rate line when cache hit + total >= 5.

**Effort:** ~1–2 days.  Worth revisiting after 30+ trades are recorded.

---

## Task T6 — Portfolio exposure % and open position correlation

**What it adds**
```
Portfolio: 3.2% exposure  ·  Correlated: MSFT (+0.82), GOOGL (+0.79)
```
Shown only when there are open positions in the portfolio.

**Why it matters**
The execution engine currently sizes positions independently.  If you're already
long MSFT and GOOGL at the time an AAPL long signal fires, adding AAPL creates
a highly correlated tech-heavy portfolio — the signal message should surface
this risk explicitly before the trader or system acts.

**Blockers**
1. No live positions ledger maintained at signal time.  The position monitor
   tracks open positions but doesn't expose them to the signal layer.
2. Correlation matrix requires historical return data for all current holdings
   vs the incoming ticker.

**Work required**
1. `app/execution/position_monitor.py` → write open positions to Redis as
   `portfolio:positions` (JSON list of `{ticker, side, qty, value}`) on every
   update cycle.
2. New `app/signals/portfolio_context.py` → reads positions + fetches 20-day
   return correlation from Polygon for each holding vs the signal ticker.
3. `telegram_alerts._fetch_context()` → call portfolio context helper.
4. `format_signal_message()` → append exposure + top correlated positions.

**Effort:** ~3–5 days.  The correlation fetch adds latency to the alert path
(consider pre-computing and caching during market hours).

---

## Task T7 — Fill quality / slippage report on execution alert

**What it adds**
On the execution alert (sent by `execution_engine.py` when an order fills):
```
Fill: $191.52 (target $191.40)  ·  Slippage: +$0.12 (+0.06%)
```

**Why it matters**
The current execution alert shows the order details but not the actual fill
quality.  Over time, tracking slippage reveals whether limit orders are being
filled, whether pre-market fills are worse than intraday, and whether the broker
routing is acceptable.

**Blocker**
Alpaca paper trading fills arrive via order update webhook / polling.  The
execution engine sends the alert immediately on order submission, before the
fill confirmation arrives.  A second "fill confirmation" message would be needed.

**Work required**
1. In `app/execution/execution_engine.py` → after order submission, start a
   background polling task watching for fill status (Alpaca `GET /orders/{id}`).
2. On fill confirmation → send a short follow-up Telegram message with actual
   fill price vs target price and slippage.
3. Store fill data in Postgres `order_fills` table for analytics.

**Effort:** ~1–2 days.  Clean separation from existing alert flow.

---

## Summary

| ID | Feature | Effort | Main Blocker |
|----|---------|--------|-------------|
| T1 | Pre-market gap % + volume | 2–3 h | Add prev_close to stock_context classifier |
| T2 | Inline approve/skip buttons | 2–3 days | New webhook service + Kafka command topic |
| T3a | TradingView link | 30 min | Exchange mapping for tickers |
| T3b | Chart image in alert | 1 day | Chart renderer service (mplfinance) |
| T4 | Options IV rank + implied move | 3–5 days | Options data source / plan cost |
| T5 | Historical win rate | 1–2 days | Need 20+ trades; Postgres aggregation |
| T6 | Portfolio exposure + correlation | 3–5 days | Live positions ledger + correlation calc |
| T7 | Fill quality / slippage report | 1–2 days | Async fill confirmation polling |
