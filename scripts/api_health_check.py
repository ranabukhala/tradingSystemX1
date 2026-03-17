#!/usr/bin/env python3
"""
API Health Check — Trading System
==================================
Tests all external API connections used by the trading pipeline.
Reads keys from .env file in the current directory.

Usage:
    python api_health_check.py
    python api_health_check.py --env /path/to/.env
    python api_health_check.py --no-telegram   # skip Telegram report
    python api_health_check.py --json-out report.json

Output:
    - Colored terminal table (pass/fail + latency + detail)
    - JSON report file (api_health_<timestamp>.json by default)
    - Telegram message summarising results
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# ── Colour helpers ─────────────────────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
DIM    = "\033[2m"

def green(s):  return f"{GREEN}{s}{RESET}"
def red(s):    return f"{RED}{s}{RESET}"
def yellow(s): return f"{YELLOW}{s}{RESET}"
def cyan(s):   return f"{CYAN}{s}{RESET}"
def bold(s):   return f"{BOLD}{s}{RESET}"
def dim(s):    return f"{DIM}{s}{RESET}"


# ── Result dataclass ───────────────────────────────────────────────────────────

@dataclass
class ApiResult:
    api:        str                      # e.g. "Polygon"
    check:      str                      # e.g. "Stock Snapshot"
    status:     str = "SKIP"            # OK | FAIL | SKIP | WARN
    latency_ms: Optional[float] = None
    detail:     str = ""                 # human-readable detail or error
    data:       dict = field(default_factory=dict)  # raw sample data

    @property
    def icon(self) -> str:
        return {"OK": "✅", "FAIL": "❌", "SKIP": "⏭ ", "WARN": "⚠️ "}.get(self.status, "❓")

    @property
    def colour_status(self) -> str:
        fn = {"OK": green, "FAIL": red, "SKIP": yellow, "WARN": yellow}.get(self.status, dim)
        return fn(self.status)


# ── .env loader ────────────────────────────────────────────────────────────────

def load_env(path: str) -> dict[str, str]:
    env: dict[str, str] = {}
    p = Path(path)
    if not p.exists():
        print(yellow(f"  [warn] .env not found at {path} — using process environment only"))
        return env
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


# ── Individual API checks ──────────────────────────────────────────────────────

async def check_polygon(keys: dict, http) -> list[ApiResult]:
    results = []
    api_key = keys.get("POLYGON_API_KEY", "")
    if not api_key:
        return [ApiResult("Polygon", c, "SKIP", detail="POLYGON_API_KEY not set")
                for c in ["Stock Snapshot", "SMA Indicator", "Options Chain"]]

    import httpx

    # 1. Stock snapshot (SPY)
    r = ApiResult("Polygon", "Stock Snapshot (SPY)")
    try:
        t0 = time.monotonic()
        resp = await http.get(
            "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/SPY",
            params={"apiKey": api_key}, timeout=10
        )
        r.latency_ms = (time.monotonic() - t0) * 1000
        if resp.status_code == 200:
            data = resp.json().get("ticker", {})
            # Price field priority: day close → prev day close → last trade → last quote
            price = (
                data.get("day", {}).get("c") or
                data.get("prevDay", {}).get("c") or
                data.get("lastTrade", {}).get("p") or
                data.get("lastQuote", {}).get("P")
            )
            r.status = "OK"
            r.detail = f"SPY last=${price}"
            r.data = {"price": price}
        else:
            r.status = "FAIL"
            r.detail = f"HTTP {resp.status_code}: {resp.text[:120]}"
    except Exception as e:
        r.status = "FAIL"; r.detail = str(e)
    results.append(r)

    # 2. SMA indicator
    r = ApiResult("Polygon", "SMA Indicator (NVDA/20d)")
    try:
        t0 = time.monotonic()
        resp = await http.get(
            "https://api.polygon.io/v1/indicators/sma/NVDA",
            params={"timespan": "day", "window": 20, "series_type": "close",
                    "limit": 1, "apiKey": api_key}, timeout=10
        )
        r.latency_ms = (time.monotonic() - t0) * 1000
        if resp.status_code == 200:
            values = resp.json().get("results", {}).get("values", [])
            if values:
                r.status = "OK"
                r.detail = f"SMA20={values[0]['value']:.2f}"
                r.data = values[0]
            else:
                r.status = "WARN"
                r.detail = "No SMA values returned"
        else:
            r.status = "FAIL"
            r.detail = f"HTTP {resp.status_code}: {resp.text[:120]}"
    except Exception as e:
        r.status = "FAIL"; r.detail = str(e)
    results.append(r)

    # 3. Options chain (NVDA)
    r = ApiResult("Polygon", "Options Chain (NVDA)")
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        t0 = time.monotonic()
        resp = await http.get(
            "https://api.polygon.io/v3/snapshot/options/NVDA",
            params={"apiKey": api_key, "limit": 10,
                    "expiration_date.gte": today}, timeout=10
        )
        r.latency_ms = (time.monotonic() - t0) * 1000
        if resp.status_code == 200:
            results_data = resp.json().get("results", [])
            r.status = "OK" if results_data else "WARN"
            r.detail = f"{len(results_data)} contracts returned" if results_data else "0 contracts (market may be closed)"
        else:
            r.status = "FAIL"
            r.detail = f"HTTP {resp.status_code}: {resp.text[:120]}"
    except Exception as e:
        r.status = "FAIL"; r.detail = str(e)
    results.append(r)

    return results


async def check_unusual_whales(keys: dict, http) -> list[ApiResult]:
    api_key = keys.get("UNUSUAL_WHALES_API_KEY", "")
    r = ApiResult("Unusual Whales", "Flow Alerts (NVDA)")
    if not api_key:
        r.status = "SKIP"; r.detail = "UNUSUAL_WHALES_API_KEY not set"
        return [r]
    try:
        t0 = time.monotonic()
        resp = await http.get(
            "https://api.unusualwhales.com/api/stock/NVDA/flow-alerts",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"limit": 5}, timeout=10
        )
        r.latency_ms = (time.monotonic() - t0) * 1000
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            r.status = "OK"
            r.detail = f"{len(data)} flow alerts returned"
        elif resp.status_code == 401:
            r.status = "FAIL"; r.detail = "401 Unauthorized — invalid API key"
        elif resp.status_code == 403:
            r.status = "FAIL"; r.detail = "403 Forbidden — plan may not include this endpoint"
        else:
            r.status = "FAIL"; r.detail = f"HTTP {resp.status_code}: {resp.text[:120]}"
    except Exception as e:
        r.status = "FAIL"; r.detail = str(e)
    return [r]


async def check_finviz(keys: dict, http) -> list[ApiResult]:
    r = ApiResult("Finviz Elite", "Screener API Connectivity")
    api_key = keys.get("FINVIZ_API_KEY", "")
    if not api_key:
        r.status = "SKIP"; r.detail = "FINVIZ_API_KEY not set"
        return [r]
    try:
        import csv, io
        t0 = time.monotonic()
        # Test with a simple screener export — high short float stocks
        # This validates auth and confirms the export API is reachable
        resp = await http.get(
            "https://elite.finviz.com/export.ashx",
            params={"v": "111", "f": "sh_short_o20", "auth": api_key},
            timeout=12,
        )
        r.latency_ms = (time.monotonic() - t0) * 1000
        if resp.status_code == 200:
            rows = list(csv.DictReader(io.StringIO(resp.text)))
            r.status = "OK"
            r.detail = f"Screener OK — {len(rows)} high-short-float stocks returned"
            r.data = {"row_count": len(rows)}
        elif resp.status_code == 401:
            r.status = "FAIL"; r.detail = "401 Unauthorized — FINVIZ_API_KEY invalid or expired"
        else:
            r.status = "FAIL"; r.detail = f"HTTP {resp.status_code}: {resp.text[:120]}"
    except Exception as e:
        r.status = "FAIL"; r.detail = str(e)
    return [r]


async def check_fmp(keys: dict, http) -> list[ApiResult]:
    results = []
    api_key = keys.get("FMP_API_KEY", "")
    if not api_key:
        return [ApiResult("FMP", c, "SKIP", detail="FMP_API_KEY not set")
                for c in ["Quote (NVDA)", "RSI Indicator (NVDA)", "Sector Performance"]]

    base = "https://financialmodelingprep.com"

    # 1. Quote — stable endpoint
    r = ApiResult("FMP", "Quote (NVDA)")
    try:
        t0 = time.monotonic()
        resp = await http.get(
            f"{base}/stable/quote",
            params={"symbol": "NVDA", "apikey": api_key}, timeout=10
        )
        r.latency_ms = (time.monotonic() - t0) * 1000
        if resp.status_code == 200:
            data = resp.json()
            if data and isinstance(data, list):
                price = data[0].get("price")
                r.status = "OK"
                r.detail = f"NVDA price=${price}"
                r.data = data[0]
            elif isinstance(data, dict) and "Error Message" in data:
                r.status = "FAIL"; r.detail = data["Error Message"]
            else:
                r.status = "WARN"; r.detail = f"Unexpected response: {str(data)[:100]}"
        else:
            r.status = "FAIL"; r.detail = f"HTTP {resp.status_code}: {resp.text[:120]}"
    except Exception as e:
        r.status = "FAIL"; r.detail = str(e)
    results.append(r)

    # 2. RSI — requires paid FMP plan (returns 402 on free/starter)
    r = ApiResult("FMP", "RSI Indicator (NVDA)")
    try:
        t0 = time.monotonic()
        resp = await http.get(
            f"{base}/stable/technical-indicators/rsi",
            params={"symbol": "NVDA", "periodLength": 14, "timeframe": "1day", "apikey": api_key},
            timeout=10
        )
        r.latency_ms = (time.monotonic() - t0) * 1000
        if resp.status_code == 200:
            data = resp.json()
            if data and isinstance(data, list):
                rsi = data[0].get("value")
                r.status = "OK"
                r.detail = f"RSI(14)={float(rsi):.2f}" if rsi is not None else "RSI value null"
            elif isinstance(data, dict) and "Error Message" in data:
                r.status = "FAIL"; r.detail = data["Error Message"]
            else:
                r.status = "WARN"; r.detail = "No RSI data returned"
        elif resp.status_code == 402:
            r.status = "WARN"
            r.detail = "Plan restriction — RSI requires FMP paid plan (Polygon RSI used instead)"
        else:
            r.status = "FAIL"; r.detail = f"HTTP {resp.status_code}: {resp.text[:120]}"
    except Exception as e:
        r.status = "FAIL"; r.detail = str(e)
    results.append(r)

    # 3. Sector performance — requires date param
    r = ApiResult("FMP", "Sector Performance")
    try:
        from datetime import date as _date
        today_str = _date.today().isoformat()
        t0 = time.monotonic()
        resp = await http.get(
            f"{base}/stable/sector-performance-snapshot",
            params={"date": today_str, "apikey": api_key}, timeout=10
        )
        r.latency_ms = (time.monotonic() - t0) * 1000
        if resp.status_code == 200:
            data = resp.json()
            if data and isinstance(data, list):
                r.status = "OK"
                r.detail = f"{len(data)} sectors returned"
                r.data = data
            elif isinstance(data, dict) and "Error Message" in data:
                r.status = "FAIL"; r.detail = data["Error Message"]
            else:
                r.status = "WARN"; r.detail = f"Unexpected: {str(data)[:100]}"
        elif resp.status_code == 402:
            r.status = "WARN"
            r.detail = "Plan restriction — sector performance requires FMP paid plan"
        else:
            r.status = "FAIL"; r.detail = f"HTTP {resp.status_code}: {resp.text[:120]}"
    except Exception as e:
        r.status = "FAIL"; r.detail = str(e)
    results.append(r)

    return results


async def check_finnhub(keys: dict, http) -> list[ApiResult]:
    results = []
    api_key = keys.get("FINNHUB_API_KEY", "")
    if not api_key:
        return [ApiResult("Finnhub", c, "SKIP", detail="FINNHUB_API_KEY not set")
                for c in ["Company News", "Earnings Calendar"]]

    # 1. Company news
    r = ApiResult("Finnhub", "Company News (NVDA)")
    try:
        from datetime import timedelta
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
        t0 = time.monotonic()
        resp = await http.get(
            "https://finnhub.io/api/v1/company-news",
            params={"symbol": "NVDA", "from": week_ago, "to": today, "token": api_key},
            timeout=10
        )
        r.latency_ms = (time.monotonic() - t0) * 1000
        if resp.status_code == 200:
            data = resp.json()
            r.status = "OK"
            r.detail = f"{len(data)} articles in last 7 days"
        elif resp.status_code == 401:
            r.status = "FAIL"; r.detail = "401 Unauthorized — invalid API key"
        else:
            r.status = "FAIL"; r.detail = f"HTTP {resp.status_code}: {resp.text[:120]}"
    except Exception as e:
        r.status = "FAIL"; r.detail = str(e)
    results.append(r)

    # 2. Earnings calendar
    r = ApiResult("Finnhub", "Earnings Calendar")
    try:
        from datetime import timedelta
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        two_weeks = (datetime.now(timezone.utc) + timedelta(days=14)).strftime("%Y-%m-%d")
        t0 = time.monotonic()
        resp = await http.get(
            "https://finnhub.io/api/v1/calendar/earnings",
            params={"from": today, "to": two_weeks, "token": api_key},
            timeout=10
        )
        r.latency_ms = (time.monotonic() - t0) * 1000
        if resp.status_code == 200:
            data = resp.json().get("earningsCalendar", [])
            r.status = "OK"
            r.detail = f"{len(data)} earnings events in next 14 days"
        else:
            r.status = "FAIL"; r.detail = f"HTTP {resp.status_code}: {resp.text[:120]}"
    except Exception as e:
        r.status = "FAIL"; r.detail = str(e)
    results.append(r)

    return results


async def check_alpaca(keys: dict, http) -> list[ApiResult]:
    results = []
    api_key    = keys.get("ALPACA_API_KEY", "")
    secret_key = keys.get("ALPACA_SECRET_KEY", "")

    if not api_key or not secret_key:
        return [ApiResult("Alpaca", c, "SKIP", detail="ALPACA_API_KEY or ALPACA_SECRET_KEY not set")
                for c in ["Account", "Paper Orders"]]

    headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": secret_key}
    base = "https://paper-api.alpaca.markets"

    # 1. Account
    r = ApiResult("Alpaca", "Paper Account")
    try:
        t0 = time.monotonic()
        resp = await http.get(f"{base}/v2/account", headers=headers, timeout=10)
        r.latency_ms = (time.monotonic() - t0) * 1000
        if resp.status_code == 200:
            data = resp.json()
            equity = data.get("equity", "?")
            buying_power = data.get("buying_power", "?")
            status = data.get("status", "?")
            r.status = "OK"
            r.detail = f"status={status} equity=${float(equity):,.2f} buying_power=${float(buying_power):,.2f}"
            r.data = {"equity": equity, "buying_power": buying_power, "status": status}
        elif resp.status_code == 401:
            r.status = "FAIL"; r.detail = "401 Unauthorized — check API key/secret"
        elif resp.status_code == 403:
            r.status = "FAIL"; r.detail = "403 Forbidden — paper trading not enabled for this key"
        else:
            r.status = "FAIL"; r.detail = f"HTTP {resp.status_code}: {resp.text[:120]}"
    except Exception as e:
        r.status = "FAIL"; r.detail = str(e)
    results.append(r)

    # 2. Orders (just a list, no actual order placed)
    r = ApiResult("Alpaca", "Paper Orders Endpoint")
    try:
        t0 = time.monotonic()
        resp = await http.get(f"{base}/v2/orders", headers=headers,
                              params={"status": "all", "limit": 5}, timeout=10)
        r.latency_ms = (time.monotonic() - t0) * 1000
        if resp.status_code == 200:
            orders = resp.json()
            r.status = "OK"
            r.detail = f"{len(orders)} recent orders accessible"
        else:
            r.status = "FAIL"; r.detail = f"HTTP {resp.status_code}: {resp.text[:120]}"
    except Exception as e:
        r.status = "FAIL"; r.detail = str(e)
    results.append(r)

    return results


# ── Telegram reporter ──────────────────────────────────────────────────────────

async def send_telegram(keys: dict, results: list[ApiResult], http) -> None:
    token   = keys.get("TELEGRAM_BOT_TOKEN") or keys.get("ADMIN_BOT_TOKEN", "")
    chat_id = keys.get("TELEGRAM_CHAT_ID")   or keys.get("ADMIN_CHAT_ID", "")
    if not token or not chat_id:
        print(yellow("  [skip] Telegram: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set"))
        return

    ok   = [r for r in results if r.status == "OK"]
    fail = [r for r in results if r.status == "FAIL"]
    warn = [r for r in results if r.status == "WARN"]
    skip = [r for r in results if r.status == "SKIP"]

    icon = "✅" if not fail else "❌"
    lines = [
        f"{icon} *API Health Check* — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"",
        f"✅ OK: {len(ok)}  ❌ FAIL: {len(fail)}  ⚠️ WARN: {len(warn)}  ⏭ SKIP: {len(skip)}",
        f"",
    ]

    if fail:
        lines.append("*FAILING:*")
        for r in fail:
            lines.append(f"  ❌ {r.api} / {r.check}")
            lines.append(f"      _{r.detail[:80]}_")

    if warn:
        lines.append("*WARNINGS:*")
        for r in warn:
            lines.append(f"  ⚠️ {r.api} / {r.check}: _{r.detail[:80]}_")

    if ok:
        lines.append("*OK:*")
        for r in ok:
            lat = f" ({r.latency_ms:.0f}ms)" if r.latency_ms else ""
            lines.append(f"  ✅ {r.api} / {r.check}{lat}")

    if skip:
        lines.append(f"\n_Skipped (no key): {', '.join(r.api for r in skip)}_")

    text = "\n".join(lines)
    try:
        resp = await http.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
        if resp.status_code == 200:
            print(green("  ✅ Telegram report sent"))
        else:
            print(yellow(f"  ⚠️  Telegram send failed: HTTP {resp.status_code} {resp.text[:100]}"))
    except Exception as e:
        print(yellow(f"  ⚠️  Telegram send error: {e}"))


# ── Terminal reporter ──────────────────────────────────────────────────────────

def print_report(results: list[ApiResult]) -> None:
    col_api    = 16
    col_check  = 32
    col_status = 6
    col_lat    = 9
    col_detail = 55

    sep = "─" * (col_api + col_check + col_status + col_lat + col_detail + 12)
    header = (
        f"  {'API':<{col_api}} {'Check':<{col_check}} {'Status':<{col_status}} "
        f"{'Lat(ms)':<{col_lat}} Detail"
    )

    print()
    print(bold(cyan("═" * len(sep))))
    print(bold(cyan("  TRADING SYSTEM — API HEALTH CHECK")))
    print(bold(cyan(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")))
    print(bold(cyan("═" * len(sep))))
    print(bold(header))
    print(dim(sep))

    current_api = None
    for r in results:
        api_label = r.api if r.api != current_api else ""
        current_api = r.api

        lat = f"{r.latency_ms:>7.0f}" if r.latency_ms is not None else "      -"
        detail = r.detail[:col_detail]

        status_col = {"OK": green, "FAIL": red, "SKIP": yellow, "WARN": yellow}.get(r.status, dim)(
            f"{r.status:<{col_status}}"
        )

        print(
            f"  {bold(api_label):<{col_api+9}} {r.check:<{col_check}} "  # +9 for bold escape
            f"{status_col} {lat}  {detail}"
        )

    print(dim(sep))
    ok   = sum(1 for r in results if r.status == "OK")
    fail = sum(1 for r in results if r.status == "FAIL")
    warn = sum(1 for r in results if r.status == "WARN")
    skip = sum(1 for r in results if r.status == "SKIP")
    total = len(results)

    summary_colour = green if fail == 0 else red
    print(bold(f"\n  Summary: {summary_colour(f'{ok}/{total} checks passed')}  "
               f"[{green(f'OK:{ok}')}  {red(f'FAIL:{fail}')}  "
               f"{yellow(f'WARN:{warn}')}  {dim(f'SKIP:{skip}')}]"))
    print()


# ── JSON reporter ──────────────────────────────────────────────────────────────

def write_json(results: list[ApiResult], path: str) -> None:
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total": len(results),
            "ok":    sum(1 for r in results if r.status == "OK"),
            "fail":  sum(1 for r in results if r.status == "FAIL"),
            "warn":  sum(1 for r in results if r.status == "WARN"),
            "skip":  sum(1 for r in results if r.status == "SKIP"),
        },
        "checks": [asdict(r) for r in results],
    }
    Path(path).write_text(json.dumps(report, indent=2, default=str))
    print(green(f"  ✅ JSON report written to {path}"))


# ── Main ───────────────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(description="Trading system API health check")
    parser.add_argument("--env", default=".env", help="Path to .env file")
    parser.add_argument("--no-telegram", action="store_true", help="Skip Telegram report")
    parser.add_argument("--json-out", default="", help="JSON output path (default: auto-named)")
    args = parser.parse_args()

    # Load env
    keys = {**load_env(args.env), **os.environ}  # process env overrides .env

    print(bold(cyan("\n  Loading API keys from: ")) + args.env)
    key_status = {
        "POLYGON_API_KEY":          bool(keys.get("POLYGON_API_KEY")),
        "FINVIZ_API_KEY":           bool(keys.get("FINVIZ_API_KEY")),
        "UNUSUAL_WHALES_API_KEY":   bool(keys.get("UNUSUAL_WHALES_API_KEY")),
        "FMP_API_KEY":              bool(keys.get("FMP_API_KEY")),
        "FINNHUB_API_KEY":          bool(keys.get("FINNHUB_API_KEY")),
        "ALPACA_API_KEY":           bool(keys.get("ALPACA_API_KEY")),
        "ALPACA_SECRET_KEY":        bool(keys.get("ALPACA_SECRET_KEY")),
        "TELEGRAM_BOT_TOKEN":       bool(keys.get("TELEGRAM_BOT_TOKEN") or keys.get("ADMIN_BOT_TOKEN")),
    }
    for k, present in key_status.items():
        icon = green("✅") if present else red("❌")
        print(f"    {icon}  {k}")

    print()

    try:
        import httpx
    except ImportError:
        print(red("  httpx not installed. Run: pip install httpx"))
        sys.exit(1)

    all_results: list[ApiResult] = []

    async with httpx.AsyncClient(follow_redirects=True) as http:
        print(bold("  Running checks...\n"))

        checks = [
            ("Polygon",         check_polygon(keys, http)),
            ("Unusual Whales",  check_unusual_whales(keys, http)),
            ("Finviz",          check_finviz(keys, http)),
            ("FMP",             check_fmp(keys, http)),
            ("Finnhub",         check_finnhub(keys, http)),
            ("Alpaca",          check_alpaca(keys, http)),
        ]

        # Run all API groups concurrently
        check_results = await asyncio.gather(
            *[coro for _, coro in checks],
            return_exceptions=True,
        )

        for (name, _), result in zip(checks, check_results):
            if isinstance(result, Exception):
                all_results.append(ApiResult(name, "All checks", "FAIL",
                                             detail=f"Unexpected error: {result}"))
            else:
                all_results.extend(result)
            sys.stdout.write(f"  {green('✓')} {name}\n")
            sys.stdout.flush()

        # Print terminal table
        print_report(all_results)

        # Write JSON
        json_path = args.json_out or f"api_health_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
        write_json(all_results, json_path)

        # Send Telegram
        if not args.no_telegram:
            print(bold("\n  Sending Telegram report..."))
            await send_telegram(keys, all_results, http)

    print()


if __name__ == "__main__":
    asyncio.run(main())