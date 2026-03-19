"""
Shared trading-universe ticker set.

Used by:
  - entity_resolver  (ticker confidence scoring)
  - finlight connector (WebSocket subscription filter)

Single source of truth — update here when adding/removing tickers.
"""
from __future__ import annotations

KNOWN_TICKERS: set[str] = {
    # Mega cap
    "AAPL","MSFT","NVDA","GOOGL","GOOG","META","AMZN","TSLA","BRK.B","LLY",
    "V","JPM","UNH","XOM","JNJ","WMT","MA","PG","AVGO","HD",
    # Large cap tech
    "AMD","INTC","QCOM","TXN","MU","AMAT","LRCX","KLAC","MRVL","ORCL",
    "CRM","NOW","SNOW","DDOG","NET","CRWD","ZS","OKTA","PANW","FTNT",
    # Financials
    "GS","MS","BAC","WFC","C","BLK","SCHW","AXP","COF","USB",
    # Healthcare/Biotech
    "PFE","MRNA","ABBV","BMY","GILD","REGN","BIIB","VRTX","ISRG","MDT",
    # Consumer
    "COST","TGT","SBUX","MCD","NKE","LULU","DECK","TPR",
    # Energy
    "CVX","SLB","COP","EOG","PXD","MPC","VLO","PSX",
    # EV/Auto
    "GM","F","RIVN","LCID","NIO","LI","XPEV",
    # ETFs
    "SPY","QQQ","IWM","DIA","GLD","SLV","USO","TLT","HYG","XLF",
    "XLK","XLE","XLV","XLI","ARKK","SOXS","SOXL","TQQQ","SQQQ",
    # High-volume meme/momentum
    "GME","AMC","BBBY","PLTR","SOFI","HOOD","COIN","MSTR","RIOT","MARA",
    "CLSK","HUT","BITF","CIFR","WULF",
    # Crypto proxies
    "IBIT","FBTC","GBTC","ETHE",
    # Aerospace/Defense
    "BA","LMT","RTX","NOC","GD","LHX",
    # Airlines/Transport
    "DAL","UAL","AAL","LUV","FDX","UPS","UBER","LYFT",
    # Automotive
    "TM",
    # Big Tech (gaps)
    "NFLX","SPOT","PINS","SNAP","SHOP","ABNB","DASH","RBLX","TWLO","ROKU",
    "SQ","PYPL","ADBE","CSCO","IBM",
    # Semiconductors (gaps)
    "ARM","ASML","TSM",
    # Healthcare/Pharma (gaps)
    "MRK","AMGN","NVO","AZN",
    # Energy (gaps)
    "HAL",
    # Retail/Consumer (gaps)
    "LOW","DLTR","DG","KR","CMG","YUM","DIS",
    # Telecom/Media
    "T","VZ","TMUS","CMCSA","WBD",
    # Industrials
    "CAT","DE","MMM","HON","GE",
    # AI/Cloud (gaps)
    "PATH","NBIS",
}
