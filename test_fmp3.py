import httpx, os
key = os.environ.get('FMP_API_KEY', '')
base = 'https://financialmodelingprep.com'
paths = [
    '/stable/upgrades-downgrades',
    '/stable/upgrades-downgrades-consensus',
    '/stable/analyst-estimates',
    '/stable/price-target',
    '/stable/price-target-summary',
    '/stable/institutional-holder',
    '/stable/insider-trading-rss-feed',
    '/stable/cik-search',
    '/stable/insider-roaster',
    '/stable/stock-grade',
]
for p in paths:
    url = base + p
    params = {'apikey': key, 'symbol': 'AAPL', 'limit': '3'}
    r = httpx.get(url, params=params, timeout=10)
    has_data = r.status_code == 200 and len(r.text.strip()) > 10
    status = 'OK' if has_data else 'FAIL'
    print('%d %s %s' % (r.status_code, status, p))