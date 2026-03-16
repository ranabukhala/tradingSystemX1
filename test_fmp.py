import httpx, os
key = os.environ.get('FMP_API_KEY', '')
base = 'https://financialmodelingprep.com'
paths = [
    '/api/v3/grade/AAPL',
    '/stable/grade/AAPL',
    '/api/v3/insider-trading',
    '/api/v4/insider-trading',
    '/stable/insider-trading-rss-feed',
]
for p in paths:
    url = base + p
    params = {'apikey': key, 'symbol': 'AAPL', 'limit': '1'}
    r = httpx.get(url, params=params, timeout=10)
    has_data = len(r.text.strip()) > 5
    status = 'DATA' if has_data else 'EMPTY'
    print('%d %s %s' % (r.status_code, status, p))