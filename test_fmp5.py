import httpx, os, json
key = os.environ.get('FMP_API_KEY', '')
base = 'https://financialmodelingprep.com'

# Check what ratings-snapshot returns
r = httpx.get(base + '/stable/ratings-snapshot', params={'apikey': key, 'symbol': 'AAPL'}, timeout=10)
print('=== /stable/ratings-snapshot ===')
print(json.dumps(json.loads(r.text)[:2], indent=2) if r.status_code == 200 else r.text[:200])
print()

# Try more insider-related paths
paths = [
    '/stable/insider-trading-transaction-type',
    '/stable/insider-ownership',
    '/stable/beneficial-ownership',
    '/stable/form-13f',
    '/stable/institutional-stock-ownership',
    '/stable/insider-trade',
    '/stable/insider',
    '/stable/ownership-by-holder',
]
for p in paths:
    url = base + p
    params = {'apikey': key, 'symbol': 'AAPL', 'limit': '3'}
    r = httpx.get(url, params=params, timeout=10)
    has_data = r.status_code == 200 and len(r.text.strip()) > 10
    status = 'OK' if has_data else 'FAIL'
    print('%d %s %s' % (r.status_code, status, p))