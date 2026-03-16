import httpx, os
key = os.environ.get('FMP_API_KEY', '')
base = 'https://financialmodelingprep.com'
paths = [
    '/stable/analyst-recommendations',
    '/stable/ratings',
    '/stable/ratings-snapshot',
    '/stable/grade',
    '/stable/grading',
    '/stable/analyst-grades',
    '/stable/upgrades-downgrades-by-company',
    '/stable/insider-trades',
    '/stable/insider-transactions',
    '/stable/ownership',
    '/stable/institutional-ownership',
    '/stable/senate-trading',
    '/stable/congress-trading',
]
for p in paths:
    url = base + p
    params = {'apikey': key, 'symbol': 'AAPL', 'limit': '3'}
    r = httpx.get(url, params=params, timeout=10)
    has_data = r.status_code == 200 and len(r.text.strip()) > 10
    status = 'OK' if has_data else 'FAIL'
    print('%d %s %s' % (r.status_code, status, p))