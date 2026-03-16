import httpx, os
key = os.environ.get('FMP_API_KEY', '')
base = 'https://financialmodelingprep.com'
paths = ['/api/v3/grade/AAPL', '/api/v4/insider-trading']
for p in paths:
    url = base + p
    params = {'apikey': key, 'symbol': 'AAPL', 'limit': '1'}
    r = httpx.get(url, params=params, timeout=10)
    print('--- %s ---' % p)
    print('Status: %d' % r.status_code)
    print(r.text[:300])
    print()