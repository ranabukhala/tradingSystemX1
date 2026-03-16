import httpx, os, json
key = os.environ.get('UNUSUAL_WHALES_API_KEY', '')
base = 'https://api.unusualwhales.com/api'
headers = {'Authorization': 'Bearer ' + key}

paths = [
    '/insider/AAPL/transactions',
    '/insider/AAPL/trades',
    '/insider/AAPL/filings',
    '/insider-transactions',
    '/stock/AAPL/insider',
    '/stock/AAPL/insider-buys',
    '/stock/AAPL/sec-filings',
    '/stock/AAPL/flow',
    '/congress/trades',
    '/senate/trades',
]
for p in paths:
    url = base + p
    params = {'limit': '3', 'symbol': 'AAPL', 'ticker': 'AAPL'}
    r = httpx.get(url, headers=headers, params=params, timeout=10)
    has_data = r.status_code == 200 and len(r.text.strip()) > 10
    status = 'OK' if has_data else 'FAIL'
    print('%d %s %s' % (r.status_code, status, p))
    if has_data:
        data = json.loads(r.text)
        if isinstance(data, dict) and 'data' in data:
            sample = data['data'][:1] if isinstance(data['data'], list) else str(data['data'])[:300]
        elif isinstance(data, list):
            sample = data[:1]
        else:
            sample = str(data)[:300]
        print(json.dumps(sample, indent=2)[:400])
        print()