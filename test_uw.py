import httpx, os, json
key = os.environ.get('UNUSUAL_WHALES_API_KEY', '')
base = 'https://api.unusualwhales.com/api'
headers = {'Authorization': 'Bearer ' + key}

paths = [
    '/insider-trades/AAPL',
    '/insider-trading/AAPL',
    '/insider/AAPL',
    '/stock/AAPL/insider-trades',
    '/stock/AAPL/insider-transactions',
    '/stock/AAPL/congress-trades',
    '/congress/AAPL',
]
for p in paths:
    url = base + p
    r = httpx.get(url, headers=headers, params={'limit': '3'}, timeout=10)
    has_data = r.status_code == 200 and len(r.text.strip()) > 10
    status = 'OK' if has_data else 'FAIL'
    print('%d %s %s' % (r.status_code, status, p))
    if has_data:
        data = json.loads(r.text)
        if isinstance(data, dict) and 'data' in data:
            items = data['data'][:2] if isinstance(data['data'], list) else data['data']
        elif isinstance(data, list):
            items = data[:2]
        else:
            items = data
        print(json.dumps(items, indent=2)[:500])
        print()