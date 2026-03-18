import httpx, os, json
key = os.environ.get('FINNHUB_API_KEY', '')
r = httpx.get('https://finnhub.io/api/v1/stock/filings',
    params={'symbol': 'AAPL', 'token': key, 'from': '2026-01-01', 'to': '2026-03-17'},
    timeout=10)
data = r.json()
if isinstance(data, list):
    print('Total filings:', len(data))
    for f in data[:5]:
        print(json.dumps(f, indent=2)[:300])