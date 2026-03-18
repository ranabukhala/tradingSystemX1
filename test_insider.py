import httpx, os, json
key = os.environ.get('FINNHUB_API_KEY', '')
r = httpx.get('https://finnhub.io/api/v1/stock/insider-transactions',
    params={'symbol': 'AAPL', 'token': key},
    timeout=10)
data = r.json()
if isinstance(data, dict):
    txns = data.get('data', [])
    print('Total transactions:', len(txns))
    for t in txns[:3]:
        print(json.dumps(t, indent=2))