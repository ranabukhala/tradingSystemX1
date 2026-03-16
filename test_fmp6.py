import httpx, os, json
key = os.environ.get('FMP_API_KEY', '')
base = 'https://financialmodelingprep.com'

r = httpx.get(base + '/stable/insider-trading-transaction-type', params={'apikey': key, 'symbol': 'AAPL', 'limit': '5'}, timeout=10)
print('=== /stable/insider-trading-transaction-type ===')
print('Status: %d' % r.status_code)
data = json.loads(r.text)
if isinstance(data, list):
    print(json.dumps(data[:3], indent=2))
else:
    print(json.dumps(data, indent=2))