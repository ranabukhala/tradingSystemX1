import httpx, os, json
key = os.environ.get('FINNHUB_API_KEY', '')
r = httpx.get('https://finnhub.io/api/v1/press-releases',
    params={'symbol': 'NVDA', 'token': key, 'from': '2026-03-10', 'to': '2026-03-17'},
    timeout=10)
data = r.json()
if isinstance(data, dict):
    releases = data.get('majorDevelopment', [])
    print('Total releases:', len(releases))
    for pr in releases[:3]:
        print('---')
        print('Date:', pr.get('datetime', ''))
        print('Head:', pr.get('headline', '')[:100])
        print('Desc:', pr.get('description', '')[:200])
elif isinstance(data, list):
    print('Total:', len(data))
    for pr in data[:3]:
        print(json.dumps(pr, indent=2)[:300])