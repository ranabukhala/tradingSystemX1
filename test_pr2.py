import httpx, os, json
key = os.environ.get('FINNHUB_API_KEY', '')

# Check DOCU press releases (they report today)
for sym in ['DOCU', 'AAPL', 'FDX']:
    r = httpx.get('https://finnhub.io/api/v1/press-releases',
        params={'symbol': sym, 'token': key, 'from': '2026-01-20', 'to': '2026-03-17'},
        timeout=10)
    data = r.json()
    releases = data.get('majorDevelopment', []) if isinstance(data, dict) else []
    print('=== %s: %d releases ===' % (sym, len(releases)))
    if releases:
        print(json.dumps(releases[0], indent=2))
    print()