import httpx, os, json
key = os.environ.get('FINVIZ_API_KEY', '')

urls = [
    'https://elite.finviz.com/api/insider.ashx?ticker=AAPL&token=' + key,
    'https://elite.finviz.com/api/quote.ashx?ticker=AAPL&token=' + key,
    'https://finviz.com/api/insider_trading.ashx?ticker=AAPL&apikey=' + key,
]
for url in urls:
    r = httpx.get(url, timeout=10)
    has_data = r.status_code == 200 and len(r.text.strip()) > 10
    status = 'OK' if has_data else 'FAIL'
    short_url = url.split('?')[0]
    print('%d %s %s' % (r.status_code, status, short_url))
    if has_data:
        print(r.text[:400])
        print()