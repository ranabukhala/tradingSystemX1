import httpx, os, json
from datetime import datetime, timezone

key = os.environ.get('FINNHUB_API_KEY', '')
base = 'https://finnhub.io/api/v1'

endpoints = [
    # News
    ('/news', {'category': 'general'}, 'General News'),
    ('/company-news', {'symbol': 'AAPL', 'from': '2026-03-17', 'to': '2026-03-17'}, 'Company News'),
    ('/press-releases', {'symbol': 'AAPL'}, 'Press Releases'),
    
    # Earnings
    ('/calendar/earnings', {'from': '2026-03-17', 'to': '2026-03-21'}, 'Earnings Calendar'),
    ('/stock/earnings', {'symbol': 'AAPL'}, 'Earnings Surprises'),
    
    # Analyst
    ('/stock/recommendation', {'symbol': 'AAPL'}, 'Analyst Recommendations'),
    ('/stock/price-target', {'symbol': 'AAPL'}, 'Price Targets'),
    ('/stock/upgrade-downgrade', {'symbol': 'AAPL'}, 'Upgrades/Downgrades'),
    
    # Insider
    ('/stock/insider-transactions', {'symbol': 'AAPL'}, 'Insider Transactions'),
    ('/stock/insider-sentiment', {'symbol': 'AAPL'}, 'Insider Sentiment (MSPR)'),
    
    # Fundamentals
    ('/stock/profile2', {'symbol': 'AAPL'}, 'Company Profile'),
    ('/stock/peers', {'symbol': 'AAPL'}, 'Peers'),
    ('/stock/metric', {'symbol': 'AAPL', 'metric': 'all'}, 'Financial Metrics'),
    
    # Sentiment
    ('/news-sentiment', {'symbol': 'AAPL'}, 'News Sentiment'),
    ('/stock/social-sentiment', {'symbol': 'AAPL', 'from': '2026-03-10', 'to': '2026-03-17'}, 'Social Sentiment'),
    
    # SEC
    ('/stock/filings', {'symbol': 'AAPL'}, 'SEC Filings'),
    
    # Alternative
    ('/stock/congressional-trading', {'symbol': 'AAPL'}, 'Congressional Trading'),
    ('/stock/lobbying', {'symbol': 'AAPL'}, 'Lobbying'),
    ('/stock/usa-spending', {'symbol': 'AAPL'}, 'USA Spending'),
    
    # Quote/Technical
    ('/quote', {'symbol': 'AAPL'}, 'Real-time Quote'),
    ('/indicator', {'symbol': 'AAPL', 'resolution': 'D', 'from': '1773500000', 'to': '1773770000', 'indicator': 'rsi', 'timeperiod': '14'}, 'Technical Indicator RSI'),
]

for path, params, label in endpoints:
    params['token'] = key
    try:
        r = httpx.get(base + path, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                size = len(data)
            elif isinstance(data, dict):
                size = len(json.dumps(data))
            else:
                size = 0
            print('200 OK   %-30s  items/bytes: %s' % (label, size))
        else:
            print('%d FAIL  %-30s  %s' % (r.status_code, label, r.text[:100]))
    except Exception as e:
        print('ERR     %-30s  %s' % (label, str(e)[:80]))