import asyncio, os
from openai import AsyncOpenAI
import asyncpg

QUERY = "UPDATE news_item SET title_embedding = " + chr(36) + "1::vector WHERE id = " + chr(36) + "2"

async def main():
    client = AsyncOpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
    print('Connecting to DB...')
    conn = await asyncio.wait_for(
        asyncpg.connect(host='postgres', port=5432, user='trading', password='tradingpass', database='trading_db'),
        timeout=10
    )
    print('Connected.')

    rows = await conn.fetch(
        "SELECT id, title FROM news_item WHERE title_embedding IS NULL AND tickers != '{}' ORDER BY created_at DESC"
    )
    print('Found %d items to backfill' % len(rows))

    done = 0
    failed = 0
    for row in rows:
        try:
            r = await client.embeddings.create(input=row['title'], model='text-embedding-3-small', dimensions=1536)
            vec = r.data[0].embedding
            vec_str = '[' + ','.join('%.8f' % v for v in vec) + ']'
            await conn.execute(QUERY, vec_str, row['id'])
            done += 1
            if done % 50 == 0:
                print('Progress: %d / %d' % (done, len(rows)))
        except Exception as e:
            failed += 1
            if failed <= 3:
                print('FAILED id=%s: %s' % (row['id'], e))
            if failed > 20:
                print('Too many failures, stopping.')
                break

    print('Done. Backfilled %d / %d (failed: %d)' % (done, len(rows), failed))
    await conn.close()

asyncio.run(main())