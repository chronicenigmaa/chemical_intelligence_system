# scripts/produce_unsent_unlinked.py
from __future__ import annotations
import argparse, os, json, time
from typing import Mapping, Iterable, List
from aiokafka import AIOKafkaProducer
from sqlalchemy import create_engine, text
from app.core.config import settings
from app.core.json_sanitize import sanitize_json

BOOT   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC  = os.getenv("KAFKA_REQUESTS_TOPIC", "chem.ingest.requests.v1")
CID    = os.getenv("KAFKA_CLIENT_ID", "ccw-ingest")
BATCH  = int(os.getenv("BATCH", "2000"))

ENGINE = create_engine(settings.DATABASE_URL.replace("+asyncpg", "+psycopg"), future=True, pool_pre_ping=True)

SQL_PAGE = """
SELECT imq.id AS qid, imq.source_item_id, imq.query, COALESCE(imq.hint,'name') AS hint
FROM item_master_query imq
JOIN item_master_clean c ON c.source_item_id = imq.source_item_id
LEFT JOIN product_chem_xref x
  ON x.product_id = c.source_item_id AND x.source='pubchem'
LEFT JOIN produced_query_log l
  ON l.source_item_id = imq.source_item_id
 AND l.query = imq.query
 AND l.hint  = COALESCE(imq.hint,'name')
WHERE x.id IS NULL          -- unlinked only
  AND l.query IS NULL       -- not produced yet
  AND imq.id > :after
ORDER BY imq.id
LIMIT :lim;
"""

def fetch_page(after: int, limit: int) -> List[Mapping]:
    with ENGINE.begin() as c:
        return c.execute(text(SQL_PAGE), {"after": after, "lim": limit}).mappings().all()

def row_key(r: Mapping) -> str:
    return f"pubchem|{(r['hint'] or 'name').lower()}|{r['source_item_id']}|{(r['query'] or '').strip().lower()}"

async def produce(rows: Iterable[Mapping]) -> int:
    prod = AIOKafkaProducer(
        bootstrap_servers=BOOT, client_id=CID, acks="all",
        enable_idempotence=True, linger_ms=150, request_timeout_ms=30000,
    )
    await prod.start()
    sent = 0
    try:
        for i, r in enumerate(rows, 1):
            q = (r["query"] or "").strip()
            if not q: continue
            key = row_key(r)
            msg = {
                "query": q,
                "hint": (r["hint"] or "auto").lower(),
                "source": "pubchem",
                "source_item_id": r["source_item_id"],
                "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            await prod.send_and_wait(TOPIC, key=key.encode(), value=json.dumps(sanitize_json(msg)).encode())
            sent += 1
    finally:
        await prod.stop()
    # log what we sent so we don’t resend next time
    if sent:
        with ENGINE.begin() as c:
            c.execute(
                text("""
                  INSERT INTO produced_query_log (qid, source_item_id, query, hint)
                  SELECT :qid, :sid, :q, :h
                  ON CONFLICT DO NOTHING
                """),
                [
                    {
                        "qid": r["qid"],
                        "sid": r["source_item_id"],
                        "q": (r["query"] or "").strip(),
                        "h": (r["hint"] or "name").lower(),
                    } for r in rows
                ]
            )
    return sent

async def main():
    import asyncio
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, default=BATCH)
    args = parser.parse_args()

    print(f"[info] KAFKA_BOOTSTRAP={BOOT} TOPIC={TOPIC} CLIENT_ID={CID}")
    after, total = 0, 0
    while True:
        page = fetch_page(after, args.batch)
        if not page:
            break
        sent = await produce(page)
        total += sent
        after = page[-1]["qid"]
        print(f"[paged] sent this page={sent}  total={total}  last_qid={after}")
    print(f"[OK] Produced {total} messages (unsent+unlinked).")

if __name__ == "__main__":
    import asyncio; asyncio.run(main())
