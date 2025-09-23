# scripts/produce_from_names.py
import asyncio, json, os, time
from aiokafka import AIOKafkaProducer
from sqlalchemy import create_engine, text
from app.core.config import settings
from app.core.json_sanitize import sanitize_json

BOOT  = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_REQUESTS_TOPIC", "chem.ingest.requests.v1")
CID   = os.getenv("KAFKA_CLIENT_ID", "ccw-ingest")
LIM   = int(os.getenv("LIMIT", "500"))

ENG = create_engine(settings.DATABASE_URL.replace("+asyncpg", "+psycopg"), future=True)

SQL = """
SELECT im.id AS source_item_id, im.name AS query, 'name' AS hint
FROM stg_item_master im
WHERE im.name IS NOT NULL AND im.name <> ''
ORDER BY im.id
LIMIT :lim
"""

async def main():
    with ENG.begin() as c:
        rows = c.execute(text(SQL), {"lim": LIM}).mappings().all()
    if not rows:
        print("[FATAL] stg_item_master has no names. Backfill names first.")
        return

    print(f"[info] publishing {len(rows)} name-based queries…")
    prod = AIOKafkaProducer(
        bootstrap_servers=BOOT,
        client_id=CID, acks="all",
        enable_idempotence=True, linger_ms=150, request_timeout_ms=30000,
    )
    await prod.start()
    sent = 0; t0 = time.perf_counter()
    try:
        for i, r in enumerate(rows, start=1):
            msg = {
                "query": r["query"].strip(),
                "hint": "name",
                "source": "pubchem",
                "requested_by": "produce_from_names",
                "source_item_id": r["source_item_id"],
                "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            await prod.send_and_wait(TOPIC, key=msg["query"].encode(), value=json.dumps(sanitize_json(msg)).encode())
            sent += 1
            if i % 200 == 0 or i == len(rows):
                dt = time.perf_counter() - t0
                print(f"[progress] sent={i}/{len(rows)} elapsed={dt:.1f}s")
    finally:
        await prod.stop()
    print(f"[OK] Produced {sent} messages to {TOPIC}")

if __name__ == "__main__":
    asyncio.run(main())
