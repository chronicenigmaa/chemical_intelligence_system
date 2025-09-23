# scripts/produce_requests.py
import asyncio, json, os, re, time, argparse
from aiokafka import AIOKafkaProducer
from sqlalchemy import create_engine, text
from app.core.config import settings
from app.core.json_sanitize import sanitize_json

BOOT   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC  = os.getenv("KAFKA_REQUESTS_TOPIC", "chem.ingest.requests.v1")
CID    = os.getenv("KAFKA_CLIENT_ID", "ccw-ingest")
ENGINE = create_engine(settings.DATABASE_URL.replace("+asyncpg", "+psycopg"), future=True)

# name filters (you can tweak via env)
EXCLUDE_NAME_RE = os.getenv(
    "EXCLUDE_NAME_RE",
    r"(drum|tote|bottle|cap|lid|empty|pallet|charge|service|assembly|kit|tubing|hose|strap|pail|pump|liner|shipping|handling)"
)
INCLUDE_NAME_RE = os.getenv(
    "INCLUDE_NAME_RE",
    r"(acid|amine|amide|alcohol|oxide|chloride|sulfate|sulfonate|acetate|glycol|ketone|benz|propyl|ethyl|methyl|hydroxide|carbonate|siloxane|phosphate)"
)

CAS_RE = r"^\d{2,7}-\d{2}-\d$"

def raw_is_jsonb(conn) -> bool:
    row = conn.execute(text("""
        select data_type from information_schema.columns
        where table_name='stg_item_master' and column_name='raw'
    """)).first()
    return bool(row and row[0] == "jsonb")

def fetch_seed(conn, limit: int):
    return conn.execute(text("""
        select source_item_id, query, hint
        from item_master_query
        where query is not null and query <> ''
        order by id
        limit :lim
    """), {"lim": limit}).mappings().all()

def fetch_cas(conn, limit: int, use_jsonb: bool):
    parts = []
    # CAS from explicit column
    parts.append("""
        select im.id as source_item_id, im.cas as query, 'cas' as hint
        from stg_item_master im
        where im.cas ~ :casre
    """)
    # CAS found anywhere in raw JSON
    if use_jsonb:
        parts.append("""
        select im.id as source_item_id,
               (regexp_matches(lower(kv.value), '(\d{2,7}-\d{2}-\d)', 'g'))[1] as query,
               'cas' as hint
        from stg_item_master im,
             lateral jsonb_each_text(im.raw) as kv(key, value)
        where kv.value ~ '\d{2,7}-\d{2}-\d'
        """)
    sql = " union all ".join(parts) + " limit :lim"
    return conn.execute(text(sql), {"lim": limit, "casre": CAS_RE}).mappings().all()

def fetch_names(conn, limit: int, filtered: bool = True):
    if filtered:
        sql = """
        select im.id as source_item_id, im.name as query, 'name' as hint
        from stg_item_master im
        where im.name is not null and im.name <> ''
          and im.name !~* :exclude
          and im.name ~* :include
        order by im.id
        limit :lim
        """
        params = {"lim": limit, "exclude": EXCLUDE_NAME_RE, "include": INCLUDE_NAME_RE}
    else:
        sql = """
        select im.id as source_item_id, im.name as query, 'name' as hint
        from stg_item_master im
        where im.name is not null and im.name <> ''
        order by im.id
        limit :lim
        """
        params = {"lim": limit}
    return conn.execute(text(sql), params).mappings().all()

async def produce(rows):
    prod = AIOKafkaProducer(
        bootstrap_servers=BOOT,
        client_id=CID,
        acks="all",
        enable_idempotence=True,
        linger_ms=150,
        request_timeout_ms=30000,
    )
    await prod.start()
    sent = 0
    t0 = time.perf_counter()
    progress_every = int(os.getenv("PROGRESS_EVERY", "1000"))
    try:
        for i, r in enumerate(rows, start=1):
            q = (r["query"] or "").strip()
            if not q:
                continue
            msg = {
                "query": q,
                "hint": (r["hint"] or "auto").lower(),
                "source": "pubchem",
                "requested_by": "produce_requests",
                "source_item_id": r.get("source_item_id"),
                "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            await prod.send_and_wait(TOPIC, key=q.encode(), value=json.dumps(sanitize_json(msg)).encode())
            sent += 1
            if i % progress_every == 0 or i == len(rows):
                dt = time.perf_counter() - t0
                rate = i / dt if dt > 0 else 0.0
                print(f"[progress] sent={i}/{len(rows)} elapsed={dt:.1f}s rate={rate:.0f}/s")
    finally:
        await prod.stop()
    print(f"[OK] Produced {sent} messages to {TOPIC}")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=int(os.getenv("LIMIT", "1000")))
    parser.add_argument("--mode", choices=["auto","seed","cas","names"], default=os.getenv("MODE","auto"))
    parser.add_argument("--unfiltered", action="store_true", help="when mode=names, do not apply exclude/include regex")
    args = parser.parse_args()

    print(f"[info] KAFKA_BOOTSTRAP={BOOT} TOPIC={TOPIC} CLIENT_ID={CID}")
    with ENGINE.begin() as conn:
        total = conn.execute(text("select count(*) from stg_item_master")).scalar_one()
        has_names = conn.execute(text("select count(*) from stg_item_master where name is not null and name <> ''")).scalar_one()
        seed_ct = conn.execute(text("select count(*) from item_master_query")).scalar_one()
        print(f"[info] stg_total={total}  stg_has_name={has_names}  seed_rows={seed_ct}")

        use_jsonb = raw_is_jsonb(conn)

        rows = []
        if args.mode == "seed":
            rows = fetch_seed(conn, args.limit)
        elif args.mode == "cas":
            rows = fetch_cas(conn, args.limit, use_jsonb)
        elif args.mode == "names":
            rows = fetch_names(conn, args.limit, filtered=not args.unfiltered)
        else:  # auto
            rows = fetch_seed(conn, args.limit)
            if not rows:
                rows = fetch_cas(conn, args.limit, use_jsonb)
            if not rows:
                rows = fetch_names(conn, args.limit, filtered=True)
            if not rows:
                rows = fetch_names(conn, args.limit, filtered=False)

    if not rows:
        print("[warn] No usable rows found in any mode. Sending smoke tests.")
        rows = [
            {"source_item_id": None, "query": "aspirin", "hint": "name"},
            {"source_item_id": None, "query": "acetone", "hint": "name"},
            {"source_item_id": None, "query": "ethanol", "hint": "name"},
            {"source_item_id": None, "query": "50-78-2", "hint": "cas"},
        ]

    print("[info] preview:", rows[:5])
    await produce(rows)

if __name__ == "__main__":
    asyncio.run(main())
