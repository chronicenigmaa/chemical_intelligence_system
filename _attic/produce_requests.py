# scripts/produce_requests.py
from __future__ import annotations
import argparse
import json
import os
import re
import time
from typing import Iterable, List, Mapping, Optional

from aiokafka import AIOKafkaProducer
from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.engine import Engine

from app.core.config import settings
from app.core.json_sanitize import sanitize_json

BOOT   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC  = os.getenv("KAFKA_REQUESTS_TOPIC", "chem.ingest.requests.v1")
CID    = os.getenv("KAFKA_CLIENT_ID", "ccw-ingest")

# regex for filtering names (tweak via env)
EXCLUDE_NAME_RE = os.getenv(
    "EXCLUDE_NAME_RE",
    r"(drum|tote|bottle|cap|lid|empty|pallet|charge|service|assembly|kit|tubing|hose|strap|pail|pump|liner|shipping|handling|case|refill|cartridge)"
)
INCLUDE_NAME_RE = os.getenv(
    "INCLUDE_NAME_RE",
    r"(acid|amine|amide|alcohol|oxide|chloride|sulfate|sulfite|sulfonate|acetate|glycol|ketone|benz|propyl|ethyl|methyl|hydroxide|carbonate|bicarbonate|phosphate|silicate|benzoate|nitrate|nitrite|bromide|iodide|citrate|lactate|tartrate|oxalate)"
)
CAS_RE = r"^\d{2,7}-\d{2}-\d$"

ENGINE: Engine = create_engine(
    settings.DATABASE_URL.replace("+asyncpg", "+psycopg"),
    future=True,
    pool_pre_ping=True,
)

def ensure_log_tables() -> None:
    """Create produced_query_log and helpful unique indexes (idempotent)."""
    with ENGINE.begin() as c:
        # Sent log: unique on (source_item_id, query, hint) so re-runs don't resend
        c.execute(text("""
            CREATE TABLE IF NOT EXISTS produced_query_log (
              qid BIGINT,
              source_item_id BIGINT,
              query TEXT NOT NULL,
              hint TEXT NOT NULL,
              produced_at timestamptz DEFAULT now()
            );
        """))
        c.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_produced_key
            ON produced_query_log (source_item_id, query, hint);
        """))
        # Helpful uniqueness on seed table too (doesn't fail if it exists)
        c.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_imq_source_query_hint
            ON item_master_query (source_item_id, query, hint);
        """))

def strip_noise_name_sql() -> str:
    """Returns the SQL body for strip_noise_name function (installed on demand)."""
    return """
    CREATE OR REPLACE FUNCTION strip_noise_name(n text) RETURNS text AS $FN$
    SELECT trim(regexp_replace(lower(n),
      '(
         \\b\\d{1,3}\\s*%(\\s*(w\\/v|v\\/v))?\\b
       )|(
         \\b(ml|l|kg|g|lb|gal|drum|tote|pail|bottle|case|kit|refill|cartridge)s?\\b
       )|(
         \\b(acs|tech(nical)?|reagent|usp|nf|bp|fcc|food|industrial|anhydrous|solution|aq\\.?|conc(entrated)?)\\b
       )|(
         [®™]
       )', ' ', 'gi'));
    $FN$ LANGUAGE sql IMMUTABLE;
    """

def row_key(r: Mapping) -> str:
    """Stable Kafka key for idempotence."""
    q = (r.get("query") or "").strip().lower()
    hint = (r.get("hint") or "auto").lower()
    sid = r.get("source_item_id")
    return f"pubchem|{hint}|{sid}|{q}"

# --------------------------
# Fetch helpers
# --------------------------
def fetch_seed_page_unsent(after_qid: int, limit: int) -> List[Mapping]:
    with ENGINE.begin() as c:
        rows = c.execute(text("""
            SELECT imq.id AS qid, imq.source_item_id, imq.query, COALESCE(imq.hint,'name') AS hint
            FROM item_master_query imq
            LEFT JOIN produced_query_log l
              ON l.source_item_id = imq.source_item_id
             AND l.query = imq.query
             AND l.hint = COALESCE(imq.hint,'name')
            WHERE imq.id > :after
              AND l.query IS NULL
              AND imq.query IS NOT NULL AND imq.query <> ''
            ORDER BY imq.id
            LIMIT :lim
        """), {"after": after_qid, "lim": limit}).mappings().all()
    return rows

def fetch_names(limit: int, filtered: bool = True) -> List[Mapping]:
    sql = """
    SELECT im.id AS source_item_id, im.name AS query, 'name' AS hint
    FROM item_master_clean im
    WHERE im.name IS NOT NULL AND im.name <> ''
    """
    params = {}
    if filtered:
        sql += " AND im.name !~* :exclude AND im.name ~* :include"
        params = {"exclude": EXCLUDE_NAME_RE, "include": INCLUDE_NAME_RE}
    sql += " ORDER BY im.id LIMIT :lim"
    params["lim"] = limit
    with ENGINE.begin() as c:
        rows = c.execute(text(sql), params).mappings().all()
    return rows

def fetch_cas(limit: int) -> List[Mapping]:
    with ENGINE.begin() as c:
        rows = c.execute(text(f"""
            SELECT im.id AS source_item_id, im.cas AS query, 'cas' AS hint
            FROM item_master_clean im
            WHERE im.cas ~ :casre
            ORDER BY im.id
            LIMIT :lim
        """), {"lim": limit, "casre": CAS_RE}).mappings().all()
    return rows

# --------------------------
# Produce & log
# --------------------------
async def produce(rows: Iterable[Mapping]) -> int:
    prod = AIOKafkaProducer(
        bootstrap_servers=BOOT,
        client_id=CID,
        acks="all",
        enable_idempotence=True,
        linger_ms=150,
        request_timeout_ms=30000,
    )
    await prod.start()
    seen = set()
    sent: List[Mapping] = []
    t0 = time.perf_counter()
    try:
        for i, r in enumerate(rows, start=1):
            q = (r.get("query") or "").strip()
            if not q:
                continue
            key = row_key(r)
            if key in seen:
                continue
            seen.add(key)
            msg = {
                "query": q,
                "hint": (r.get("hint") or "auto").lower(),
                "source": "pubchem",
                "source_item_id": r.get("source_item_id"),
                "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            await prod.send_and_wait(TOPIC, key=key.encode(), value=json.dumps(sanitize_json(msg)).encode())
            sent.append(r)
            if i % 1000 == 0:
                dt = time.perf_counter() - t0
                print(f"[progress] queued={i} unique={len(sent)} elapsed={dt:.1f}s")
    finally:
        await prod.stop()
    # log sent
    if sent:
        with ENGINE.begin() as c:
            c.execute(
                text("""
                  INSERT INTO produced_query_log (qid, source_item_id, query, hint)
                  VALUES (:qid, :sid, :q, :h)
                  ON CONFLICT ON CONSTRAINT uq_produced_key DO NOTHING
                """),
                [
                    {
                        "qid": r.get("qid"),
                        "sid": r.get("source_item_id"),
                        "q": (r.get("query") or "").strip(),
                        "h": (r.get("hint") or "name").lower(),
                    }
                    for r in sent
                ],
            )
    return len(sent)

# --------------------------
# Main
# --------------------------
async def main() -> None:
    import asyncio

    parser = argparse.ArgumentParser(description="Produce PubChem ingest requests to Kafka with de-duplication and paging.")
    parser.add_argument("--mode", choices=["auto", "seed", "cas", "names"], default=os.getenv("MODE", "seed"))
    parser.add_argument("--all", action="store_true", help="page through entire seed (seed mode)")
    parser.add_argument("--batch", type=int, default=int(os.getenv("BATCH", "2000")), help="page size when using --all")
    parser.add_argument("--limit", type=int, default=int(os.getenv("LIMIT", "1000")), help="single-batch cap for non-paged runs")
    parser.add_argument("--unfiltered", action="store_true", help="when mode=names, skip include/exclude regex")
    parser.add_argument("--install-strip-fn", action="store_true", help="create/replace strip_noise_name() in DB")
    args = parser.parse_args()

    print(f"[info] KAFKA_BOOTSTRAP={BOOT} TOPIC={TOPIC} CLIENT_ID={CID}")
    ensure_log_tables()
    if args.install_strip_fn:
        with ENGINE.begin() as c:
            c.execute(text(strip_noise_name_sql()))
        print("[info] installed strip_noise_name()")

    total = 0

    if args.mode == "seed":
        if args.all:
            after = 0
            while True:
                page = fetch_seed_page_unsent(after_qid=after, limit=args.batch)
                if not page:
                    break
                if total == 0:
                    print("[info] preview:", page[:5])
                sent = await produce(page)
                total += sent
                after = page[-1]["qid"]
                print(f"[paged] sent so far={total}, last_qid={after}")
            print(f"[OK] Produced {total} messages (seed, paged, unsent only).")
            return
        else:
            page = fetch_seed_page_unsent(after_qid=0, limit=args.limit)
            if not page:
                print("[warn] nothing unsent to publish from item_master_query")
                return
            print("[info] preview:", page[:5])
            total = await produce(page)
            print(f"[OK] Produced {total} messages (seed).")
            return

    # Non-seed modes (single batch; still dedup + log across runs)
    if args.mode == "cas":
        rows = fetch_cas(args.limit)
    elif args.mode == "names":
        rows = fetch_names(args.limit, filtered=not args.unfiltered)
    else:  # auto: seed → cas → names
        rows = []
        seed = fetch_seed_page_unsent(after_qid=0, limit=args.limit)
        if seed:
            rows = seed
        else:
            cas = fetch_cas(args.limit)
            rows = cas if cas else fetch_names(args.limit, filtered=True)
            if not rows:
                rows = fetch_names(args.limit, filtered=False)

    if not rows:
        print("[warn] no rows to publish in mode:", args.mode)
        return

    print("[info] preview:", rows[:5])
    total = await produce(rows)
    print(f"[OK] Produced {total} messages ({args.mode}).")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
