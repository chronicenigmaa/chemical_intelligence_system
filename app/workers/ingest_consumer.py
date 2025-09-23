# app/workers/ingest_consumer.py
"""
Kafka → PubChem → Postgres worker with:
- small poll batches & frequent commits (avoids CommitFailedError)
- bounded scrape timeouts
- UPSERTs into scraped_compounds and product_chem_xref
- typed JSONB bindparams (no ::jsonb in SQL text)
"""

from __future__ import annotations
import asyncio
import json
import os
import re
import time
import traceback
from typing import Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from sqlalchemy import text, bindparam, Integer, String
from sqlalchemy.dialects.postgresql import insert as pg_insert, JSONB
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import settings
from app.core.json_sanitize import sanitize_json
from app.scrapers.manager import ScrapingManager
from app.db.session import AsyncSessionLocal
from app.db.models import ScrapedCompound

# -----------------------------
# Env-configurable knobs
# -----------------------------
REQ_TOPIC = os.getenv("KAFKA_REQUESTS_TOPIC", "chem.ingest.requests.v1")
RES_TOPIC = os.getenv("KAFKA_RESULTS_TOPIC",  "chem.ingest.results.v1")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC",      "chem.ingest.dlq.v1")
BOOT      = os.getenv("KAFKA_BOOTSTRAP",      "localhost:9092")
GROUP_ID  = os.getenv("KAFKA_GROUP_ID",       "ccw-ingest-workers")
CLIENT_ID = os.getenv("KAFKA_CLIENT_ID",      "ccw-ingest")

# Poll/commit behavior (tune via env without code changes)
MAX_POLL_RECORDS      = int(os.getenv("MAX_POLL_RECORDS", "100"))
MAX_POLL_INTERVAL_MS  = int(os.getenv("MAX_POLL_INTERVAL_MS", "900000"))  # 15 min
SESSION_TIMEOUT_MS    = int(os.getenv("SESSION_TIMEOUT_MS", "30000"))
HEARTBEAT_INTERVAL_MS = int(os.getenv("HEARTBEAT_INTERVAL_MS", "3000"))
BATCH_COMMIT_EVERY    = int(os.getenv("BATCH_COMMIT_EVERY", "20"))
SCRAPE_TIMEOUT_S      = float(os.getenv("SCRAPE_TIMEOUT_S", "12.0"))

# Minimal validation rules
CAS_RE = re.compile(r"^\d{2,7}-\d{2}-\d$")
MIN_Q_LEN = int(os.getenv("MIN_QUERY_LEN", "3"))
MAX_Q_LEN = int(os.getenv("MAX_QUERY_LEN", "200"))

def validate(req: dict) -> tuple[bool, Optional[str]]:
    q = (req.get("query") or "").strip()
    hint = (req.get("hint") or "auto").lower()
    if not q:
        return False, "EMPTY_QUERY"
    if len(q) < MIN_Q_LEN or len(q) > MAX_Q_LEN:
        return False, "BAD_QUERY_LENGTH"
    if hint == "cas" and not CAS_RE.match(q):
        return False, "INVALID_CAS"
    return True, None

async def persist_and_link(query: str, chosen: dict | None, source_item_id: Optional[int]) -> bool:
    """
    - Upsert into scraped_compounds on (source, external_id).
    - Upsert into product_chem_xref on (source, external_id, product_id) if product id provided.
    Uses typed JSONB bindparams to avoid ::jsonb SQL text casts.
    """
    if not chosen or not chosen.get("CID"):
        return False

    chosen = sanitize_json(chosen)
    cid = str(chosen["CID"])

    async with AsyncSessionLocal() as s:
        try:
            # --- UPSERT scraped_compounds ---
            table = ScrapedCompound.__table__
            stmt = pg_insert(table).values(
                query=query,
                source="pubchem",
                external_id=cid,
                properties=chosen
            ).on_conflict_do_update(
                # safer to reference the unique constraint if you named it:
                # constraint="uq_scraped_source_ext",
                index_elements=[table.c.source, table.c.external_id],
                set_={"query": query, "properties": chosen}
            )
            await s.execute(stmt)

            # --- UPSERT product_chem_xref (only if we know product id) ---
            if source_item_id:
                upsert_xref = text("""
                    INSERT INTO product_chem_xref
                        (product_id, source, external_id, cas, match_confidence, properties_ref)
                    VALUES
                        (:pid, 'pubchem', :cid, NULL, 0.95, :props)
                    ON CONFLICT ON CONSTRAINT uq_xref_source_ext_prod DO UPDATE
                        SET properties_ref = EXCLUDED.properties_ref,
                            updated_at = now()
                """).bindparams(
                    bindparam("pid",   type_=Integer),
                    bindparam("cid",   type_=String),
                    bindparam("props", type_=JSONB),
                )
                await s.execute(upsert_xref, {"pid": source_item_id, "cid": cid, "props": chosen})

            await s.commit()
            return True

        except SQLAlchemyError:
            await s.rollback()
            print("[persist_and_link] SQLAlchemyError\n", traceback.format_exc())
            return False
        except Exception:
            await s.rollback()
            print("[persist_and_link] ERROR\n", traceback.format_exc())
            return False

async def run_worker() -> None:
    consumer = AIOKafkaConsumer(
        REQ_TOPIC,
        bootstrap_servers=BOOT,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=MAX_POLL_RECORDS,
        max_poll_interval_ms=MAX_POLL_INTERVAL_MS,
        session_timeout_ms=SESSION_TIMEOUT_MS,
        heartbeat_interval_ms=HEARTBEAT_INTERVAL_MS,
        client_id=f"{CLIENT_ID}-consumer",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOT,
        acks="all",
        client_id=f"{CLIENT_ID}-producer",
    )
    mgr = ScrapingManager()

    await consumer.start()
    await producer.start()

    print(f"[worker] DB = {settings.DATABASE_URL}")
    print(f"[worker] started on {BOOT} | group={GROUP_ID} | topics: {REQ_TOPIC} → ({RES_TOPIC} / {DLQ_TOPIC})")
    print(f"[worker] poll={MAX_POLL_RECORDS} max_poll_interval_ms={MAX_POLL_INTERVAL_MS} commit_every={BATCH_COMMIT_EVERY} timeout_s={SCRAPE_TIMEOUT_S}")

    processed_since_commit = 0
    try:
        while True:
            batches = await consumer.getmany(timeout_ms=400, max_records=MAX_POLL_RECORDS)
            if not batches:
                # keep loop responsive; also gives heartbeats time
                await asyncio.sleep(0)
                continue

            for _tp, messages in batches.items():
                for m in messages:
                    # yield a tick to keep the event loop snappy
                    await asyncio.sleep(0)

                    # Parse
                    try:
                        req = json.loads(m.value)
                    except Exception:
                        await producer.send_and_wait(DLQ_TOPIC, key=m.key, value=json.dumps({
                            "error": "BAD_JSON",
                            "raw": m.value.decode(errors="replace")
                        }).encode())
                        processed_since_commit += 1
                        if processed_since_commit >= BATCH_COMMIT_EVERY:
                            await consumer.commit()
                            processed_since_commit = 0
                        continue

                    # Validate
                    ok, why = validate(req)
                    if not ok:
                        await producer.send_and_wait(DLQ_TOPIC, key=m.key, value=json.dumps({
                            **req, "error": why, "stage": "validate", "attempts": 0
                        }).encode())
                        processed_since_commit += 1
                        if processed_since_commit >= BATCH_COMMIT_EVERY:
                            await consumer.commit()
                            processed_since_commit = 0
                        continue

                    # Scrape (bounded by timeout)
                    t0 = time.perf_counter()
                    try:
                        res = await asyncio.wait_for(mgr.scrape_compound(req["query"]), timeout=SCRAPE_TIMEOUT_S)
                    except asyncio.TimeoutError:
                        await producer.send_and_wait(DLQ_TOPIC, key=m.key, value=json.dumps({
                            **req, "error": "SCRAPE_TIMEOUT", "stage": "scrape", "attempts": 1
                        }).encode())
                        processed_since_commit += 1
                        if processed_since_commit >= BATCH_COMMIT_EVERY:
                            await consumer.commit()
                            processed_since_commit = 0
                        continue
                    except Exception:
                        await producer.send_and_wait(DLQ_TOPIC, key=m.key, value=json.dumps({
                            **req, "error": "SCRAPE_EXCEPTION", "stage": "scrape", "attempts": 1
                        }).encode())
                        processed_since_commit += 1
                        if processed_since_commit >= BATCH_COMMIT_EVERY:
                            await consumer.commit()
                            processed_since_commit = 0
                        continue

                    # Basic quality checks
                    chosen = res.get("normalized") if isinstance(res, dict) else None
                    core_ok = bool(chosen and (chosen.get("MolecularFormula") or chosen.get("IUPACName")))
                    mw_ok = True
                    if chosen and "MolecularWeight" in chosen and chosen["MolecularWeight"] is not None:
                        try:
                            mw = float(chosen["MolecularWeight"])
                            mw_ok = 1.0 <= mw <= 2000.0
                        except Exception:
                            mw_ok = True  # don't fail on parse

                    if not chosen or not core_ok or not mw_ok:
                        await producer.send_and_wait(DLQ_TOPIC, key=m.key, value=json.dumps({
                            **req,
                            "error": "NO_RESULT" if not chosen else ("MISSING_CORE_PROPS" if not core_ok else "BAD_PROPERTY_RANGE"),
                            "stage": "scrape",
                            "attempts": 1
                        }).encode())
                        processed_since_commit += 1
                        if processed_since_commit >= BATCH_COMMIT_EVERY:
                            await consumer.commit()
                            processed_since_commit = 0
                        continue

                    # Persist & link
                    persisted = await persist_and_link(req["query"], chosen, req.get("source_item_id"))
                    if persisted:
                        payload = sanitize_json({
                            "query": req["query"],
                            "source": "pubchem",
                            "cid": str(chosen.get("CID")),
                            "molecular_formula": chosen.get("MolecularFormula"),
                            "molecular_weight": chosen.get("MolecularWeight"),
                            "iupac_name": chosen.get("IUPACName"),
                            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                            "latency_ms": round((time.perf_counter() - t0) * 1000, 2),
                        })
                        await producer.send_and_wait(RES_TOPIC, key=m.key, value=json.dumps(payload).encode())
                    else:
                        await producer.send_and_wait(DLQ_TOPIC, key=m.key, value=json.dumps({
                            **req, "error": "NO_RESULT_OR_DB_FAIL", "stage": "persist", "attempts": 1
                        }).encode())

                    # Manual commit gate
                    processed_since_commit += 1
                    if processed_since_commit >= BATCH_COMMIT_EVERY:
                        await consumer.commit()
                        processed_since_commit = 0

            # Commit any tail messages at the end of this getmany cycle
            if processed_since_commit:
                await consumer.commit()
                processed_since_commit = 0

    finally:
        await consumer.stop()
        await producer.stop()
        print("[worker] stopped")

if __name__ == "__main__":
    asyncio.run(run_worker())
