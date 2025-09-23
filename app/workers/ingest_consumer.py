# app/workers/ingest_consumer.py
"""
Kafka → PubChem → Postgres worker with proper UPSERTs.

Run (each in its own terminal for parallelism):
  python -m app.workers.ingest_consumer
"""




from __future__ import annotations
import asyncio
import json
import os
import re
import time
from typing import Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import settings
from app.core.json_sanitize import sanitize_json
from app.scrapers.manager import ScrapingManager
from app.db.session import AsyncSessionLocal
from app.db.models import ScrapedCompound

# --- Kafka config (env-overridable) ---
REQ_TOPIC = os.getenv("KAFKA_REQUESTS_TOPIC", "chem.ingest.requests.v1")
RES_TOPIC = os.getenv("KAFKA_RESULTS_TOPIC",  "chem.ingest.results.v1")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC",      "chem.ingest.dlq.v1")
BOOT      = os.getenv("KAFKA_BOOTSTRAP",      "localhost:9092")
GROUP_ID  = os.getenv("KAFKA_GROUP_ID",       "ccw-ingest-workers")
CLIENT_ID = os.getenv("KAFKA_CLIENT_ID",      "ccw-ingest")

# --- Validation ---
CAS_RE = re.compile(r"^\d{2,7}-\d{2}-\d$")

from app.core.config import settings
print("[worker] DB =", settings.DATABASE_URL)

def validate(req: dict) -> tuple[bool, Optional[str]]:
    q = (req.get("query") or "").strip()
    hint = (req.get("hint") or "auto").lower()
    if not q:
        return False, "EMPTY_QUERY"
    if hint == "cas" and not CAS_RE.match(q):
        return False, "INVALID_CAS"
    return True, None

# --- Persistence (UPSERTs) ---
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import text
import json, traceback

async def persist_and_link(query: str, chosen: dict | None, source_item_id: int | None) -> bool:
    if not chosen or not chosen.get("CID"):
        return False
    chosen = sanitize_json(chosen)
    cid = str(chosen["CID"])

    async with AsyncSessionLocal() as s:
        try:
            # --- UPSERT scraped_compounds (relies on JSONB column in your model) ---
            table = ScrapedCompound.__table__
            stmt = pg_insert(table).values(
                query=query, source="pubchem", external_id=cid, properties=chosen
            ).on_conflict_do_update(
                index_elements=[table.c.source, table.c.external_id],
                set_={"query": query, "properties": chosen}
            )
            await s.execute(stmt)

            # --- UPSERT product_chem_xref; cast JSON explicitly to jsonb ---
            if source_item_id:
                await s.execute(
                    text("""
                        insert into product_chem_xref
                          (product_id, source, external_id, cas, match_confidence, properties_ref)
                        values
                          (:pid, 'pubchem', :cid, null, 0.95, :props::jsonb)
                        on conflict (source, external_id, product_id) do update
                          set properties_ref = excluded.properties_ref,
                              updated_at = now()
                    """),
                    {"pid": source_item_id, "cid": cid, "props": json.dumps(chosen)}
                )

            await s.commit()
            return True

        except Exception:
            await s.rollback()
            print("[persist_and_link] ERROR\n", traceback.format_exc())
            return False
# --- Worker loop ---
async def run_worker() -> None:
    consumer = AIOKafkaConsumer(
        REQ_TOPIC,
        bootstrap_servers=BOOT,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=200,
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
    print(f"[worker] started on {BOOT} | topics: {REQ_TOPIC} → ({RES_TOPIC} / {DLQ_TOPIC})")
    try:
        while True:
            batches = await consumer.getmany(timeout_ms=500, max_records=500)
            for _tp, messages in batches.items():
                for m in messages:
                    # Parse and validate
                    try:
                        req = json.loads(m.value)
                    except Exception:
                        # Bad JSON → DLQ
                        dlq = {"error": "BAD_JSON", "raw": m.value.decode(errors="replace")}
                        await producer.send_and_wait(DLQ_TOPIC, key=m.key, value=json.dumps(dlq).encode())
                        await consumer.commit()
                        continue

                    ok, why = validate(req)
                    if not ok:
                        await producer.send_and_wait(
                            DLQ_TOPIC,
                            key=m.key,
                            value=json.dumps(sanitize_json({
                                **req, "error": why, "stage": "validate", "attempts": 0
                            })).encode()
                        )
                        await consumer.commit()
                        continue

                    # Scrape PubChem
                    t0 = time.perf_counter()
                    try:
                        res = await mgr.scrape_compound(req["query"])
                    except Exception:
                        await producer.send_and_wait(
                            DLQ_TOPIC,
                            key=m.key,
                            value=json.dumps(sanitize_json({
                                **req, "error": "SCRAPE_EXCEPTION", "stage": "scrape", "attempts": 1
                            })).encode()
                        )
                        await consumer.commit()
                        continue

                    chosen = res.get("normalized") if isinstance(res, dict) else None

                    # Minimal quality gate: molecular weight sanity
                    mw_ok = True
                    try:
                        if chosen and "MolecularWeight" in chosen and chosen["MolecularWeight"] is not None:
                            mw = float(chosen["MolecularWeight"])
                            mw_ok = 1.0 <= mw <= 2000.0
                    except Exception:
                        mw_ok = True  # don't block on parse error

                    if not chosen or not mw_ok:
                        await producer.send_and_wait(
                            DLQ_TOPIC,
                            key=m.key,
                            value=json.dumps(sanitize_json({
                                **req,
                                "error": "NO_RESULT" if not chosen else "BAD_PROPERTY_RANGE",
                                "stage": "scrape",
                                "attempts": 1
                            })).encode()
                        )
                        await consumer.commit()
                        continue

                    # Persist and link
                    persisted = await persist_and_link(
                        req["query"], chosen, req.get("source_item_id")
                    )
                    if persisted:
                        payload = sanitize_json({
                            "query": req["query"],
                            "source": "pubchem",
                            "cid": str(chosen.get("CID")) if chosen else None,
                            "molecular_formula": chosen.get("MolecularFormula") if chosen else None,
                            "molecular_weight": chosen.get("MolecularWeight") if chosen else None,
                            "iupac_name": chosen.get("IUPACName") if chosen else None,
                            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                            "latency_ms": round((time.perf_counter() - t0) * 1000, 2),
                        })
                        await producer.send_and_wait(
                            RES_TOPIC, key=m.key, value=json.dumps(payload).encode()
                        )
                    else:
                        await producer.send_and_wait(
                            DLQ_TOPIC,
                            key=m.key,
                            value=json.dumps(sanitize_json({
                                **req, "error": "NO_RESULT_OR_DB_FAIL", "stage": "persist", "attempts": 1
                            })).encode()
                        )

                    # Mark message consumed
                    await consumer.commit()
    finally:
        await consumer.stop()
        await producer.stop()
        print("[worker] stopped")

if __name__ == "__main__":
    asyncio.run(run_worker())
