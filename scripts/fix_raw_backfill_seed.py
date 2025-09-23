# scripts/patch_raw_jsonb_backfill_seed.py
"""
Fix stg_item_master.raw -> JSONB (with safe fallback), cast key cols to TEXT,
backfill normalized columns from raw, and seed item_master_query.

Run:
  python -m scripts.patch_raw_jsonb_backfill_seed
"""

from __future__ import annotations
import json
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError, OperationalError
from app.core.config import settings

SYNC_URL = settings.DATABASE_URL.replace("+asyncpg", "+psycopg")
engine = create_engine(SYNC_URL, future=True)

DDL_CREATE = """
CREATE TABLE IF NOT EXISTS item_master_query (
  id BIGSERIAL PRIMARY KEY,
  source_item_id BIGINT,
  query TEXT NOT NULL,
  hint TEXT DEFAULT 'auto',
  CONSTRAINT item_master_query_hint_chk CHECK (hint IN ('auto','name','cas')),
  UNIQUE (source_item_id, query)
);
"""

ALTER_TEXT_COLS = [
    "ALTER TABLE stg_item_master ALTER COLUMN name TYPE text USING name::text;",
    "ALTER TABLE stg_item_master ALTER COLUMN sku TYPE text USING sku::text;",
    "ALTER TABLE stg_item_master ALTER COLUMN description TYPE text USING description::text;",
    "ALTER TABLE stg_item_master ALTER COLUMN cas TYPE text USING cas::text;",
]

BACKFILL_SQL_JSONB = """
-- Use JSONB operator now that raw is jsonb
UPDATE stg_item_master
SET name = COALESCE(NULLIF(name,''),
                    NULLIF(raw->>'Display Name',''),
                    NULLIF(raw->>'Name',''))
WHERE name IS NULL OR name = '';

UPDATE stg_item_master
SET sku = COALESCE(NULLIF(sku,''),
                   NULLIF(raw->>'Name',''),
                   NULLIF(raw->>'Internal ID',''))
WHERE sku IS NULL OR sku = '';

UPDATE stg_item_master
SET description = COALESCE(NULLIF(description,''),
                           NULLIF(raw->>'Description',''))
WHERE description IS NULL OR description = '';

UPDATE stg_item_master
SET cas = COALESCE(NULLIF(cas,''),
                   NULLIF(raw->>'CAS',''),
                   NULLIF(raw->>'CAS No',''),
                   NULLIF(raw->>'CAS Number',''),
                   NULLIF(raw->>'CAS#',''))
WHERE cas IS NULL OR cas = '';
"""

SEED_SQL = """
INSERT INTO item_master_query (source_item_id, query, hint)
SELECT im.id,
       COALESCE(NULLIF(im.cas,''), NULLIF(im.name,'')) AS query,
       CASE WHEN im.cas ~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$' THEN 'cas' ELSE 'name' END AS hint
FROM stg_item_master im
WHERE COALESCE(NULLIF(im.cas,''), NULLIF(im.name,'')) IS NOT NULL
ON CONFLICT DO NOTHING;
"""

def column_type_is_jsonb(conn) -> bool:
    row = conn.execute(text("""
        SELECT data_type
        FROM information_schema.columns
        WHERE table_name='stg_item_master' AND column_name='raw'
    """)).first()
    return (row and row[0] == "jsonb")

def try_alter_raw_to_jsonb(conn) -> bool:
    try:
        conn.execute(text("ALTER TABLE stg_item_master ALTER COLUMN raw TYPE jsonb USING raw::jsonb;"))
        return True
    except ProgrammingError as e:
        print("[info] Direct ALTER raw::jsonb failed, will try row-by-row rescue:", e)
        return False

def row_by_row_rescue(conn):
    # create temp column
    conn.execute(text("ALTER TABLE stg_item_master ADD COLUMN IF NOT EXISTS raw_jsonb jsonb;"))
    # fetch id + raw text
    rows = conn.execute(text("SELECT id, raw FROM stg_item_master WHERE raw IS NOT NULL")).mappings().all()
    good, bad = 0, 0
    for r in rows:
        rid, raw_text = r["id"], r["raw"]
        jval = None
        if isinstance(raw_text, dict):
            jval = raw_text
        else:
            try:
                jval = json.loads(raw_text)
            except Exception:
                jval = None
        conn.execute(text("UPDATE stg_item_master SET raw_jsonb = :j::jsonb WHERE id = :id"),
                     {"j": json.dumps(jval) if jval is not None else None, "id": rid})
        good += 1 if jval is not None else 0
        bad  += 0 if jval is not None else 1
    print(f"[info] Row-by-row parse: ok={good} bad={bad}")
    # swap columns
    conn.execute(text("ALTER TABLE stg_item_master DROP COLUMN raw;"))
    conn.execute(text("ALTER TABLE stg_item_master RENAME COLUMN raw_jsonb TO raw;"))

def main():
    print(f"[info] SYNC_URL={SYNC_URL}")
    with engine.begin() as conn:
        # Ensure crosswalk table exists
        conn.execute(text(DDL_CREATE))

        # 1) Ensure raw is JSONB
        if not column_type_is_jsonb(conn):
            print("[fix] stg_item_master.raw is not jsonb → converting…")
            if not try_alter_raw_to_jsonb(conn):
                row_by_row_rescue(conn)
        else:
            print("[ok] stg_item_master.raw is already jsonb")


        # 3) Backfill from JSONB
        conn.execute(text(BACKFILL_SQL_JSONB))

        # 4) Seed crosswalk
        conn.execute(text(SEED_SQL))

        # report
        seeded = conn.execute(text("SELECT COUNT(*) FROM item_master_query")).scalar_one()
        usable = conn.execute(text("""
            SELECT COUNT(*) FROM stg_item_master
            WHERE COALESCE(NULLIF(name,''), NULLIF(cas,'')) IS NOT NULL
        """)).scalar_one()
        print(f"[ok] stg_item_master with name/cas: {usable}")
        print(f"[ok] item_master_query rows: {seeded}")

if __name__ == "__main__":
    try:
        main()
    except OperationalError as e:
        print("[error] DB connection failed:", e)
        raise
