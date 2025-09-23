# scripts/fix_and_seed_item_master.py
"""
Fix stg_item_master to ensure it has a BIGSERIAL id PK, align dependent tables,
then (re)build item_master_query from stg_item_master.

Run:
  python -m scripts.fix_and_seed_item_master
"""

from __future__ import annotations
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError, OperationalError
from app.core.config import settings

# Use sync driver (psycopg v3) for DDL
SYNC_URL = settings.DATABASE_URL.replace("+asyncpg", "+psycopg")

DDL_CREATE_STG_IF_MISSING = """
CREATE TABLE IF NOT EXISTS stg_item_master (
  id BIGSERIAL PRIMARY KEY,
  sku TEXT,
  name TEXT,
  description TEXT,
  cas TEXT,
  brand TEXT,
  family TEXT,
  vendor TEXT,
  raw JSONB,
  loaded_at timestamptz DEFAULT now()
);
"""

DDL_CREATE_ITEM_MASTER_QUERY = """
CREATE TABLE IF NOT EXISTS item_master_query (
  id BIGSERIAL PRIMARY KEY,
  source_item_id BIGINT,
  query TEXT NOT NULL,
  hint TEXT DEFAULT 'auto',
  CONSTRAINT item_master_query_hint_chk CHECK (hint IN ('auto','name','cas')),
  UNIQUE (source_item_id, query)
);
"""

DDL_CREATE_PRODUCT_CHEM_XREF = """
CREATE TABLE IF NOT EXISTS product_chem_xref (
  id BIGSERIAL PRIMARY KEY,
  product_id BIGINT NOT NULL,
  source TEXT NOT NULL,
  external_id TEXT NOT NULL,
  cas TEXT,
  match_confidence NUMERIC,
  properties_ref JSONB,
  updated_at timestamptz DEFAULT now(),
  UNIQUE (source, external_id, product_id)
);
"""

SEED_INSERT = """
INSERT INTO item_master_query (source_item_id, query, hint)
SELECT im.id,
       COALESCE(NULLIF(im.cas, ''), im.name) AS query,
       CASE WHEN im.cas ~ '^\d{2,7}-\d{2}-\d$' THEN 'cas' ELSE 'name' END AS hint
FROM stg_item_master im
WHERE im.name IS NOT NULL OR (im.cas IS NOT NULL AND im.cas <> '')
ON CONFLICT DO NOTHING;
"""

def main() -> int:
    print(f"[info] Using DB URL: {SYNC_URL}")
    engine = create_engine(SYNC_URL, future=True)

    with engine.begin() as conn:
        # 0) Ensure base tables exist (idempotent)
        conn.execute(text(DDL_CREATE_STG_IF_MISSING))
        conn.execute(text(DDL_CREATE_ITEM_MASTER_QUERY))
        conn.execute(text(DDL_CREATE_PRODUCT_CHEM_XREF))

        # 1) Check if stg_item_master.id exists; add if missing and set PK
        has_id = conn.execute(text("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'stg_item_master' AND column_name = 'id'
        """)).first() is not None

        if not has_id:
            print("[fix] stg_item_master.id is missing → adding BIGSERIAL id and PK.")
            # Add column
            conn.execute(text("ALTER TABLE stg_item_master ADD COLUMN id BIGSERIAL;"))
            # Backfill any NULLs to default sequence
            conn.execute(text("UPDATE stg_item_master SET id = DEFAULT WHERE id IS NULL;"))

            # Ensure NOT NULL
            conn.execute(text("ALTER TABLE stg_item_master ALTER COLUMN id SET NOT NULL;"))

            # Add PK if not present
            has_pk = conn.execute(text("""
                SELECT 1
                FROM information_schema.table_constraints
                WHERE table_name = 'stg_item_master' AND constraint_type = 'PRIMARY KEY'
            """)).first() is not None
            if not has_pk:
                conn.execute(text("ALTER TABLE stg_item_master ADD PRIMARY KEY (id);"))

        # 2) Ensure item_master_query.source_item_id is BIGINT
        print("[fix] Aligning item_master_query.source_item_id to BIGINT (idempotent).")
        try:
            conn.execute(text("ALTER TABLE item_master_query ALTER COLUMN source_item_id TYPE BIGINT;"))
        except ProgrammingError:
            # Column might not exist or already BIGINT; ignore benign errors.
            pass

        # 3) Ensure product_chem_xref.product_id is BIGINT (keeps types aligned)
        print("[fix] Aligning product_chem_xref.product_id to BIGINT (idempotent).")
        try:
            conn.execute(text("ALTER TABLE product_chem_xref ALTER COLUMN product_id TYPE BIGINT;"))
        except ProgrammingError:
            pass

        # 4) Build (or rebuild) the seed from stg_item_master
        print("[run] Seeding item_master_query from stg_item_master …")
        result = conn.execute(text(SEED_INSERT))
        # rowcount can be -1 depending on driver, so we don’t rely on it strictly.
        print("[ok] Seed insert completed.")

    print("[done] Schema fixed and seed populated.")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except OperationalError as e:
        print(f"[error] Database connection failed: {e}")
        sys.exit(2)
    except Exception as e:
        print(f"[error] {e}")
        sys.exit(1)
