# scripts/load_pubchem_results.py
from __future__ import annotations
import argparse, os, sys
import pandas as pd
from sqlalchemy import create_engine, text

CAS_RE = r'^\d{2,7}-\d{2}-\d$'

def get_engine():
    try:
        from app.core.config import settings
        db_url = settings.DATABASE_URL.replace("+asyncpg", "+psycopg")
    except Exception:
        db_url = os.getenv("DATABASE_URL", "postgresql+psycopg://zain:Zain2025!!!@localhost:5432/chemdb")
    return create_engine(db_url, future=True)

DDL_RAW = """
CREATE TABLE IF NOT EXISTS stg_pubchem_results_raw (
  "query_name"        text,
  "query_hint"        text,
  "source_item_id"    bigint,
  "CID"               text,
  "Title"             text,
  "IUPACName"         text,
  "MolecularFormula"  text,
  "MolecularWeight"   text,
  "CanonicalSMILES"   text,
  "InChI"             text,
  "InChIKey"          text,
  "Synonyms"          text
);
"""

DDL_CANON = """
CREATE TABLE IF NOT EXISTS stg_pubchem_results (
  product_id         bigint,
  pubchem_cid        text,
  cas                text,
  name               text,
  iupac_name         text,
  molecular_formula  text,
  molecular_weight   numeric,
  canonical_smiles   text,
  inchi              text,
  inchikey           text
);
"""

SQL_TRANSFORM = f"""
TRUNCATE stg_pubchem_results;

-- Normalize raw (trim strings; NULL empty; normalize CID)
WITH raw AS (
  SELECT
    NULLIF(btrim(r."query_name"),'')        AS query_name,
    lower(NULLIF(btrim(r."query_hint"),'')) AS query_hint,
    r."source_item_id",
    NULLIF(btrim(r."CID"),'')               AS cid,
    NULLIF(btrim(r."Title"),'')             AS title,
    NULLIF(btrim(r."IUPACName"),'')         AS iupac,
    NULLIF(btrim(r."MolecularFormula"),'')  AS mf,
    NULLIF(btrim(r."MolecularWeight"),'')   AS mw,
    NULLIF(btrim(r."CanonicalSMILES"),'')   AS smiles,
    NULLIF(btrim(r."InChI"),'')             AS inchi,
    NULLIF(btrim(r."InChIKey"),'')          AS inchikey
  FROM stg_pubchem_results_raw r
),

-- 1) explicit product_id from CSV
ins_explicit AS (
  INSERT INTO stg_pubchem_results
    (product_id, pubchem_cid, cas, name, iupac_name, molecular_formula, molecular_weight, canonical_smiles, inchi, inchikey)
  SELECT
    r."source_item_id" AS product_id,
    r.cid              AS pubchem_cid,
    CASE WHEN r.query_name ~ '{CAS_RE}' THEN r.query_name ELSE NULL END AS cas,
    r.title, r.iupac, r.mf, NULLIF(r.mw,'')::numeric, r.smiles, r.inchi, r.inchikey
  FROM raw r
  WHERE r."source_item_id" IS NOT NULL
  RETURNING 1
),
-- 2) map by CAS in query_name → item_master_clean.cas
ins_by_cas AS (
  INSERT INTO stg_pubchem_results
    (product_id, pubchem_cid, cas, name, iupac_name, molecular_formula, molecular_weight, canonical_smiles, inchi, inchikey)
  SELECT
    imc.source_item_id,
    r.cid,
    r.query_name AS cas,
    r.title, r.iupac, r.mf, NULLIF(r.mw,'')::numeric, r.smiles, r.inchi, r.inchikey
  FROM raw r
  JOIN item_master_clean imc
    ON r.query_name ~ '{CAS_RE}'
   AND imc.cas = r.query_name
  WHERE r."source_item_id" IS NULL
  RETURNING 1
),
-- 3) map by original query text → item_master_query.query  (case-insensitive)
ins_by_query AS (
  INSERT INTO stg_pubchem_results
    (product_id, pubchem_cid, cas, name, iupac_name, molecular_formula, molecular_weight, canonical_smiles, inchi, inchikey)
  SELECT
    imq.source_item_id,
    r.cid,
    CASE WHEN r.query_name ~ '{CAS_RE}' THEN r.query_name ELSE NULL END AS cas,
    r.title, r.iupac, r.mf, NULLIF(r.mw,'')::numeric, r.smiles, r.inchi, r.inchikey
  FROM raw r
  JOIN item_master_query imq
    ON lower(imq.query) = lower(r.query_name)
  WHERE r."source_item_id" IS NULL
    AND NOT (r.query_name ~ '{CAS_RE}')  -- avoid duplicating CAS-mapped rows
  RETURNING 1
)
SELECT 1;
"""

UPSERT_XREF = """
-- one row per (product_id, pubchem_cid)
WITH d AS (
  SELECT
    s.product_id,
    s.pubchem_cid,
    -- prefer any non-null CAS (if duplicates disagree, we just keep one)
    MAX(NULLIF(s.cas,'')) AS cas
  FROM stg_pubchem_results s
  WHERE s.product_id IS NOT NULL
    AND s.pubchem_cid IS NOT NULL
  GROUP BY s.product_id, s.pubchem_cid
)
INSERT INTO product_chem_xref
  (product_id, source, external_id, cas, match_confidence, properties_ref, updated_at)
SELECT
  d.product_id,
  'pubchem' AS source,
  d.pubchem_cid AS external_id,
  d.cas,
  0.99::numeric,
  NULL,
  now()
FROM d
ON CONFLICT (product_id, source, external_id) DO UPDATE
SET cas = COALESCE(EXCLUDED.cas, product_chem_xref.cas),
    match_confidence = GREATEST(product_chem_xref.match_confidence, EXCLUDED.match_confidence),
    updated_at = now();
"""


UPSERT_REGISTRY = """
-- choose one best row per CID; prefer richer info
WITH d AS (
  SELECT DISTINCT ON (s.pubchem_cid)
    s.pubchem_cid,
    s.name,
    s.iupac_name,
    s.molecular_formula,
    NULLIF(s.molecular_weight::text,'')::numeric AS molecular_weight,  -- <— robust cast
    s.canonical_smiles,
    s.inchi,
    s.inchikey
  FROM stg_pubchem_results s
  WHERE s.pubchem_cid IS NOT NULL
  ORDER BY
    s.pubchem_cid,
    (s.molecular_weight IS NULL),
    (s.iupac_name IS NULL),
    (s.canonical_smiles IS NULL)
)
INSERT INTO chemical_registry
  (source, external_id, cid, canonical_name, iupac_name, formula, mol_weight,
   canonical_smiles, inchi, inchikey, properties, created_at, updated_at)
SELECT
  'pubchem',
  d.pubchem_cid,
  NULLIF(d.pubchem_cid,'')::bigint,
  d.name,
  d.iupac_name,
  d.molecular_formula,
  d.molecular_weight,                 -- numeric now
  d.canonical_smiles,
  d.inchi,
  d.inchikey,
  NULL, now(), now()
FROM d
ON CONFLICT (source, external_id) DO UPDATE
SET canonical_name   = COALESCE(EXCLUDED.canonical_name, chemical_registry.canonical_name),
    iupac_name       = COALESCE(EXCLUDED.iupac_name,     chemical_registry.iupac_name),
    formula          = COALESCE(EXCLUDED.formula,        chemical_registry.formula),
    mol_weight       = COALESCE(EXCLUDED.mol_weight,     chemical_registry.mol_weight),
    canonical_smiles = COALESCE(EXCLUDED.canonical_smiles, chemical_registry.canonical_smiles),
    inchi            = COALESCE(EXCLUDED.inchi,          chemical_registry.inchi),
    inchikey         = COALESCE(EXCLUDED.inchikey,       chemical_registry.inchikey),
    updated_at       = now();
"""



LINK_CHEM_ID = """
UPDATE product_chem_xref x
SET chem_id = r.id, updated_at = now()
FROM chemical_registry r
WHERE x.source='pubchem'
  AND r.source='pubchem'
  AND r.external_id = x.external_id
  AND (x.chem_id IS DISTINCT FROM r.id);
"""

COUNTS = """
SELECT
  (SELECT COUNT(*) FROM stg_pubchem_results_raw) AS raw_rows,
  (SELECT COUNT(*) FROM stg_pubchem_results)     AS staged_rows,
  (SELECT COUNT(*) FROM stg_pubchem_results WHERE product_id IS NOT NULL AND pubchem_cid IS NOT NULL) AS ready_rows,
  (SELECT COUNT(*) FROM product_chem_xref WHERE source='pubchem') AS xref_rows,
  (SELECT COUNT(*) FROM chemical_registry WHERE source='pubchem')  AS registry_rows;
"""

SAMPLES = """
SELECT * FROM stg_pubchem_results
WHERE product_id IS NULL OR pubchem_cid IS NULL
LIMIT 10;
"""

def main():
    ap = argparse.ArgumentParser(description="Load pubchem_results.csv → link products & registry")
    ap.add_argument("--csv", required=True, help="Path to pubchem_results.csv")
    args = ap.parse_args()

    if not os.path.exists(args.csv):
        print(f"[err] CSV not found: {args.csv}", file=sys.stderr)
        sys.exit(1)

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(DDL_RAW))
        conn.execute(text(DDL_CANON))
        conn.execute(text("TRUNCATE stg_pubchem_results_raw"))

    # Load CSV
    df = pd.read_csv(args.csv)

    expected = ["query_name","query_hint","source_item_id","CID","Title","IUPACName",
                "MolecularFormula","MolecularWeight","CanonicalSMILES","InChI","InChIKey","Synonyms"]
    for col in expected:
        if col not in df.columns:
            df[col] = None
    # coerce numeric-looking cols
    if "MolecularWeight" in df.columns:
        df["MolecularWeight"] = pd.to_numeric(df["MolecularWeight"], errors="coerce")
    if "source_item_id" in df.columns:
        try:
            df["source_item_id"] = pd.to_numeric(df["source_item_id"], errors="coerce").astype("Int64")
        except Exception:
            pass

    df = df[expected]

    with engine.begin() as conn:
        df.to_sql("stg_pubchem_results_raw", con=conn, if_exists="append", index=False)
        conn.execute(text(SQL_TRANSFORM))

        # Diagnostics before upsert
        row = conn.execute(text(COUNTS)).mappings().first()
        print(f"[stage] raw={row['raw_rows']} staged={row['staged_rows']} ready={row['ready_rows']}")
        if row["ready_rows"] == 0:
            print("[warn] No rows with BOTH product_id and pubchem_cid. Showing up to 10 incomplete rows…")
            for r in conn.execute(text(SAMPLES)).mappings():
                print(dict(r))

        # Upserts
        conn.execute(text(UPSERT_XREF))
        conn.execute(text(UPSERT_REGISTRY))
        conn.execute(text(LINK_CHEM_ID))

        # Summary after upsert
        row2 = conn.execute(text(COUNTS)).mappings().first()
        print(f"[OK] xref_rows={row2['xref_rows']} registry_rows={row2['registry_rows']}")

        # View (optional)
        conn.execute(text("""
        CREATE OR REPLACE VIEW vw_product_properties AS
        SELECT
          p.source_item_id AS product_id,
          p.sku, p.name, p.cas AS product_cas,
          x.external_id AS pubchem_cid,
          r.formula, r.mol_weight, r.iupac_name,
          r.canonical_smiles, r.inchikey
        FROM item_master_clean p
        JOIN product_chem_xref x ON x.product_id=p.source_item_id AND x.source='pubchem'
        LEFT JOIN chemical_registry r ON r.id=x.chem_id;
        """))

if __name__ == "__main__":
    main()
