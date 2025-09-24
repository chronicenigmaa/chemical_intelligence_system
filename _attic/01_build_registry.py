# scripts/week5/01_build_registry.py  (patched SQL)
from __future__ import annotations
from sqlalchemy import create_engine, text
from app.core.config import settings

ENGINE = create_engine(settings.DATABASE_URL.replace("+asyncpg","+psycopg"), future=True)

SQL = """
INSERT INTO chemical_registry
  (source, external_id, cid, canonical_name, iupac_name, formula, mol_weight,
   canonical_smiles, inchi, inchikey, synonyms, properties, updated_at, created_at)
SELECT
  sc.source,
  sc.external_id,
  NULLIF((sc.properties::jsonb->>'CID'),'')::bigint,
  NULLIF((sc.properties::jsonb->>'Title'),''),
  NULLIF((sc.properties::jsonb->>'IUPACName'),''),
  NULLIF((sc.properties::jsonb->>'MolecularFormula'),''),
  NULLIF((sc.properties::jsonb->>'MolecularWeight'),'')::numeric,
  NULLIF((sc.properties::jsonb->>'CanonicalSMILES'),''),
  NULLIF((sc.properties::jsonb->>'InChI'),''),
  NULLIF((sc.properties::jsonb->>'InChIKey'),''),
  COALESCE(
    (
      SELECT array_agg(elem #>> '{}')
      FROM jsonb_array_elements(COALESCE(sc.properties::jsonb->'Synonyms','[]'::jsonb)) AS elem
    ),
    ARRAY[]::text[]
  ) AS synonyms,
  sc.properties::jsonb,
  now(), now()
FROM scraped_compounds sc
WHERE sc.source = 'pubchem'
ON CONFLICT (source, external_id) DO UPDATE
SET cid              = EXCLUDED.cid,
    canonical_name   = COALESCE(EXCLUDED.canonical_name, chemical_registry.canonical_name),
    iupac_name       = COALESCE(EXCLUDED.iupac_name,     chemical_registry.iupac_name),
    formula          = COALESCE(EXCLUDED.formula,        chemical_registry.formula),
    mol_weight       = COALESCE(EXCLUDED.mol_weight,     chemical_registry.mol_weight),
    canonical_smiles = COALESCE(EXCLUDED.canonical_smiles, chemical_registry.canonical_smiles),
    inchi            = COALESCE(EXCLUDED.inchi,          chemical_registry.inchi),
    inchikey         = COALESCE(EXCLUDED.inchikey,       chemical_registry.inchikey),
    synonyms         = CASE
                         WHEN EXCLUDED.synonyms IS NOT NULL
                              AND array_length(EXCLUDED.synonyms,1) > 0
                         THEN EXCLUDED.synonyms
                         ELSE chemical_registry.synonyms
                       END,
    properties       = COALESCE(EXCLUDED.properties, chemical_registry.properties),
    updated_at       = now();
"""

if __name__ == "__main__":
    with ENGINE.begin() as c:
        c.execute(text(SQL))
    print("[OK] chemical_registry upserted from scraped_compounds.")
