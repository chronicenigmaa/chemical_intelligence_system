-- 0) Ensure trigram for fuzzy title→name
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 1) Auto-attach SDS to a product if not set (fuzzy match filename/title to item name)
WITH cand AS (
  SELECT d.id AS doc_id, imc.source_item_id,
         GREATEST(similarity(lower(d.title), lower(imc.name)),
                  similarity(lower(d.filename), lower(imc.name))) AS sim,
         row_number() OVER (PARTITION BY d.id ORDER BY
            GREATEST(similarity(lower(d.title), lower(imc.name)),
                     similarity(lower(d.filename), lower(imc.name))) DESC) AS rn
  FROM sds_document d
  JOIN item_master_clean imc ON true
  WHERE d.product_id IS NULL
)
UPDATE sds_document d
SET product_id = c.source_item_id
FROM cand c
WHERE c.doc_id=d.id AND c.rn=1 AND c.sim >= 0.45;

-- 2) Pick a "main CAS" from components per SDS (highest % first; otherwise any)
WITH comp AS (
  SELECT doc_id, cas, name,
         COALESCE(conc_high, conc_low, 100) AS pct,
         row_number() OVER (PARTITION BY doc_id
                            ORDER BY COALESCE(conc_high, conc_low, 100) DESC NULLS LAST,
                                     lower(name)) AS rn
  FROM sds_component
  WHERE cas IS NOT NULL
),
main AS (
  SELECT doc_id, cas, name FROM comp WHERE rn=1
)

-- 2a) Fill product CAS if missing
UPDATE item_master_clean c
SET cas = m.cas
FROM sds_document d
JOIN main m ON m.doc_id=d.id
WHERE d.product_id = c.source_item_id
  AND (c.cas IS NULL OR c.cas='');

-- 3) Create CAS links in product_chem_xref (dedupbed)
CREATE UNIQUE INDEX IF NOT EXISTS uq_xref_triplet
  ON product_chem_xref(product_id, source, external_id);

WITH d AS (
  SELECT DISTINCT d.product_id, e.norm_value AS cas
  FROM sds_document d
  JOIN sds_entity   e ON e.doc_id=d.id
  WHERE d.product_id IS NOT NULL
    AND e.entity_type='CAS'
    AND e.norm_value IS NOT NULL
)
INSERT INTO product_chem_xref(product_id, source, external_id, cas, match_confidence, updated_at)
SELECT d.product_id, 'cas', d.cas, d.cas, 0.97, now()
FROM d
ON CONFLICT (product_id, source, external_id) DO UPDATE
SET cas = EXCLUDED.cas,
    match_confidence = GREATEST(product_chem_xref.match_confidence, EXCLUDED.match_confidence),
    updated_at = now();

-- 4) Ensure each CAS exists in chemical_registry (source='cas')
INSERT INTO chemical_registry(source, external_id, canonical_name, properties, created_at, updated_at)
SELECT 'cas', d.cas, NULL, NULL, now(), now()
FROM (
  SELECT DISTINCT e.norm_value AS cas
  FROM sds_entity e
  WHERE e.entity_type='CAS' AND e.norm_value IS NOT NULL
) d
ON CONFLICT (source, external_id) DO NOTHING;

-- 5) Link xref.chem_id to registry by CAS
UPDATE product_chem_xref x
SET chem_id = r.id, updated_at = now()
FROM chemical_registry r
WHERE x.source='cas'
  AND r.source='cas'
  AND r.external_id = x.external_id
  AND (x.chem_id IS DISTINCT FROM r.id);

-- 6) Optional: If the same CAS is known in PubChem registry, convert to PubChem link
UPDATE product_chem_xref x
SET source='pubchem',
    external_id = r.external_id,
    updated_at  = now()
FROM chemical_registry rc
JOIN chemical_registry r
  ON r.source='pubchem'
 AND r.cid::text = rc.properties->>'cid'  -- if you store mapping; else skip this block
WHERE x.source='cas'
  AND rc.source='cas'
  AND x.chem_id = rc.id;
