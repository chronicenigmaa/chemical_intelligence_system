# scripts/orchestrate_ingest.py
from __future__ import annotations
import argparse, os, json, time, asyncio
from typing import Iterable, Mapping, List, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from aiokafka import AIOKafkaProducer

# --- Settings / env ----------------------------------------------------------
try:
    from app.core.config import settings
    DB_URL = settings.DATABASE_URL.replace("+asyncpg", "+psycopg")
except Exception:
    DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://zain:Zain2025!!!@localhost:5432/chemdb")

BOOT   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC  = os.getenv("KAFKA_REQUESTS_TOPIC", "chem.ingest.requests.v1")
CID    = os.getenv("KAFKA_CLIENT_ID", "ccw-ingest")

try:
    from app.core.json_sanitize import sanitize_json
except Exception:
    def sanitize_json(x): return x

ENGINE: Engine = create_engine(DB_URL, future=True, pool_pre_ping=True)

CAS_RE = r"^\d{2,7}-\d{2}-\d$"

ALIASES = [
  (r"^\s*xylenes?\s*$",       "1330-20-7", "cas",  "mixed xylenes"),
  (r"^\s*white\s*spirit(s)?\s*$","Stoddard solvent","name","synonym"),
  (r"^\s*mineral\s*spirit(s)?\s*$","Stoddard solvent","name","synonym"),
  (r"^\s*muriatic\s*acid\s*$","hydrochloric acid","name","vernacular"),
  (r"^\s*caustic\s*soda\s*$","sodium hydroxide","name","vernacular"),
  (r"^\s*caustic\s*potash\s*$","potassium hydroxide","name","vernacular"),
  (r"^\s*soda\s*ash\s*$","sodium carbonate","name","vernacular"),
  (r"^\s*isopropyl\s*alcohol\s*$","isopropanol","name","synonym"),
  (r"^\s*denatured\s*alcohol\s*$","ethanol","name","synonym"),
  (r"^\s*wood\s*alcohol\s*$","methanol","name","vernacular"),
  (r"^\s*grain\s*alcohol\s*$","ethanol","name","vernacular"),
  (r"^\s*tsp\s*$","trisodium phosphate","name","synonym"),
  (r"^\s*mek\s*$",            "78-93-3",   "cas",  "methyl ethyl ketone"),
  (r"^\s*mibk\s*$",           "108-10-1",  "cas",  "methyl isobutyl ketone"),
  (r"^\s*dcm\s*$",            "75-09-2",   "cas",  "dichloromethane"),
  (r"^\s*ipa\s*$",            "isopropanol","name","abbrev"),
  (r"^\s*nmp\s*$",            "1-Methyl-2-pyrrolidone","name","abbrev"),
]

STRIP_NOISE_FN = """
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

DDL_SQL = """
CREATE TABLE IF NOT EXISTS item_master_query (
  id BIGSERIAL PRIMARY KEY,
  source_item_id BIGINT,
  query TEXT NOT NULL,
  hint TEXT DEFAULT 'name',
  CONSTRAINT item_master_query_hint_chk CHECK (hint IN ('auto','name','cas')),
  UNIQUE (source_item_id, query, hint)
);
CREATE TABLE IF NOT EXISTS produced_query_log (
  qid BIGINT,
  source_item_id BIGINT,
  query TEXT NOT NULL,
  hint TEXT NOT NULL,
  produced_at timestamptz DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_produced_key
  ON produced_query_log (source_item_id, query, hint);

CREATE TABLE IF NOT EXISTS name_alias_map (
  pattern text PRIMARY KEY,
  replacement text NOT NULL,
  hint text NOT NULL CHECK (hint IN ('name','cas')),
  note text
);

-- Registry + links (Week 5)
CREATE TABLE IF NOT EXISTS chemical_registry (
  id bigserial PRIMARY KEY,
  source text NOT NULL,
  external_id text NOT NULL,
  cid bigint,
  canonical_name text,
  iupac_name text,
  formula text,
  mol_weight numeric,
  canonical_smiles text,
  inchi text,
  inchikey text,
  synonyms text[],
  properties jsonb,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  UNIQUE (source, external_id)
);
CREATE INDEX IF NOT EXISTS ix_chemreg_source_ext ON chemical_registry (source, external_id);
CREATE INDEX IF NOT EXISTS ix_chemreg_cid ON chemical_registry (cid);
CREATE INDEX IF NOT EXISTS ix_chemreg_syn ON chemical_registry USING gin (synonyms);

ALTER TABLE IF EXISTS product_chem_xref
  ADD COLUMN IF NOT EXISTS chem_id bigint NULL REFERENCES chemical_registry(id);
CREATE INDEX IF NOT EXISTS ix_xref_chem_id ON product_chem_xref(chem_id);

CREATE TABLE IF NOT EXISTS chemical_family (
  id serial PRIMARY KEY,
  name text UNIQUE NOT NULL,
  pattern text NOT NULL,
  note text
);
CREATE TABLE IF NOT EXISTS product_family (
  id bigserial PRIMARY KEY,
  product_id bigint NOT NULL,
  family_id int NOT NULL REFERENCES chemical_family(id),
  confidence numeric NOT NULL DEFAULT 0.8,
  created_at timestamptz DEFAULT now(),
  UNIQUE (product_id, family_id)
);
"""

FAMILY_SEED = """
INSERT INTO chemical_family(name, pattern, note) VALUES
('Acids','(acid|acetate|benzoate|citrate|tartrate|oxalate)','organic/inorganic acids & salts'),
('Bases','(hydroxide|amine|ammonia|ammonium)','bases'),
('Alcohols','(alcohol|anol|ethanol|isopropanol|methanol|butanol|propanol)','alcohols'),
('Ketones','(ketone|methyl ethyl ketone|mek|acetone)','ketones'),
('Glycols','(glycol|ethylene glycol|propylene glycol|peg)','glycols'),
('Aromatics','(benzene|toluene|xylene|cumene|styrene|phenol)','aromatic HC'),
('Halogenated','(chloride|chloro|bromo|iodo|dichloro|trichloro|methylene chloride|dcm)','halogenated'),
('Inorganics','(sulfate|carbonate|bicarbonate|phosphate|silicate|nitrate|nitrite|bromide|iodide)','inorganic salts'),
('Hydrocarbons','(mineral spirits|stoddard|heptane|hexane|naphtha|white spirit)','aliphatic HC')
ON CONFLICT DO NOTHING;
"""

# --- Helpers -----------------------------------------------------------------
def sql_exec(sql: str, params: Optional[dict] = None):
    with ENGINE.begin() as c:
        c.execute(text(sql), params or {})

def ensure_prereqs():
    with ENGINE.begin() as c:
        c.execute(text(DDL_SQL))
        c.execute(text(STRIP_NOISE_FN))
        for pat, repl, hint, note in ALIASES:
            c.execute(text("""
              INSERT INTO name_alias_map(pattern,replacement,hint,note)
              VALUES (:p,:r,:h,:n) ON CONFLICT DO NOTHING;
            """), {"p": pat, "r": repl, "h": hint, "n": note})
        c.execute(text(FAMILY_SEED))
        c.execute(text("UPDATE produced_query_log SET hint = lower(hint);"))

def build_scope_view(unlinked_only: bool):
    if unlinked_only:
        sql_exec("DROP VIEW IF EXISTS unlinked;")
        sql_exec("""
        CREATE TEMP VIEW unlinked AS
        SELECT c.source_item_id, c.sku, c.name, c.cas
        FROM item_master_clean c
        LEFT JOIN product_chem_xref x
          ON x.product_id=c.source_item_id AND x.source='pubchem'
        WHERE x.id IS NULL;
        """)

def build_seed(unlinked_only: bool, wide_names: bool):
    scope = "unlinked" if unlinked_only else "item_master_clean"
    build_scope_view(unlinked_only)

    # CAS seeds
    sql_exec(f"""
      INSERT INTO item_master_query (source_item_id, query, hint)
      SELECT source_item_id, cas, 'cas' FROM {scope}
      WHERE cas ~ '{CAS_RE}'
      ON CONFLICT DO NOTHING;
    """)
    sql_exec(f"""
      INSERT INTO item_master_query (source_item_id, query, hint)
      SELECT u.source_item_id, (regexp_matches(c.name,'([0-9]{{2,7}}-[0-9]{{2}}-[0-9])','g'))[1], 'cas'
      FROM {scope} u JOIN item_master_clean c USING (source_item_id)
      WHERE c.name ~ '[0-9]{{2,7}}-[0-9]{{2}}-[0-9]'
      ON CONFLICT DO NOTHING;
    """)
    sql_exec(f"""
      INSERT INTO item_master_query (source_item_id, query, hint)
      SELECT u.source_item_id, (regexp_matches(c.sku,'([0-9]{{2,7}}-[0-9]{{2}}-[0-9])','g'))[1], 'cas'
      FROM {scope} u JOIN item_master_clean c USING (source_item_id)
      WHERE c.sku ~ '[0-9]{{2,7}}-[0-9]{{2}}-[0-9]'
      ON CONFLICT DO NOTHING;
    """)
    sql_exec(f"""
      WITH cas_from_raw AS (
        SELECT s.id AS source_item_id, MIN(m.m[1]) AS cas_in_raw
        FROM stg_item_master s
        JOIN {scope} u ON u.source_item_id=s.id
        CROSS JOIN LATERAL jsonb_each_text(s.raw) kv
        CROSS JOIN LATERAL regexp_match(lower(kv.value),'([0-9]{{2,7}}-[0-9]{{2}}-[0-9])') AS m(m)
        GROUP BY s.id
      )
      INSERT INTO item_master_query (source_item_id, query, hint)
      SELECT source_item_id, cas_in_raw, 'cas'
      FROM cas_from_raw
      WHERE cas_in_raw IS NOT NULL
      ON CONFLICT DO NOTHING;
    """)

    # Aliases
    sql_exec(f"""
      INSERT INTO item_master_query (source_item_id, query, hint)
      SELECT u.source_item_id, m.replacement, m.hint
      FROM {scope} u
      JOIN item_master_clean c USING (source_item_id)
      JOIN name_alias_map m ON lower(c.name) ~ m.pattern
      ON CONFLICT DO NOTHING;
    """)

    # Chemical-like names
    sql_exec(f"""
      INSERT INTO item_master_query (source_item_id, query, hint)
      SELECT u.source_item_id, initcap(strip_noise_name(c.name)), 'name'
      FROM {scope} u JOIN item_master_clean c USING (source_item_id)
      WHERE c.name IS NOT NULL AND c.name <> ''
        AND length(strip_noise_name(c.name)) >= 3
        AND strip_noise_name(c.name) ~* '(acid|amine|amide|alcohol|oxide|chloride|sulfate|sulfite|sulfonate|acetate|glycol|ketone|benz|propyl|ethyl|methyl|hydroxide|carbonate|bicarbonate|phosphate|silicate|benzoate|nitrate|nitrite|bromide|iodide|citrate|lactate|tartrate|oxalate)'
      ON CONFLICT DO NOTHING;
    """)

    if wide_names:
        sql_exec(f"""
          INSERT INTO item_master_query (source_item_id, query, hint)
          SELECT u.source_item_id, initcap(strip_noise_name(c.name)), 'name'
          FROM {scope} u JOIN item_master_clean c USING (source_item_id)
          WHERE c.name IS NOT NULL AND c.name <> ''
            AND length(strip_noise_name(c.name)) >= 3
          ON CONFLICT DO NOTHING;
        """)

def clear_log_for_unlinked():
    sql_exec("""
    WITH u AS (
      SELECT c.source_item_id
      FROM item_master_clean c
      LEFT JOIN product_chem_xref x
        ON x.product_id=c.source_item_id AND x.source='pubchem'
      WHERE x.id IS NULL
    )
    DELETE FROM produced_query_log l
    USING u
    WHERE l.source_item_id = u.source_item_id;
    """)

def priority_case() -> str:
    # Priority: CAS → alias/common names → other names
    alias_list = (
        "'isopropanol','ethanol','sodium carbonate','sodium hydroxide',"
        "'potassium hydroxide','calcium hydroxide','ammonium hydroxide',"
        "'stoddard solvent','hydrochloric acid','methanol','trisodium phosphate',"
        "'1-methyl-2-pyrrolidone'"
    )
    return f"""
    CASE
      WHEN imq.query ~ '{CAS_RE}' THEN 1
      WHEN lower(imq.query) IN ({alias_list}) THEN 2
      ELSE 3
    END
    """

def fetch_page_unlinked(after: int, limit: int, topn: int) -> List[Mapping]:
    # Rank per product in-query; filter rn<=topn if topn>0
    pri = priority_case()
    topn_clause = "WHERE rn <= :topn" if topn and topn > 0 else ""
    SQL = f"""
    WITH u AS (
      SELECT c.source_item_id
      FROM item_master_clean c
      LEFT JOIN product_chem_xref x
        ON x.product_id=c.source_item_id AND x.source='pubchem'
      WHERE x.id IS NULL
    ),
    q AS (
      SELECT imq.id, imq.source_item_id, imq.query,
             lower(coalesce(imq.hint,'name')) AS hint,
             {pri} AS pri
      FROM item_master_query imq
      JOIN u ON u.source_item_id = imq.source_item_id
      LEFT JOIN produced_query_log l
        ON l.source_item_id=imq.source_item_id
       AND l.query=imq.query
       AND lower(l.hint)=lower(coalesce(imq.hint,'name'))
      WHERE l.query IS NULL
    ),
    r AS (
      SELECT q.*,
             row_number() OVER (PARTITION BY source_item_id ORDER BY pri, id) rn
      FROM q
    ),
    f AS (
      SELECT * FROM r {topn_clause}
    )
    SELECT id AS qid, source_item_id, query, hint
    FROM f
    WHERE id > :after
    ORDER BY id
    LIMIT :lim;
    """
    with ENGINE.begin() as c:
        return c.execute(text(SQL), {"after": after, "lim": limit, "topn": topn}).mappings().all()

def fetch_page_seed(after: int, limit: int) -> List[Mapping]:
    SQL = """
    SELECT imq.id AS qid,
           imq.source_item_id,
           imq.query,
           lower(coalesce(imq.hint,'name')) AS hint
    FROM item_master_query imq
    LEFT JOIN produced_query_log l
      ON l.source_item_id=imq.source_item_id
     AND l.query=imq.query
     AND lower(l.hint)=lower(coalesce(imq.hint,'name'))
    WHERE l.query IS NULL
      AND imq.id > :after
    ORDER BY imq.id
    LIMIT :lim;
    """
    with ENGINE.begin() as c:
        return c.execute(text(SQL), {"after": after, "lim": limit}).mappings().all()

def kafka_key(r: Mapping) -> str:
    return f"pubchem|{(r['hint'] or 'name').lower()}|{r['source_item_id']}|{(r['query'] or '').strip().lower()}"

async def produce(rows: Iterable[Mapping]) -> int:
    prod = AIOKafkaProducer(
        bootstrap_servers=BOOT, client_id=CID, acks="all",
        enable_idempotence=True, linger_ms=150, request_timeout_ms=30000,
    )
    await prod.start()
    sent = 0
    try:
        for r in rows:
            q = (r["query"] or "").strip()
            if not q: continue
            msg = {
                "query": q,
                "hint": (r["hint"] or "name"),
                "source": "pubchem",
                "source_item_id": r["source_item_id"],
                "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            await prod.send_and_wait(TOPIC, key=kafka_key(r).encode(), value=json.dumps(sanitize_json(msg)).encode())
            sent += 1
    finally:
        await prod.stop()
    if sent:
        with ENGINE.begin() as c:
            c.execute(
                text("""
                INSERT INTO produced_query_log (qid, source_item_id, query, hint)
                VALUES (:qid,:sid,:q,:h)
                ON CONFLICT DO NOTHING
                """),
                [{"qid": r["qid"], "sid": r["source_item_id"],
                  "q": (r["query"] or "").strip(),
                  "h": (r["hint"] or "name").lower()} for r in rows]
            )
    return sent

async def produce_unlinked_paged(batch: int, topn: int) -> int:
    after, total = 0, 0
    while True:
        page = fetch_page_unlinked(after, batch, topn)
        if not page: break
        total += await produce(page)
        after = page[-1]["qid"]
        print(f"[unlinked] sent page={len(page)} total={total} last_qid={after}")
    print(f"[unlinked] DONE. total sent={total}")
    return total

async def produce_seed_paged(batch: int) -> int:
    after, total = 0, 0
    while True:
        page = fetch_page_seed(after, batch)
        if not page: break
        total += await produce(page)
        after = page[-1]["qid"]
        print(f"[seed] sent page={len(page)} total={total} last_qid={after}")
    print(f"[seed] DONE. total sent={total}")
    return total

# --- Week 5: registry, link, families ---------------------------------------
REGISTRY_SQL = """
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
    (SELECT array_agg(elem #>> '{}')
     FROM jsonb_array_elements(COALESCE(sc.properties::jsonb->'Synonyms','[]'::jsonb)) AS elem),
    ARRAY[]::text[]
  ) AS synonyms,
  sc.properties::jsonb,
  now(), now()
FROM scraped_compounds sc
WHERE sc.source='pubchem'
ON CONFLICT (source, external_id) DO UPDATE
SET cid              = EXCLUDED.cid,
    canonical_name   = COALESCE(EXCLUDED.canonical_name, chemical_registry.canonical_name),
    iupac_name       = COALESCE(EXCLUDED.iupac_name,     chemical_registry.iupac_name),
    formula          = COALESCE(EXCLUDED.formula,        chemical_registry.formula),
    mol_weight       = COALESCE(EXCLUDED.mol_weight,     chemical_registry.mol_weight),
    canonical_smiles = COALESCE(EXCLUDED.canonical_smiles, chemical_registry.canonical_smiles),
    inchi            = COALESCE(EXCLUDED.inchi,          chemical_registry.inchi),
    inchikey         = COALESCE(EXCLUDED.inchikey,       chemical_registry.inchikey),
    synonyms         = CASE WHEN EXCLUDED.synonyms IS NOT NULL
                              AND array_length(EXCLUDED.synonyms,1)>0
                            THEN EXCLUDED.synonyms ELSE chemical_registry.synonyms END,
    properties       = COALESCE(EXCLUDED.properties, chemical_registry.properties),
    updated_at       = now();
"""

ASSIGN_PROPS_SQL = """
UPDATE product_chem_xref x
SET chem_id = r.id, updated_at=now()
FROM chemical_registry r
WHERE x.source='pubchem' AND r.source='pubchem'
  AND r.external_id = x.external_id
  AND (x.chem_id IS DISTINCT FROM r.id);

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
"""

FAMILIES_SQL = """
INSERT INTO product_family (product_id, family_id, confidence)
SELECT p.source_item_id, f.id, 0.85
FROM item_master_clean p
JOIN chemical_family f ON strip_noise_name(p.name) ~* f.pattern
LEFT JOIN product_family pf ON pf.product_id=p.source_item_id AND pf.family_id=f.id
WHERE pf.id IS NULL;

INSERT INTO product_family (product_id, family_id, confidence)
SELECT p.source_item_id, f.id, 0.8
FROM item_master_clean p
JOIN product_chem_xref x ON x.product_id=p.source_item_id AND x.source='pubchem'
JOIN chemical_registry r ON r.id=x.chem_id
JOIN chemical_family f ON coalesce(lower(r.iupac_name), lower(r.canonical_name)) ~* f.pattern
LEFT JOIN product_family pf ON pf.product_id=p.source_item_id AND pf.family_id=f.id
WHERE pf.id IS NULL;
"""

def registry_link_families():
    with ENGINE.begin() as c:
        c.execute(text(REGISTRY_SQL))
        c.execute(text(ASSIGN_PROPS_SQL))
        c.execute(text(FAMILIES_SQL))

# --- Reporting ---------------------------------------------------------------
def counts_30min() -> Mapping[str, int]:
    with ENGINE.begin() as c:
        produced = c.execute(text("SELECT COUNT(*) FROM produced_query_log WHERE produced_at >= now()-interval '30 min'")).scalar_one()
        scraped  = c.execute(text("SELECT COUNT(*) FROM scraped_compounds WHERE created_at >= now()-interval '30 min'")).scalar_one()
        linked   = c.execute(text("SELECT COUNT(*) FROM product_chem_xref WHERE updated_at >= now()-interval '30 min'")).scalar_one()
    return {"produced_30m": produced, "scraped_30m": scraped, "linked_30m": linked}

def coverage() -> Mapping[str, float]:
    SQL = """
    SELECT COUNT(*)::int AS products,
           COUNT(x.id)::int AS linked,
           ROUND(100.0*COUNT(x.id)/NULLIF(COUNT(*),0),2) AS pct_linked
    FROM item_master_clean p
    LEFT JOIN product_chem_xref x ON x.product_id=p.source_item_id AND x.source='pubchem';
    """
    with ENGINE.begin() as c:
        r = c.execute(text(SQL)).mappings().first()
    return dict(r)

# --- Main --------------------------------------------------------------------
async def main():
    ap = argparse.ArgumentParser(description="Replay → Seed → Produce → Registry → Link → Families → Report")
    ap.add_argument("--replay-unlinked", action="store_true", help="clear produced_query_log for currently unlinked products first")
    ap.add_argument("--unlinked-only", action="store_true", help="limit seeding to unlinked products")
    ap.add_argument("--wide-names", action="store_true", help="also seed any cleaned name (>=3 chars)")
    ap.add_argument("--cap-topn", type=int, default=3, help="limit to top-N queries per unlinked product (0 = no cap)")
    ap.add_argument("--batch", type=int, default=int(os.getenv("BATCH","3000")))
    ap.add_argument("--produce-rest", action="store_true", help="after unlinked, also produce remaining unsent seed")
    args = ap.parse_args()

    print(f"[env] DB={DB_URL}  BOOT={BOOT}  TOPIC={TOPIC}  CID={CID}")
    ensure_prereqs()

    if args.replay_unlinked:
        print("[replay] clearing produced_query_log for unlinked …")
        clear_log_for_unlinked()

    print("[seed] building …")
    build_seed(unlinked_only=args.unlinked_only, wide_names=args.wide_names)

    print("[produce] unlinked (paged) …")
    sent_unlinked = await produce_unlinked_paged(batch=args.batch, topn=args.cap_topn)

    sent_rest = 0
    if args.produce_rest:
        print("[produce] rest of seed (paged) …")
        sent_rest = await produce_seed_paged(batch=args.batch)

    print("[week5] registry → link → families …")
    registry_link_families()

    act = counts_30min()
    cov = coverage()
    print(f"[activity-30m] produced={act['produced_30m']} scraped={act['scraped_30m']} linked={act['linked_30m']}")
    print(f"[coverage] products={cov['products']} linked={cov['linked']} pct={cov['pct_linked']}%")
    print(f"[summary] produced unlinked={sent_unlinked}  rest={sent_rest}")

if __name__ == "__main__":
    asyncio.run(main())
