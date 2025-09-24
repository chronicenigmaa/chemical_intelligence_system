# scripts/build_seed.py
from __future__ import annotations
import argparse, os, sys
from sqlalchemy import create_engine, text
from app.core.config import settings

ENGINE = create_engine(settings.DATABASE_URL.replace("+asyncpg", "+psycopg"), future=True)

STRIP_FN_SQL = """
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

ALIAS_DDL = """
CREATE TABLE IF NOT EXISTS name_alias_map (
  pattern text PRIMARY KEY,
  replacement text NOT NULL,
  hint text NOT NULL CHECK (hint IN ('name','cas')),
  note text
);
"""

ALIAS_SEED = [
  ("^\\s*xylenes?\\s*$", "1330-20-7", "cas", "mixed xylenes"),
  ("^\\s*soda\\s+ash\\s*$", "sodium carbonate", "name", "common"),
  ("^\\s*caustic\\s+soda\\s*$", "sodium hydroxide", "name", "common"),
  ("^\\s*caustic\\s+potash\\s*$", "potassium hydroxide", "name", "common"),
  ("^\\s*hydrated\\s+lime\\s*$", "calcium hydroxide", "name", "common"),
  ("^\\s*aqua\\s+ammonia\\s*$", "ammonium hydroxide", "name", "common"),
  ("^\\s*isopropyl\\s+alcohol\\s*$", "isopropanol", "name", "synonym"),
  ("^\\s*denatured\\s+alcohol\\s*$", "ethanol", "name", "synonym"),
  ("^\\s*mineral\\s+spirits?\\s*$", "Stoddard solvent", "name", "synonym"),
  ("^\\s*muriatic\\s+acid\\s*$", "hydrochloric acid", "name", "synonym"),
  ("^\\s*mek\\s*$", "78-93-3", "cas", "methyl ethyl ketone"),
  ("^\\s*mibk\\s*$", "108-10-1", "cas", "methyl isobutyl ketone"),
  ("^\\s*dcm\\s*$", "75-09-2", "cas", "dichloromethane"),
]

IMQ_DDL = """
CREATE TABLE IF NOT EXISTS item_master_query (
  id BIGSERIAL PRIMARY KEY,
  source_item_id BIGINT,
  query TEXT NOT NULL,
  hint TEXT DEFAULT 'name',
  CONSTRAINT item_master_query_hint_chk CHECK (hint IN ('auto','name','cas')),
  UNIQUE (source_item_id, query, hint)
);
"""

UNLINKED_VIEW = """
DROP VIEW IF EXISTS unlinked;
CREATE TEMP VIEW unlinked AS
SELECT c.source_item_id, c.sku, c.name, c.cas
FROM item_master_clean c
LEFT JOIN product_chem_xref x
  ON x.product_id = c.source_item_id AND x.source='pubchem'
WHERE x.id IS NULL;
"""

INSERTS = {
  "cas_from_product": """
    INSERT INTO item_master_query (source_item_id, query, hint)
    SELECT source_item_id, cas, 'cas'
    FROM {scope}
    WHERE cas ~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$'
    ON CONFLICT DO NOTHING;
  """,
  "cas_from_name": """
    INSERT INTO item_master_query (source_item_id, query, hint)
    SELECT u.source_item_id, (regexp_matches(c.name,'([0-9]{2,7}-[0-9]{2}-[0-9])','g'))[1], 'cas'
    FROM {scope} u JOIN item_master_clean c USING (source_item_id)
    WHERE c.name ~ '[0-9]{2,7}-[0-9]{2}-[0-9]'
    ON CONFLICT DO NOTHING;
  """,
  "cas_from_sku": """
    INSERT INTO item_master_query (source_item_id, query, hint)
    SELECT u.source_item_id, (regexp_matches(c.sku,'([0-9]{2,7}-[0-9]{2}-[0-9])','g'))[1], 'cas'
    FROM {scope} u JOIN item_master_clean c USING (source_item_id)
    WHERE c.sku ~ '[0-9]{2,7}-[0-9]{2}-[0-9]'
    ON CONFLICT DO NOTHING;
  """,
  "cas_from_raw": """
    INSERT INTO item_master_query (source_item_id, query, hint)
    SELECT u.source_item_id,
           (regexp_matches(lower(kv.value),'([0-9]{2,7}-[0-9]{2}-[0-9])','g'))[1], 'cas'
    FROM {scope} u
    JOIN stg_item_master s ON s.id=u.source_item_id,
         LATERAL jsonb_each_text(s.raw) kv
    WHERE kv.value ~ '[0-9]{2,7}-[0-9]{2}-[0-9]'
    ON CONFLICT DO NOTHING;
  """,
  "aliases": """
    INSERT INTO item_master_query (source_item_id, query, hint)
    SELECT u.source_item_id, m.replacement, m.hint
    FROM {scope} u
    JOIN item_master_clean c USING (source_item_id)
    JOIN name_alias_map m ON lower(c.name) ~ m.pattern
    ON CONFLICT DO NOTHING;
  """,
  "clean_names": """
    INSERT INTO item_master_query (source_item_id, query, hint)
    SELECT u.source_item_id, initcap(strip_noise_name(c.name)), 'name'
    FROM {scope} u JOIN item_master_clean c USING (source_item_id)
    WHERE c.name IS NOT NULL AND c.name <> ''
      AND length(strip_noise_name(c.name)) >= 3
      AND strip_noise_name(c.name) ~* '(acid|amine|amide|alcohol|oxide|chloride|sulfate|sulfite|sulfonate|acetate|glycol|ketone|benz|propyl|ethyl|methyl|hydroxide|carbonate|bicarbonate|phosphate|silicate|benzoate|nitrate|nitrite|bromide|iodide|citrate|lactate|tartrate|oxalate)'
    ON CONFLICT DO NOTHING;
  """
}

def run_sql(sql: str, **fmt):
    if fmt:
        for k, v in fmt.items():
            sql = sql.replace(f"{{{k}}}", str(v))  # replace only our {scope} token
    with ENGINE.begin() as c:
        c.execute(text(sql))

def count_new():
    with ENGINE.begin() as c:
        return c.execute(text("SELECT COUNT(*) FROM item_master_query")).scalar_one()

def build_seed(unlinked_only: bool):
    with ENGINE.begin() as c:
        c.execute(text(IMQ_DDL))
        c.execute(text(STRIP_FN_SQL))
        c.execute(text(ALIAS_DDL))
        for pat, repl, hint, note in ALIAS_SEED:
            c.execute(text("""
              INSERT INTO name_alias_map(pattern,replacement,hint,note)
              VALUES (:p,:r,:h,:n)
              ON CONFLICT DO NOTHING;
            """), {"p": pat, "r": repl, "h": hint, "n": note})

    scope = "unlinked" if unlinked_only else "item_master_clean"
    if unlinked_only:
        run_sql(UNLINKED_VIEW)

    before = count_new()
    for k in ["cas_from_product","cas_from_name","cas_from_sku","cas_from_raw","aliases","clean_names"]:
        run_sql(INSERTS[k], scope=scope)
    after = count_new()
    print(f"[seed] added {after - before} queries (scope={scope})")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--unlinked-only", action="store_true", help="build seed only for items not yet linked")
    args = ap.parse_args()
    build_seed(unlinked_only=args.unlinked_only)
