from __future__ import annotations
from sqlalchemy import create_engine, text
from app.core.config import settings

ENGINE = create_engine(settings.DATABASE_URL.replace("+asyncpg","+psycopg"), future=True)

LINK_SQL = """
update product_chem_xref x
set chem_id = r.id, updated_at=now()
from chemical_registry r
where x.source='pubchem'
  and r.source='pubchem'
  and r.external_id = x.external_id
  and (x.chem_id is distinct from r.id);
"""

# Optional: materialize a table for quick BI; otherwise use a view
CREATE_VIEW = """
create or replace view vw_product_properties as
select
  p.source_item_id as product_id,
  p.sku,
  p.name,
  p.cas as product_cas,
  x.external_id as pubchem_cid,
  r.formula,
  r.mol_weight,
  r.iupac_name,
  r.canonical_smiles,
  r.inchikey
from item_master_clean p
join product_chem_xref x on x.product_id = p.source_item_id and x.source='pubchem'
left join chemical_registry r on r.id = x.chem_id;
"""

if __name__ == "__main__":
    with ENGINE.begin() as c:
        c.execute(text(LINK_SQL))
        c.execute(text(CREATE_VIEW))
    print("[OK] product_chem_xref.chem_id set; vw_product_properties created.")
