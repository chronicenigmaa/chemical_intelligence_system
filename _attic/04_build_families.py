from __future__ import annotations
from sqlalchemy import create_engine, text
from app.core.config import settings

ENGINE = create_engine(settings.DATABASE_URL.replace("+asyncpg","+psycopg"), future=True)

SQL = """
-- assign families from normalized product names
insert into product_family (product_id, family_id, confidence)
select p.source_item_id, f.id, 0.85
from item_master_clean p
join chemical_family f
  on strip_noise_name(p.name) ~* f.pattern
left join product_family pf
  on pf.product_id=p.source_item_id and pf.family_id=f.id
where pf.id is null;

-- also try registry names (IUPAC/canonical) if product name didn’t match
insert into product_family (product_id, family_id, confidence)
select p.source_item_id, f.id, 0.8
from item_master_clean p
join product_chem_xref x on x.product_id=p.source_item_id and x.source='pubchem'
join chemical_registry r on r.id=x.chem_id
join chemical_family f
  on coalesce(lower(r.iupac_name), lower(r.canonical_name)) ~* f.pattern
left join product_family pf
  on pf.product_id=p.source_item_id and pf.family_id=f.id
where pf.id is null;
"""

if __name__ == "__main__":
    with ENGINE.begin() as c:
        c.execute(text(SQL))
    print("[OK] product families assigned.")
