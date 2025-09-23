-- Crosswalk of scrape queries (built from Item Master)
create table if not exists item_master_query (
  id serial primary key,
  source_item_id int,
  query text not null,
  hint text check (hint in ('auto','name','cas')) default 'auto',
  unique (source_item_id, query)
);

-- Link products to PubChem compounds
create table if not exists product_chem_xref (
  id serial primary key,
  product_id int not null,      -- FK to your product row (stg_item_master.id or prod table)
  source text not null,         -- 'pubchem'
  external_id text not null,    -- CID
  cas text,
  match_confidence numeric,
  properties_ref jsonb,
  updated_at timestamptz default now(),
  unique (source, external_id, product_id)
);

-- Optional DLQ table for validation failures
create table if not exists ingest_dlq (
  id serial primary key,
  query text,
  reason text,
  stage text,
  attempts int,
  payload_json jsonb,
  created_at timestamptz default now()
);
