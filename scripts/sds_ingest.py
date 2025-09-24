# scripts/week7/sds_ingest.py
from __future__ import annotations
import argparse, os, re, sys, hashlib
from pathlib import Path
from typing import List, Dict, Any, Optional

# ------------- third-party deps -------------
# pip install pdfplumber pdf2image pytesseract pillow sqlalchemy
import pdfplumber
from pdf2image import convert_from_path
import pytesseract
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ------------- config / db engine -------------
def get_engine():
    try:
        from app.core.config import settings  # your existing config
        db_url = settings.DATABASE_URL.replace("+asyncpg", "+psycopg")
        tpath = getattr(settings, "TESSERACT_PATH", None)
        ppath = getattr(settings, "POPPLER_PATH", None)
    except Exception:
        db_url = os.getenv("DATABASE_URL", "postgresql+psycopg://zain:Zain2025!!!@localhost:5432/chemdb")
        tpath = os.getenv("TESSERACT_PATH")
        ppath = os.getenv("POPPLER_PATH")
    if tpath:
        pytesseract.pytesseract.tesseract_cmd = tpath
    return create_engine(db_url, future=True), ppath

# ------------- SQL DDL (tables, constraints, helper fn) -------------
DDL = """
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- main docs
CREATE TABLE IF NOT EXISTS sds_document(
  id             bigserial PRIMARY KEY,
  product_id     bigint NULL,
  title          text,
  filename       text NOT NULL,
  file_sha256    text NOT NULL,
  pages          int  NOT NULL DEFAULT 0,
  created_at     timestamptz DEFAULT now(),
  UNIQUE(file_sha256)
);

-- per-page text (OCR or native)
CREATE TABLE IF NOT EXISTS sds_page(
  id        bigserial PRIMARY KEY,
  doc_id    bigint NOT NULL REFERENCES sds_document(id) ON DELETE CASCADE,
  page_no   int NOT NULL,
  has_text  boolean NOT NULL DEFAULT true,
  ocr_used  boolean NOT NULL DEFAULT false,
  ocr_conf  numeric,
  text      text,
  created_at timestamptz DEFAULT now(),
  UNIQUE(doc_id, page_no)
);

-- per-section text (S1..S16)
CREATE TABLE IF NOT EXISTS sds_section(
  doc_id     bigint REFERENCES sds_document(id) ON DELETE CASCADE,
  section_no int CHECK (section_no BETWEEN 1 AND 16),
  title      text,
  text       text,
  PRIMARY KEY (doc_id, section_no)
);

-- parsed entities (dedup via unique index)
CREATE TABLE IF NOT EXISTS sds_entity(
  id           bigserial PRIMARY KEY,
  doc_id       bigint NOT NULL REFERENCES sds_document(id) ON DELETE CASCADE,
  page_no      int,
  section      text,
  entity_type  text NOT NULL CHECK (entity_type IN ('CAS','UN','GHS_H','GHS_P','SIGNAL','NAME')),
  value        text,
  norm_value   text,
  chem_id      bigint NULL,
  created_at   timestamptz DEFAULT now()
);

-- Replace any partial index with a non-partial one to support ON CONFLICT (cols...)
DROP INDEX IF EXISTS uq_sds_entity_doc_type_val;
CREATE UNIQUE INDEX IF NOT EXISTS ux_sds_entity_doc_type_val
  ON sds_entity(doc_id, entity_type, norm_value);

-- Section 3 components (do NOT assume column flavor; runtime handles generated/plain)
CREATE TABLE IF NOT EXISTS sds_component(
  id          bigserial PRIMARY KEY,
  doc_id      bigint NOT NULL REFERENCES sds_document(id) ON DELETE CASCADE,
  page_no     int,
  name        text,
  cas         text,
  cas_norm    text,  -- if it already exists as GENERATED in your DB, this line is ignored
  name_key    text,
  conc_low    numeric,
  conc_high   numeric,
  conc_units  text,
  created_at  timestamptz DEFAULT now()
);

-- Ensure NOT NULL/DEFAULT only when columns exist and are plain; harmless if generated exists
ALTER TABLE sds_component
  ALTER COLUMN name_key SET DEFAULT '',
  ALTER COLUMN name_key DROP NOT NULL,
  ALTER COLUMN cas_norm DROP NOT NULL;

-- Unique index for dedup
CREATE UNIQUE INDEX IF NOT EXISTS ux_sds_component_doc_casnorm_namekey
  ON sds_component (doc_id, cas_norm, name_key);

-- Hazards (H/P codes) — one row per (doc, code)
CREATE TABLE IF NOT EXISTS sds_hazard(
  id        bigserial PRIMARY KEY,
  doc_id    bigint NOT NULL REFERENCES sds_document(id) ON DELETE CASCADE,
  page_no   int,
  section   text,
  code      text NOT NULL,
  statement text,
  created_at timestamptz DEFAULT now(),
  UNIQUE(doc_id, code)
);

-- search index
CREATE INDEX IF NOT EXISTS ix_sds_page_tsv
  ON sds_page USING gin (to_tsvector('english', coalesce(text,'')));

-- CAS normalizer (check-digit)
CREATE OR REPLACE FUNCTION cas_normalize(s text)
RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  d text; a text; b text; c text; body text; rev text; i int; sum int := 0;
BEGIN
  IF s IS NULL OR s='' THEN RETURN NULL; END IF;
  d := regexp_replace(s,'[^0-9]','','g');
  IF length(d) < 5 OR length(d) > 10 THEN RETURN NULL; END IF;
  a := substring(d FROM 1 FOR length(d)-3);
  b := substring(d FROM length(d)-2 FOR 2);
  c := right(d,1);
  IF a !~ '^\d{2,7}$' OR b !~ '^\d{2}$' OR c !~ '^\d$' THEN RETURN NULL; END IF;
  body := a||b; rev := reverse(body);
  FOR i IN 1..length(rev) LOOP sum := sum + (substring(rev FROM i FOR 1))::int * i; END LOOP;
  IF (sum % 10) <> c::int THEN RETURN NULL; END IF;
  RETURN a||'-'||b||'-'||c;
END $$;

-- pivot view: sections as columns
CREATE OR REPLACE VIEW vw_sds_sections_pivot AS
SELECT
  d.id AS doc_id,
  d.filename,
  MAX(CASE WHEN s.section_no=1  THEN s.text END) AS s1_identification,
  MAX(CASE WHEN s.section_no=2  THEN s.text END) AS s2_hazards,
  MAX(CASE WHEN s.section_no=3  THEN s.text END) AS s3_composition,
  MAX(CASE WHEN s.section_no=4  THEN s.text END) AS s4_first_aid,
  MAX(CASE WHEN s.section_no=5  THEN s.text END) AS s5_fire_fighting,
  MAX(CASE WHEN s.section_no=6  THEN s.text END) AS s6_accidental_release,
  MAX(CASE WHEN s.section_no=7  THEN s.text END) AS s7_handling_storage,
  MAX(CASE WHEN s.section_no=8  THEN s.text END) AS s8_exposure_ppe,
  MAX(CASE WHEN s.section_no=9  THEN s.text END) AS s9_physical_chemical,
  MAX(CASE WHEN s.section_no=10 THEN s.text END) AS s10_stability_reactivity,
  MAX(CASE WHEN s.section_no=11 THEN s.text END) AS s11_toxicology,
  MAX(CASE WHEN s.section_no=12 THEN s.text END) AS s12_ecology,
  MAX(CASE WHEN s.section_no=13 THEN s.text END) AS s13_disposal,
  MAX(CASE WHEN s.section_no=14 THEN s.text END) AS s14_transport,
  MAX(CASE WHEN s.section_no=15 THEN s.text END) AS s15_regulatory,
  MAX(CASE WHEN s.section_no=16 THEN s.text END) AS s16_other
FROM sds_document d
LEFT JOIN sds_section s ON s.doc_id=d.id
GROUP BY d.id, d.filename;
"""

# ------------- helpers -------------
CAS_RE = re.compile(r'(?<!\d)\d{2,7}-\d{2}-\d(?!\d)')
H_CODE = re.compile(r'\bH\d{3}[A-Z]?\b')
P_CODE = re.compile(r'\bP\d{3}[A-Z]?(?:\+P\d{3}[A-Z]?)?\b')
SIGNAL = re.compile(r'\b(DANGER|WARNING)\b', re.I)
SECH = re.compile(r'(?:^|\n)\s*(SECTION|SEC\.)\s*(\d{1,2})\s*[:\.-]?\s*(.+?)\s*(?=\n|$)', re.I)

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

def extract_pages(pdf: Path, poppler_path: Optional[str]) -> List[Dict[str, Any]]:
    pages: List[Dict[str, Any]] = []
    # Try native text
    with pdfplumber.open(str(pdf)) as doc:
        for i, pg in enumerate(doc.pages, start=1):
            txt = (pg.extract_text() or "").strip()
            pages.append({"page_no": i, "text": txt, "has_text": bool(txt), "ocr_used": False, "ocr_conf": None})
    # OCR where needed
    needs_ocr = [p for p in pages if not p["has_text"] or len(p["text"]) < 30]
    if needs_ocr:
        images = convert_from_path(str(pdf), dpi=300, poppler_path=poppler_path)
        for i, img in enumerate(images, start=1):
            p = pages[i-1]
            if not p["has_text"] or len(p["text"]) < 30:
                txt = pytesseract.image_to_string(img) or ""
                p["text"] = txt.strip()
                p["ocr_used"] = True
                p["ocr_conf"] = None
    return pages

def parse_conc(line: str):
    # matches "10-20 %" or "10 to 20 %" or single "15%"
    m = re.search(r'(?:(\d+(?:\.\d+)?)\s*(?:-|to|–)\s*(\d+(?:\.\d+)?))\s*%|\b(\d+(?:\.\d+)?)\s*%', line)
    if m:
        if m.group(1) and m.group(2): return float(m.group(1)), float(m.group(2)), '%'
        if m.group(3): v = float(m.group(3)); return v, v, '%'
    return None, None, None

def guess_section_tag(line: str) -> Optional[str]:
    m = re.search(r'\bSECTION\s*\d+\b', line, re.I)
    return m.group(0).upper() if m else None

# ------------- schema introspection -------------
def is_generated(engine: Engine, table: str, column: str) -> bool:
    """Return True if the given column is a GENERATED ALWAYS column in the current schema."""
    q = text("""
        SELECT is_generated = 'ALWAYS'
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = :t
          AND column_name = :c
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(q, {"t": table, "c": column}).scalar()
    return bool(row)

# ------------- linking SQL -------------
LINK_SQL = """
-- 1) fuzzy attach SDS to product if product_id null
WITH cand AS (
  SELECT d.id AS doc_id, imc.source_item_id,
         GREATEST(similarity(lower(d.title), lower(imc.name)),
                  similarity(lower(d.filename), lower(imc.name))) AS sim,
         row_number() OVER (PARTITION BY d.id ORDER BY
           GREATEST(similarity(lower(d.title), lower(imc.name)),
                    similarity(lower(d.filename), lower(imc.name))) DESC
         ) AS rn
  FROM sds_document d
  JOIN item_master_clean imc ON true
  WHERE d.product_id IS NULL
)
UPDATE sds_document d
SET product_id = c.source_item_id
FROM cand c
WHERE c.doc_id=d.id AND c.rn=1 AND c.sim >= 0.45;

-- 2) pick "main CAS" from components (highest %)
WITH comp AS (
  SELECT doc_id, cas_norm AS cas, name,
         COALESCE(conc_high, conc_low, 100) AS pct,
         row_number() OVER (PARTITION BY doc_id ORDER BY COALESCE(conc_high, conc_low, 100) DESC NULLS LAST, lower(name)) rn
  FROM sds_component
  WHERE cas_norm <> ''
), main AS (SELECT doc_id, cas, name FROM comp WHERE rn=1)

-- fill product CAS if missing
UPDATE item_master_clean c
SET cas = m.cas
FROM sds_document d
JOIN main m ON m.doc_id=d.id
WHERE d.product_id=c.source_item_id
  AND (c.cas IS NULL OR c.cas='');

-- 3) create CAS links (dedup)
CREATE UNIQUE INDEX IF NOT EXISTS uq_xref_triplet
  ON product_chem_xref(product_id, source, external_id);

WITH d AS (
  SELECT DISTINCT d.product_id, e.norm_value AS cas
  FROM sds_document d
  JOIN sds_entity e ON e.doc_id=d.id
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

-- 4) ensure registry rows for CAS and link chem_id
INSERT INTO chemical_registry(source, external_id, canonical_name, created_at, updated_at)
SELECT 'cas', d.cas, NULL, now(), now()
FROM (
  SELECT DISTINCT e.norm_value AS cas
  FROM sds_entity e
  WHERE e.entity_type='CAS' AND e.norm_value IS NOT NULL
) d
ON CONFLICT (source, external_id) DO NOTHING;

UPDATE product_chem_xref x
SET chem_id = r.id, updated_at = now()
FROM chemical_registry r
WHERE x.source='cas'
  AND r.source='cas'
  AND r.external_id = x.external_id
  AND (x.chem_id IS DISTINCT FROM r.id);
"""

# ------------- main pipeline -------------
def main():
    ap = argparse.ArgumentParser(description="SDS ingest → DB, dedup, sections, and linking")
    ap.add_argument("--dir", required=True, help="Folder containing SDS PDFs")
    args = ap.parse_args()

    engine, poppler_path = get_engine()
    with engine.begin() as c:
        # Ensure schema (non-destructive)
        c.execute(text(DDL))

    # Decide which INSERT to use for sds_component based on the actual DB schema
    use_generated_cas_norm = is_generated(engine, "sds_component", "cas_norm")

    # Statements
    STMT_ENTITY = text("""
      INSERT INTO sds_entity(doc_id, page_no, section, entity_type, value, norm_value)
      VALUES (:doc,:pg,:sec,'CAS',:val, cas_normalize(:val))
      ON CONFLICT (doc_id, entity_type, norm_value) DO NOTHING;
    """)

    STMT_COMPONENT_PLAIN = text("""
      INSERT INTO sds_component (
        doc_id, page_no, name, cas, cas_norm, name_key, conc_low, conc_high, conc_units
      )
      VALUES (
        :doc, :pg, NULLIF(:name,''), :cas,
        COALESCE(cas_normalize(:cas),''), lower(coalesce(:name,'')), :lo, :hi, :unit
      )
      ON CONFLICT (doc_id, cas_norm, name_key) DO NOTHING;
    """)

    STMT_COMPONENT_GENERATED = text("""
      INSERT INTO sds_component (
        doc_id, page_no, name, cas, name_key, conc_low, conc_high, conc_units
      )
      VALUES (
        :doc, :pg, NULLIF(:name,''), :cas,
        lower(coalesce(:name,'')), :lo, :hi, :unit
      )
      ON CONFLICT (doc_id, cas_norm, name_key) DO NOTHING;
    """)

    STMT_HAZARD = text("""
      INSERT INTO sds_hazard(doc_id, page_no, section, code, statement)
      VALUES (:doc,:pg,:sec,:code,:stmt)
      ON CONFLICT (doc_id, code) DO UPDATE SET statement = EXCLUDED.statement;
    """)

    stmt_component = STMT_COMPONENT_GENERATED if use_generated_cas_norm else STMT_COMPONENT_PLAIN

    folder = Path(args.dir)
    pdfs = sorted([p for p in folder.glob("*.pdf")])
    if not pdfs:
        print(f"[warn] no PDFs found in {folder}")
        sys.exit(0)

    for pdf in pdfs:
        digest = sha256_file(pdf)
        with engine.begin() as c:
            # Upsert doc and get id
            doc_id = c.execute(text("""
                INSERT INTO sds_document(title, filename, file_sha256, pages)
                VALUES (:title, :fn, :sha, 0)
                ON CONFLICT (file_sha256)
                DO UPDATE SET title=EXCLUDED.title
                RETURNING id;
            """), {"title": pdf.stem, "fn": pdf.name, "sha": digest}).scalar_one()

            # Extract pages (ocr where needed)
            pages = extract_pages(pdf, poppler_path)
            c.execute(text("UPDATE sds_document SET pages=:n WHERE id=:id"), {"n": len(pages), "id": doc_id})

            # store/replace pages
            for p in pages:
                c.execute(text("""
                    INSERT INTO sds_page(doc_id, page_no, has_text, ocr_used, ocr_conf, text)
                    VALUES (:doc,:pg,:has,:ocr,:conf,:txt)
                    ON CONFLICT (doc_id, page_no) DO UPDATE
                    SET has_text=EXCLUDED.has_text,
                        ocr_used=EXCLUDED.ocr_used,
                        ocr_conf=EXCLUDED.ocr_conf,
                        text=EXCLUDED.text;
                """), {"doc": doc_id, "pg": p["page_no"], "has": p["has_text"], "ocr": p["ocr_used"],
                       "conf": p["ocr_conf"], "txt": p["text"]})

            # -------- parse entities / hazards / components (dedup aware) --------
            section = None
            for p in pages:
                txt = p["text"] or ""
                page_no = p["page_no"]
                for line in txt.splitlines():
                    sec_hit = guess_section_tag(line)
                    if sec_hit: section = sec_hit

                    # CAS
                    for cas in CAS_RE.findall(line):
                        c.execute(STMT_ENTITY, {"doc": doc_id, "pg": page_no, "sec": section, "val": cas})

                        # Component row (name up to CAS)
                        name_part = line.split(cas)[0].strip(" ,;:-")
                        lo, hi, unit = parse_conc(line)
                        c.execute(stmt_component, {
                            "doc": doc_id,
                            "pg": page_no,
                            "name": name_part,
                            "cas": cas,
                            "lo": lo, "hi": hi, "unit": unit
                        })

                    # Hazards
                    for h in H_CODE.findall(line):
                        c.execute(STMT_HAZARD, {
                            "doc": doc_id, "pg": page_no, "sec": section,
                            "code": h, "stmt": line.strip()
                        })
                    for pcode in P_CODE.findall(line):
                        c.execute(STMT_HAZARD, {"doc": doc_id, "pg": page_no, "sec": section, "code": pcode, "stmt": line.strip()})
                    for sig in SIGNAL.findall(line):
                        sigu = sig.upper()
                        c.execute(text("""
                          INSERT INTO sds_entity(doc_id, page_no, section, entity_type, value, norm_value)
                          VALUES (:doc,:pg,:sec,'SIGNAL',:val,:val)
                          ON CONFLICT (doc_id, entity_type, norm_value) DO NOTHING;
                        """), {"doc": doc_id, "pg": page_no, "sec": section, "val": sigu})

            # -------- build per-section text once --------
            full = "\n".join((p["text"] or "") for p in pages)
            parts = []
            for m in SECH.finditer(full):
                parts.append((int(m.group(2)), m.start(), m.group(3).strip()))
            parts.sort(key=lambda x: x[1])
            for i, (secno, start, title) in enumerate(parts):
                end = parts[i+1][1] if i+1 < len(parts) else len(full)
                body = full[start:end].strip()
                c.execute(text("""
                    INSERT INTO sds_section(doc_id, section_no, title, text)
                    VALUES (:doc,:sec,:title,:body)
                    ON CONFLICT (doc_id, section_no) DO UPDATE
                    SET title = COALESCE(EXCLUDED.title, sds_section.title),
                        text  = EXCLUDED.text;
                """), {"doc": doc_id, "sec": secno, "title": title, "body": body})

            print(f"[ok] stored & parsed {pdf.name} → doc_id={doc_id}, pages={len(pages)}")

    # -------- after all docs: link to products + xref + registry --------
    with engine.begin() as c:
        c.execute(text(LINK_SQL))

        # small report
        row = c.execute(text("""
          SELECT
            (SELECT COUNT(*) FROM sds_document) AS docs,
            (SELECT COUNT(*) FROM sds_entity WHERE entity_type='CAS') AS cas_rows,
            (SELECT COUNT(*) FROM sds_component) AS components,
            (SELECT COUNT(*) FROM sds_hazard) AS hazards,
            (SELECT COUNT(*) FROM product_chem_xref WHERE source IN ('cas','pubchem')) AS linked_products
        """)).mappings().first()
        print(f"[summary] docs={row['docs']} cas_entities={row['cas_rows']} "
              f"components={row['components']} hazards={row['hazards']} "
              f"linked_products={row['linked_products']}")

if __name__ == "__main__":
    main()
