# scripts/week7/02_parse_sds_entities.py
from __future__ import annotations
import re, os
from sqlalchemy import create_engine, text

CAS = re.compile(r'(?<!\d)\d{2,7}-\d{2}-\d(?!\d)')
H_CODE = re.compile(r'\bH\d{3}[A-Z]?\b')
P_CODE = re.compile(r'\bP\d{3}[A-Z]?(?:\+P\d{3}[A-Z]?)?\b')
SIGNAL = re.compile(r'\b(DANGER|WARNING)\b', re.I)

def get_engine():
    try:
        from app.core.config import settings
        db_url = settings.DATABASE_URL.replace("+asyncpg", "+psycopg")
    except Exception:
        db_url = os.getenv("DATABASE_URL", "postgresql+psycopg://zain:Zain2025!!!@localhost:5432/chemdb")
    return create_engine(db_url, future=True)

def guess_section(line: str) -> str:
    m = re.search(r'\bSECTION\s*\d+\b', line, re.I)
    return m.group(0).upper() if m else None

def parse_concentration(fragment: str):
    # crude % parser like "5-10%" or "≤ 5 %"
    m = re.search(r'(\d+(?:\.\d+)?)\s*(?:-|to|–)\s*(\d+(?:\.\d+)?)\s*%|\b(\d+(?:\.\d+)?)\s*%', fragment)
    if m:
        if m.group(1) and m.group(2):
            return float(m.group(1)), float(m.group(2)), '%'
        if m.group(3):
            v = float(m.group(3))
            return v, v, '%'
    return None, None, None

def main():
    engine = get_engine()
    with engine.begin() as c:
        # Work on docs that have pages but no entities parsed yet
        docs = c.execute(text("""
            SELECT d.id
            FROM sds_document d
            WHERE NOT EXISTS (SELECT 1 FROM sds_entity e WHERE e.doc_id=d.id)
        """)).scalars().all()

        for doc_id in docs:
            pages = c.execute(text("SELECT page_no, text FROM sds_page WHERE doc_id=:id ORDER BY page_no"),
                              {"id": doc_id}).mappings().all()

            for p in pages:
                page_no, txt = p["page_no"], (p["text"] or "")
                section = None
                for line in txt.splitlines():
                    sec = guess_section(line)
                    if sec: section = sec

                    # CAS entities + components (simple heuristic: line with CAS also has a name and maybe a %)
                    for cas in CAS.findall(line):
                        c.execute(text("""
                            INSERT INTO sds_entity(doc_id, page_no, section, entity_type, value, norm_value)
                            VALUES (:doc,:pg,:sec,'CAS',:val, cas_normalize(:val))
                            ON CONFLICT DO NOTHING;
                        """), {"doc": doc_id, "pg": page_no, "sec": section, "val": cas})

                        # component row (name = text up to CAS; crude but useful for Section 3)
                        name_part = line.split(cas)[0].strip().strip(",;:- ")
                        lo, hi, unit = parse_concentration(line)
                        c.execute(text("""
                            INSERT INTO sds_component(doc_id, page_no, name, cas, conc_low, conc_high, conc_units)
                            VALUES (:doc,:pg, NULLIF(:name,''), cas_normalize(:cas), :lo, :hi, :unit)
                        """), {"doc": doc_id, "pg": page_no, "name": name_part, "cas": cas,
                               "lo": lo, "hi": hi, "unit": unit})

                    # hazards
                    for h in H_CODE.findall(line):
                        c.execute(text("""
                            INSERT INTO sds_hazard(doc_id, page_no, section, code, statement)
                            VALUES (:doc,:pg,:sec,:code,:stmt)
                        """), {"doc": doc_id, "pg": page_no, "sec": section, "code": h, "stmt": line.strip()})
                    for pcode in P_CODE.findall(line):
                        c.execute(text("""
                                       INSERT INTO sds_entity(doc_id, page_no, section, entity_type, value, norm_value)
                                       VALUES (:doc, :pg, :sec, 'CAS', :val,
                                               cas_normalize(:val)) ON CONFLICT (doc_id, entity_type, norm_value) DO NOTHING;
                                       """), {"doc": doc_id, "pg": page_no, "sec": section, "val": cas})

                        # Component row (dedup on index uq_sds_component_doc_cas_name)
                        c.execute(text("""
                                       INSERT INTO sds_component(doc_id, page_no, name, cas, conc_low, conc_high, conc_units)
                                       VALUES (:doc, :pg, NULLIF(:name, ''), cas_normalize(:cas), :lo, :hi,
                                               :unit) ON CONFLICT
                                       ON CONSTRAINT uq_sds_component_doc_cas_name DO NOTHING;
                                       """),
                                  {"doc": doc_id, "pg": page_no, "name": name_part, "cas": cas, "lo": lo, "hi": hi,
                                   "unit": unit})

                        # Hazard codes (dedup on doc_id+code)
                        c.execute(text("""
                                       INSERT INTO sds_hazard(doc_id, page_no, section, code, statement)
                                       VALUES (:doc, :pg, :sec, :code, :stmt) ON CONFLICT
                                       ON CONSTRAINT uq_sds_hazard_doc_code DO
                                       UPDATE SET
                                           statement = EXCLUDED.statement; -- keep freshest
                                       """),
                                  {"doc": doc_id, "pg": page_no, "sec": section, "code": h, "stmt": line.strip()})

            print(f"[ok] parsed entities for doc_id={doc_id}")

if __name__ == "__main__":
    main()
