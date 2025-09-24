# scripts/week7/01_load_sds.py
from __future__ import annotations
import argparse, hashlib, os, sys
from pathlib import Path
from typing import Optional

import pdfplumber
from pdf2image import convert_from_path
import pytesseract
from PIL import Image

from sqlalchemy import create_engine, text

CAS_RE = r'(?<!\d)\d{2,7}-\d{2}-\d(?!\d)'

def get_engine():
    try:
        from app.core.config import settings
        db_url = settings.DATABASE_URL.replace("+asyncpg","+psycopg")
        tpath  = getattr(settings, "TESSERACT_PATH", None)
        if tpath: pytesseract.pytesseract.tesseract_cmd = tpath
        poppler = getattr(settings, "POPPLER_PATH", None)
    except Exception:
        db_url = os.getenv("DATABASE_URL", "postgresql+psycopg://zain:Zain2025!!!@localhost:5432/chemdb")
        tpath  = os.getenv("TESSERACT_PATH")  # e.g. C:\Program Files\Tesseract-OCR\tesseract.exe
        if tpath: pytesseract.pytesseract.tesseract_cmd = tpath
        poppler = os.getenv("POPPLER_PATH")   # e.g. C:\poppler-24.02.0\Library\bin
    return create_engine(db_url, future=True), poppler

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""): h.update(chunk)
    return h.hexdigest()

def extract_text_or_ocr(pdf_path: Path, poppler_path: Optional[str]):
    pages = []
    # 1) try pdf text
    with pdfplumber.open(str(pdf_path)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            txt = (page.extract_text() or "").strip()
            pages.append({"page_no": i, "text": txt, "has_text": bool(txt), "ocr_used": False, "ocr_conf": None})
    # 2) OCR where needed
    if any(not p["has_text"] or len(p["text"]) < 30 for p in pages):
        images = convert_from_path(str(pdf_path), dpi=300, poppler_path=poppler_path)
        for i, img in enumerate(images, start=1):
            if not pages[i-1]["has_text"] or len(pages[i-1]["text"]) < 30:
                txt = pytesseract.image_to_string(img)
                pages[i-1]["text"] = (txt or "").strip()
                pages[i-1]["ocr_used"] = True
                pages[i-1]["ocr_conf"] = None  # set if you parse confidences
    return pages

def main():
    ap = argparse.ArgumentParser(description="Load SDS PDFs into sds_document/sds_page")
    ap.add_argument("--dir", required=True, help="Folder with SDS PDFs")
    ap.add_argument("--company", default=None, help="Source company name for metadata")
    ap.add_argument("--store-bytes", action="store_true", help="(Optional) store file bytes in DB (discouraged for large sets)")
    args = ap.parse_args()

    engine, poppler_path = get_engine()
    folder = Path(args.dir)
    pdfs = [p for p in folder.glob("*.pdf")]

    if not pdfs:
        print(f"[warn] no PDFs in {folder}")
        sys.exit(0)

    with engine.begin() as c:
        for pdf in pdfs:
            digest = sha256_file(pdf)
            # upsert doc header
            row = c.execute(text("""
                INSERT INTO sds_document (source_company, title, filename, file_sha256, pages)
                VALUES (:company, :title, :fn, :sha, 0)
                ON CONFLICT (file_sha256) DO UPDATE SET title=EXCLUDED.title
                RETURNING id;
            """), {"company": args.company, "title": pdf.stem, "fn": pdf.name, "sha": digest}).scalar_one()

            doc_id = row
            pages = extract_text_or_ocr(pdf, poppler_path)
            # set pages count
            c.execute(text("UPDATE sds_document SET pages=:n WHERE id=:id"), {"n": len(pages), "id": doc_id})

            # store pages
            for p in pages:
                c.execute(text("""
                    INSERT INTO sds_page (doc_id, page_no, has_text, ocr_used, ocr_conf, text)
                    VALUES (:doc_id, :page_no, :has_text, :ocr_used, :ocr_conf, :text)
                    ON CONFLICT (doc_id, page_no) DO UPDATE
                    SET has_text=EXCLUDED.has_text, ocr_used=EXCLUDED.ocr_used, ocr_conf=EXCLUDED.ocr_conf, text=EXCLUDED.text;
                """), {"doc_id": doc_id, **p})

            print(f"[ok] stored {pdf.name} → doc_id={doc_id}, pages={len(pages)}")

if __name__ == "__main__":
    main()
