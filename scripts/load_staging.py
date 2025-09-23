"""
Load Item Master, Customers, and Sales Excel into staging tables.
Then seed item_master_query with CAS or name queries.

Run:
  python -m scripts.load_staging
"""

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB
from app.core.config import settings
from app.core.json_sanitize import sanitize_json   # from earlier step

# Use sync psycopg driver for pandas
SYNC_URL = settings.DATABASE_URL.replace("+asyncpg", "+psycopg")
engine = create_engine(SYNC_URL, future=True)


def series_or_na(df: pd.DataFrame, col: str) -> pd.Series:
    return df[col] if col in df.columns else pd.Series([pd.NA] * len(df), index=df.index)


def upsert(df: pd.DataFrame, table: str, mapping: dict[str, str]) -> None:
    if df is None or df.empty:
        print(f"[WARN] {table}: no rows to load.")
        return

    out = pd.DataFrame({ tgt: series_or_na(df, src) for tgt, src in mapping.items() })
    # sanitize raw row
    out["raw"] = [sanitize_json(rec) for rec in df.to_dict(orient="records")]

    # numeric coercions
    for num_col in ("qty", "revenue"):
        if num_col in out.columns:
            out[num_col] = pd.to_numeric(out[num_col], errors="coerce")

    out.to_sql(
        table,
        engine,
        if_exists="append",
        index=False,
        dtype={"raw": JSONB},
    )
    print(f"[OK] Loaded {len(out)} rows into {table}")


def main():
    # adjust paths if you keep your XLSX elsewhere
    items = pd.read_excel("./data/ItemMasterCCWResults352.xls.xlsx")
    cust  = pd.read_excel("./data/CCWCustomersActiveResults27.xls.xlsx")
    sales = pd.read_excel("./data/CCWSalesLinesLast24monthsResults449.xlsx")

    upsert(items, "stg_item_master", {
        "sku": "ItemNo", "name": "ItemName", "description": "Description",
        "cas": "CAS", "brand": "Brand", "family": "Family", "vendor": "Vendor",
    })
    upsert(cust, "stg_customers", {
        "customer_code": "CustomerCode", "name": "CustomerName",
        "segment": "Segment", "region": "Region",
    })
    upsert(sales, "stg_sales_lines", {
        "tx_date": "Date", "customer_code": "CustomerCode",
        "sku": "ItemNo", "qty": "Qty", "revenue": "Revenue",
    })

    # build/refresh seed queries
    with engine.begin() as conn:
        conn.execute(text("""
            insert into item_master_query (source_item_id, query, hint)
            select im.id,
                   coalesce(nullif(im.cas,''), im.name) as query,
                   case when im.cas ~ '^\d{2,7}-\d{2}-\d$' then 'cas' else 'name' end as hint
            from stg_item_master im
            where im.name is not null or (im.cas is not null and im.cas <> '')
            on conflict do nothing;
        """))
    print("[OK] item_master_query seeded.")


if __name__ == "__main__":
    main()
