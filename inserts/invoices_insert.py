"""Insert Alvys invoice data into SQL Server (stable version)
----------------------------------------------------------------
Adds **INSERTED_DTTM** audit column populated with the current UTC timestamp (`datetime2`) for every row in both tables.

Other fixes retained:
* pyodbc `07002` workaround (no `method="multi"`).
* Removed unsupported `executemany_mode`.
* NaN/NaT → `None` before upload.
"""
import os
import json
import time
from datetime import datetime, timezone
from typing import List, Optional

import pandas as pd
from sqlalchemy import create_engine, types

# ────────────────────────────────────────────
# CONFIG – customise per environment
# ────────────────────────────────────────────
SQL_SERVER = "ksm-ksmta-sqlsrv-001.database.windows.net"
SQL_DATABASE = "KSMTA"
SQL_USERNAME = "importuser"
SQL_PASSWORD = "B2_SBD-Omicron-B00ts2!"

SCHEMA = "TBXX"
INVOICE_TABLE = f"{SCHEMA}.INVOICES_RAW"
LINE_ITEM_TABLE = f"{SCHEMA}.INVOICE_LINE_ITEMS_RAW"
DATA_DIR = "alvys_weekly_data"  # folder containing the weekly *.json files
CHUNK_SIZE = 1_000              # executemany batch size

# One consistent timestamp per run
RUN_TS = datetime.now(tz=timezone.utc).replace(tzinfo=None)  # naive UTC datetime (SQL Server datetime2)

# ────────────────────────────────────────────
# COLUMN LISTS & SQL TYPES
# ────────────────────────────────────────────
INVOICE_COLS: List[str] = [
    "ID", "INVOICE_NUMBER", "INVOICE_TYPE", "INVOICE_STATUS",
    "CREATED_DTTM", "BILLED_DATE", "FILE_ID", "CUSTOMER_ID", "INVOICE_AMOUNT",
    "INSERTED_DTTM",
]

LINE_ITEM_COLS: List[str] = [
    "ID", "INVOICE_ID", "INVOICE_NUMBER", "LINE_ITEM_NAME", "LINE_ITEM_AMOUNT",
    "LINE_ITEM_CURRENCY_CODE", "LINE_ITEM_RATE", "LINE_ITEM_UNITS", "LINE_ITEM_UNIT_TYPE",
    "LOAD_NUMBER", "LINE_ITEM_CATEGORY", "FILE_ID", "INSERTED_DTTM",
]

DTYPE_INVOICES = {
    "ID": types.VARCHAR(100),
    "INVOICE_NUMBER": types.VARCHAR(100),
    "INVOICE_TYPE": types.VARCHAR(50),
    "INVOICE_STATUS": types.VARCHAR(50),
    "CREATED_DTTM": types.DateTime(),
    "BILLED_DATE": types.DateTime(),
    "FILE_ID": types.VARCHAR(50),
    "CUSTOMER_ID": types.VARCHAR(100),
    "INVOICE_AMOUNT": types.Numeric(18, 2),
    "INSERTED_DTTM": types.DateTime(),
}

DTYPE_LINE_ITEMS = {
    "ID": types.VARCHAR(150),
    "INVOICE_ID": types.VARCHAR(100),
    "INVOICE_NUMBER": types.VARCHAR(100),
    "LINE_ITEM_NAME": types.VARCHAR(100),
    "LINE_ITEM_AMOUNT": types.Numeric(18, 2),
    "LINE_ITEM_CURRENCY_CODE": types.VARCHAR(10),
    "LINE_ITEM_RATE": types.Numeric(18, 2),
    "LINE_ITEM_UNITS": types.VARCHAR(20),
    "LINE_ITEM_UNIT_TYPE": types.VARCHAR(50),
    "LOAD_NUMBER": types.VARCHAR(100),
    "LINE_ITEM_CATEGORY": types.VARCHAR(50),
    "FILE_ID": types.VARCHAR(50),
    "INSERTED_DTTM": types.DateTime(),
}

# ────────────────────────────────────────────
# DB ENGINE
# ────────────────────────────────────────────

def get_engine():
    conn = (
        f"mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}"
        f"?driver=ODBC+Driver+17+for+SQL+Server"
    )
    return create_engine(conn, fast_executemany=True)

# ────────────────────────────────────────────
# HELPERS
# ────────────────────────────────────────────

def _s(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    return str(val).strip() or None


def _dt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce").dt.tz_localize(None)

# ────────────────────────────────────────────
# FLATTENERS
# ────────────────────────────────────────────

def flatten_invoices(raw: list[dict], file_id: str) -> pd.DataFrame:
    recs = []
    for inv in raw:
        recs.append([
            _s(inv.get("Id"))[:100],
            _s(inv.get("Number"))[:100],
            _s(inv.get("Type"))[:50],
            _s(inv.get("Status"))[:50],
            inv.get("CreatedDate"),
            inv.get("InvoicedDate"),
            file_id[:50] if file_id else None,
            _s(inv.get("Customer", {}).get("Id"))[:100],
            float(inv.get("Total", {}).get("Amount") or 0),
            RUN_TS,
        ])
    df = pd.DataFrame(recs, columns=INVOICE_COLS)
    df["CREATED_DTTM"] = _dt(df["CREATED_DTTM"])
    df["BILLED_DATE"] = _dt(df["BILLED_DATE"])
    return df


def flatten_line_items(raw: list[dict], file_id: str) -> pd.DataFrame:
    recs = []
    for inv in raw:
        inv_id = _s(inv.get("Id"))[:100]
        inv_num = _s(inv.get("Number"))[:100]
        for li in inv.get("LineItems", []):
            currency = li.get("Amount", {}).get("Currency")
            currency_code = None
            if isinstance(currency, dict):
                currency_code = _s(currency.get("Code"))[:10]
            elif currency is not None:
                currency_code = _s(currency)[:10]

            rate = li.get("Rate", {})
            recs.append([
                _s(li.get("Id"))[:150],
                inv_id,
                inv_num,
                _s(li.get("Name"))[:100],
                float(li.get("Amount", {}).get("Amount") or 0),
                currency_code,
                float(rate.get("Rate") or 0),
                _s(rate.get("Units"))[:20] if rate.get("Units") else None,
                _s(rate.get("UnitOfMeasurement"))[:50] if rate.get("UnitOfMeasurement") else None,
                _s(li.get("LoadNumber"))[:100] if li.get("LoadNumber") else None,
                _s(li.get("Category"))[:50] if li.get("Category") else None,
                file_id[:50] if file_id else None,
                RUN_TS,
            ])
    return pd.DataFrame(recs, columns=LINE_ITEM_COLS)

# ────────────────────────────────────────────
# BULK INSERT
# ────────────────────────────────────────────

def bulk_insert(engine, table: str, df: pd.DataFrame, dtypes: dict):
    if df.empty:
        print(f"⚠️  Nothing to insert into {table} – DataFrame empty.")
        return
    df = df.where(pd.notnull(df), None)
    start = time.perf_counter()
    df.to_sql(
        name=table.split(".")[-1],
        schema=SCHEMA,
        con=engine,
        if_exists="append",
        index=False,
        chunksize=CHUNK_SIZE,
        dtype=dtypes,
    )
    dur = time.perf_counter() - start
    print(f"✅ {len(df):,} rows inserted into {table} in {dur:.1f}s")

# ────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────

def main() -> None:
    invoice_frames, line_item_frames = [], []

    for fname in sorted(os.listdir(DATA_DIR)):
        if not (fname.startswith("INVOICES_API_") and fname.endswith(".json")):
            continue
        path = os.path.join(DATA_DIR, fname)
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not data:
            continue
        file_id = _s(data[0].get("FILE_ID"))
        print(f"Processing {fname} … {len(data):,} objects (FILE_ID={file_id})")
        invoice_frames.append(flatten_invoices(data, file_id))
        line_item_frames.append(flatten_line_items(data, file_id))

    invoices_df = (
        pd.concat(invoice_frames, ignore_index=True) if invoice_frames else pd.DataFrame(columns=INVOICE_COLS)
    )
    line_items_df = (
        pd.concat(line_item_frames, ignore_index=True) if line_item_frames else pd.DataFrame(columns=LINE_ITEM_COLS)
    )

    print(f"Found {len(invoices_df):,} invoices & {len(line_items_df):,} line items. Uploading …")

    engine = get_engine()
    bulk_insert(engine, INVOICE_TABLE, invoices_df, DTYPE_INVOICES)
    bulk_insert(engine, LINE_ITEM_TABLE, line_items_df, DTYPE_LINE_ITEMS)


if __name__ == "__main__":
    main()
