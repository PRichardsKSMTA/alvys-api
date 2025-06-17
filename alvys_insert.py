import os
import json
import pyodbc
import pandas as pd
import sys
import time
from datetime import datetime
from typing import List, Dict
from dotenv import load_dotenv # type: ignore

load_dotenv()

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
DRIVER = "{ODBC Driver 17 for SQL Server}"
DATA_DIR = "alvys_weekly_data"
BATCH_SIZE = 500

def get_conn():
    return pyodbc.connect(
        f"DRIVER={DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}"
    )

def load_json(filename: str) -> List[Dict]:
    with open(os.path.join(DATA_DIR, filename), encoding="utf-8") as f:
        return json.load(f)

def safe_datetime(val):
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00")) if val else None
    except Exception:
        return None

def sanitize_driver(d: Dict) -> Dict:
    return {
        "ID": d.get("Id"),
        "EMPLOYEE_ID": d.get("EmployeeId"),
        "DRIVER_TYPE": d.get("Type"),
        "SUBSIDIARY_ID": d.get("SubsidiaryId"),
        "ZIP_CODE": d.get("Address", {}).get("ZipCode"),
        "FLEET_ID": d.get("Fleet", {}).get("Id"),
        "FLEET_NAME": d.get("Fleet", {}).get("Name"),
        "CREATED_DTTM": safe_datetime(d.get("CreatedAt")),
        "IS_ACTIVE": int(d.get("IsActive", False)),
        "HIRED_DTTM": safe_datetime(d.get("HiredAt")),
        "FILE_ID": d.get("FILE_ID")
    }

def sanitize_truck(t: Dict) -> Dict:
    return {
        "ID": t.get("Id"),
        "TRUCK_NUM": t.get("TruckNum"),
        "VIN_NUMBER": t.get("VinNumber"),
        "YEAR": str(t.get("Year")) if t.get("Year") is not None else None,
        "MAKE": t.get("Make"),
        "MODEL": t.get("Model"),
        "LICENSE_STATE": t.get("LicenseState"),
        "TRUCK_TYPE": t.get("TruckType"),
        "SUBSIDIARY_ID": t.get("SubsidiaryId"),
        "FLEET_ID": t.get("Fleet", {}).get("Id"),
        "FLEET_NAME": t.get("Fleet", {}).get("Name"),
        "CREATED_DTTM": safe_datetime(t.get("CreatedAt")),
        "FILE_ID": t.get("FILE_ID")
    }

def sanitize_trailer(t: Dict) -> Dict:
    return {
        "ID": t.get("Id"),
        "TRAILER_NUM": t.get("TrailerNum"),
        "TRAILER_TYPE": t.get("TrailerType"),
        "TRAILER_STATUS": t.get("Status"),
        "CREATED_DTTM": safe_datetime(t.get("CreatedAt")),
        "FILE_ID": t.get("FILE_ID")
    }

def sanitize_customer(c: Dict) -> Dict:
    return {
        "ID": c.get("Id"),
        "CUSTOMER_NAME": c.get("Name"),
        "COMPANY_NUMBER": c.get("CompanyNumber"),
        "CUSTOMER_TYPE": c.get("Type"),
        "CUSTOMER_STATUS": c.get("Status"),
        "BILLING_ADDRESS": ", ".join([c.get("BillingAddress", {}).get(k, '') for k in ("Street", "City", "State", "ZipCode")]),
        "CREATED_DTTM": safe_datetime(c.get("DateCreated")),
        "INVOICING_NAME": c.get("InvoicingInformation", {}).get("InvoicingName"),
        "INVOICING_ALIAS": c.get("InvoicingInformation", {}).get("InvoicingNameAlias"),
        "FILE_ID": c.get("FILE_ID")
    }

def batch_insert(table: str, records: List[Dict], conn):
    if not records:
        print(f"No data to insert for {table}.")
        return

    inserted_dttm = datetime.now()
    for rec in records:
        rec["INSERTED_DTTM"] = inserted_dttm

    df = pd.DataFrame(records)
    columns = ", ".join(df.columns)
    placeholders = ", ".join(["?" for _ in df.columns])
    insert_sql = f"INSERT INTO dbo.{table} ({columns}) VALUES ({placeholders})"

    cursor = conn.cursor()
    cursor.fast_executemany = True
    start = time.time()
    for i in range(0, len(df), BATCH_SIZE):
        batch = df.iloc[i:i + BATCH_SIZE].where(pd.notnull(df), None).values.tolist()
        cursor.executemany(insert_sql, batch)
    conn.commit()
    duration = time.time() - start
    print(f"✅ Inserted {len(df)} records into {table} in {duration:.2f} seconds")

def main():
    args = [arg.lower() for arg in sys.argv[1:]]
    run_all = len(args) == 0

    conn = get_conn()

    if run_all or "trailers" in args:
        print("Loading trailers JSON...")
        trailers = [sanitize_trailer(t) for t in load_json("TRAILERS.json")]
        batch_insert("ALVYS_TRAILERS_RAW", trailers, conn)

    if run_all or "trucks" in args:
        print("Loading trucks JSON...")
        trucks = [sanitize_truck(t) for t in load_json("TRUCKS.json")]
        batch_insert("ALVYS_TRUCKS_RAW", trucks, conn)

    if run_all or "drivers" in args:
        print("Loading drivers JSON...")
        drivers = [sanitize_driver(d) for d in load_json("DRIVERS.json")]
        batch_insert("ALVYS_DRIVERS_RAW", drivers, conn)

    if run_all or "customers" in args:
        print("Loading customers JSON...")
        customers = [sanitize_customer(c) for c in load_json("CUSTOMERS.json")]
        batch_insert("ALVYS_CUSTOMERS_RAW", customers, conn)

    print("\n✅ All data inserted.")

if __name__ == "__main__":
    main()
