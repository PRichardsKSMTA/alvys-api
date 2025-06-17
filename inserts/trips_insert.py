"""Insert Alvys trip & stop data into SQL Server efficiently
---------------------------------------------------------
* Vectorised pandas bulk-insert (`fast_executemany=True`).
* Adds `INSERTED_DTTM` audit column (single UTC timestamp per run).
* Null-safe helpers prevent slicing errors.
"""
import os
import json
import time
from datetime import datetime, timezone
from typing import List, Optional, Any

import pandas as pd
from sqlalchemy import create_engine, types

# ────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────
SQL_SERVER = "ksm-ksmta-sqlsrv-001.database.windows.net"
SQL_DATABASE = "KSMTA"
SQL_USERNAME = "importuser"
SQL_PASSWORD = "B2_SBD-Omicron-B00ts2!"

SCHEMA = "TBXX"
TRIP_TABLE = f"{SCHEMA}.TRIPS_RAW"
STOP_TABLE = f"{SCHEMA}.TRIP_STOPS_RAW"
DATA_DIR = "alvys_weekly_data"
CHUNK_SIZE = 1_000
RUN_TS = datetime.now(tz=timezone.utc).replace(tzinfo=None)

# ────────────────────────────────────────────
# COLUMNS & DTYPES
# ────────────────────────────────────────────
TRIP_COLS: List[str] = [
    "ID", "TRIP_NUMBER", "TRIP_STATUS", "LOAD_NUMBER", "TENDER_AS",
    "TOTAL_MILEAGE", "MILEAGE_SOURCE", "MILEAGE_PROFILE_NAME", "EMPTY_MILEAGE",
    "LOADED_MILEAGE", "PICKUP_DTTM", "DELIVERY_DTTM", "PICKED_UP_DTTM",
    "DELIVERED_DTTM", "CARRIER_ASSIGNED_DTTM", "RELEASED_DTTM", "TRIP_VALUE",
    "TRUCK_ID", "TRUCK_FLEET_ID", "TRUCK_FLEET_NAME", "TRAILER_ID",
    "TRAILER_TYPE", "DRIVER1_ID", "DRIVER1_TYPE", "DRIVER1_FLEET_ID",
    "DRIVER2_ID", "DRIVER2_TYPE", "DRIVER2_FLEET_ID", "OWNER_OPERATOR_ID",
    "RELEASED_BY", "DISPATCHED_BY", "DISPATCHER_ID", "IS_CARRIER_PAY_ON_HOLD",
    "CARRIER_ID", "CARRIER_INVOICE", "CARRIER_RATE", "CARRIER_LINEHAUL",
    "CARRIER_FUEL", "CARRIER_ACCESSORIALS", "CARRIER_TOTAL_PAYABLE",
    "UPDATED_DTTM", "FILE_ID", "INSERTED_DTTM",
]

STOP_COLS: List[str] = [
    "ID", "TRIP_ID", "TRIP_NUMBER", "STOP_SEQUENCE", "IS_APPOINTMENT_REQUESTED",
    "IS_APPOINTMENT_CONFIRMED", "EARLIEST_APPOINTMENT_DTTM", "LATEST_APPOINTMENT_DTTM",
    "STREET_ADDRESS", "CITY", "STATE_PROVINCE", "POSTAL_CD", "LATITUDE", "LONGITUDE",
    "STOP_STATUS", "STOP_TYPE", "STOP_SCHEDULE_TYPE", "LOADING_TYPE",
    "ARRIVED_DTTM", "DEPARTED_DTTM", "FILE_ID", "INSERTED_DTTM",
]

# NOTE: Many VARCHAR lengths chosen generically; adjust if DB schema differs.
NUM18_2 = types.Numeric(18, 2)
NUM18_6 = types.Numeric(18, 6)
DTYPE_TRIPS = {
    "ID": types.VARCHAR(100),
    "TRIP_NUMBER": types.VARCHAR(100),
    "TRIP_STATUS": types.VARCHAR(50),
    "LOAD_NUMBER": types.VARCHAR(100),
    "TENDER_AS": types.VARCHAR(50),
    "TOTAL_MILEAGE": NUM18_2,
    "MILEAGE_SOURCE": types.VARCHAR(50),
    "MILEAGE_PROFILE_NAME": types.VARCHAR(100),
    "EMPTY_MILEAGE": NUM18_2,
    "LOADED_MILEAGE": NUM18_2,
    "PICKUP_DTTM": types.DateTime(),
    "DELIVERY_DTTM": types.DateTime(),
    "PICKED_UP_DTTM": types.DateTime(),
    "DELIVERED_DTTM": types.DateTime(),
    "CARRIER_ASSIGNED_DTTM": types.DateTime(),
    "RELEASED_DTTM": types.DateTime(),
    "TRIP_VALUE": NUM18_2,
    "TRUCK_ID": types.VARCHAR(100),
    "TRUCK_FLEET_ID": types.VARCHAR(100),
    "TRUCK_FLEET_NAME": types.VARCHAR(100),
    "TRAILER_ID": types.VARCHAR(100),
    "TRAILER_TYPE": types.VARCHAR(50),
    "DRIVER1_ID": types.VARCHAR(100),
    "DRIVER1_TYPE": types.VARCHAR(50),
    "DRIVER1_FLEET_ID": types.VARCHAR(100),
    "DRIVER2_ID": types.VARCHAR(100),
    "DRIVER2_TYPE": types.VARCHAR(50),
    "DRIVER2_FLEET_ID": types.VARCHAR(100),
    "OWNER_OPERATOR_ID": types.VARCHAR(100),
    "RELEASED_BY": types.VARCHAR(100),
    "DISPATCHED_BY": types.VARCHAR(100),
    "DISPATCHER_ID": types.VARCHAR(100),
    "IS_CARRIER_PAY_ON_HOLD": types.Integer,
    "CARRIER_ID": types.VARCHAR(100),
    "CARRIER_INVOICE": types.VARCHAR(100),
    "CARRIER_RATE": NUM18_2,
    "CARRIER_LINEHAUL": NUM18_2,
    "CARRIER_FUEL": NUM18_2,
    "CARRIER_ACCESSORIALS": NUM18_2,
    "CARRIER_TOTAL_PAYABLE": NUM18_2,
    "UPDATED_DTTM": types.DateTime(),
    "FILE_ID": types.VARCHAR(50),
    "INSERTED_DTTM": types.DateTime(),
}
DTYPE_STOPS = {
    "ID": types.VARCHAR(100),
    "TRIP_ID": types.VARCHAR(100),
    "TRIP_NUMBER": types.VARCHAR(100),
    "STOP_SEQUENCE": types.Integer,
    "IS_APPOINTMENT_REQUESTED": types.Integer,
    "IS_APPOINTMENT_CONFIRMED": types.Integer,
    "EARLIEST_APPOINTMENT_DTTM": types.DateTime(),
    "LATEST_APPOINTMENT_DTTM": types.DateTime(),
    "STREET_ADDRESS": types.VARCHAR(200),
    "CITY": types.VARCHAR(100),
    "STATE_PROVINCE": types.VARCHAR(50),
    "POSTAL_CD": types.VARCHAR(20),
    "LATITUDE": NUM18_6,
    "LONGITUDE": NUM18_6,
    "STOP_STATUS": types.VARCHAR(50),
    "STOP_TYPE": types.VARCHAR(50),
    "STOP_SCHEDULE_TYPE": types.VARCHAR(50),
    "LOADING_TYPE": types.VARCHAR(50),
    "ARRIVED_DTTM": types.DateTime(),
    "DEPARTED_DTTM": types.DateTime(),
    "FILE_ID": types.VARCHAR(50),
    "INSERTED_DTTM": types.DateTime(),
}

# ────────────────────────────────────────────
# ENGINE
# ────────────────────────────────────────────

def get_engine():
    conn = (
        f"mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
    )
    return create_engine(conn, fast_executemany=True)

# ────────────────────────────────────────────
# HELPERS
# ────────────────────────────────────────────

def _s(val: Optional[Any], max_len: int | None = None) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    return s[:max_len] if max_len else s


def _f(val) -> Optional[float]:
    try:
        return float(val) if val not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _dt(series: pd.Series | Any) -> pd.Series | Optional[datetime]:
    if isinstance(series, pd.Series):
        return pd.to_datetime(series, utc=True, errors="coerce").dt.tz_localize(None)
    return pd.to_datetime(series, utc=True, errors="coerce").tz_localize(None) if series else None


def g(d: Optional[dict], *keys):
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d

# ────────────────────────────────────────────
# FLATTENERS
# ────────────────────────────────────────────

def flatten_trip(trip: dict, file_id: str):
    return [
        _s(trip.get("Id"), 100),
        _s(trip.get("TripNumber"), 100),
        _s(trip.get("Status"), 50),
        _s(trip.get("LoadNumber"), 100),
        _s(trip.get("TenderAs"), 50),
        _f(g(trip, "TotalMileage", "Distance", "Value")),
        _s(g(trip, "TotalMileage", "Source"), 50),
        _s(g(trip, "TotalMileage", "ProfileName"), 100),
        _f(g(trip, "EmptyMileage", "Distance", "Value")),
        _f(g(trip, "LoadedMileage", "Distance", "Value")),
        trip.get("PickupDate"),
        trip.get("DeliveryDate"),
        trip.get("PickedUpAt"),
        trip.get("DeliveredAt"),
        trip.get("CarrierAssignedAt"),
        trip.get("ReleasedAt"),
        _f(g(trip, "TripValue", "Amount")),
        _s(g(trip, "Truck", "Id"), 100),
        _s(g(trip, "Truck", "Fleet", "Id"), 100),
        _s(g(trip, "Truck", "Fleet", "Name"), 100),
        _s(g(trip, "Trailer", "Id"), 100),
        _s(g(trip, "Trailer", "EquipmentType"), 50),
        _s(g(trip, "Driver1", "Id"), 100),
        _s(g(trip, "Driver1", "ContractorType"), 50),
        _s(g(trip, "Driver1", "Fleet", "Id"), 100),
        _s(g(trip, "Driver2", "Id"), 100),
        _s(g(trip, "Driver2", "ContractorType"), 50),
        _s(g(trip, "Driver2", "Fleet", "Id"), 100),
        _s(g(trip, "OwnerOperator", "Id"), 100),
        _s(trip.get("ReleasedBy"), 100),
        _s(trip.get("DispatchedBy"), 100),
        _s(trip.get("DispatcherId"), 100),
        int(trip.get("CarrierPayOnHold") or False),
        _s(g(trip, "Carrier", "Id"), 100),
        _s(g(trip, "Carrier", "CarrierInvoiceNumber"), 100),
        _f(g(trip, "Carrier", "Rate", "Amount")),
        _f(g(trip, "Carrier", "Linehaul", "Amount")),
        _f(g(trip, "Carrier", "Fuel", "Amount")),
        _f(g(trip, "Carrier", "Accessorials", "Amount")),
        _f(g(trip, "Carrier", "TotalPayable", "Amount")),
        trip.get("UpdatedAt"),
        file_id,
        RUN_TS,
    ]


def flatten_stops(trip: dict, file_id: str):
    trip_id = _s(trip.get("Id"), 100)
    trip_num = _s(trip.get("TripNumber"), 100)
    stops = []
    for seq, stop in enumerate(trip.get("Stops", []), 1):
        address = stop.get("Address", {})
        coords = stop.get("Coordinates", {})
        # Determine appointment window fields
        earliest = stop.get("AppointmentDate") or g(stop, "StopWindow", "Begin")
        latest = g(stop, "StopWindow", "End") if stop.get("StopWindow") else None
        stops.append([
            _s(stop.get("Id") or f"{trip_id}_{seq}", 100),
            trip_id,
            trip_num,
            seq,
            int(stop.get("AppointmentRequested") or False),
            int(stop.get("AppointmentConfirmed") or False),
            earliest,
            latest,
            _s(address.get("Street"), 200),
            _s(address.get("City"), 100),
            _s(address.get("State"), 50),
            _s(address.get("ZipCode"), 20),
            _f(coords.get("Latitude")),
            _f(coords.get("Longitude")),
            _s(stop.get("Status"), 50),
            _s(stop.get("StopType"), 50),
            _s(stop.get("ScheduleType"), 50),
            _s(stop.get("LoadingType"), 50),
            stop.get("ArrivedAt"),
            stop.get("DepartedAt"),
            file_id,
            RUN_TS,
        ])
    return stops

# ────────────────────────────────────────────
# BUILD DATAFRAMES
# ────────────────────────────────────────────

def build_dfs():
    trip_rows: list[list] = []
    stop_rows: list[list] = []
    for fname in sorted(os.listdir(DATA_DIR)):
        if not fname.startswith("TRIPS_API_") or not fname.endswith(".json"):
            continue
        with open(os.path.join(DATA_DIR, fname), encoding="utf-8") as f:
            data = json.load(f)
        if not data:
            continue
        file_id = _s(data[0].get("FILE_ID"), 50)
        print(f"Processing {fname} … {len(data):,} objects (FILE_ID={file_id})")
        for trip in data:
            trip_rows.append(flatten_trip(trip, file_id))
            stop_rows.extend(flatten_stops(trip, file_id))

    trips_df = pd.DataFrame(trip_rows, columns=TRIP_COLS)
    stops_df = pd.DataFrame(stop_rows, columns=STOP_COLS)

    # Parse datetime columns
    datetime_trip_cols = [
        "PICKUP_DTTM", "DELIVERY_DTTM", "PICKED_UP_DTTM", "DELIVERED_DTTM",
        "CARRIER_ASSIGNED_DTTM", "RELEASED_DTTM", "UPDATED_DTTM",
    ]
    trips_df[datetime_trip_cols] = trips_df[datetime_trip_cols].apply(_dt)

    datetime_stop_cols = [
        "EARLIEST_APPOINTMENT_DTTM", "LATEST_APPOINTMENT_DTTM", "ARRIVED_DTTM", "DEPARTED_DTTM",
    ]
    stops_df[datetime_stop_cols] = stops_df[datetime_stop_cols].apply(_dt)

    return trips_df, stops_df

# ────────────────────────────────────────────
# BULK INSERT
# ────────────────────────────────────────────

def bulk_insert(engine, table: str, df: pd.DataFrame, dtype_map: dict):
    if df.empty:
        print(f"⚠️  No records for {table}.")
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
        dtype=dtype_map,
    )
    print(f"✅ {len(df):,} rows inserted into {table} in {time.perf_counter() - start:.1f}s")

# ────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────

def main():
    trips_df, stops_df = build_dfs()
    print(f"Found {len(trips_df):,} trips & {len(stops_df):,} stops. Uploading …")
    eng = get_engine()
    bulk_insert(eng, TRIP_TABLE, trips_df, DTYPE_TRIPS)
    bulk_insert(eng, STOP_TABLE, stops_df, DTYPE_STOPS)


if __name__ == "__main__":
    main()
