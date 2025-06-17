import requests
import os
import json
import sys
from datetime import datetime
from dotenv import load_dotenv # type: ignore

load_dotenv()

TENANT_ID = os.getenv("ALVYS_TENANT_ID")
AUTH_URL = f"https://integrations.alvys.com/api/authentication/{TENANT_ID}/token"
API_VERSION = "1"
BASE_URL = f"https://integrations.alvys.com/api/p/v{API_VERSION}"

CREDENTIALS = {
    "client_id": os.getenv("ALVYS_CLIENT_ID"),
    "client_secret": os.getenv("ALVYS_CLIENT_SECRET"),
    "grant_type": os.getenv("ALVYS_GRANT_TYPE", "client_credentials")
}

WEEK_RANGES = [
    ("2025-03-30T00:00:00.001Z", "2025-04-05T23:59:59.999Z"),
    ("2025-04-06T00:00:00.001Z", "2025-04-12T23:59:59.999Z"),
    ("2025-04-13T00:00:00.001Z", "2025-04-19T23:59:59.999Z"),
    ("2025-04-20T00:00:00.001Z", "2025-04-26T23:59:59.999Z"),
    ("2025-04-27T00:00:00.001Z", "2025-05-03T23:59:59.999Z")
]

OUTPUT_DIR = "alvys_weekly_data"
PAGE_SIZE = 200

def get_token():
    response = requests.post(AUTH_URL, data=CREDENTIALS)
    response.raise_for_status()
    return response.json()["access_token"]

def fetch_paginated_data(url, headers, base_payload, max_items=None):
    page = 0
    items = []
    while True:
        payload = dict(base_payload)
        payload["page"] = page
        payload["pageSize"] = PAGE_SIZE
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        result = response.json()
        batch = result.get("Items") or result.get("items") or []
        if not batch:
            break
        items.extend(batch)
        if len(batch) < PAGE_SIZE or (max_items and len(items) >= max_items):
            break
        page += 1
    return items if not max_items else items[:max_items]

def save_json(data, filename):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

def format_range(start, end):
    start_fmt = start[:10].replace("-", "")
    end_fmt = end[:10].replace("-", "")
    return f"{start_fmt}-{end_fmt}"

def get_file_id():
    return datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:-3]  # yyyymmddHHMMSSmmm

def main():
    args = [arg.lower() for arg in sys.argv[1:]]
    run_all = len(args) == 0

    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "content-type": "application/*+json"
    }

    endpoints = {
        "trips": {
            "url": f"{BASE_URL}/trips/search",
            "extra_payload": {
                "status": ["Completed"],
                "range_field": "updatedAtRange"
            }
        },
        "loads": {
            "url": f"{BASE_URL}/loads/search",
            "extra_payload": {
                "status": ["Open"],
                "range_field": "updatedAtRange"
            }
        },
        "invoices": {
            "url": f"{BASE_URL}/invoices/search",
            "extra_payload": {
                "status": ["Paid"],
                "range_field": "invoicedDateRange"
            }
        },
    }

    for name, config in endpoints.items():
        if run_all or name in args:
            for start, end in WEEK_RANGES:
                label = format_range(start, end)
                print(f"Fetching {name.upper()} for {label}...")
                range_field = config["extra_payload"].get("range_field", "updatedAtRange")
                extra_payload = {k: v for k, v in config["extra_payload"].items() if k != "range_field"}

                payload = {
                    "page": 0,
                    "pageSize": PAGE_SIZE,
                    range_field: {
                        "start": start,
                        "end": end
                    },
                    **extra_payload
                }
                data = fetch_paginated_data(config["url"], headers, payload)
                file_id = get_file_id()
                for rec in data:
                    rec["FILE_ID"] = file_id
                filename = f"{name.upper()}_API_{label}.json"
                save_json(data, filename)

    if run_all or "drivers" in args:
        print("Fetching Active Drivers...")
        drivers = fetch_paginated_data(
            f"{BASE_URL}/drivers/search", headers,
            {
                "name": "",
                "employeeId": "",
                "fleetName": "",
                "isActive": True
            }
        )
        file_id = get_file_id()
        for rec in drivers:
            rec["FILE_ID"] = file_id
        save_json(drivers, "ACTIVE_DRIVERS.json")

    if run_all or "trucks" in args:
        print("Fetching Active Trucks...")
        trucks = fetch_paginated_data(
            f"{BASE_URL}/trucks/search", headers,
            {
                "truckNumber": "",
                "fleetName": "",
                "vinNumber": "",
                "isActive": True,
                "registeredName": ""
            }
        )
        file_id = get_file_id()
        for rec in trucks:
            rec["FILE_ID"] = file_id
        save_json(trucks, "ACTIVE_TRUCKS.json")

    if run_all or "trailers" in args:
        print("Fetching Active Trailers...")
        trailers = fetch_paginated_data(
            f"{BASE_URL}/trailers/search", headers,
            {
                "status": ["Active"],
                "trailerNumber": "",
                "fleetName": "",
                "vinNumber": ""
            }
        )
        file_id = get_file_id()
        for rec in trailers:
            rec["FILE_ID"] = file_id
        save_json(trailers, "ACTIVE_TRAILERS.json")

    if run_all or "customers" in args:
        print("Fetching Active Customers...")
        customers = fetch_paginated_data(
            f"{BASE_URL}/customers/search", headers,
            {
                "statuses": ["Active"]
            }
        )
        file_id = get_file_id()
        for rec in customers:
            rec["FILE_ID"] = file_id
        save_json(customers, "ACTIVE_CUSTOMERS.json")

    print("\n✅ Data pull complete.")

if __name__ == "__main__":
    main()