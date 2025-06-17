import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Tuple

import requests
from dotenv import load_dotenv  # type: ignore
from config import build_auth_urls

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

def save_json(data, filename, output_dir: str | Path = OUTPUT_DIR):
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

def format_range(start, end):
    start_fmt = start[:10].replace("-", "")
    end_fmt = end[:10].replace("-", "")
    return f"{start_fmt}-{end_fmt}"

def get_file_id():
    return datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:-3]  # yyyymmddHHMMSSmmm


def export_endpoints(
    entities: Iterable[str],
    credentials: Dict[str, str],
    date_range: Tuple[datetime, datetime],
    output_dir: str | Path,
):
    """Export selected API endpoints for the given date range."""
    urls = build_auth_urls(credentials["tenant_id"], API_VERSION)
    token_resp = requests.post(urls["auth_url"], data={
        "client_id": credentials["client_id"],
        "client_secret": credentials["client_secret"],
        "grant_type": credentials.get("grant_type", "client_credentials"),
    })
    token_resp.raise_for_status()
    token = token_resp.json()["access_token"]
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "content-type": "application/*+json",
    }

    start_iso = (
        date_range[0].astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
    end_iso = (
        date_range[1].astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )

    endpoints = {
        "trips": {
            "url": f"{urls['base_url']}/trips/search",
            "extra_payload": {"status": ["Completed"], "range_field": "updatedAtRange"},
        },
        "loads": {
            "url": f"{urls['base_url']}/loads/search",
            "extra_payload": {"status": ["Open"], "range_field": "updatedAtRange"},
        },
        "invoices": {
            "url": f"{urls['base_url']}/invoices/search",
            "extra_payload": {"status": ["Paid"], "range_field": "invoicedDateRange"},
        },
    }

    os.makedirs(output_dir, exist_ok=True)

    for name, cfg in endpoints.items():
        if name not in entities:
            continue
        range_field = cfg["extra_payload"].get("range_field", "updatedAtRange")
        extra_payload = {k: v for k, v in cfg["extra_payload"].items() if k != "range_field"}
        payload = {
            range_field: {"start": start_iso, "end": end_iso},
            **extra_payload,
        }
        data = fetch_paginated_data(cfg["url"], headers, payload)
        file_id = get_file_id()
        for rec in data:
            rec["FILE_ID"] = file_id
        fname = f"{name.upper()}_API_{format_range(start_iso, end_iso)}.json"
        save_json(data, fname, output_dir)

    # These endpoints do not use date ranges
    if "drivers" in entities:
        drivers = fetch_paginated_data(
            f"{urls['base_url']}/drivers/search",
            headers,
            {"name": "", "employeeId": "", "fleetName": "", "status": []},
        )
        file_id = get_file_id()
        for rec in drivers:
            rec["FILE_ID"] = file_id
        save_json(drivers, "DRIVERS.json", output_dir)

    if "trucks" in entities:
        trucks = fetch_paginated_data(
            f"{urls['base_url']}/trucks/search",
            headers,
            {
                "truckNumber": "",
                "fleetName": "",
                "vinNumber": "",
                "registeredName": "",
                "status": [],
            },
        )
        file_id = get_file_id()
        for rec in trucks:
            rec["FILE_ID"] = file_id
        save_json(trucks, "TRUCKS.json", output_dir)

    if "trailers" in entities:
        trailers = fetch_paginated_data(
            f"{urls['base_url']}/trailers/search",
            headers,
            {
                "status": [],
                "trailerNumber": "",
                "fleetName": "",
                "vinNumber": "",
            },
        )
        file_id = get_file_id()
        for rec in trailers:
            rec["FILE_ID"] = file_id
        save_json(trailers, "TRAILERS.json", output_dir)

    if "customers" in entities:
        customers = fetch_paginated_data(
            f"{urls['base_url']}/customers/search",
            headers,
            {"statuses": ["Active", "Inactive", "Disabled"]},
        )
        file_id = get_file_id()
        for rec in customers:
            rec["FILE_ID"] = file_id
        save_json(customers, "CUSTOMERS.json", output_dir)

    if "carriers" in entities:
        carriers = fetch_paginated_data(
            f"{urls['base_url']}/carriers/search",
            headers,
            {
                "status": [
                    "Pending",
                    "Active",
                    "Expired Insurance",
                    "Interested",
                    "Invited",
                    "Packet Sent",
                    "Packet Completed",
                ],
            },
        )
        file_id = get_file_id()
        for rec in carriers:
            rec["FILE_ID"] = file_id
        save_json(carriers, "CARRIERS.json", output_dir)


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
        print("Fetching Drivers...")
        drivers = fetch_paginated_data(
            f"{BASE_URL}/drivers/search", headers,
            {
                "name": "",
                "employeeId": "",
                "fleetName": "",
                "status": []
            }
        )
        file_id = get_file_id()
        for rec in drivers:
            rec["FILE_ID"] = file_id
        save_json(drivers, "DRIVERS.json")

    if run_all or "trucks" in args:
        print("Fetching Trucks...")
        trucks = fetch_paginated_data(
            f"{BASE_URL}/trucks/search", headers,
            {
                "truckNumber": "",
                "fleetName": "",
                "vinNumber": "",
                "registeredName": "",
                "status": []
            }
        )
        file_id = get_file_id()
        for rec in trucks:
            rec["FILE_ID"] = file_id
        save_json(trucks, "TRUCKS.json")

    if run_all or "trailers" in args:
        print("Fetching Trailers...")
        trailers = fetch_paginated_data(
            f"{BASE_URL}/trailers/search", headers,
            {
                "status": [],
                "trailerNumber": "",
                "fleetName": "",
                "vinNumber": ""
            }
        )
        file_id = get_file_id()
        for rec in trailers:
            rec["FILE_ID"] = file_id
        save_json(trailers, "TRAILERS.json")

    if run_all or "customers" in args:
        print("Fetching Customers...")
        customers = fetch_paginated_data(
            f"{BASE_URL}/customers/search", headers,
            {
                "statuses": ["Active", "Inactive", "Disabled"]
            }
        )
        file_id = get_file_id()
        for rec in customers:
            rec["FILE_ID"] = file_id
        save_json(customers, "CUSTOMERS.json")

    if run_all or "carriers" in args:
        print("Fetching Carriers...")
        carriers = fetch_paginated_data(
            f"{BASE_URL}/carriers/search", headers,
            {
                "status": [
                    "Pending",
                    "Active",
                    "Expired Insurance",
                    "Interested",
                    "Invited",
                    "Packet Sent",
                    "Packet Completed",
                ]
            }
        )
        file_id = get_file_id()
        for rec in carriers:
            rec["FILE_ID"] = file_id
        save_json(carriers, "CARRIERS.json")

    print("\n✅ Data pull complete.")

if __name__ == "__main__":
    main()
