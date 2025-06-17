#!/usr/bin/env python
"""Alvys Pipeline – unified CLI entry‑point
================================================
This script lets you *export* last‑week Alvys data to JSON, *insert* JSON
into SQL Server, or run both steps in sequence – all resolved per‑client by
SCAC.

Usage examples
--------------
# Export only last week’s loads for QWIK Logistics (SCAC = QWIK)
python main.py export loads --scac QWIK

# Insert last week’s JSON that is already on disk (all entities)
python main.py insert all --scac QWIK

# End‑to‑end export ➔ insert for loads & trips
python main.py export-insert loads trips --scac QWIK

Design highlights
-----------------
* **Argparse sub‑commands** provide a clean UX without extra packages.
* **Dynamic credential lookup** via `config.get_credentials(scac)` – the
  function queries the shared `dbo.CLIENTS` table and returns the
  tenant‑specific OAuth and API details.
* **Week range helper** (`utils.dates.get_last_week_range`) returns an
  ISO‑8601 (Sunday 00:00 → Saturday 23:59:59.999) tuple for *n* weeks ago.
* **Lazy imports** – export/insert modules are imported only when needed,
  keeping startup fast.
* **Dry‑run mode** (`--dry-run`) lets you validate without writing files or
  touching the DB.
"""
from __future__ import annotations

import argparse
import importlib
import sys
from pathlib import Path
from typing import Iterable, List

# ────────────────────────────────────────────
# INTERNAL MODULES (small, fast to import)
# ────────────────────────────────────────────
from utils.dates import get_last_week_range
from config import get_credentials

# Default output folder shared by export & insert steps
DATA_DIR = Path("alvys_weekly_data")

# Entities we support – map to both API endpoints *and* insert modules
ENTITIES = [
    "loads",
    "trips",
    "invoices",
    "drivers",
    "trucks",
    "trailers",
    "customers",
]

# ────────────────────────────────────────────
# ARGPARSE
# ────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("Alvys multi‑tenant ingestion CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    # export ---------------------------------------------------------------
    exp = sub.add_parser("export", help="Export JSON from Alvys API")
    exp.add_argument("entities", nargs="*", default=["all"], choices=ENTITIES + ["all"],
                     help="Which entities to export (default: all)")
    exp.add_argument("--scac", required=True, help="Client SCAC (schema & creds resolver)")
    exp.add_argument("--weeks-ago", type=int, default=1,
                     help="How many weeks back to pull (1 = last week)")
    exp.add_argument("--dry-run", action="store_true", help="Skip network + file writes")

    # insert ---------------------------------------------------------------
    ins = sub.add_parser("insert", help="Insert JSON into SQL Server")
    ins.add_argument("entities", nargs="*", default=["all"], choices=ENTITIES + ["all"],
                     help="Which entities to insert (default: all)")
    ins.add_argument("--scac", required=True, help="Client SCAC (schema in target DB)")
    ins.add_argument("--dry-run", action="store_true", help="Skip DB writes")

    # export‑insert --------------------------------------------------------
    ei = sub.add_parser("export-insert", help="Run export and insert in one step")
    ei.add_argument("entities", nargs="*", default=["all"], choices=ENTITIES + ["all"],
                    help="Which entities (default: all)")
    ei.add_argument("--scac", required=True)
    ei.add_argument("--weeks-ago", type=int, default=1)
    ei.add_argument("--dry-run", action="store_true")

    return p

# ────────────────────────────────────────────
# HELPERS
# ────────────────────────────────────────────

def normalise(entity_args: Iterable[str]) -> List[str]:
    """Expand ["all"] or mixed list into the canonical entity list order."""
    ent_set = set(entity_args)
    if not ent_set or ent_set == {"all"}:
        return ENTITIES
    return [e for e in ENTITIES if e in ent_set]

# ────────────────────────────────────────────
# EXPORT LOGIC
# ────────────────────────────────────────────

def run_export(scac: str, entities: List[str], weeks_ago: int, dry_run: bool):
    start, end = get_last_week_range(weeks_ago)
    creds = get_credentials(scac)

    if dry_run:
        print("[DRY‑RUN] Would export:", entities, "for", scac, "range", start, "→", end)
        return

    # Import lazily to avoid heavy deps if export isn’t requested
    from alvys_export import export_endpoints  # type: ignore

    export_endpoints(
        entities=entities,
        credentials=creds,
        date_range=(start, end),
        output_dir=DATA_DIR,
    )

# ────────────────────────────────────────────
# INSERT LOGIC
# ────────────────────────────────────────────

def run_insert(scac: str, entities: List[str], dry_run: bool):
    if dry_run:
        print("[DRY‑RUN] Would insert:", entities, "into schema", scac)
        return

    for ent in entities:
        mod_name = f"inserts.{ent}_insert"
        try:
            mod = importlib.import_module(mod_name)
        except ModuleNotFoundError as exc:
            sys.exit(f"❌ Insert module not found: {mod_name} → {exc}")

        if hasattr(mod, "main"):
            print(f"→ inserting {ent.upper()} …")
            mod.main()  # module handles its own CLI / config
        else:
            sys.exit(f"❌ {mod_name} lacks a main() entry‑point")

# ────────────────────────────────────────────
# MAIN DISPATCH
# ────────────────────────────────────────────

def main(argv: List[str] | None = None):
    args = build_parser().parse_args(argv)
    ents = normalise(args.entities)

    if args.cmd == "export":
        run_export(args.scac, ents, args.weeks_ago, args.dry_run)

    elif args.cmd == "insert":
        run_insert(args.scac, ents, args.dry_run)

    elif args.cmd == "export-insert":
        run_export(args.scac, ents, args.weeks_ago, args.dry_run)
        # Skip insert if export was dry‑run but insert wasn’t explicitly dry‑run
        run_insert(args.scac, ents, args.dry_run)


if __name__ == "__main__":
    main()
