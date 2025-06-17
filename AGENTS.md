# AGENT Guidelines

This repository houses a small extraction and loading pipeline for Alvys API data.
The code pulls JSON from various endpoints and inserts the flattened results into
Azure SQL tables. Future updates should follow these conventions:

## Code Structure
- `alvys_export.py` – handles OAuth and paginated export logic.
- `alvys_insert.py` – inserts JSON from the `alvys_weekly_data` folder into
tables prefixed with `ALVYS_`.
- `inserts/active_entities_insert.py` – similar insert logic for the
`*_RAW` tables used by active processes.
- `main.py` – CLI entry point. Entities list must remain in sync with the
available export/insert functions.

## JSON Data
The `alvys_weekly_data` directory stores JSON responses for each API endpoint.
These files are **read‑only** – they will be overwritten whenever exports run.
Use them only to inspect the structure of returned objects or to debug insert
failures. Do not manually edit them.

## Testing
Run a basic syntax check after changes:

```bash
python -m py_compile alvys_export.py alvys_insert.py inserts/active_entities_insert.py main.py
```

Installing dependencies may fail in restricted environments but should be
attempted:

```bash
pip install -q -r requirements.txt
```

