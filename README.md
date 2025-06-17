# Alvys Data Pipeline

This repository contains a small command line utility for exporting data from the
Alvys TMS API and inserting the JSON files into SQL Server.  It is primarily
used to pull the previous week’s data for a specific customer (identified by
its SCAC code) and load it into a raw staging schema.

## Requirements

- Python 3.10+
- A SQL Server database accessible via ODBC
- Credentials for the Alvys API

Install Python dependencies using `pip`:

```bash
pip install -r requirements.txt
```

Configuration is provided via environment variables.  An example `.env` file is
included in `dev.example.env`:

```bash
# Alvys API Auth
ALVYS_TENANT_ID="value"
ALVYS_CLIENT_ID="value"
ALVYS_CLIENT_SECRET="value"
ALVYS_GRANT_TYPE="value"

# SQL Server (used by ingestion scripts)
SQL_SERVER="value"
SQL_DATABASE="value"
SQL_USERNAME="value"
SQL_PASSWORD="value"

# Optional: Used by the config module
ALVYS_SQL_CONN_STR="value"
```

Place a copy of this file at the project root as `.env` and fill in the real
values for your environment.

## Usage

The entry point for the pipeline is `main.py`.  It exposes a few subcommands
(`export`, `insert` and `export-insert`) as described in the inline help:

```text
$ python main.py export loads --scac QWIK
$ python main.py insert all --scac QWIK
$ python main.py export-insert loads trips --scac QWIK
```

The tool automatically locates API credentials for the supplied SCAC code using
`config.get_credentials(scac)`.  Data is written to the `alvys_weekly_data`
folder and can then be inserted into SQL Server.  The `--dry-run` flag skips
network and database operations for testing.

Supported entities include:

- loads
- trips
- invoices
- drivers
- trucks
- trailers
- customers
- carriers

Exported JSON files follow the naming pattern `*_API_yyyymmdd-yyyymmdd.json` and
are placed under `alvys_weekly_data`.  Insert scripts expect these files to
already be present in that folder.

## Additional Scripts

The `inserts/` directory contains dedicated loaders for different entity types
(`loads_insert.py`, `trips_insert.py`, `invoices_insert.py`, etc.).  They can be
invoked directly but are also called from `main.py`.

Utility functions such as week range calculation reside under `utils/`.

## Development

This codebase is intentionally small and does not rely on any frameworks.  It is
meant as a simple reference implementation for ingesting Alvys data on a weekly
basis.  Feel free to adapt the SQL schema or add new entities as required.
